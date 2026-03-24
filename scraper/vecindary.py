import asyncio
import re
from urllib.parse import urljoin
from playwright.async_api import async_playwright, Page
from scraper.builder import PropertySnapshotBuilder


class VecindaryScraper:
    def __init__(self):
        self.source_name = "vecindary"
        self.base_url = "https://vecindary.com"

    async def run_pipeline(self, start_url: str, max_pages: int = None):
        resultados_validos = []

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context()
            page_lista = await context.new_page()

            print(f"[{self.source_name}] Fase A: Recolección de URLs...")
            inmuebles_base = await self._recolectar_urls(page_lista, start_url, max_pages)
            await page_lista.close()
            print(f"[{self.source_name}] Fase A terminada. {len(inmuebles_base)} URLs encontradas.")

            print(f"[{self.source_name}] Fase B: Extracción de detalles (Concurrente)...")
            semaforo = asyncio.Semaphore(3)

            async def procesar_con_semaforo(item):
                async with semaforo:
                    nueva_pestaña = await context.new_page()
                    try:
                        snapshot = await self._extraer_detalle(
                            nueva_pestaña,
                            item["url"],
                            item.get("precio"),
                            item.get("titulo")
                        )
                        return snapshot
                    except Exception as e:
                        print(f"Error en {item.get('url')}: {e}")
                        return None
                    finally:
                        await nueva_pestaña.close()

            tareas = [procesar_con_semaforo(item) for item in inmuebles_base]
            resultados_crudos = await asyncio.gather(*tareas)
            resultados_validos = [r for r in resultados_crudos if r is not None]

            await context.close()
            await browser.close()

        return resultados_validos

    async def _recolectar_urls(self, page: Page, base_search_url: str, max_pages: int = None):
        inmuebles_encontrados = []
        urls_vistas = set()
        current_url = base_search_url
        paginas_escaneadas = 0

        while current_url:
            if max_pages and paginas_escaneadas >= max_pages:
                break

            await page.goto(current_url, timeout=60000)

            # Las tarjetas de Vecindary son enlaces (a) que apuntan a "/clasificado/..."
            tarjetas = await page.locator("a[href^='/clasificado/']").all()

            for tarjeta in tarjetas:
                href = await tarjeta.get_attribute("href")
                if href and not href.startswith("http"):
                    href = urljoin(self.base_url, href)

                titulo = ""
                precio_limpio = None

                try:
                    # Extraemos todo el texto de la tarjeta y buscamos patrones
                    texto_tarjeta = await tarjeta.inner_text(timeout=2000)
                    lineas = [linea.strip() for linea in texto_tarjeta.split('\n') if linea.strip()]

                    for linea in lineas:
                        if "$" in linea:
                            precio_str = re.sub(r'[^\d]', '', linea)
                            if precio_str:
                                precio_limpio = float(precio_str)
                        elif not titulo and len(linea) > 15:  # Asumimos que la primera línea larga es el título
                            titulo = linea
                except Exception:
                    pass

                if href and href not in urls_vistas:
                    urls_vistas.add(href)
                    inmuebles_encontrados.append({
                        "url": href,
                        "precio": precio_limpio,
                        "titulo": titulo
                    })

            paginas_escaneadas += 1

            # Paginación (Buscamos enlaces que terminen en "/pagina-X")
            next_page_num = paginas_escaneadas + 1
            siguiente_btn = page.locator(f"a[href$='/pagina-{next_page_num}']")

            if await siguiente_btn.count() > 0:
                relative_url = await siguiente_btn.first.get_attribute("href")
                current_url = urljoin(self.base_url, relative_url)
            else:
                current_url = None

        return inmuebles_encontrados

    async def _extraer_detalle(self, page: Page, url: str, precio_base: float, titulo_base: str):
        await page.goto(url, timeout=60000)

        # 1. Extraer ID de la URL (Vecindary pone el ID al final: ...-showroom-9756)
        match_id = re.search(r'-(\d+)$', url)
        external_id = match_id.group(1) if match_id else url.split("/")[-1]

        builder = PropertySnapshotBuilder(source_name=self.source_name, external_id=external_id, url=url)

        if precio_base: builder.set_price(precio_base, "USD")

        # 2. Título (h1)
        try:
            titulo = await page.locator("h1").inner_text(timeout=3000)
            builder.set_general_info(titulo=titulo, descripcion=None)
        except Exception:
            builder.set_general_info(titulo=titulo_base, descripcion=None)

        # 3. Ubicación (Suele estar en un h2 con la clase font-normal, ej: Centro Comercial Palo Verde...)
        try:
            # Buscamos un texto que contenga Caracas dentro de un h2
            ubicacion_text = await page.locator("h2:has-text('Caracas')").first.inner_text(timeout=3000)
            partes = [p.strip() for p in ubicacion_text.split(",")]

            municipio = "Caracas"
            urbanismo = partes[0] if partes else ""

            # Intentamos ubicar el municipio real si está listado
            for p in partes:
                if p in ["Sucre", "Baruta", "Chacao", "El Hatillo", "Libertador"]:
                    municipio = p

            builder.set_location(municipio=municipio, urbanismo=urbanismo)
        except Exception:
            pass

        # 4. Características Principales (m2, baños)
        try:
            # Buscamos todos los h2 que tengan span dentro o números
            h2_elementos = await page.locator("h2").all_inner_texts()
            for texto in h2_elementos:
                texto_limpio = texto.lower()
                if "m²" in texto_limpio:
                    m2_val = float(re.search(r'\d+', texto_limpio).group()) if re.search(r'\d+', texto_limpio) else None
                    builder.add_features(m2_totales=m2_val)
                elif "baño" in texto_limpio:
                    bano_val = float(re.search(r'\d+', texto_limpio).group()) if re.search(r'\d+',
                                                                                           texto_limpio) else None
                    builder.add_features(banos=bano_val)
                elif "habita" in texto_limpio:
                    hab_val = int(re.search(r'\d+', texto_limpio).group()) if re.search(r'\d+', texto_limpio) else None
                    builder.add_features(habitaciones=hab_val)
        except Exception:
            pass

        # 5. Amenidades Extras (En la sección Detalles adicionales del inmueble)
        try:
            # Extraemos todos los p dentro de listas ul > li.list-disc
            extras_lista = await page.locator("li.list-disc p").all_inner_texts()
            if extras_lista:
                builder.add_extra_data("amenidades", [e.strip() for e in extras_lista])
        except Exception:
            pass

        return builder.build()