import asyncio
import re
from urllib.parse import urljoin
from playwright.async_api import async_playwright, Page
from scraper.builder import PropertySnapshotBuilder
from scraper.utils import optimizar_pagina

class TuresidenciaScraper:
    def __init__(self):
        self.source_name = "turesidencia"
        self.base_url = "https://www.turesidencia.net"

    async def run_pipeline(self, start_url: str, max_pages: int = None, save_callback=None):
        resultados_totales = 0

        # --- FASE A: RECOLECCIÓN DE URLs ---
        async with async_playwright() as p:
            # NOTA: En mercadolibre y quarto, recuerda usar headless=False y el user_agent aquí si lo configuraste
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context()
            page_lista = await context.new_page()

            await optimizar_pagina(page_lista)

            print(f"[{self.source_name}] Fase A: Recolección de URLs...")
            inmuebles_base = await self._recolectar_urls(page_lista, start_url, max_pages)

            await page_lista.close()
            await context.close()
            await browser.close()
            print(f"[{self.source_name}] Fase A terminada. {len(inmuebles_base)} URLs encontradas.")

        if not inmuebles_base:
            return 0

        # --- FASE B: EXTRACCIÓN POR LOTES (MICRO-BATCHING) ---
        tamaño_lote = 100
        total_lotes = (len(inmuebles_base) // tamaño_lote) + 1

        for i in range(0, len(inmuebles_base), tamaño_lote):
            lote_actual = inmuebles_base[i: i + tamaño_lote]
            num_lote = (i // tamaño_lote) + 1
            print(f"[{self.source_name}] Procesando Lote {num_lote}/{total_lotes} ({len(lote_actual)} inmuebles)...")

            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                context = await browser.new_context()
                semaforo = asyncio.Semaphore(3)

                async def procesar_con_semaforo(item):
                    async with semaforo:
                        nueva_pestaña = await context.new_page()

                        await optimizar_pagina(page_lista)

                        try:
                            # Dependiendo del scraper, _extraer_detalle recibe diferentes parámetros.
                            # Revisa cómo era en el original (ej: item.get("precio"), item.get("titulo"))
                            # y ajústalo aquí si es necesario.
                            snapshot = await self._extraer_detalle(
                                nueva_pestaña,
                                item["url"],
                                item.get("precio", None),
                                item.get("titulo", "")
                            )
                            return snapshot
                        except Exception as e:
                            print(f"Error en {item.get('url')}: {e}")
                            return None
                        finally:
                            await nueva_pestaña.close()

                tareas = [procesar_con_semaforo(item) for item in lote_actual]
                resultados_crudos = await asyncio.gather(*tareas)
                resultados_validos = [r for r in resultados_crudos if r is not None]

                # ¡GUARDADO INMEDIATO EN LA BD!
                if save_callback and resultados_validos:
                    await save_callback(resultados_validos)
                    resultados_totales += len(resultados_validos)

                await context.close()
                await browser.close()

            print(f"[{self.source_name}] Lote {num_lote} guardado en BD. RAM liberada. Descanso...")
            await asyncio.sleep(3)  # Pausa entre lotes

        return resultados_totales

    async def _recolectar_urls(self, page: Page, base_search_url: str, max_pages: int = None):
        inmuebles_encontrados = []
        urls_vistas = set()
        current_url = base_search_url
        paginas_escaneadas = 0

        while current_url:
            if max_pages and paginas_escaneadas >= max_pages:
                break

            await page.goto(current_url, timeout=60000)

            # Selector para las tarjetas de la lista
            tarjetas = await page.locator("div.gb_wrapper").all()

            for tarjeta in tarjetas:
                enlace_loc = tarjeta.locator("a.gb_title_link").first
                if await enlace_loc.count() > 0:
                    href = await enlace_loc.get_attribute("href")
                    if href and not href.startswith("http"):
                        href = urljoin(self.base_url, href)

                    titulo = ""
                    try:
                        titulo = await enlace_loc.inner_text(timeout=2000)
                    except Exception:
                        pass

                    if href and href not in urls_vistas:
                        urls_vistas.add(href)
                        inmuebles_encontrados.append({
                            "url": href,
                            "titulo": titulo.strip()
                        })

            paginas_escaneadas += 1

            # Paginación
            siguiente_btn = page.locator("a[title='Próximo']")
            if await siguiente_btn.count() > 0:
                relative_url = await siguiente_btn.first.get_attribute("href")
                current_url = urljoin(self.base_url, relative_url)
            else:
                current_url = None

        return inmuebles_encontrados

    async def _extraer_detalle(self, page: Page, url: str, titulo_base: str):
        await page.goto(url, timeout=60000)

        # 1. Extraer ID
        try:
            codigo_text = await page.locator("div.gb_ad_globalid").inner_text(timeout=5000)
            external_id = codigo_text.replace("ID :", "").strip()
        except Exception:
            external_id = url.split("/")[-1]

        builder = PropertySnapshotBuilder(source_name=self.source_name, external_id=external_id, url=url)

        # 2. Título (h2)
        try:
            titulo_text = await page.locator("div.gb_item_detail_wrapper h2").first.inner_text(timeout=3000)
            titulo_base = titulo_text.strip()
        except Exception:
            pass

        # 3. Descripción
        descripcion_limpia = None
        try:
            descripcion_limpia = await page.locator("div.description span.sbody").inner_text(timeout=3000)
        except Exception:
            pass

        builder.set_general_info(titulo=titulo_base, descripcion=descripcion_limpia)

        # 4. Ubicación
        try:
            ubicacion_text = await page.locator(
                "li:has(div.gb_ad_heading:has-text('Ubicacion')) div.gb_ad_heading_details").inner_text(timeout=3000)
            partes = [p.strip() for p in ubicacion_text.split("\n") if p.strip()]

            urbanismo = partes[0] if partes else ""
            municipio = "Caracas"  # Fallback
            if len(partes) > 1:
                sub_partes = partes[1].split(",")
                if len(sub_partes) >= 3:
                    municipio = sub_partes[2].strip()

            builder.set_location(municipio=municipio, urbanismo=urbanismo)
        except Exception:
            pass

        # 5. Precio (Intento directo y Fallback con Regex)
        precio_limpio = None
        try:
            precio_text = await page.locator(
                "li:has(div.gb_ad_heading:has-text('Precio')) div.gb_ad_heading_details").inner_text(timeout=3000)
            precio_str = re.sub(r'[^\d]', '', precio_text)

            if precio_str:
                precio_limpio = float(precio_str)
            else:
                # FALLBACK: Si dice "En Negociacion", buscamos en la descripción algo como "250 $" o "$ 250"
                if descripcion_limpia:
                    match = re.search(r'(?:usd|\$)\s*(\d+[\d\.,]*)|(\d+[\d\.,]*)\s*(?:usd|\$)',
                                      descripcion_limpia.lower())
                    if match:
                        num_str = match.group(1) if match.group(1) else match.group(2)
                        precio_limpio = float(num_str.replace(',', '').replace('.', ''))
        except Exception:
            pass

        if precio_limpio:
            builder.set_price(precio_limpio, "USD")

        return builder.build()