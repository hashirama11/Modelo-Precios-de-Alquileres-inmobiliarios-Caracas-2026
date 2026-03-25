import asyncio
import re
from urllib.parse import urljoin
from playwright.async_api import async_playwright, Page
from scraper.builder import PropertySnapshotBuilder
from scraper.utils import optimizar_pagina

class QuartoScraper:
    def __init__(self):
        self.source_name = "quarto"
        self.base_url = "https://quartoapp.com"

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
            # Esperamos a que carguen las tarjetas de React
            await page.wait_for_selector("div[status='active'][type='quarto']", timeout=15000)

            tarjetas = await page.locator("div[status='active'][type='quarto']").all()

            for tarjeta in tarjetas:
                # En Quarto, a veces la tarjeta entera es un enlace o tiene un 'a' padre
                enlace_loc = tarjeta.locator("xpath=ancestor::a").first
                if await enlace_loc.count() > 0:
                    href = await enlace_loc.get_attribute("href")
                    if href and not href.startswith("http"):
                        href = urljoin(self.base_url, href)

                    precio_limpio = None
                    try:
                        # Extraemos el texto completo y buscamos el patrón de precio (ej. $350.00)
                        texto_tarjeta = await tarjeta.inner_text()
                        match = re.search(r'\$\s*([\d\.,]+)', texto_tarjeta)
                        if match:
                            precio_str = match.group(1).replace(',', '')
                            precio_limpio = float(precio_str)
                    except Exception:
                        pass

                    if href and href not in urls_vistas:
                        urls_vistas.add(href)
                        inmuebles_encontrados.append({
                            "url": href,
                            "precio": precio_limpio
                        })

            paginas_escaneadas += 1

            # Paginación: Como no pasaste el HTML del botón, usamos una técnica genérica
            # Si la URL tiene paginación por query params, la inyectamos manualmente
            if "&page=" in current_url:
                current_page = int(re.search(r'&page=(\d+)', current_url).group(1))
                current_url = re.sub(r'&page=\d+', f'&page={current_page + 1}', current_url)
            else:
                current_url = f"{current_url}&page=2"

            # Validamos si la siguiente página tiene resultados, sino rompemos
            # (Esto se perfeccionará si Quarto usa un botón de "Siguiente" específico)

        return inmuebles_encontrados

    async def _extraer_detalle(self, page: Page, url: str, precio_base: float):
        await page.goto(url, timeout=60000)

        # 1. Extraer ID de la URL (Quarto suele tener la URL como /propiedades/alquiler/12345)
        external_id = url.split("/")[-1].split("?")[0]

        builder = PropertySnapshotBuilder(source_name=self.source_name, external_id=external_id, url=url)

        if precio_base: builder.set_price(precio_base, "USD")

        # 2. Título (h1)
        titulo = ""
        try:
            titulo = await page.locator("h1").inner_text(timeout=5000)
        except Exception:
            pass

        # 3. Descripción
        descripcion_limpia = None
        try:
            descripcion_limpia = await page.locator("section:has(h2:has-text('Descripción')) p").inner_text(
                timeout=3000)
        except Exception:
            pass

        builder.set_general_info(titulo=titulo, descripcion=descripcion_limpia)

        # 4. Ubicación (Del título "Los Dos Caminos, Mun Sucre #245")
        if titulo:
            partes = [p.strip() for p in titulo.split(",")]
            urbanismo = partes[0] if len(partes) > 0 else ""
            municipio = partes[1].replace("Mun ", "").split("#")[0].strip() if len(partes) > 1 else "Caracas"
            builder.set_location(municipio=municipio, urbanismo=urbanismo)

        # 5. Características Dinámicas (Iterando sobre los elementos del DOM)
        try:
            caracteristicas = await page.locator("section:has(h2:has-text('Características')) p").all_inner_texts()
            amenidades_validas = []

            for c in caracteristicas:
                # El texto viene tipo "Habitaciones : 1" o "Piscina : No"
                partes = c.split(":")
                if len(partes) == 2:
                    llave = partes[0].strip().lower()
                    valor = partes[1].strip().lower()

                    if "m2" in llave:
                        m2_val = float(re.search(r'\d+', valor).group()) if re.search(r'\d+', valor) else None
                        builder.add_features(m2_totales=m2_val)
                    elif "habitaciones" in llave:
                        hab_val = int(re.search(r'\d+', valor).group()) if re.search(r'\d+', valor) else None
                        builder.add_features(habitaciones=hab_val)
                    elif "baños" in llave:
                        bano_val = float(re.search(r'\d+', valor).group()) if re.search(r'\d+', valor) else None
                        builder.add_features(banos=bano_val)
                    else:
                        # Si es un extra (Piscina, Gimnasio, etc) y dice "Si", lo agregamos
                        if valor == "si":
                            amenidades_validas.append(partes[0].strip())

            if amenidades_validas:
                builder.add_extra_data("amenidades", amenidades_validas)
        except Exception:
            pass

        return builder.build()