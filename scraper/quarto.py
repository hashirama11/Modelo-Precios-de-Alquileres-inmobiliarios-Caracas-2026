import asyncio
import re
from urllib.parse import urljoin
from playwright.async_api import async_playwright, Page
from scraper.builder import PropertySnapshotBuilder
from scraper.utils import optimizar_pagina
import logging

logger = logging.getLogger(__name__)


class QuartoScraper:
    def __init__(self):
        self.source_name = "quarto"
        self.base_url = "https://quartoapp.com"
        self.user_agent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

    async def run_pipeline(self, start_url: str, max_pages: int = None, save_callback=None):
        resultados_totales = 0

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=False)
            context = await browser.new_context(user_agent=self.user_agent)
            page_lista = await context.new_page()

            await optimizar_pagina(page_lista)
            logger.info(f"[{self.source_name}] Fase A: Recolección de URLs (SPA Mode)...")
            inmuebles_base = await self._recolectar_urls(page_lista, start_url, max_pages)

            await page_lista.close()
            await context.close()
            await browser.close()

        if not inmuebles_base:
            logger.warning(f"[{self.source_name}] 0 URLs. Revisa si el sitio cambió su enrutamiento.")
            return 0

        tamaño_lote = 50
        total_lotes = (len(inmuebles_base) // tamaño_lote) + 1

        for i in range(0, len(inmuebles_base), tamaño_lote):
            lote_actual = inmuebles_base[i: i + tamaño_lote]
            num_lote = (i // tamaño_lote) + 1
            logger.info(f"[{self.source_name}] ⏳ Procesando Lote {num_lote}/{total_lotes}...")

            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=False)
                context = await browser.new_context(user_agent=self.user_agent)
                semaforo = asyncio.Semaphore(3)

                async def procesar_con_semaforo(item):
                    async with semaforo:
                        nueva_pestaña = await context.new_page()
                        await optimizar_pagina(nueva_pestaña)
                        try:
                            snapshot = await self._extraer_detalle(
                                nueva_pestaña, item["url"], item.get("precio"), item.get("titulo")
                            )
                            return snapshot
                        except Exception as e:
                            logger.error(f"[{self.source_name}] Error en {item.get('url')}: {e}")
                            return None
                        finally:
                            await nueva_pestaña.close()

                tareas = [procesar_con_semaforo(item) for item in lote_actual]
                resultados_crudos = await asyncio.gather(*tareas)
                resultados_validos = [r for r in resultados_crudos if r is not None]

                if save_callback and resultados_validos:
                    await save_callback(resultados_validos)
                    resultados_totales += len(resultados_validos)

                await context.close()
                await browser.close()

            await asyncio.sleep(2)

        return resultados_totales

    async def _recolectar_urls(self, page: Page, base_search_url: str, max_pages: int = None):
        inmuebles_encontrados = []
        urls_vistas = set()
        paginas_escaneadas = 0

        logger.info(f"[{self.source_name}] Cargando SPA React (Carga Inicial Única)...")
        # ¡IMPORTANTE! Solo cargamos la URL una vez fuera del bucle
        await page.goto(base_search_url, timeout=60000)

        while True:
            if max_pages and paginas_escaneadas >= max_pages:
                break

            logger.debug(f"[{self.source_name}] Esperando a que React dibuje la página {paginas_escaneadas + 1}...")
            await asyncio.sleep(8)  # Le damos tiempo al servidor de Quarto

            # Scroll Suave para Lazy Loading
            for _ in range(3):
                await page.evaluate("window.scrollBy(0, 800)")
                await asyncio.sleep(1)

            # Extraemos todo
            todos_los_enlaces = await page.locator("a").all()
            enlaces_validos = 0

            for enlace in todos_los_enlaces:
                try:
                    href = await enlace.get_attribute("href")
                    if not href: continue

                    if not href.startswith("http"):
                        href = urljoin(self.base_url, href)

                    if any(palabra in href.lower() for palabra in ["/propiedad", "/inmueble", "/detalle", "/p/"]):
                        if href in urls_vistas: continue

                        texto_completo = await enlace.inner_text(timeout=1000)
                        precio_limpio = None
                        match = re.search(r'(?:\$|USD)\s*([\d\.,]+)', texto_completo, re.IGNORECASE)
                        if match:
                            precio_str = match.group(1).replace(',', '')
                            precio_limpio = float(precio_str)

                        urls_vistas.add(href)
                        inmuebles_encontrados.append({
                            "url": href,
                            "precio": precio_limpio,
                            "titulo": "Pendiente (Fase B)"
                        })
                        enlaces_validos += 1
                except Exception:
                    pass

            logger.info(f"[{self.source_name}] Enlaces extraídos en página {paginas_escaneadas + 1}: {enlaces_validos}")
            paginas_escaneadas += 1

            # --- LA MAGIA SPA: Clicar en Siguiente SIN recargar ---
            # Seleccionamos botones típicos de librerías como Material UI o Bootstrap que usa React
            siguiente_btn = page.locator(
                "button[aria-label='Next page'], a[aria-label='Next page'], ul.pagination li:last-child a, .MuiPaginationItem-next")

            if await siguiente_btn.count() > 0:
                # Verificamos por Javascript si el botón está deshabilitado (llegamos a la última página)
                is_disabled = await siguiente_btn.first.evaluate(
                    "node => node.disabled || node.classList.contains('disabled')")

                if not is_disabled:
                    logger.info(f"[{self.source_name}] -> Haciendo clic en 'Siguiente' internamente...")
                    await siguiente_btn.first.click()
                    # NO hacemos goto(). Solo esperamos a que React re-dibuje la pantalla
                    await asyncio.sleep(5)
                else:
                    logger.info(
                        f"[{self.source_name}] Botón 'Siguiente' detectado como inactivo. Fin de la paginación.")
                    break
            else:
                logger.info(f"[{self.source_name}] No se encontró botón 'Siguiente'. Fin de la paginación.")
                break

        return inmuebles_encontrados


    async def _extraer_detalle(self, page: Page, url: str, precio_base: float, titulo_base: str):
        await page.goto(url, timeout=60000)
        try:
            await page.wait_for_load_state("networkidle", timeout=10000)
        except:
            pass

        external_id = url.split("/")[-1].split("?")[0]
        builder = PropertySnapshotBuilder(source_name=self.source_name, external_id=external_id, url=url)

        if precio_base: builder.set_price(precio_base, "USD")

        try:
            titulo = await page.locator("h1").inner_text(timeout=3000)
        except:
            titulo = titulo_base

        descripcion_limpia = None
        try:
            # Estrategia más amplia para la descripción
            desc_locs = await page.locator("p").all_inner_texts()
            # Tomamos el párrafo más largo como descripción
            if desc_locs:
                descripcion_limpia = max(desc_locs, key=len)
        except:
            pass

        builder.set_general_info(titulo=titulo, descripcion=descripcion_limpia)

        if titulo and titulo != "Extraer en detalle":
            partes = [p.strip() for p in titulo.split(",")]
            urbanismo = partes[0] if len(partes) > 0 else ""
            municipio = partes[1].replace("Mun ", "").split("#")[0].strip() if len(partes) > 1 else "Caracas"
            builder.set_location(municipio=municipio, urbanismo=urbanismo)

        try:
            # Buscamos características en todo el texto del cuerpo
            texto_body = await page.locator("body").inner_text()

            amenidades_validas = []

            # Buscamos "Habitaciones 3" o "3 Habitaciones"
            hab_match = re.search(r'(\d+)\s*habitaci', texto_body, re.IGNORECASE)
            if hab_match: builder.add_features(habitaciones=int(hab_match.group(1)))

            bano_match = re.search(r'(\d+)\s*baño', texto_body, re.IGNORECASE)
            if bano_match: builder.add_features(banos=float(bano_match.group(1)))

            m2_match = re.search(r'(\d+)\s*m2', texto_body, re.IGNORECASE)
            if m2_match: builder.add_features(m2_totales=float(m2_match.group(1)))

        except:
            pass

        return builder.build()