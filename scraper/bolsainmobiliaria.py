import asyncio
import re
from playwright.async_api import async_playwright, Page
from scraper.builder import PropertySnapshotBuilder
from scraper.utils import optimizar_pagina
import logging

# Instanciamos el logger
logger = logging.getLogger(__name__)

class BolsaInmobiliariaScraper:
    def __init__(self):
        self.source_name = "bolsainmobiliaria"
        self.base_url = "https://bolsainmobiliariacaracas.com"

    async def run_pipeline(self, start_url: str, max_pages: int = None, save_callback=None):
        resultados_totales = 0

        # --- FASE A: RECOLECCIÓN DE URLs ---
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context()
            page_lista = await context.new_page()

            # Optimizamos la página de la lista
            await optimizar_pagina(page_lista)

            logger.info(f"[{self.source_name}] Fase A: Recolección de URLs iniciada...")
            inmuebles_base = await self._recolectar_urls(page_lista, start_url, max_pages)

            await page_lista.close()
            await context.close()
            await browser.close()
            logger.info(f"[{self.source_name}] Fase A terminada. {len(inmuebles_base)} URLs encontradas.")

        if not inmuebles_base:
            logger.warning(f"[{self.source_name}] No se encontraron URLs para procesar.")
            return 0

        # --- FASE B: EXTRACCIÓN POR LOTES (MICRO-BATCHING) ---
        tamaño_lote = 100
        total_lotes = (len(inmuebles_base) // tamaño_lote) + 1

        for i in range(0, len(inmuebles_base), tamaño_lote):
            lote_actual = inmuebles_base[i: i + tamaño_lote]
            num_lote = (i // tamaño_lote) + 1
            logger.info(f"[{self.source_name}] ⏳ Procesando Lote {num_lote}/{total_lotes} ({len(lote_actual)} URLs)...")

            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                context = await browser.new_context()
                semaforo = asyncio.Semaphore(3)

                async def procesar_con_semaforo(item):
                    async with semaforo:
                        nueva_pestaña = await context.new_page()

                        # ¡CORRECCIÓN AQUÍ! Aplicamos la optimización a la nueva_pestaña, NO a page_lista
                        await optimizar_pagina(nueva_pestaña)

                        try:
                            snapshot = await self._extraer_detalle(
                                nueva_pestaña,
                                item["url"],
                                item.get("precio", None),
                                item.get("titulo", "")
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

                # ¡GUARDADO INMEDIATO EN LA BD!
                if save_callback and resultados_validos:
                    await save_callback(resultados_validos)
                    resultados_totales += len(resultados_validos)

                await context.close()
                await browser.close()

            logger.info(f"[{self.source_name}] ✅ Lote {num_lote} guardado en BD. RAM liberada. Descansando 3s...")
            await asyncio.sleep(3)  # Pausa entre lotes

        logger.info(f"[{self.source_name}] 🎉 Pipeline completado. {resultados_totales} inmuebles procesados exitosamente.")
        return resultados_totales

    async def _recolectar_urls(self, page: Page, base_search_url: str, max_pages: int = None):
        inmuebles_encontrados = []
        urls_vistas = set()
        current_url = base_search_url
        paginas_escaneadas = 0

        while current_url:
            if max_pages and paginas_escaneadas >= max_pages:
                break

            logger.debug(f"[{self.source_name}] Escaneando página: {current_url}")
            await page.goto(current_url, timeout=60000)
            tarjetas = await page.locator("div.item").all()

            for tarjeta in tarjetas:
                enlace_loc = tarjeta.locator("div.title h2 a").first
                if await enlace_loc.count() > 0:
                    href = await enlace_loc.get_attribute("href")
                    titulo = await enlace_loc.inner_text()

                    precio_limpio = None
                    try:
                        precio_text = await tarjeta.locator("div.areaPrecio p.precio").inner_text(timeout=2000)
                        precio_str = re.sub(r'[^\d]', '', precio_text.replace(',', ''))
                        if precio_str:
                            precio_limpio = float(precio_str)
                    except Exception:
                        pass

                    if href and href not in urls_vistas:
                        urls_vistas.add(href)
                        inmuebles_encontrados.append({
                            "url": href,
                            "precio": precio_limpio,
                            "titulo": titulo.strip()
                        })

            paginas_escaneadas += 1

            # Paginación
            siguiente_btn = page.locator("a.page-link[aria-label='Next']")
            if await siguiente_btn.count() > 0:
                current_url = await siguiente_btn.first.get_attribute("href")
            else:
                current_url = None

        return inmuebles_encontrados

    async def _extraer_detalle(self, page: Page, url: str, precio_base: float, titulo_base: str):
        # Mantenemos el timeout alto por si la página es lenta, pero sin imágenes cargará rápido
        await page.goto(url, timeout=60000)

        # 1. Extraer ID
        try:
            codigo_text = await page.locator("li:has(strong:has-text('Código:'))").inner_text(timeout=5000)
            external_id = codigo_text.replace("Código:", "").strip()
        except Exception:
            external_id = url.split("/")[-1]

        builder = PropertySnapshotBuilder(source_name=self.source_name, external_id=external_id, url=url)

        if precio_base: builder.set_price(precio_base, "USD")

        # 2. Descripción completa y Extras (amenidades)
        try:
            descripcion_raw = await page.locator("div.content").inner_text(timeout=3000)
            builder.set_general_info(titulo=titulo_base, descripcion=descripcion_raw.strip())

            extras_lista = await page.locator("div.content ul li").all_inner_texts()
            if extras_lista:
                builder.add_extra_data("amenidades", [e.strip(" .") for e in extras_lista])
        except Exception:
            builder.set_general_info(titulo=titulo_base, descripcion=None)

        # 3. Ubicación
        try:
            ciudad = await page.locator("li:has(strong:has-text('Ciudad:'))").inner_text(timeout=3000)
            partes_titulo = titulo_base.split("-")
            urbanismo = partes_titulo[-1].strip() if len(partes_titulo) > 1 else ""

            builder.set_location(
                municipio=ciudad.replace("Ciudad:", "").strip(),
                urbanismo=urbanismo
            )
        except Exception:
            pass

        # 4. Características Principales
        try:
            area_text = await page.locator("li:has(strong:has-text('Área Construida:'))").inner_text(timeout=2000)
            m2 = float(re.search(r'\d+', area_text).group()) if re.search(r'\d+', area_text) else None

            habs_text = await page.locator("li:has(strong:has-text('Habitaciones:'))").inner_text(timeout=2000)
            habs = int(re.search(r'\d+', habs_text).group()) if re.search(r'\d+', habs_text) else None

            banos_text = await page.locator("li:has(strong:has-text('Baños:'))").inner_text(timeout=2000)
            banos = float(re.search(r'\d+', banos_text).group()) if re.search(r'\d+', banos_text) else None

            builder.add_features(m2_totales=m2, habitaciones=habs, banos=banos)
        except Exception:
            pass

        return builder.build()