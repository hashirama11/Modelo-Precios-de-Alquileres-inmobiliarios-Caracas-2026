"""
Clase para manejar PlayWrigth y el proceso explicito de scraper de MLSCaracas
El script se ejcutara en worker.py
"""

## Dependencias
import re
import asyncio
from playwright.async_api import async_playwright, Page
from scraper.builder import PropertySnapshotBuilder
from scraper.utils import optimizar_pagina
import logging

# Instanciamos el logger
logger = logging.getLogger(__name__)


class MLSCaracasScraper:
    def __init__(self):
        self.source_name = "mlscaracas"
        self.base_url = "https://mlscaracas.com"

    async def run_pipeline(self, start_url: str, max_pages: int = None, save_callback=None):
        resultados_totales = 0

        # --- FASE A: RECOLECCIÓN DE URLs ---
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context()
            page_lista = await context.new_page()

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

                        # BUG CORREGIDO: Aplicamos optimización a la nueva pestaña
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

        logger.info(
            f"[{self.source_name}] 🎉 Pipeline completado. {resultados_totales} inmuebles procesados exitosamente.")
        return resultados_totales

    async def _recolectar_urls(self, page: Page, base_search_url: str, max_pages: int = None):
        inmuebles_encontrados = []
        urls_vistas = set()

        current_url = base_search_url
        paginas_escaneadas = 0

        # Bucle While: Navega hasta que no haya más páginas o alcance el límite
        while current_url:
            if max_pages and paginas_escaneadas >= max_pages:
                break

            logger.debug(f"[{self.source_name}] Escaneando página: {current_url}")
            await page.goto(current_url, timeout=60000)
            tarjetas = await page.locator("div.item").all()

            for tarjeta in tarjetas:
                enlace_loc = tarjeta.locator("h2.title-dot a")
                if await enlace_loc.count() > 0:
                    href = await enlace_loc.first.get_attribute("href")
                    titulo = await enlace_loc.first.inner_text()  # NUEVO: Capturar título

                    precio_limpio = None
                    try:
                        precio_text = await tarjeta.locator("span.pr2").inner_text(timeout=2000)
                        precio_limpio = float(re.sub(r'[^\d.]', '', precio_text.replace(',', '')))
                    except Exception:
                        pass

                    if href and href not in urls_vistas:
                        urls_vistas.add(href)
                        inmuebles_encontrados.append({
                            "url": href,
                            "precio": precio_limpio,
                            "titulo": titulo  # NUEVO: Guardamos el título para la Fase B
                        })

            paginas_escaneadas += 1

            # Lógica de Paginación Dinámica
            siguiente_btn = page.locator("a.page-link[aria-label='Next']")
            if await siguiente_btn.count() > 0:
                current_url = await siguiente_btn.first.get_attribute("href")
            else:
                current_url = None  # Rompe el bucle, ya no hay botón Siguiente

        return inmuebles_encontrados

    # Añadimos titulo_base a los parámetros
    async def _extraer_detalle(self, page: Page, url: str, precio_base: float, titulo_base: str):
        await page.goto(url, timeout=60000)

        try:
            codigo_text = await page.locator("li:has-text('Código:')").inner_text(timeout=5000)
            external_id = codigo_text.replace("Código:", "").strip()
        except Exception:
            return None

        builder = PropertySnapshotBuilder(source_name=self.source_name, external_id=external_id, url=url)

        # Inyectamos el precio y el título extraídos en la Fase A
        if precio_base:
            builder.set_price(precio_base, "USD")

        # NUEVO: Enviamos el título y una descripción vacía por defecto
        builder.set_general_info(titulo=titulo_base, descripcion=None)

        # ... (Ubicación y Características se mantienen igual) ...
        try:
            municipio = await page.locator("li:has-text('Municipio:')").inner_text(timeout=3000)
            urbanizacion = await page.locator("li:has-text('Urbanización:')").inner_text(timeout=3000)
            builder.set_location(municipio=municipio.replace("Municipio:", "").strip(),
                                 urbanismo=urbanizacion.replace("Urbanización:", "").strip())
        except Exception:
            pass

        try:
            area_text = await page.locator("li:has-text('Área Construida:')").inner_text(timeout=3000)
            m2 = float(re.search(r'\d+', area_text).group()) if re.search(r'\d+', area_text) else None
            habitaciones_text = await page.locator("li:has-text('Habitaciones:')").inner_text(timeout=3000)
            habs = int(re.search(r'\d+', habitaciones_text).group()) if re.search(r'\d+', habitaciones_text) else None
            builder.add_features(m2_totales=m2, habitaciones=habs)
        except Exception:
            pass

        # NUEVO: Extracción de descripción corregida
        try:
            # Tomamos el contenedor completo en lugar de un solo párrafo
            desc_loc = page.locator("div.col-md-12:has(h3:has-text('Descripción Adicional'))")
            descripcion_raw = await desc_loc.inner_text(timeout=3000)
            # Limpiamos el título de la sección del texto resultante
            descripcion_limpia = descripcion_raw.replace("Descripción Adicional", "").strip()
            # Actualizamos la info general
            builder.set_general_info(titulo=titulo_base, descripcion=descripcion_limpia)
        except Exception:
            pass

        try:
            features_items = await page.locator("div.list-info-2a ul li").all_inner_texts()
            if features_items:
                builder.add_extra_data("amenidades", features_items)
        except Exception:
            pass

        return builder.build()