import asyncio
import re
from urllib.parse import urljoin
from playwright.async_api import async_playwright, Page
from scraper.builder import PropertySnapshotBuilder
from scraper.utils import optimizar_pagina
import logging

# Instanciamos el logger
logger = logging.getLogger(__name__)

class RemaxScraper:
    def __init__(self):
        self.source_name = "remax"
        self.base_url = "https://www.remax.com.ve"

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

                        # BUG CORREGIDO: Aplicamos optimización a la nueva_pestaña
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

            # Selector de la tarjeta principal según tu HTML
            tarjetas = await page.locator("div.inmueble-item").all()

            for tarjeta in tarjetas:
                enlace_loc = tarjeta.locator("a").first
                if await enlace_loc.count() > 0:
                    href = await enlace_loc.get_attribute("href")
                    # RE/MAX a veces usa rutas relativas
                    if href and not href.startswith("http"):
                        href = urljoin(self.base_url, href)

                    titulo = ""
                    try:
                        titulo_text = await tarjeta.locator("h3.nombre-inmueble").inner_text(timeout=2000)
                        titulo = titulo_text.strip()
                    except Exception:
                        pass

                    precio_limpio = None
                    try:
                        # Extraemos el precio: USD $1.400
                        precio_text = await tarjeta.locator("b.precio-valor").inner_text(timeout=2000)
                        precio_str = re.sub(r'[^\d]', '', precio_text)
                        if precio_str:
                            precio_limpio = float(precio_str)
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

            # Paginación basada en el SVG (Flecha Siguiente)
            siguiente_btn = page.locator("a:has(svg path[d^='M10 6L8.59'])")
            if await siguiente_btn.count() > 0:
                relative_url = await siguiente_btn.first.get_attribute("href")
                if relative_url and relative_url != "#":
                    current_url = urljoin(self.base_url, relative_url)
                else:
                    current_url = None
            else:
                current_url = None

        return inmuebles_encontrados

    async def _extraer_detalle(self, page: Page, url: str, precio_base: float, titulo_base: str):
        await page.goto(url, timeout=60000)

        # 1. Extraer ID (Obligatorio)
        try:
            codigo_loc = page.locator("p.codigo small")
            external_id = await codigo_loc.inner_text(timeout=5000)
            external_id = external_id.strip()
        except Exception:
            return None

        builder = PropertySnapshotBuilder(source_name=self.source_name, external_id=external_id, url=url)

        if precio_base: builder.set_price(precio_base, "USD")

        # 2. Descripción
        descripcion_limpia = None
        try:
            descripcion_limpia = await page.locator("div.texto-descripcion").inner_text(timeout=3000)
        except Exception:
            pass
        builder.set_general_info(titulo=titulo_base, descripcion=descripcion_limpia)

        # 3. Ubicación (Del bloque Ubicación)
        try:
            bloque_ubi = page.locator("div.datos-inmueble:has(h2:has-text('Ubicación'))")
            ciudad = await bloque_ubi.locator("li:has(b:has-text('Ciudad:'))").inner_text(timeout=3000)
            urbanizacion = await bloque_ubi.locator("li:has(b:has-text('Urbanización:'))").inner_text(timeout=3000)

            builder.set_location(
                municipio=ciudad.replace("Ciudad:", "").strip(),
                urbanismo=urbanizacion.replace("Urbanización:", "").strip()
            )
        except Exception:
            pass

        # 4. Características (Del bloque Datos del inmueble)
        try:
            bloque_datos = page.locator("div.lista-datos")

            area_text = await bloque_datos.locator("li:has(b:has-text('Área de Construcción:'))").inner_text(
                timeout=2000)
            m2 = float(re.search(r'\d+', area_text).group()) if re.search(r'\d+', area_text) else None

            habs_text = await bloque_datos.locator("li:has(b:has-text('Habitaciones:'))").inner_text(timeout=2000)
            habs = int(re.search(r'\d+', habs_text).group()) if re.search(r'\d+', habs_text) else None

            banos_text = await bloque_datos.locator("li:has(b:has-text('Baños:'))").inner_text(timeout=2000)
            banos = float(re.search(r'\d+', banos_text).group()) if re.search(r'\d+', banos_text) else None

            builder.add_features(m2_totales=m2, habitaciones=habs, banos=banos)
        except Exception:
            pass

        return builder.build()