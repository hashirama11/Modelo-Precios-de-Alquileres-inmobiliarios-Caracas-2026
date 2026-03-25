import asyncio
import re
from playwright.async_api import async_playwright, Page
from scraper.builder import PropertySnapshotBuilder
from scraper.utils import optimizar_pagina
import logging

# Instanciamos el logger
logger = logging.getLogger(__name__)

class MercadolibreScraper:
    def __init__(self):
        self.source_name = "mercadolibre"
        self.base_url = "https://listado.mercadolibre.com.ve"
        # Disfrazamos a nuestro bot con un User-Agent estándar de Mac
        self.user_agent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

    async def run_pipeline(self, start_url: str, max_pages: int = None, save_callback=None):
        resultados_totales = 0

        # --- FASE A: RECOLECCIÓN DE URLs ---
        async with async_playwright() as p:
            # ¡CRÍTICO PARA MERCADO LIBRE! headless=False para evadir el WAF
            browser = await p.chromium.launch(headless=False)
            context = await browser.new_context(user_agent=self.user_agent)
            page_lista = await context.new_page()

            await optimizar_pagina(page_lista)

            logger.info(f"[{self.source_name}] Fase A: Recolección de URLs iniciada (Modo Visible)...")
            inmuebles_base = await self._recolectar_urls(page_lista, start_url, max_pages)

            await page_lista.close()
            await context.close()
            await browser.close()
            logger.info(f"[{self.source_name}] Fase A terminada. {len(inmuebles_base)} URLs encontradas.")

        if not inmuebles_base:
            logger.warning(f"[{self.source_name}] No se encontraron URLs. (Posible bloqueo de CAPTCHA)")
            return 0

        # --- FASE B: EXTRACCIÓN POR LOTES (MICRO-BATCHING) ---
        tamaño_lote = 100
        total_lotes = (len(inmuebles_base) // tamaño_lote) + 1

        for i in range(0, len(inmuebles_base), tamaño_lote):
            lote_actual = inmuebles_base[i: i + tamaño_lote]
            num_lote = (i // tamaño_lote) + 1
            logger.info(f"[{self.source_name}] ⏳ Procesando Lote {num_lote}/{total_lotes} ({len(lote_actual)} URLs)...")

            async with async_playwright() as p:
                # Nuevamente headless=False para que no nos bloqueen al visitar el detalle
                browser = await p.chromium.launch(headless=False)
                context = await browser.new_context(user_agent=self.user_agent)
                semaforo = asyncio.Semaphore(3)

                async def procesar_con_semaforo(item):
                    async with semaforo:
                        nueva_pestaña = await context.new_page()

                        # BUG CORREGIDO: Aplicamos optimización a nueva_pestaña
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

            # Selector de los items de la lista
            tarjetas = await page.locator("li.ui-search-layout__item").all()

            for tarjeta in tarjetas:
                enlace_loc = tarjeta.locator("h3.poly-component__title-wrapper a").first
                if await enlace_loc.count() > 0:
                    href = await enlace_loc.get_attribute("href")
                    # Mercado Libre puede ser lento cargando títulos, bajamos el timeout o lo envolvemos en try
                    try:
                        titulo = await enlace_loc.inner_text(timeout=2000)
                    except Exception:
                        titulo = "Sin título"

                    precio_limpio = None
                    try:
                        # Extraemos el precio del grid
                        precio_text = await tarjeta.locator(
                            "div.poly-price__current span.andes-money-amount__fraction").first.inner_text(timeout=2000)
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
                            "titulo": titulo.strip()
                        })

            paginas_escaneadas += 1

            # Paginación usando el botón Siguiente
            siguiente_btn = page.locator("a.andes-pagination__link[title='Siguiente']")
            if await siguiente_btn.count() > 0:
                current_url = await siguiente_btn.first.get_attribute("href")
            else:
                current_url = None

        return inmuebles_encontrados

    async def _extraer_detalle(self, page: Page, url: str, precio_base: float, titulo_base: str):
        await page.goto(url, timeout=60000)

        # 1. Extraer ID desde la URL (Ejemplo: MLV-974607580 -> MLV974607580)
        match_id = re.search(r'(MLV-?\d+)', url)
        external_id = match_id.group(1).replace("-", "") if match_id else url.split("/")[-1]

        builder = PropertySnapshotBuilder(source_name=self.source_name, external_id=external_id, url=url)

        # 2. Precio (Mercado Libre tiene una etiqueta meta perfecta para esto si la base falla)
        if precio_base:
            builder.set_price(precio_base, "USD")
        else:
            try:
                precio_meta = await page.locator("meta[itemprop='price']").first.get_attribute("content", timeout=3000)
                if precio_meta:
                    builder.set_price(float(precio_meta), "USD")
            except Exception:
                pass

        # 3. Título y Descripción
        try:
            titulo = await page.locator("h1.ui-pdp-title").inner_text(timeout=3000)
        except Exception:
            titulo = titulo_base

        descripcion_limpia = None
        try:
            descripcion_limpia = await page.locator("p.ui-pdp-description__content").inner_text(timeout=3000)
        except Exception:
            pass

        builder.set_general_info(titulo=titulo, descripcion=descripcion_limpia)

        # 4. Ubicación (ML suele poner la ubicación cruda en un script o en el mapa. Usamos un default seguro por ahora)
        builder.set_location(municipio="Caracas", urbanismo="")

        # 5. Características desde la tabla Andes (andes-table)
        try:
            amenidades_validas = []
            filas = await page.locator("tr.andes-table__row").all()

            for fila in filas:
                llave = await fila.locator("th").inner_text(timeout=1000)
                valor = await fila.locator("td span.andes-table__column--value").inner_text(timeout=1000)

                llave_lower = llave.lower()
                valor_lower = valor.lower()

                # Atributos numéricos principales
                if "superficie total" in llave_lower or "superficie construida" in llave_lower:
                    m2 = float(re.search(r'\d+', valor).group()) if re.search(r'\d+', valor) else None
                    builder.add_features(m2_totales=m2)
                elif "habitaciones" in llave_lower:
                    habs = int(re.search(r'\d+', valor).group()) if re.search(r'\d+', valor) else None
                    builder.add_features(habitaciones=habs)
                elif "baños" in llave_lower:
                    banos = float(re.search(r'\d+', valor).group()) if re.search(r'\d+', valor) else None
                    builder.add_features(banos=banos)

                # Amenidades booleanas (Todo lo que diga "Sí")
                elif valor_lower == "sí" or valor_lower == "si":
                    amenidades_validas.append(llave.strip())

            if amenidades_validas:
                builder.add_extra_data("amenidades", amenidades_validas)
        except Exception:
            pass

        return builder.build()