import asyncio
import re
from urllib.parse import urljoin
from playwright.async_api import async_playwright, Page
from scraper.builder import PropertySnapshotBuilder
from scraper.utils import optimizar_pagina

class RentAHouseScraper:
    def __init__(self):
        self.source_name = "rentahouse"
        self.base_url = "https://rentahouse.com.ve"

    async def run_pipeline(self, start_url: str, max_pages: int = None, save_callback=None):
        resultados_totales = 0

        # ==========================================
        # FASE A: RECOLECCIÓN DE URLs
        # ==========================================
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context()
            page_lista = await context.new_page()

            await optimizar_pagina(page_lista)

            print(f"[{self.source_name}] Fase A: Recolección de URLs...")
            inmuebles_base = await self._recolectar_urls(page_lista, start_url, max_pages)

            # ¡CRÍTICO! Cerramos el navegador de la Fase A para liberar RAM inicial
            await page_lista.close()
            await context.close()
            await browser.close()

            print(f"[{self.source_name}] Fase A terminada. {len(inmuebles_base)} URLs encontradas.")

        if not inmuebles_base:
            return 0

        # ==========================================
        # FASE B: EXTRACCIÓN POR LOTES (MICRO-BATCHING)
        # ==========================================
        tamaño_lote = 100  # Procesaremos de 100 en 100
        total_lotes = (len(inmuebles_base) // tamaño_lote) + 1

        for i in range(0, len(inmuebles_base), tamaño_lote):
            lote_actual = inmuebles_base[i: i + tamaño_lote]
            num_lote = (i // tamaño_lote) + 1
            print(f"[{self.source_name}] Procesando Lote {num_lote}/{total_lotes} ({len(lote_actual)} inmuebles)...")

            # Abrimos un navegador NUEVO y FRESCO solo para este lote
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                context = await browser.new_context()
                semaforo = asyncio.Semaphore(3)  # 3 pestañas concurrentes a la vez

                async def procesar_con_semaforo(item):
                    async with semaforo:
                        nueva_pestaña = await context.new_page()

                        await optimizar_pagina(page_lista)

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

                tareas = [procesar_con_semaforo(item) for item in lote_actual]
                resultados_crudos = await asyncio.gather(*tareas)
                resultados_validos = [r for r in resultados_crudos if r is not None]

                # ¡GUARDADO INMEDIATO EN LA BASE DE DATOS!
                if save_callback and resultados_validos:
                    await save_callback(resultados_validos)
                    resultados_totales += len(resultados_validos)

                # ¡CRÍTICO! Cerramos el navegador para vaciar la memoria RAM
                await context.close()
                await browser.close()

            # Pequeño respiro de 5 segundos para no saturar al servidor destino
            print(f"[{self.source_name}] Lote {num_lote} finalizado. RAM limpiada. Descansando 5s...")
            await asyncio.sleep(5)

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
            # Buscamos las tarjetas de propiedades (según tu HTML)
            tarjetas = await page.locator("div.property-list").all()

            for tarjeta in tarjetas:
                enlace_loc = tarjeta.locator("a").first
                if await enlace_loc.count() > 0:
                    href = await enlace_loc.get_attribute("href")

                    # Limpieza de título desde h4.card-title
                    titulo = ""
                    try:
                        titulo_text = await tarjeta.locator("h4.card-title").inner_text(timeout=2000)
                        titulo = titulo_text.replace("\n", " ").strip()
                    except Exception:
                        pass

                    # Limpieza de precio (ej. "USD 400.000")
                    precio_limpio = None
                    try:
                        precio_text = await tarjeta.locator("div.price strong").inner_text(timeout=2000)
                        # Reemplazamos espacios invisibles y quitamos puntos de miles y letras
                        precio_text = precio_text.replace('\xa0', ' ').replace('&nbsp;', ' ')
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

            # Lógica de Paginación Relativa (ej. "/propiedades_ubicadas_en_caracas.html?page=2")
            siguiente_btn = page.locator("a.page-link:has-text('Siguiente')")
            if await siguiente_btn.count() > 0:
                relative_url = await siguiente_btn.first.get_attribute("href")
                # urljoin une "https://rentahouse..." con "/propiedades..." de forma segura
                current_url = urljoin(self.base_url, relative_url)
            else:
                current_url = None

        return inmuebles_encontrados

    async def _extraer_detalle(self, page: Page, url: str, precio_base: float, titulo_base: str):
        await page.goto(url, timeout=60000)

        # 1. Extraer ID Externo (Obligatorio)
        try:
            codigo_loc = page.locator("li.dotted:has(span:has-text('Código RAH:')) span.float-right")
            external_id = await codigo_loc.inner_text(timeout=5000)
            external_id = external_id.strip()
        except Exception:
            return None

        builder = PropertySnapshotBuilder(source_name=self.source_name, external_id=external_id, url=url)

        if precio_base: builder.set_price(precio_base, "USD")

        # 2. Extraer Descripción Completa
        descripcion_limpia = None
        try:
            desc_loc = page.locator("div.propertyDescription p")
            descripcion_limpia = await desc_loc.inner_text(timeout=3000)
        except Exception:
            pass
        builder.set_general_info(titulo=titulo_base, descripcion=descripcion_limpia)

        # 3. Ubicación
        try:
            ciudad = await page.locator("li.dotted:has(span:has-text('Ciudad:')) span.float-right").inner_text(
                timeout=3000)
            urbanizacion = await page.locator(
                "li.dotted:has(span:has-text('Urbanización:')) span.float-right").inner_text(timeout=3000)
            builder.set_location(municipio=ciudad.strip(), urbanismo=urbanizacion.strip())
        except Exception:
            pass

        # 4. Características Principales (M2, Baños, Habitaciones)
        try:
            area_text = await page.locator("li.dotted:has(span:has-text('Área Privada:')) span.float-right").inner_text(
                timeout=3000)
            m2 = float(re.search(r'\d+', area_text).group()) if re.search(r'\d+', area_text) else None

            # Intentamos buscar "Baños Completos" o "Medios Baños"
            banos_text = await page.locator("li.dotted:has(span:has-text('Baños')) span.float-right").first.inner_text(
                timeout=3000)
            banos = float(re.search(r'\d+', banos_text).group()) if banos_text and re.search(r'\d+',
                                                                                             banos_text) else None

            habs_text = await page.locator(
                "li.dotted:has(span:has-text('Habitaciones:')) span.float-right").first.inner_text(timeout=3000)
            habs = int(re.search(r'\d+', habs_text).group()) if habs_text and re.search(r'\d+', habs_text) else None

            builder.add_features(m2_totales=m2, habitaciones=habs, banos=banos)
        except Exception:
            pass

        # 5. Extras (Emojis ✅ / ❌)
        try:
            amenidades_validas = []
            # Buscamos todos los li dentro de las listas de detalles mínimos (Estacionamiento, AC, etc)
            lista_extras = await page.locator("ul.property-detailes-list-min li").all_inner_texts()

            for extra in lista_extras:
                # Si tiene el check verde, lo guardamos limpiando el emoji
                if "✅" in extra:
                    texto_limpio = extra.replace("✅", "").strip()
                    amenidades_validas.append(texto_limpio)

            if amenidades_validas:
                builder.add_extra_data("amenidades", amenidades_validas)
        except Exception:
            pass

        return builder.build()