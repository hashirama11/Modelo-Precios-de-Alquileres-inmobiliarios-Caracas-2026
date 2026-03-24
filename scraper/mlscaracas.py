"""
Clase para manejar PlayWrigth y el proceso explicito de scraper de MLSCaracas
El script se ejcutara en worker.py
"""

## Dependencias
import re
from playwright.async_api import async_playwright, Page
from scraper.builder import PropertySnapshotBuilder


class MLSCaracasScraper:
    def __init__(self):
        self.source_name = "mlscaracas"
        self.base_url = "https://mlscaracas.com"

    async def run_pipeline(self, start_url: str, max_pages: int = 3):
        """Punto de entrada principal para el Worker"""
        resultados = []
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            # 1. Recolectar URLs de las páginas de listado
            urls_inmuebles = await self._recolectar_urls(page, start_url, max_pages)

            # 2. Visitar cada URL y extraer los detalles
            for url in urls_inmuebles:
                snapshot = await self._extraer_detalle(page, url)
                if snapshot:
                    resultados.append(snapshot)

            await browser.close()
        return resultados

    async def _recolectar_urls(self, page: Page, base_search_url: str, max_pages: int):
        urls = []
        for i in range(1, max_pages + 1):
            # Observa cómo manejamos la paginación según tu análisis
            paginated_url = f"{base_search_url}&page={i}"
            await page.goto(paginated_url)

            # Buscamos todos los enlaces dentro de los títulos de la lista
            enlaces = await page.locator("h2.title-dot a").all()
            for enlace in enlaces:
                href = await enlace.get_attribute("href")
                if href:
                    urls.append(href)
        return list(set(urls))  # Eliminar duplicados por si acaso

    async def _extraer_detalle(self, page: Page, url: str):
        await page.goto(url)

        # 1. Extraer ID Externo (del HTML que me pasaste)
        # Buscamos el <li> que contiene "Código:"
        codigo_text = await page.locator("li:has-text('Código:')").inner_text()
        external_id = codigo_text.replace("Código:", "").strip()  # Queda "1351436"

        # 2. Inicializar el Builder
        builder = PropertySnapshotBuilder(source_name=self.source_name, external_id=external_id, url=url)

        # 3. Extraer Precio (Requiere limpieza con expresiones regulares)
        # Del fragmento: <span class="pr2">US$2,500 </span>
        precio_text = await page.locator("span.pr2").inner_text()
        precio_limpio = re.sub(r'[^\d.]', '', precio_text.replace(',', ''))  # Elimina US$ y comas
        if precio_limpio:
            builder.set_price(float(precio_limpio), "USD")

        # 4. Extraer Ubicación
        municipio = await page.locator("li:has-text('Municipio:')").inner_text()
        urbanizacion = await page.locator("li:has-text('Urbanización:')").inner_text()
        builder.set_location(
            municipio=municipio.replace("Municipio:", "").strip(),
            urbanismo=urbanizacion.replace("Urbanización:", "").strip()
        )

        # 5. Extraer Características (Área, Habitaciones, Baños)
        area_text = await page.locator("li:has-text('Área Construida:')").inner_text()
        habitaciones = await page.locator("li:has-text('Habitaciones:')").inner_text()
        banos = await page.locator("li:has-text('Baño:')").inner_text()

        # Limpiamos los números
        m2 = float(re.search(r'\d+', area_text).group()) if re.search(r'\d+', area_text) else None
        habs = int(re.search(r'\d+', habitaciones).group()) if re.search(r'\d+', habitaciones) else None

        builder.add_features(m2_totales=m2, habitaciones=habs)

        # 6. Extraer Descripción
        descripcion = await page.locator("div.title:has(h3:has-text('Descripción Adicional')) + p").inner_text()
        builder.set_general_info(descripcion=descripcion)

        # 7. Listas de características internas/externas (Extra data)
        # Aquí recolectamos todos los <li> de las listas de características y las guardamos crudas
        features_items = await page.locator("div.list-info-2a ul li").all_inner_texts()
        builder.add_extra_data("amenidades", features_items)  # Guarda la lista entera en el JSON

        # Finalmente, retornamos el modelo validado
        return builder.build()