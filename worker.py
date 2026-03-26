import asyncio
import logging
import sys
from scraper.remax import RemaxScraper
from db.database import engine, AsyncSessionLocal
from sqlalchemy.future import select
from scraper.rentahouse import RentAHouseScraper
from scraper.bolsainmobiliaria import BolsaInmobiliariaScraper
from scraper.quarto import QuartoScraper
from scraper.vecindary import VecindaryScraper
from scraper.turesidencia import TuresidenciaScraper
from scraper.mercadolibre import MercadolibreScraper

# Importamos nuestros módulos (Ajusta las rutas según tu estructura exacta)
from db.models import Base, Inmueble, InmuebleSnapshot
from scraper.mlscaracas import MLSCaracasScraper


"""
El Plan de Acción Final
Con este cambio, tienes el control absoluto de tu software:

Para el uso normal del día a día: Si solo quieres prender el orquestador para que espere al domingo, escribes python worker.py.

Para la gran cosecha de hoy: Vas a combinar ambas herramientas (caffeinate y tu nueva bandera --seed).

Ejecuta esto en tu terminal ahora mismo para arrancar:

Bash
caffeinate -i python worker.py --seed
"""
# Configuración básica de logs para ver qué hace el worker
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


async def init_db():
    """Crea las tablas en SQLite si no existen (ideal para desarrollo)"""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def procesar_y_guardar(snapshots: list):
    """
    Recibe la lista de PropertySnapshot (diccionarios o Pydantic models)
    y aplica la lógica de Historial (Padre-Hijo).
    """
    async with AsyncSessionLocal() as session:
        for snap_data in snapshots:
            # Si snap_data es un modelo Pydantic, usamos .model_dump() o .dict()
            # Asumiremos que es un dict por ahora basado en nuestro Builder temporal
            source = snap_data.get("source_name")
            ext_id = str(snap_data.get("external_id"))
            precio_nuevo = snap_data.get("precio")

            # 1. ¿Existe el Inmueble Padre?
            stmt = select(Inmueble).where(Inmueble.source_name == source, Inmueble.external_id == ext_id)
            result = await session.execute(stmt)
            inmueble = result.scalars().first()

            if not inmueble:
                # CASO A: Es un inmueble totalmente nuevo. Creamos Padre e Hijo.
                logger.info(f"NUEVO INMUEBLE: {source} - {ext_id}")
                nuevo_inmueble = Inmueble(
                    source_name=source,
                    external_id=ext_id,
                    url=snap_data.get("url")
                )
                session.add(nuevo_inmueble)
                await session.flush()  # Para obtener el ID del padre generado por SQLite

                nuevo_snapshot = InmuebleSnapshot(
                    inmueble_id=nuevo_inmueble.id,
                    precio=precio_nuevo,
                    moneda=snap_data.get("moneda"),
                    titulo=snap_data.get("titulo"),
                    descripcion=snap_data.get("descripcion"),
                    ubicacion=snap_data.get("ubicacion"),
                    caracteristicas=snap_data.get("caracteristicas"),
                    condiciones=snap_data.get("condiciones"),
                    raw_extra_data=snap_data.get("raw_extra_data")
                )
                session.add(nuevo_snapshot)

            else:
                # CASO B: El inmueble ya existe. Buscamos su último Snapshot.
                stmt_snap = select(InmuebleSnapshot).where(
                    InmuebleSnapshot.inmueble_id == inmueble.id
                ).order_by(InmuebleSnapshot.scraped_at.desc())

                result_snap = await session.execute(stmt_snap)
                ultimo_snapshot = result_snap.scalars().first()

                # Lógica de negocio: Solo guardamos si el precio cambió
                # (Podrías agregar más condiciones aquí, ej: si cambió el status de activo)
                if ultimo_snapshot and ultimo_snapshot.precio != precio_nuevo:
                    logger.info(f"CAMBIO DE PRECIO detectado para {ext_id}: {ultimo_snapshot.precio} -> {precio_nuevo}")
                    nuevo_snapshot = InmuebleSnapshot(
                        inmueble_id=inmueble.id,
                        precio=precio_nuevo,
                        moneda=snap_data.get("moneda"),
                        titulo=snap_data.get("titulo"),
                        descripcion=snap_data.get("descripcion"),
                        ubicacion=snap_data.get("ubicacion"),
                        caracteristicas=snap_data.get("caracteristicas"),
                        condiciones=snap_data.get("condiciones"),
                        raw_extra_data=snap_data.get("raw_extra_data")
                    )
                    session.add(nuevo_snapshot)
                else:
                    logger.debug(f"Sin cambios para {source} - {ext_id}")

        # Guardamos todos los cambios en bloque (Batch Commit)
        await session.commit()
        logger.info("Sincronización con base de datos finalizada.")


async def job_mls_caracas():
    """La tarea principal que ejecutará el planificador"""
    logger.info("Iniciando Job: MLSCaracasScraper")
    start_url = "https://mlscaracas.com/s/venezuela/miranda-dtto-capital/caracas//?id_country=95&id_region=3556&id_city=859026"
    scraper = MLSCaracasScraper()
    try:
        # Pasamos el callback y recibimos el total de guardados
        total_guardados = await scraper.run_pipeline(start_url, save_callback=procesar_y_guardar)
        logger.info(f"Scraping MLS completado. {total_guardados} inmuebles guardados.")
    except Exception as e:
        logger.error(f"Error durante la ejecución del job MLS: {e}")


async def job_rentahouse_caracas():
    logger.info("Iniciando Job: RentAHouseScraper")
    start_url = "https://rentahouse.com.ve/propiedades_ubicadas_en_caracas.html?priceMin=0&priceMax=0&m2Min=0&m2Max=0&orderBy=entryTimestamp%20desc&country=venezuela&state=distrito-metropolitano&countrySlug=venezuela"
    scraper = RentAHouseScraper()
    try:
        total_guardados = await scraper.run_pipeline(start_url, save_callback=procesar_y_guardar)
        logger.info(f"Scraping RAH completado. {total_guardados} inmuebles guardados.")
    except Exception as e:
        logger.error(f"Error durante la ejecución del job RAH: {e}")


async def job_remax_caracas():
    logger.info("Iniciando Job: RemaxScraper")
    urls_remax = [
        "https://www.remax.com.ve/inmuebles/apartamento/alquiler?ubi=Caracas%2C+Baruta%2C+Miranda%2C+VEN",
        "https://www.remax.com.ve/inmuebles/apartamento/alquiler?ubi=Caracas%2C+Chacao%2C+Miranda%2C+VEN",
        "https://www.remax.com.ve/inmuebles/apartamento/alquiler?ubi=Caracas%2C+Libertador%2C+Distrito+Capital%2C+VEN",
        "https://www.remax.com.ve/inmuebles/apartamento/alquiler?ubi=Caracas%2C+Sucre%2C+Miranda%2C+VEN",
        "https://www.remax.com.ve/inmuebles/apartamento/alquiler?ubi=Caracas%2C+El+Hatillo%2C+Miranda%2C+VEN"
    ]
    scraper = RemaxScraper()
    total_guardados = 0

    try:
        for url in urls_remax:
            try:
                guardados_zona = await scraper.run_pipeline(url, save_callback=procesar_y_guardar)
                total_guardados += guardados_zona
                await asyncio.sleep(2)
            except Exception as e:
                logger.error(f"⚠️ Error en zona {url}: {e}. Continuando...")
                continue

        logger.info(f"Scraping REMAX completado. {total_guardados} inmuebles guardados en total.")
    except Exception as e:
        logger.error(f"Error durante la ejecución del job REMAX: {e}")


async def job_bolsainmobiliaria_caracas():
    logger.info("Iniciando Job: BolsaInmobiliariaScraper")
    start_url = "https://bolsainmobiliariacaracas.com/s/apartamento/alquiler/venezuela/miranda-dtto-capital/caracas?id_country=95&id_region=3556&id_city=859026&id_property_type=2&business_type%5B%5D=for_rent"
    scraper = BolsaInmobiliariaScraper()
    try:
        total_guardados = await scraper.run_pipeline(start_url, save_callback=procesar_y_guardar)
        logger.info(f"Scraping Bolsa Inmobiliaria completado. {total_guardados} inmuebles guardados.")
    except Exception as e:
        logger.error(f"Error durante la ejecución del job Bolsa Inmobiliaria: {e}")


async def job_quarto_caracas():
    logger.info("Iniciando Job: QuartoScraper")
    # URL limpia: Alquiler (type_id=1) en Caracas/Miranda (city_id=1 y municipios principales)
    start_url = "https://quartoapp.com/propiedades/alquiler?type_id=1&city_id=1&municipality_id=1%2C2%2C3%2C4%2C5"
    scraper = QuartoScraper()
    try:
        total_guardados = await scraper.run_pipeline(start_url, save_callback=procesar_y_guardar)
        logger.info(f"Scraping Quarto completado. {total_guardados} inmuebles guardados.")
    except Exception as e:
        logger.error(f"Error durante la ejecución del job Quarto: {e}")


async def job_vecindary_caracas():
    logger.info("Iniciando Job: VecindaryScraper")
    start_url = "https://vecindary.com/inmuebles/venezuela-distrito-capital-caracas-libertador/en-alquiler-temporario"
    scraper = VecindaryScraper()
    try:
        total_guardados = await scraper.run_pipeline(start_url, save_callback=procesar_y_guardar)
        logger.info(f"Scraping Vecindary completado. {total_guardados} inmuebles guardados.")
    except Exception as e:
        logger.error(f"Error durante la ejecución del job Vecindary: {e}")


async def job_turesidencia_caracas():
    logger.info("Iniciando Job: TuresidenciaScraper")
    start_url = "https://www.turesidencia.net/habitaciones-alquiler/categories/11-habitaciones-en-alquiler-caracas/ads"
    scraper = TuresidenciaScraper()
    try:
        total_guardados = await scraper.run_pipeline(start_url, save_callback=procesar_y_guardar)
        logger.info(f"Scraping TuResidencia completado. {total_guardados} inmuebles guardados.")
    except Exception as e:
        logger.error(f"Error durante la ejecución del job TuResidencia: {e}")


async def job_mercadolibre_caracas():
    logger.info("Iniciando Job: MercadolibreScraper (Por Municipios)")

    # Dividimos la búsqueda para evadir el límite máximo de páginas de Mercado Libre
    urls_mercadolibre = [
        "https://listado.mercadolibre.com.ve/inmuebles/apartamentos/alquiler/distrito-capital/chacao/",
        "https://listado.mercadolibre.com.ve/inmuebles/apartamentos/alquiler/distrito-capital/baruta/",
        "https://listado.mercadolibre.com.ve/inmuebles/apartamentos/alquiler/distrito-capital/sucre/",
        "https://listado.mercadolibre.com.ve/inmuebles/apartamentos/alquiler/distrito-capital/libertador/",
        "https://listado.mercadolibre.com.ve/inmuebles/apartamentos/alquiler/distrito-capital/el-hatillo/"
    ]

    scraper = MercadolibreScraper()
    total_guardados_general = 0

    try:
        for url in urls_mercadolibre:
            logger.info(f"🔍 Explorando zona Mercado Libre: {url.split('/')[-2].upper()}")
            try:
                # No le pasamos max_pages para que corra hasta el final de esa zona
                guardados_zona = await scraper.run_pipeline(url, save_callback=procesar_y_guardar)
                total_guardados_general += guardados_zona
                await asyncio.sleep(5)  # Descanso entre zonas
            except Exception as e:
                logger.error(f"⚠️ Error en zona ML {url}: {e}. Continuando...")
                continue

        logger.info(f"✅ Scraping Mercado Libre completado. {total_guardados_general} inmuebles guardados en total.")
    except Exception as e:
        logger.error(f"Error crítico durante la ejecución del job Mercado Libre: {e}")


# Configuramos un log hermoso, detallado y con la hora exacta
logging.basicConfig(
    level=logging.DEBUG,  # <--- ¡LA MAGIA ESTÁ AQUÍ! Cambiamos de INFO a DEBUG
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Silenciamos librerías de terceros (como Playwright o SQLite) para que no hagan "ruido basura"
logging.getLogger("asyncio").setLevel(logging.WARNING)
logging.getLogger("playwright").setLevel(logging.WARNING)


async def main():
    await init_db()
    logger.info("🚀 PIPELINE INICIADO")

    # Lista de fuentes para iterar
    fuentes = [
        #("MLS", job_mls_caracas),
        #("Rent-A-House", job_rentahouse_caracas),
        #("REMAX", job_remax_caracas),
        #("Bolsa Inmobiliaria", job_bolsainmobiliaria_caracas),
        ("Quarto", job_quarto_caracas),
        #("Vecindary", job_vecindary_caracas),
        #("Tu Residencia", job_turesidencia_caracas),
        #("Mercado Libre", job_mercadolibre_caracas),
    ]

    for nombre, job in fuentes:
        try:
            logger.info(f"--- Procesando fuente: {nombre} ---")
            await job()
        except Exception as e:
            logger.error(f"❌ Error crítico en {nombre}: {e}. Saltando a la siguiente fuente...")

    logger.info("✅ PIPELINE FINALIZADO")

if __name__ == "__main__":
    # Inicia el bucle de eventos asíncrono
    asyncio.run(main())