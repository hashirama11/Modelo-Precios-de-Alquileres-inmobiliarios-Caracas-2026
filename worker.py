import asyncio
import logging
import sys
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
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

    # URL base (idealmente esto iría en variables de entorno o un config.py)
    start_url = "https://mlscaracas.com/s/venezuela/miranda-dtto-capital/caracas//?id_country=95&id_region=3556&id_city=859026"
    scraper = MLSCaracasScraper()
    try:
        # Limitamos a 2 páginas por ahora para no saturar durante el desarrollo
        resultados = await scraper.run_pipeline(start_url)
        logger.info(f"Scraping completado. {len(resultados)} inmuebles extraídos.")

        if resultados:
            await procesar_y_guardar(resultados)

    except Exception as e:
        logger.error(f"Error durante la ejecución del job: {e}")


async def job_rentahouse_caracas():
    logger.info("Iniciando Job: RentAHouseScraper")
    # Tu URL exacta con filtros
    start_url = "https://rentahouse.com.ve/propiedades_ubicadas_en_caracas.html?priceMin=0&priceMax=0&m2Min=0&m2Max=0&orderBy=entryTimestamp%20desc&country=venezuela&state=distrito-metropolitano&countrySlug=venezuela"

    scraper = RentAHouseScraper()
    try:
        # Prueba de 2 páginas (aprox 24-30 registros)
        resultados = await scraper.run_pipeline(start_url)
        logger.info(f"Scraping RAH completado. {len(resultados)} inmuebles extraídos.")

        if resultados:
            await procesar_y_guardar(resultados)

    except Exception as e:
        logger.error(f"Error durante la ejecución del job RAH: {e}")


async def job_remax_caracas():
    logger.info("Iniciando Job: RemaxScraper")

    # Las 5 URLs de las distintas zonas de Caracas
    urls_remax = [
        "https://www.remax.com.ve/inmuebles/apartamento/alquiler?ubi=Caracas%2C+Baruta%2C+Miranda%2C+VEN",
        "https://www.remax.com.ve/inmuebles/apartamento/alquiler?ubi=Caracas%2C+Chacao%2C+Miranda%2C+VEN",
        "https://www.remax.com.ve/inmuebles/apartamento/alquiler?ubi=Caracas%2C+Libertador%2C+Distrito+Capital%2C+VEN",
        "https://www.remax.com.ve/inmuebles/apartamento/alquiler?ubi=Caracas%2C+Sucre%2C+Miranda%2C+VEN",
        "https://www.remax.com.ve/inmuebles/apartamento/alquiler?ubi=Caracas%2C+El+Hatillo%2C+Miranda%2C+VEN"
    ]

    scraper = RemaxScraper()
    todos_los_resultados = []

    try:
        for url in urls_remax:
            try:
                # Intentamos procesar la zona
                resultados_zona = await scraper.run_pipeline(url)
                todos_los_resultados.extend(resultados_zona)
                # Pequeña pausa de 2 segundos para no saturar al servidor
                await asyncio.sleep(2)
            except Exception as e:
                logger.error(f"⚠️ Error en zona {url}: {e}. Continuando con la siguiente...")
                continue  # Si una zona falla, no detiene el Job completo

        logger.info(f"Scraping REMAX completado. {len(todos_los_resultados)} inmuebles.")

        if todos_los_resultados:
            await procesar_y_guardar(todos_los_resultados)

    except Exception as e:
        logger.error(f"Error durante la ejecución del job REMAX: {e}")


async def job_bolsainmobiliaria_caracas():
    logger.info("Iniciando Job: BolsaInmobiliariaScraper")

    start_url = "https://bolsainmobiliariacaracas.com/s/apartamento/alquiler/venezuela/miranda-dtto-capital/caracas?id_country=95&id_region=3556&id_city=859026&id_property_type=2&business_type%5B%5D=for_rent"

    scraper = BolsaInmobiliariaScraper()
    try:
        # Prueba de 2 páginas
        resultados = await scraper.run_pipeline(start_url)
        logger.info(f"Scraping Bolsa Inmobiliaria completado. {len(resultados)} inmuebles extraídos.")

        if resultados:
            await procesar_y_guardar(resultados)

    except Exception as e:
        logger.error(f"Error durante la ejecución del job Bolsa Inmobiliaria: {e}")


async def job_quarto_caracas():
    logger.info("Iniciando Job: QuartoScraper")

    start_url = "https://quartoapp.com/propiedades/alquiler?type_id=1&category_id=&price_min=100&price_max=&city_id=1&municipality_id=1%2C2%2C3%2C4%2C5&urbanization_id=1%2C2%2C3%2C4%2C5%2C6%2C7%2C8%2C9%2C10%2C11%2C12%2C13%2C14%2C15%2C16%2C17%2C18%2C19%2C20%2C21%2C22%2C23%2C24%2C25%2C26%2C27%2C28%2C29%2C30%2C31%2C32%2C33%2C34%2C35%2C36%2C37%2C38%2C39%2C40%2C41%2C42%2C43%2C44%2C45%2C46%2C47%2C48%2C49%2C50%2C51%2C52%2C53%2C54%2C55%2C56%2C57%2C58%2C59%2C60%2C61%2C62%2C63%2C64%2C65%2C66%2C67%2C68%2C69%2C70%2C71%2C72%2C73%2C74%2C75%2C76%2C77%2C78%2C79%2C80%2C81%2C82%2C83%2C84%2C85%2C86%2C87%2C88%2C89%2C90%2C91%2C92%2C93%2C94%2C95%2C96%2C97%2C98%2C99%2C100%2C101%2C102%2C103%2C104%2C105%2C106%2C107%2C108%2C109%2C110%2C111"

    scraper = QuartoScraper()
    try:
        resultados = await scraper.run_pipeline(start_url)
        logger.info(f"Scraping Quarto completado. {len(resultados)} inmuebles extraídos.")

        if resultados:
            await procesar_y_guardar(resultados)

    except Exception as e:
        logger.error(f"Error durante la ejecución del job Quarto: {e}")


async def job_vecindary_caracas():
    logger.info("Iniciando Job: VecindaryScraper")

    # URL de búsqueda base
    start_url = "https://vecindary.com/inmuebles/venezuela-distrito-capital-caracas-libertador/en-alquiler-temporario"

    scraper = VecindaryScraper()
    try:
        resultados = await scraper.run_pipeline(start_url)
        logger.info(f"Scraping Vecindary completado. {len(resultados)} inmuebles extraídos.")

        if resultados:
            await procesar_y_guardar(resultados)

    except Exception as e:
        logger.error(f"Error durante la ejecución del job Vecindary: {e}")


async def job_turesidencia_caracas():
    logger.info("Iniciando Job: TuresidenciaScraper")

    start_url = "https://www.turesidencia.net/habitaciones-alquiler/categories/11-habitaciones-en-alquiler-caracas/ads"

    scraper = TuresidenciaScraper()
    try:
        # Limitado a 1 página para la prueba rápida
        resultados = await scraper.run_pipeline(start_url)
        logger.info(f"Scraping TuResidencia completado. {len(resultados)} inmuebles extraídos.")

        if resultados:
            await procesar_y_guardar(resultados)

    except Exception as e:
        logger.error(f"Error durante la ejecución del job TuResidencia: {e}")


async def job_mercadolibre_caracas():
    logger.info("Iniciando Job: MercadolibreScraper")

    start_url = "https://listado.mercadolibre.com.ve/inmuebles/distrito-capital/alquiler_NoIndex_True#applied_filter_id%3Dstate%26applied_filter_name%3DUbicaci%C3%B3n%26applied_filter_order%3D5%26applied_value_id%3DTUxWUERJU2wxMzkxMA%26applied_value_name%3DDistrito+Capital%26applied_value_order%3D8%26applied_value_results%3D30500%26is_custom%3Dfalse"

    scraper = MercadolibreScraper()
    try:
        # Prueba de 1 página para validar la extracción
        resultados = await scraper.run_pipeline(start_url)
        logger.info(f"Scraping Mercado Libre completado. {len(resultados)} inmuebles extraídos.")

        if resultados:
            await procesar_y_guardar(resultados)

    except Exception as e:
        logger.error(f"Error durante la ejecución del job Mercado Libre: {e}")


async def main():
    await init_db()

    # Configuramos el planificador semanal
    scheduler = AsyncIOScheduler()
    scheduler.add_job(job_mls_caracas, CronTrigger(day_of_week='sun', hour=2, minute=0))
    scheduler.add_job(job_rentahouse_caracas, CronTrigger(day_of_week='sun', hour=3, minute=0))
    scheduler.add_job(job_remax_caracas, CronTrigger(day_of_week='sun', hour=4, minute=0))
    scheduler.add_job(job_bolsainmobiliaria_caracas, CronTrigger(day_of_week='sun', hour=5, minute=0))
    scheduler.add_job(job_quarto_caracas, CronTrigger(day_of_week='sun', hour=6, minute=0))
    scheduler.add_job(job_vecindary_caracas, CronTrigger(day_of_week='sun', hour=7, minute=0))
    scheduler.add_job(job_turesidencia_caracas, CronTrigger(day_of_week='sun', hour=8, minute=0))
    scheduler.add_job(job_mercadolibre_caracas, CronTrigger(day_of_week='sun', hour=9, minute=0))

    scheduler.start()

    # =================================================================
    # EL INTERRUPTOR: Solo entra aquí si escribimos "--seed" en la terminal
    # =================================================================
    if "--seed" in sys.argv:
        logger.info("INICIANDO CARGA MASIVA INICIAL (--seed detectado)... Esto tomará horas.")

        #await job_mls_caracas()
        await job_rentahouse_caracas()
        await job_remax_caracas()
        await job_bolsainmobiliaria_caracas()
        await job_quarto_caracas()
        await job_vecindary_caracas()
        await job_turesidencia_caracas()
        #await job_mercadolibre_caracas()

        logger.info("¡CARGA MASIVA FINALIZADA EXITOSAMENTE! El worker entra en modo reposo.")
    else:
        logger.info("Worker iniciado en MODO PRODUCCIÓN (Normal). Ejecución programada para los domingos.")

    # Mantenemos el proceso vivo en background
    try:
        while True:
            await asyncio.sleep(3600)
    except (KeyboardInterrupt, SystemExit):
        logger.info("Apagando worker...")



if __name__ == "__main__":
    # Inicia el bucle de eventos asíncrono
    asyncio.run(main())