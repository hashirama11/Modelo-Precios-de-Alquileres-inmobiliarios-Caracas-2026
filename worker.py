import asyncio
import logging
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from db.database import engine, AsyncSessionLocal
from sqlalchemy.future import select
from scraper.rentahouse import RentAHouseScraper

# Importamos nuestros módulos (Ajusta las rutas según tu estructura exacta)
from db.models import Base, Inmueble, InmuebleSnapshot
from scraper.mlscaracas import MLSCaracasScraper

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
        resultados = await scraper.run_pipeline(start_url, max_pages=2)
        logger.info(f"Scraping RAH completado. {len(resultados)} inmuebles extraídos.")

        if resultados:
            await procesar_y_guardar(resultados)

    except Exception as e:
        logger.error(f"Error durante la ejecución del job RAH: {e}")

async def main():
    # Aseguramos que la DB exista antes de arrancar
    await init_db()

    # Configuramos el planificador
    scheduler = AsyncIOScheduler()

    # Programamos la tarea para que se ejecute cada 6 horas
    #scheduler.add_job(job_mls_caracas, 'interval', hours=6, next_run_time=None)  # next_run_time=None evita que corra inmediatamente si no quieres
    scheduler.add_job(job_rentahouse_caracas, CronTrigger(hour=3, minute=0))

    scheduler.start()
    logger.info("Worker iniciado. Presiona Ctrl+C para salir.")

    # Como esta es la primera vez, vamos a forzar una ejecución manual de prueba
    logger.info("Ejecutando primera prueba en seco...")
    #await job_mls_caracas()
    await job_rentahouse_caracas()

    # Mantenemos el proceso vivo
    try:
        while True:
            await asyncio.sleep(3600)
    except (KeyboardInterrupt, SystemExit):
        logger.info("Apagando worker...")


if __name__ == "__main__":
    # Inicia el bucle de eventos asíncrono
    asyncio.run(main())