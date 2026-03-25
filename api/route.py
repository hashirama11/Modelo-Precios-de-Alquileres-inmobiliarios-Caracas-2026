from fastapi import APIRouter, HTTPException, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from typing import List

from db.database import AsyncSessionLocal
from db.models import Inmueble, InmuebleSnapshot

# Creamos el "mini" router
router = APIRouter()

# --- Dependencia de Base de Datos ---
async def get_db():
    async with AsyncSessionLocal() as session:
        yield session

# --- Endpoints ---
@router.get("/inmuebles/activos", response_model=List[dict])
async def get_inmuebles_activos(
        db: AsyncSession = Depends(get_db),
        limit: int = 50,
        offset: int = 0
):
    """
    Retorna la lista de inmuebles con el estado de su último snapshot capturado.
    Ideal para alimentar un frontend o un análisis inicial de mercado.
    """
    query = select(Inmueble).limit(limit).offset(offset)
    result = await db.execute(query)
    inmuebles = result.scalars().all()

    respuesta = []
    for inmueble in inmuebles:
        # Buscamos el snapshot más reciente para este inmueble
        snap_query = select(InmuebleSnapshot).where(
            InmuebleSnapshot.inmueble_id == inmueble.id
        ).order_by(InmuebleSnapshot.scraped_at.desc()).limit(1)

        snap_result = await db.execute(snap_query)
        ultimo_snapshot = snap_result.scalars().first()

        if ultimo_snapshot:
            respuesta.append({
                "id_interno": inmueble.id,
                "url": inmueble.url,
                "fecha_actualizacion": ultimo_snapshot.scraped_at,
                "precio": ultimo_snapshot.precio,
                "titulo": ultimo_snapshot.titulo,
                "ubicacion": ultimo_snapshot.ubicacion,
                "caracteristicas": ultimo_snapshot.caracteristicas
            })

    return respuesta

@router.get("/inmuebles/{inmueble_id}/historial")
async def get_historial_inmueble(inmueble_id: int, db: AsyncSession = Depends(get_db)):
    """
    Retorna todos los cambios de estado (snapshots) que ha tenido un inmueble específico.
    """
    query = select(InmuebleSnapshot).where(
        InmuebleSnapshot.inmueble_id == inmueble_id
    ).order_by(InmuebleSnapshot.scraped_at.asc())

    result = await db.execute(query)
    snapshots = result.scalars().all()

    if not snapshots:
        raise HTTPException(status_code=404, detail="Inmueble no encontrado o sin historial.")

    return [{
        "fecha": s.scraped_at,
        "precio": s.precio,
        "moneda": s.moneda
    } for s in snapshots]