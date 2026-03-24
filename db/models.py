"""
Aquí implementamos el patrón de Historial (Parent-Child).

Inmueble (Padre): Guarda la identidad única del anuncio.

InmuebleSnapshot (Hijo): Guarda el estado de ese anuncio cada vez que el scraper lo visita.
"""

## Dependencias
from sqlalchemy import Column, Integer, String, Float, Boolean, ForeignKey, DateTime, JSON, UniqueConstraint
from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy.sql import func
import datetime

Base = declarative_base()


class Inmueble(Base):
    __tablename__ = 'inmuebles'

    id = Column(Integer, primary_key=True, autoincrement=True)
    source_name = Column(String, index=True, nullable=False)  # ej. "mercadolibre"
    external_id = Column(String, index=True, nullable=False)  # ej. "MLV12345"
    url = Column(String, nullable=False)

    # Metadatos de control
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    is_active = Column(Boolean, default=True)  # Para marcar si la publicación fue pausada/eliminada

    # Relación uno-a-muchos con los snapshots (Historial)
    snapshots = relationship("InmuebleSnapshot", back_populates="inmueble", cascade="all, delete-orphan")

    # Un inmueble es único por su plataforma y su ID externo
    __table_args__ = (UniqueConstraint('source_name', 'external_id', name='_source_external_uc'),)


class InmuebleSnapshot(Base):
    __tablename__ = 'inmuebles_snapshots'

    id = Column(Integer, primary_key=True, autoincrement=True)
    inmueble_id = Column(Integer, ForeignKey('inmuebles.id'), nullable=False)

    # Fecha exacta en la que el scraper capturó este dato
    scraped_at = Column(DateTime(timezone=True), default=lambda: datetime.datetime.now(datetime.timezone.utc))

    # Datos que pueden cambiar y queremos trackear
    precio = Column(Float, nullable=True)
    moneda = Column(String, default="USD")
    titulo = Column(String, nullable=True)
    descripcion = Column(String, nullable=True)

    # Para no crear 50 columnas en SQLite, agrupamos los datos estructurados en JSON.
    # SQLite soporta JSON nativamente en versiones recientes, y SQLAlchemy lo maneja perfecto.
    ubicacion = Column(JSON, default=dict)
    caracteristicas = Column(JSON, default=dict)
    condiciones = Column(JSON, default=dict)
    raw_extra_data = Column(JSON, default=dict)

    # Relación inversa
    inmueble = relationship("Inmueble", back_populates="snapshots")