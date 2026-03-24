# Diseño Orientado a Composicion Pydantic

# Dependencias
from pydantic import BaseModel, Field, HttpUrl
from datetime import datetime, timezone
from typing import List, Dict, Any


# 1. Sub-modelo: Ubicación (Clave para mapa interactivo)
class LocationInfo(BaseModel):
    municipio: str | None = Field(default=None, description="Ej: Chacao, Baruta, Libertador")
    parroquia: str | None = Field(default=None)
    urbanismo: str | None = Field(default=None, description="Ej: Altamira, El Rosal, La Candelaria")
    latitud: float | None = None
    longitud: float | None = None


# 2. Sub-modelo: Características Físicas
class PropertyFeatures(BaseModel):
    m2_totales: float | None = None
    m2_techados: float | None = None
    habitaciones: int | None = None
    banos: float | None = None  # Float porque existen "medios baños"
    puestos_estacionamiento: int | None = None
    # Extras muy relevantes en Caracas:
    pozo_agua: bool | None = None
    planta_electrica: bool | None = None
    amoblado: bool | None = None


# 3. Sub-modelo: Condiciones de Alquiler
class RentalConditions(BaseModel):
    meses_deposito: int | None = None
    meses_adelanto: int | None = None
    meses_comision: int | None = None
    contrato_comision: int | None = None
    acepta_mascotas: bool | None = None
    acepta_menores: bool | None = None


# 4. MODELO PRINCIPAL (Lo que el Scraper extrae en cada ejecución)
class PropertySnapshot(BaseModel):
    """
    Representa una 'fotografía' del estado de un inmueble en un momento dado.
    """
    # Identidad y Origen (Obligatorios para vincular el historial)
    source_name: str = Field(..., description="Nombre del sitio web, ej: 'mercadolibre', 'conlallave'")
    external_id: str = Field(..., description="El ID único que usa la página web original para esa publicación")
    url: HttpUrl = Field(...) ## Direccion web

    # Datos Temporales (Generado automáticamente al instanciar)
    scraped_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    # Datos Generales (Datos Importantes)
    titulo: str | None = None
    descripcion: str | None = None
    tipo_inmueble: str | None = Field(default=None, description="Apartamento, Casa, Anexo, Habitacion, Local")

    # Precio (IMPORTANTE)
    precio: float | None = None
    moneda: str | None = Field(default="USD")

    # Componentes anidados
    ubicacion: LocationInfo = Field(default_factory=LocationInfo)
    caracteristicas: PropertyFeatures = Field(default_factory=PropertyFeatures)
    condiciones: RentalConditions = Field(default_factory=RentalConditions)

    # Metadatos flexibles para atrapar datos raros sin romper el esquema
    raw_extra_data: Dict[str, Any] = Field(default_factory=dict)