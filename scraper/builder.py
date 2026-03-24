## Dependencias
from typing import Any, Dict


class PropertySnapshotBuilder:
    """
    Implementa el Patrón Builder para construir un PropertySnapshot Pydantic
    paso a paso durante el web scraping.
    """

    def __init__(self, source_name: str, external_id: str, url: str):
        # Inicializamos los diccionarios internos que luego pasaremos al modelo Pydantic
        self._data = {
            "source_name": source_name,
            "external_id": external_id,
            "url": url,
            "ubicacion": {},
            "caracteristicas": {},
            "condiciones": {},
            "raw_extra_data": {}
        }

    def set_general_info(self, titulo: str = None, descripcion: str = None, tipo_inmueble: str = None):
        if titulo: self._data["titulo"] = titulo
        if descripcion: self._data["descripcion"] = descripcion
        if tipo_inmueble: self._data["tipo_inmueble"] = tipo_inmueble
        return self

    def set_price(self, precio: float, moneda: str = "USD"):
        self._data["precio"] = precio
        self._data["moneda"] = moneda
        return self

    def set_location(self, municipio: str = None, parroquia: str = None, urbanismo: str = None):
        if municipio: self._data["ubicacion"]["municipio"] = municipio
        if parroquia: self._data["ubicacion"]["parroquia"] = parroquia
        if urbanismo: self._data["ubicacion"]["urbanismo"] = urbanismo
        return self

    def add_features(self, m2_totales: float = None, habitaciones: int = None, banos: float = None,
                     pozo_agua: bool = None, planta_electrica: bool = None):
        if m2_totales is not None: self._data["caracteristicas"]["m2_totales"] = m2_totales
        if habitaciones is not None: self._data["caracteristicas"]["habitaciones"] = habitaciones
        if banos is not None: self._data["caracteristicas"]["banos"] = banos
        if pozo_agua is not None: self._data["caracteristicas"]["pozo_agua"] = pozo_agua
        if planta_electrica is not None: self._data["caracteristicas"]["planta_electrica"] = planta_electrica
        return self

    def add_extra_data(self, key: str, value: Any):
        """Para capturar cualquier dato raro que no esté en nuestro esquema base"""
        self._data["raw_extra_data"][key] = value
        return self

    def build(self):  # -> PropertySnapshot (Añade el tipado según cómo llamaste a tu esquema Pydantic)
        """
        Instancia y valida el modelo Pydantic final con los datos recopilados.
        Si falta un dato, Pydantic usará los valores 'None' por defecto.
        """
        # Aquí instanciamos el modelo Pydantic.
        # NOTA: Asegúrate de importar tu clase PropertySnapshot real.
        # return PropertySnapshot(**self._data)

        return self._data  # Retorno temporal como diccionario para ilustrar