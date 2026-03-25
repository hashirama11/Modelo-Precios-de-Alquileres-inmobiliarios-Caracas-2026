import sqlite3
import pandas as pd
import ast
import os
import logging

# Configuración del logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def parse_json_column(texto):
    if pd.isna(texto) or texto == "": return {}
    try:
        return ast.literal_eval(str(texto))
    except:
        return {}


def extraer_lista_amenidades(texto):
    """Busca y extrae la lista de amenidades y las convierte a minúsculas para buscar fácil."""
    try:
        data = ast.literal_eval(str(texto))
        if isinstance(data, dict):
            for key, value in data.items():
                if isinstance(value, list):
                    return [str(v).strip().lower() for v in value]
        elif isinstance(data, list):
            return [str(v).strip().lower() for v in data]
    except:
        pass
    return []


def ejecutar_pipeline_limpieza():
    ruta_base = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    db_cruda = os.path.join(ruta_base, 'inmuebles.db')
    db_limpia = os.path.join(ruta_base, 'inmuebles_clean.db')

    logger.info("Iniciando pipeline de limpieza de datos (Capa Bronce -> Capa Oro)...")

    if not os.path.exists(db_cruda):
        logger.error(f"No se encontró la base de datos origen en: {db_cruda}")
        return

    # 1. Extracción
    logger.info("1. Extrayendo datos crudos...")
    conn_cruda = sqlite3.connect(db_cruda)
    query = """
    SELECT 
        i.source_name as portal, i.external_id, s.id as snapshot_id,
        s.precio, s.moneda, s.ubicacion, s.caracteristicas,
        s.raw_extra_data as amenidades, s.scraped_at as fecha_registro
    FROM inmuebles i JOIN inmuebles_snapshots s ON i.id = s.inmueble_id
    """
    df = pd.read_sql_query(query, conn_cruda)
    conn_cruda.close()

    # 2. Transformación: Aplanado JSON
    logger.info("2. Aplanando estructuras JSON (Ubicación y Características)...")
    df_ubi = df['ubicacion'].apply(parse_json_column).apply(pd.Series)
    df_carac = df['caracteristicas'].apply(parse_json_column).apply(pd.Series)
    df = pd.concat([df, df_ubi, df_carac], axis=1).drop(columns=['ubicacion', 'caracteristicas'])

    # 3. Transformación: Ingeniería de Amenidades (Top Descubiertas)
    logger.info("3. Extrayendo variables de alto valor (Amenidades del Top)...")
    df['lista_amenidades'] = df['amenidades'].apply(extraer_lista_amenidades)

    # Aquí definimos tu Top de amenidades detectadas.
    # La clave es cómo se llamará la columna, el valor es qué palabra buscará.
    top_amenidades = {
        'tiene_lavanderia': 'zona de lavandería',
        'tiene_armarios': 'armarios empotrados',
        'tiene_parque_infantil': 'parque infantil',
        'tiene_planta_electrica': 'planta electrica',
        'tiene_pozo': 'pozo',
        'tiene_calentador': 'calentador',
        'tiene_microondas': 'horno microndas',
        'tiene_estudio': 'biblioteca/estudio',
        'tiene_secadora': 'secadora',
        'tiene_cable': 'cable',
        'tiene_balcon': 'balcon terraza',
        'tiene_piscina': 'piscina',  # Siempre buena tenerla
        'tiene_vigilancia': 'vigilancia'  # Siempre buena tenerla
    }

    # Creamos las columnas dinámicamente
    for col_name, keyword in top_amenidades.items():
        df[col_name] = df['lista_amenidades'].apply(
            lambda lista: 1 if any(keyword in item for item in lista) else 0
        )

    # Borramos la basura que ya no necesitamos
    df = df.drop(columns=['amenidades', 'lista_amenidades'])

    # 4. Transformación: Casteo y Limpieza
    logger.info("4. Estandarizando tipos de datos numéricos...")
    for col in ['m2_totales', 'habitaciones', 'banos']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    if 'municipio' in df.columns:
        df['municipio'] = df['municipio'].astype(str).str.strip().str.title()
        df.loc[df['municipio'].isin(['Nan', 'None']), 'municipio'] = None

    # 5. Carga
    logger.info(f"5. Guardando datos en Capa Oro ({db_limpia})...")
    conn_limpia = sqlite3.connect(db_limpia)
    # Al usar if_exists='replace', SOBRESCRIBE la tabla antigua con esta nueva estructura perfecta
    df.to_sql('inmuebles_limpios', conn_limpia, if_exists='replace', index=False)
    conn_limpia.close()

    logger.info(f"✅ Pipeline completado. {len(df)} registros limpios exportados con sus amenidades separadas.")


if __name__ == "__main__":
    ejecutar_pipeline_limpieza()