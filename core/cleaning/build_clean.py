import sqlite3
import pandas as pd
import ast
import os
import logging
import numpy as np

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

    # 1. Extracción (AHORA CON inmueble_id y snapshot_id EXPLICITOS)
    conn_cruda = sqlite3.connect(db_cruda)
    query = """
    SELECT 
        i.id as inmueble_id, s.id as snapshot_id,
        s.titulo, i.source_name as portal, i.external_id, 
        s.precio, s.moneda, s.ubicacion, s.caracteristicas,
        s.raw_extra_data as amenidades, s.scraped_at as fecha_registro
    FROM inmuebles i JOIN inmuebles_snapshots s ON i.id = s.inmueble_id
    """
    df = pd.read_sql_query(query, conn_cruda)
    conn_cruda.close()

    registros_iniciales = len(df)

    # 2. Filtrado Lógico y Estadístico (Método de Tukey)
    logger.info("Aplicando filtros de operación (Residencial y Alquiler)...")
    df['titulo_lower'] = df['titulo'].astype(str).str.lower()
    df['es_nulo'] = df['titulo'].isna() | (df['titulo'] == '') | (df['titulo_lower'] == 'nan') | (
                df['titulo_lower'] == 'none')

    # Eliminar inmuebles comerciales primero
    mask_comercial = df['titulo_lower'].str.contains('local|galpón|galpon|comercio|oficina|terreno|consultorio',
                                                     na=False)
    df_residencial = df[~mask_comercial].copy()

    mask_alquiler_res = df_residencial['titulo_lower'].str.contains('alquiler|arriendo', na=False)
    mask_nulo_res = df_residencial['es_nulo']

    alquileres_confirmados = df_residencial[mask_alquiler_res]['precio'].dropna()
    Q1 = alquileres_confirmados.quantile(0.25)
    Q3 = alquileres_confirmados.quantile(0.75)
    limite_superior = Q3 + (3 * (Q3 - Q1))

    df_residencial['es_alquiler_inferido'] = np.where(
        mask_nulo_res & (df_residencial['precio'] <= limite_superior), True, False
    )

    df = df_residencial[mask_alquiler_res | df_residencial['es_alquiler_inferido']].copy()

    # 3. Transformación: Aplanado JSON
    logger.info("Aplanando variables JSON...")
    df_ubi = df['ubicacion'].apply(parse_json_column).apply(pd.Series)

    # NUEVO: Asegurar que extraemos coordenadas limpias
    if 'latitud' in df_ubi.columns and 'longitud' in df_ubi.columns:
        df['latitud'] = pd.to_numeric(df_ubi['latitud'], errors='coerce')
        df['longitud'] = pd.to_numeric(df_ubi['longitud'], errors='coerce')
    df_carac = df['caracteristicas'].apply(parse_json_column).apply(pd.Series)
    df = pd.concat([df, df_ubi, df_carac], axis=1).drop(columns=['ubicacion', 'caracteristicas'])

    # 4. Transformación: Ingeniería de Amenidades
    df['lista_amenidades'] = df['amenidades'].apply(extraer_lista_amenidades)
    top_amenidades = {
        'tiene_lavanderia': 'zona de lavandería', 'tiene_armarios': 'armarios empotrados',
        'tiene_parque_infantil': 'parque infantil', 'tiene_planta_electrica': 'planta electrica',
        'tiene_pozo': 'pozo', 'tiene_calentador': 'calentador', 'tiene_microondas': 'horno microndas',
        'tiene_estudio': 'biblioteca/estudio', 'tiene_secadora': 'secadora',
        'tiene_cable': 'cable', 'tiene_balcon': 'balcon terraza',
        'tiene_piscina': 'piscina', 'tiene_vigilancia': 'vigilancia'
    }
    for col_name, keyword in top_amenidades.items():
        df[col_name] = df['lista_amenidades'].apply(lambda lista: 1 if any(keyword in item for item in lista) else 0)

    # Limpieza final conservando título y los IDs
    columnas_basura = ['amenidades', 'lista_amenidades', 'titulo_lower', 'es_nulo', 'es_alquiler_inferido']
    df = df.drop(columns=[col for col in columnas_basura if col in df.columns])

    # 5. Transformación: Casteo Numérico
    for col in ['m2_totales', 'habitaciones', 'banos']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    if 'municipio' in df.columns:
        df['municipio'] = df['municipio'].astype(str).str.strip().str.title()
        df.loc[df['municipio'].isin(['Nan', 'None']), 'municipio'] = None

    # 6. Carga en Capa Oro
    conn_limpia = sqlite3.connect(db_limpia)
    # index=False omite el índice de Pandas, pero las columnas inmueble_id y snapshot_id se guardan perfectamente
    df.to_sql('inmuebles_limpios', conn_limpia, if_exists='replace', index=False)
    conn_limpia.close()
    logger.info(
        f"✅ Pipeline Finalizado. Capa Oro lista con {len(df)} registros. IDs relacionales y título conservados.")


if __name__ == "__main__":
    ejecutar_pipeline_limpieza()