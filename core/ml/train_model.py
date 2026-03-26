import sqlite3
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
import joblib
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Diccionario "Salvavidas" para el mapa de calor
# Si el scraper no trae coordenadas, centramos el punto en el municipio
COORDENADAS_CARACAS = {
    'Chacao': {'lat': 10.4936, 'lon': -66.8525},
    'Baruta': {'lat': 10.4356, 'lon': -66.8778},
    'Sucre': {'lat': 10.4950, 'lon': -66.8000},
    'Libertador': {'lat': 10.5000, 'lon': -66.9166},
    'El Hatillo': {'lat': 10.4250, 'lon': -66.8250}
}


def entrenar_y_guardar_modelo():
    ruta_base = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    db_path = os.path.join(ruta_base, 'inmuebles_clean.db')
    modelo_path = os.path.join(ruta_base, 'core/ml/modelo_alquiler.joblib')

    logger.info("Cargando datos de Capa Oro...")
    conn = sqlite3.connect(db_path)
    # Seleccionamos TODA la tabla (*) para evitar que SQLite colapse si falta una columna
    df = pd.read_sql_query("SELECT * FROM inmuebles_limpios", conn)
    conn.close()

    # 1. Aseguramos que existan las columnas de coordenadas
    if 'latitud' not in df.columns: df['latitud'] = None
    if 'longitud' not in df.columns: df['longitud'] = None

    # 2. Inyectamos las coordenadas por municipio si están vacías
    def fill_lat(row):
        if pd.isna(row['latitud']) and row['municipio'] in COORDENADAS_CARACAS:
            return COORDENADAS_CARACAS[row['municipio']]['lat']
        return row['latitud']

    def fill_lon(row):
        if pd.isna(row['longitud']) and row['municipio'] in COORDENADAS_CARACAS:
            return COORDENADAS_CARACAS[row['municipio']]['lon']
        return row['longitud']

    df['latitud'] = df.apply(fill_lat, axis=1)
    df['longitud'] = df.apply(fill_lon, axis=1)

    # Limpieza rápida para el modelo (solo usamos los que tienen precio y m2)
    df_ml = df.dropna(subset=['precio', 'municipio', 'm2_totales', 'habitaciones']).copy()

    if df_ml.empty:
        logger.error("❌ No hay datos suficientes (faltan m2 o precio) para entrenar el modelo.")
        return

    # Definir Features (X) y Target (y)
    X = df_ml[['municipio', 'm2_totales', 'habitaciones']]
    y = df_ml['precio']

    # Crear el Preprocesador: OneHotEncoder para que el modelo entienda los nombres de los municipios
    categorical_features = ['municipio']
    categorical_transformer = OneHotEncoder(handle_unknown='ignore')

    preprocessor = ColumnTransformer(
        transformers=[
            ('cat', categorical_transformer, categorical_features)
        ],
        remainder='passthrough'
    )

    # Crear el Pipeline final: Preprocesador + Modelo de Regresión
    clf = Pipeline(steps=[('preprocessor', preprocessor),
                          ('regressor', LinearRegression())])

    logger.info("Entrenando modelo de Regresión Lineal Múltiple...")
    clf.fit(X, y)

    # Guardar el modelo entrenado
    logger.info(f"Guardando modelo en {modelo_path}...")
    joblib.dump(clf, modelo_path)

    # Guardar el DF original para que Streamlit pueda usarlo en los gráficos
    df.to_pickle(os.path.join(ruta_base, 'core/ml/datos_visualizacion.pkl'))
    logger.info("✅ ¡Entrenamiento y exportación finalizados con éxito!")


if __name__ == "__main__":
    entrenar_y_guardar_modelo()