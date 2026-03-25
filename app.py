import streamlit as st
import sqlite3
import pandas as pd
import plotly.express as px
import os

# 1. Configuración de la página
st.set_page_config(page_title="Dashboard | Alquiler Caracas", page_icon="🏙️", layout="wide")

st.title("🏙️ Observatorio Inmobiliario de Caracas")
st.markdown("Analizando el mercado a partir de nuestra **Capa Oro**.")


# 2. Función para cargar datos (Usamos @st.cache_data para que sea ultrarrápido)
@st.cache_data
def cargar_datos():
    db_path = os.path.abspath('inmuebles_clean.db')
    conn = sqlite3.connect(db_path)
    # Leemos la tabla completa limpia
    df = pd.read_sql_query("SELECT * FROM inmuebles_limpios", conn)
    conn.close()
    return df


try:
    df = cargar_datos()
    st.sidebar.success(f"✅ BD Conectada: {len(df):,} inmuebles listos.")
except Exception as e:
    st.error(f"❌ Error conectando a inmuebles_clean.db: {e}")
    st.info("Asegúrate de haber ejecutado el pipeline de limpieza (core/cleaning/build_clean_db.py) primero.")
    st.stop()

# 3. KPIs Principales
st.subheader("Resumen del Mercado")
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("Total Inmuebles", f"{len(df):,}")
with col2:
    # Calculamos el precio promedio (ignorando nulos)
    precio_promedio = df['precio'].mean()
    st.metric("Precio Promedio", f"${precio_promedio:,.0f}")
with col3:
    # Porcentaje de apartamentos con pozo
    pct_pozo = (df['tiene_pozo'].sum() / len(df)) * 100 if 'tiene_pozo' in df.columns else 0
    st.metric("Con Pozo de Agua", f"{pct_pozo:.1f}%")
with col4:
    # Porcentaje de apartamentos con planta
    pct_planta = (df['tiene_planta_electrica'].sum() / len(df)) * 100 if 'tiene_planta_electrica' in df.columns else 0
    st.metric("Con Planta Eléctrica", f"{pct_planta:.1f}%")

st.divider()

# 4. Gráficos Interactivos
col_graf_1, col_graf_2 = st.columns(2)

with col_graf_1:
    st.subheader("📍 Top 10 Zonas con más Oferta")
    # Agrupamos por municipio y contamos
    if 'municipio' in df.columns and not df['municipio'].isna().all():
        df_muni = df['municipio'].value_counts().reset_index().head(10)
        df_muni.columns = ['Municipio', 'Cantidad']
        fig_bar = px.bar(df_muni, x='Municipio', y='Cantidad',
                         color='Cantidad', color_continuous_scale='Viridis',
                         text_auto=True)
        st.plotly_chart(fig_bar, use_container_width=True)
    else:
        st.info("Datos de municipio no disponibles o nulos.")

with col_graf_2:
    st.subheader("💧 Impacto del Pozo de Agua en el Precio")
    if 'tiene_pozo' in df.columns:
        # Filtramos outliers extremos (ej. alquileres de más de 15k) para ver bien la caja
        df_filtrado = df[df['precio'] <= 15000].copy()
        df_filtrado['Tiene Pozo'] = df_filtrado['tiene_pozo'].map({1: 'Sí', 0: 'No'})

        # Gráfico de cajas (Boxplot) para ver la distribución de precios
        fig_box = px.box(df_filtrado, x='Tiene Pozo', y='precio', color='Tiene Pozo',
                         category_orders={"Tiene Pozo": ["Sí", "No"]})
        st.plotly_chart(fig_box, use_container_width=True)
    else:
        st.info("Columna 'tiene_pozo' no encontrada.")