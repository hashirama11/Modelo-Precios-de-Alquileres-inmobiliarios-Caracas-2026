import streamlit as st
import pandas as pd
import joblib
import plotly.express as px
import plotly.graph_objects as go
import os
import numpy as np

# Configuración de la página
st.set_config = st.set_page_config(layout="wide", page_title="Caracas Real Estate AI")

# --- ESTILO CSS ---
st.markdown("""
<style>
    .reportview-container { background: #fafafa; }
    .main .block-container { padding-top: 2rem; }
    h1, h2, h3 { color: #2c3e50; font-family: 'Helvetica Neue', sans-serif; font-weight: 300; letter-spacing: -1px; }
    .stMetric { background-color: white; padding: 20px; border-radius: 10px; box-shadow: 0 4px 6px rgba(0,0,0,0.02); border: 1px solid #f0f0f0; }
    .stMetric div[data-testid="stMetricValue"] { color: #3498db; font-size: 2.5rem; }
    .stButton>button { background-color: #3498db; color: white; border-radius: 20px; border: none; padding: 10px 25px; font-weight: bold; transition: all 0.3s; }
    .stButton>button:hover { background-color: #2980b9; transform: translateY(-2px); }
</style>
""", unsafe_allow_html=True)


# --- CARGA DE DATOS ---
@st.cache_resource
def load_assets():
    ruta_base = os.path.dirname(os.path.abspath(__file__))
    modelo = joblib.load(os.path.join(ruta_base, 'core/ml/modelo_alquiler.joblib'))
    df = pd.read_pickle(os.path.join(ruta_base, 'core/ml/datos_visualizacion.pkl'))
    # Limpieza preventiva de NaNs en columnas críticas para evitar errores de casteo
    for col in ['m2_totales', 'habitaciones', 'precio']:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    return modelo, df


try:
    modelo, df = load_assets()
    municipios_validos = sorted(df['municipio'].dropna().unique().tolist())
    municipios_validos.append("Otra Zona (Estimación General)")
except Exception as e:
    st.error(f"Error cargando assets: {e}")
    st.stop()

st.title("Caracas Rental Price Intelligence")
st.markdown("---")

col_inputs, col_fit = st.columns([1, 2], gap="large")

# --- PANEL SUPERIOR ---
with col_inputs:
    st.subheader("Parámetros del Inmueble")
    municipio_input = st.selectbox("Selecciona Municipio", municipios_validos, index=0)

    if municipio_input == "Otra Zona (Estimación General)":
        df_local_stats = df.dropna(subset=['m2_totales', 'habitaciones'])
        st.info("💡 **Modo de Estimación General:** Calculando basándonos en el promedio global.")
    else:
        df_local_stats = df[df['municipio'] == municipio_input].dropna(subset=['m2_totales', 'habitaciones'])
        cantidad_datos = len(df_local_stats)
        if cantidad_datos < 10:
            st.warning(f"⚠️ **Baja Confianza:** Solo hay {cantidad_datos} registros aquí.")
        else:
            st.success(f"📊 **Alta Confianza:** {cantidad_datos} inmuebles registrados.")


    # FIX: Manejo robusto de NaNs antes de convertir a int
    def get_safe_val(series, func, default):
        val = func(series)
        return int(val) if pd.notnull(val) else default


    min_m2 = get_safe_val(df_local_stats['m2_totales'], np.min, 10)
    max_m2 = get_safe_val(df_local_stats['m2_totales'], np.max, 500)
    min_hab = get_safe_val(df_local_stats['habitaciones'], np.min, 1)
    max_hab = get_safe_val(df_local_stats['habitaciones'], np.max, 5)

    m2_input = st.number_input(f"Metros Cuadrados ({min_m2} - {max_m2} m²)",
                               min_value=min_m2, max_value=max_m2, value=int(min_m2 + (max_m2 - min_m2) / 3), step=5)

    hab_input = st.slider(f"Número de Habitaciones", min_value=min_hab, max_value=max_hab, value=min_hab)

    btn_predict = st.button("Predecir Precio Inteligente")

    if btn_predict:
        municipio_modelo = "Desconocido" if municipio_input == "Otra Zona (Estimación General)" else municipio_input
        input_df = pd.DataFrame(
            {'municipio': [municipio_modelo], 'm2_totales': [m2_input], 'habitaciones': [hab_input]})
        prediccion = max(0, modelo.predict(input_df)[0])

        if prediccion <= 0:
            st.error("⚠️ Combinación improbable.")
            st.metric(label="Precio Estimado", value="N/A")
        else:
            st.metric(label="Precio de Alquiler Estimado", value=f"${prediccion:,.0f}")

# --- PANEL DERECHO ---
# --- PANEL DERECHO: GRÁFICA CON RECORTE DE OUTLIERS ---
with col_fit:
    st.subheader("Ajuste Dinámico del Modelo")

    if municipio_input == "Otra Zona (Estimación General)":
        df_local = df.dropna(subset=['m2_totales', 'precio'])
        titulo_grafico = "Distribución Global de Datos en Caracas"
    else:
        df_local = df[df['municipio'] == municipio_input].dropna(subset=['m2_totales', 'precio'])
        titulo_grafico = f"Distribución de Datos en {municipio_input}"

    if not df_local.empty:
        # DETECCIÓN DE OUTLIERS PARA ZOOM:
        # Calculamos el percentil 95 (ignoramos el 5% de los precios más absurdamente altos)
        limite_superior_visual = df_local['precio'].quantile(0.95)
        # Solo para la visualización, filtramos lo que esté muy por encima del mercado normal
        df_visual = df_local[df_local['precio'] <= limite_superior_visual * 1.5]

        # Recalculamos márgenes sobre los datos filtrados
        margin_x = (df_visual['m2_totales'].max() - df_visual['m2_totales'].min()) * 0.1 if len(df_visual) > 1 else 10
        margin_y = (df_visual['precio'].max() - df_visual['precio'].min()) * 0.1 if len(df_visual) > 1 else 100

        fig_fit = px.scatter(df_visual,
                             x="m2_totales",
                             y="precio",
                             color="habitaciones",
                             title=titulo_grafico,
                             labels={"m2_totales": "Metros Cuadrados", "precio": "Precio ($)", "habitaciones": "Habs"},
                             color_continuous_scale=px.colors.sequential.Blues,
                             opacity=0.8)

        fig_fit.update_traces(marker=dict(size=12, line=dict(width=1, color='DarkSlateGrey')))

        # FORZAMOS EL ZOOM:
        fig_fit.update_xaxes(range=[df_visual['m2_totales'].min() - margin_x, df_visual['m2_totales'].max() + margin_x])
        fig_fit.update_yaxes(range=[df_visual['precio'].min() - margin_y, df_visual['precio'].max() + margin_y])

        # Línea de tendencia basada en el rango visual
        x_range = np.linspace(df_visual['m2_totales'].min(), df_visual['m2_totales'].max(), 100)
        municipio_linea = "Desconocido" if municipio_input == "Otra Zona (Estimación General)" else municipio_input
        line_df = pd.DataFrame(
            {'municipio': [municipio_linea] * 100, 'm2_totales': x_range, 'habitaciones': [hab_input] * 100})
        y_range = modelo.predict(line_df)
        fig_fit.add_trace(go.Scatter(x=x_range, y=y_range, name='Tendencia', line=dict(color='#e74c3c', width=4)))

        fig_fit.update_layout(plot_bgcolor='rgba(242,242,242,0.5)', paper_bgcolor='white')
        st.plotly_chart(fig_fit, width='stretch')
    else:
        st.info("No hay datos suficientes para graficar esta zona.")
# --- PANEL INFERIOR (MAPA) ---
st.subheader("Mapa de Calor de Precios por Zona")
df_mapa = df.groupby('municipio').agg(
    {'precio': 'median', 'latitud': 'mean', 'longitud': 'mean', 'inmueble_id': 'count'}).reset_index()
df_mapa = df_mapa.dropna(subset=['latitud', 'longitud'])

# FIX: Cambiado density_mapbox por density_map (Estándar 2026)
fig_mapa = px.density_map(df_mapa, lat='latitud', lon='longitud', z='precio', radius=30,
                          hover_name='municipio', center=dict(lat=10.4806, lon=-66.9036), zoom=11,
                          map_style="carto-positron", color_continuous_scale=px.colors.sequential.YlOrRd)

fig_mapa.update_layout(margin={"r": 0, "t": 40, "l": 0, "b": 0}, paper_bgcolor='white')
st.plotly_chart(fig_mapa, width='stretch')