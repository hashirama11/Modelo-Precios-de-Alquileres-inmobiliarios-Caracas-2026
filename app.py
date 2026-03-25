import streamlit as st
import sqlite3
import pandas as pd
import os

# Configuración inicial de la página (Debe ser la primera instrucción de Streamlit)
st.set_page_config(
    page_title="Dashboard | Alquiler Caracas",
    page_icon="🏙️",
    layout="wide"
)

# Título principal
st.title("🏙️ Observatorio Inmobiliario de Caracas")
st.markdown("Bienvenido al panel de control. Aquí visualizaremos los datos de la **Capa Oro**.")

# Barra lateral (Sidebar) para futuros filtros
st.sidebar.header("Filtros de Búsqueda")
st.sidebar.info("Aquí agregaremos los filtros por Municipio, Precio y Habitaciones más adelante.")

# Prueba de conexión rápida para verificar que todo funciona
db_path = os.path.abspath('inmuebles.db')

if os.path.exists(db_path):
    st.success(f"✅ Base de datos conectada exitosamente ({os.path.getsize(db_path) / (1024*1024):.2f} MB).")
    st.info("Nota: Actualmente apuntando a la base de datos cruda. Próximamente apuntará a 'inmuebles_clean.db'.")
else:
    st.error("❌ No se encontró la base de datos local.")