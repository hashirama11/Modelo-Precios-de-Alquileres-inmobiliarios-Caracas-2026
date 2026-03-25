# 🏙️ Observatorio Inmobiliario de Caracas (Data Pipeline & Dashboard)

![Python](https://img.shields.io/badge/Python-3.10+-blue.svg?logo=python&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-005571?style=flat&logo=fastapi)
![Streamlit](https://img.shields.io/badge/Streamlit-FF4B4B?style=flat&logo=streamlit&logoColor=white)
![SQLite](https://img.shields.io/badge/SQLite-07405E?style=flat&logo=sqlite&logoColor=white)
![Playwright](https://img.shields.io/badge/Playwright-2EAD33?style=flat&logo=playwright&logoColor=white)
![Pandas](https://img.shields.io/badge/Pandas-150458?style=flat&logo=pandas&logoColor=white)

> **Una solución integral de Ingeniería de Datos y Machine Learning diseñada para monitorear, extraer y analizar en tiempo real el comportamiento del mercado de alquileres inmobiliarios en Caracas, Venezuela.**

El sistema utiliza el **patrón de diseño Strategy** para orquestar múltiples scrapers, procesando los datos a través de una arquitectura de datos en medallón (Capa Bronce a Capa Oro) para finalmente servir predicciones e insights mediante una API RESTful y un Dashboard interactivo.

---

## 🏗️ Arquitectura y Estructura del Monorepo

El proyecto está diseñado bajo una arquitectura orientada a microservicios. Está dividido en **4 grandes responsabilidades** o "interruptores" independientes que garantizan la escalabilidad y el mantenimiento:

```text
AlquilerCaracas/
│
├── db/                     # Capa de persistencia (Modelos SQLAlchemy)
├── scraper/                # Maquinaria de extracción (Playwright + Strategy)
├── worker.py               # 🔘 INTERRUPTOR 1: Inicia el Scraping (Capa Bronce)
│
├── core/                   # 🧠 EL CEREBRO: Transformación y Modelado
│   ├── cleaning/           # Pipeline ETL (Aplanado JSON, Feature Engineering)
│   └── ml/                 # Entrenamiento y guardado de modelos predictivos
├── pipeline.py             # 🔘 INTERRUPTOR 2: Transforma inmuebles.db -> inmuebles_clean.db
│
├── api/                    # 🔌 LA TOMA DE CORRIENTE: Endpoints modulares
│   └── route.py            # Definición de rutas (APIRouter)
├── main.py                 # 🔘 INTERRUPTOR 3: Levanta FastAPI (Servicio B2B)
│
├── dashboard/              # 🎨 EL ESCAPARATE: Componentes visuales
├── app.py                  # 🔘 INTERRUPTOR 4: Levanta Streamlit (Dashboard interactivo)
│
├── inmuebles.db            # 🥉 Capa Bronce: Datos Crudos e Históricos
└── inmuebles_clean.db      # 🥇 Capa Oro: Datos Limpios, listos para ML/BI

## 🛠️ Componentes y Stack Tecnológico

* **🕸️ Scraping Automatizado (`Playwright`):** Soporta ejecución asíncrona y evasión de bloqueos, capturando datos dinámicos de múltiples portales inmobiliarios de forma resiliente.
* **🧹 Data Engineering (`Pandas` / `NumPy`):** Pipeline automático que desempaqueta estructuras JSON complejas, imputa nulos y aplica *Feature Engineering* para crear variables booleanas de alto valor (ej. Pozo de Agua, Planta Eléctrica).
* **🔌 API Backend (`FastAPI`):** Altísimo rendimiento, documentación automática (Swagger) y validación estricta de datos para servir la información histórica.
* **🗄️ Base de Datos (`SQLAlchemy` / `SQLite`):** Almacenamiento relacional asíncrono diseñado específicamente para rastrear el historial de precios y la inflación.
* **📊 Visualización (`Streamlit` / `Plotly`):** Dashboard interactivo en tiempo real para análisis exploratorio (Top municipios, impacto de amenidades en el precio, KPIs).
* **🤖 Machine Learning (`Scikit-Learn`):** *(En desarrollo)* Modelos de regresión para estimar precios justos de mercado.

---

## 🗂️ Estructura de Datos (Modelo Histórico)

Para soportar la realidad de negociación y las fluctuaciones de precios en Venezuela, la base de datos utiliza un modelo relacional Padre-Hijo, separando la identidad del inmueble de su precio temporal:



| Nivel | Tabla | Descripción de Campos Clave |
| :--- | :--- | :--- |
| **Padre** | `inmuebles` | Identidad estática: `id`, `source_name` (portal), `external_id`, `url`, `created_at`. |
| **Hijo** | `inmuebles_snapshots` | Fotografías temporales (solo se registran si hay cambios): `precio`, `moneda`, `ubicacion` (JSON), `caracteristicas` (JSON), `raw_extra_data` (JSON con amenidades extra), `scraped_at`. |

---

## 🧠 Aproximación del Modelo de Machine Learning

El modelo en desarrollo busca entender el peso específico de cada característica en el mercado caraqueño. El algoritmo aprenderá que ciertas zonas (ej. Chacao) tienen un coeficiente de precio base distinto, mientras que variables físicas tienen un impacto directo en el valor final.

La función objetivo base a estimar es:

$$Precio_{predicho} = \beta_0 + \beta_1(m^2) + \beta_2(habitaciones) + \beta_3(zona\_score)$$

Variables adicionales derivadas del Feature Engineering (como `tiene_pozo`, `tiene_planta_electrica`, `tiene_vigilancia`) se incorporan al modelo como variables dicotómicas (**1** o **0**) para afinar la precisión de la predicción y medir su valor real en el mercado.

---

## 🚀 Cómo Empezar (Quick Start)

### 1. Instalación
Clona el repositorio e instala las dependencias necesarias:

```bash
git clone [https://github.com/tu-usuario/AlquilerCaracas.git](https://github.com/tu-usuario/AlquilerCaracas.git)
cd AlquilerCaracas
pip install -r requirements.txt


# Paso 1: Recolectar datos crudos (Scraping)
python worker.py

# Paso 2: Limpiar y estandarizar datos (Capa Bronce -> Oro)
python core/cleaning/build_clean_db.py

# Paso 3: Ver el dashboard interactivo
streamlit run app.py

# Paso 4 (Opcional): Iniciar la API REST
uvicorn main:app --reload