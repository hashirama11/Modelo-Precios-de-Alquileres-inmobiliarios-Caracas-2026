# Proyecto de Ciencia de Datos
## Precio Alquileres Caracas 2026

AlquilerCaracas/
│
├── db/                     # Tu capa de persistencia base (modelos SQLAlchemy y conexión)
├── scraper/                # La maquinaria de extracción (Playwright)
├── worker.py               # 🔘 INTERRUPTOR 1: Comando para iniciar el Scraping
│
├── core/                   # 🧠 EL CEREBRO: Donde ocurre la magia
│   ├── cleaning/           # Scripts para limpiar datos (Tratar JSON, nulos, outliers)
│   └── ml/                 # Scripts para entrenar y guardar modelos de Machine Learning
├── pipeline.py             # 🔘 INTERRUPTOR 2: Lee inmuebles.db, limpia, entrena y crea clean.db
│
├── api/                    # 🔌 LA TOMA DE CORRIENTE: endpoints separados
│   └── routes.py           # Aquí defines tus @app.get()
├── main.py                 # 🔘 INTERRUPTOR 3: Comando para encender FastAPI (Solo inicializa)
│
├── dashboard/              # 🎨 EL ESCAPARATE: Cosas exclusivas de Streamlit (imágenes, css)
│   └── views.py            # Componentes visuales separados si crece mucho
└── app.py                  # 🔘 INTERRUPTOR 4: Comando para encender Streamlit
│
├── inmuebles.db            # (Datos Crudos / Capa Bronce) -> Generado por worker.py
└── inmuebles_clean.db      # (Datos Limpios / Capa Oro) -> Generado por pipeline.py

El modelo aprenderá que, por ejemplo, Chacao tiene un coeficiente de precio base mayor que Libertador, pero que los metros cuadrados ($m^2$) tienen un impacto constante.
$$Precio_{predicho} = \beta_0 + \beta_1(m^2) + \beta_2(habitaciones) + \beta_3(zona\_score)$$

Proyecto que trabaja bajo el patron de diseño Strategy.

### Componentes de la aplicacion

- Api Backend *FastApi*
  -- Altísimo rendimiento, documentación automática (Swagger) y validación con Pydantic.
- Scraping *Playwright*
  -- Soporta ejecución asíncrona (ideal para FastAPI/Asyncio).
- Data Orchestration *APScheduler*
  -- Para ejecutar el "worker" de scraping en intervalos.
- Data Manipulation *Pandas / NumPy*
  -- El estándar de oro para limpiar los datos antes de guardarlos.
- Machine Learning *Scikit-Learn*
  -- Para el modelo de regresión que estimará los precios.
- ORM *SQLAlchemy*
  -- Funciona de maravilla con FastAPI para definir tus modelos de datos.

### Estructura de datos inicial

Table Sources: id, name (ej. Conlallave, Marketplace), base_url.

Table Locations: id, municipio, parroquia, urbanismo (Aquí es donde normalizarás los nombres para que el mapa funcione).

Table Listings:

id_externo (el ID que usa el sitio web original).

source_id (FK a Sources).

location_id (FK a Locations).

price, currency (importante en Venezuela manejar la moneda).

m2, rooms, bathrooms, features (JSON con extras: pozo, planta eléctrica, etc.).

captured_at (Timestamp).

### Estructura del Proyecto
/app
  /api          <-- Endpoints de FastAPI
  /core         <-- Lógica del modelo ML y configuración
  /db           <-- Modelos de SQLAlchemy y migraciones
  /scrapers     <-- Clase base y estrategias por sitio
  /models       <-- Archivos .pkl del modelo entrenado
main.py         <-- Punto de entrada
worker.py       <-- El script que corre el scraping (tu worker service)