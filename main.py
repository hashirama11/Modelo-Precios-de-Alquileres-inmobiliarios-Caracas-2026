from fastapi import FastAPI
# Importamos el router que acabamos de crear
from api.route import router as api_router

app = FastAPI(
    title="API Alquiler Caracas",
    description="Endpoints para consultar datos scrapeados de inmuebles.",
    version="1.0.0"
)

# Dejamos la ruta raíz (root) aquí como un simple "Health Check" para saber si la API está viva
@app.get("/")
async def root():
    return {"mensaje": "Bienvenido a la API de Alquileres Caracas. Visita /docs para la documentación."}

# Enchufamos todas las rutas complejas a nuestra aplicación principal
app.include_router(api_router)