# db/database.py
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker

DATABASE_URL = "sqlite+aiosqlite:///./inmuebles.db"

# Motor de la base de datos
engine = create_async_engine(DATABASE_URL, echo=False)

# Fábrica de sesiones (lo que estabas buscando)
AsyncSessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)