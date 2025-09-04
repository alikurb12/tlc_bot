# main.py
import logging
import uvicorn
from fastapi import FastAPI
from contextlib import asynccontextmanager
from database import init_db, close_db
from webhook import router

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('exchange_router.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

app = FastAPI()

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Запуск универсального обработчика сигналов...")
    init_db()
    try:
        yield
    finally:
        close_db()
        logger.info("Обработчик остановлен")

app = FastAPI(lifespan=lifespan)
app.include_router(router)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=5000)