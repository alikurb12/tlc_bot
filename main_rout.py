# main_rout.py
import logging
import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
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

# Базовая защита от сканеров
BLOCKED_PATHS = [".git", ".env", "wp-admin", "phpmyadmin", "administrator", "backup", "sql"]


async def security_middleware(request: Request, call_next):
    """Middleware для базовой безопасности"""
    client_ip = request.client.host
    path = request.url.path

    # Блокируем опасные пути
    if any(blocked in path.lower() for blocked in BLOCKED_PATHS):
        logger.warning(f"Блокирован сканер от {client_ip}: {path}")
        return JSONResponse(
            status_code=404,
            content={"status": "error", "message": "Not Found"}
        )

    # Логируем только реальные запросы (не сканеры)
    if path not in ["/", "/health"] and not path.startswith("/."):
        logger.info(f"Запрос от {client_ip}: {request.method} {path}")

    response = await call_next(request)
    return response


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Управление жизненным циклом приложения"""
    logger.info("Запуск универсального обработчика сигналов...")
    init_db()
    logger.info("База данных инициализирована")
    try:
        yield
    finally:
        close_db()
        logger.info("Обработчик остановлен")


# Создаем app с lifespan
app = FastAPI(
    title="TLC Trading Bot API",
    description="API для обработки торговых сигналов",
    version="1.0.0",
    lifespan=lifespan
)

# Добавляем middleware к приложению
app.middleware("http")(security_middleware)


# Добавляем корневой endpoint чтобы боты не получали 404
@app.get("/")
async def root():
    return {
        "status": "success",
        "message": "TLC Trading Bot API is running",
        "endpoints": {
            "webhook": "POST /webhook",
            "health": "GET /health",
            "queue_status": "GET /queue/status"
        }
    }


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    from datetime import datetime
    return {
        "status": "healthy",
        "service": "TLC Trading Bot",
        "timestamp": datetime.now().isoformat()
    }


# Подключаем роутер webhook
app.include_router(router)

if __name__ == "__main__":
    logger.info("Starting server on http://0.0.0.0:5000")
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=5000,
        log_config=None
    )