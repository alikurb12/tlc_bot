import logging
import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager
from database import init_db, close_db
from webhook import router

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('exchange_router.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

BLOCKED_PATHS = [
    # Статические файлы
    "favicon.ico", "robots.txt", ".well-known", "sitemap.xml",

    # Системные пути
    ".git", ".env", ".htaccess", ".htpasswd",

    # CMS и админки
    "wp-admin", "wp-login", "phpmyadmin", "administrator",
    "admin", "backend", "manager",

    # Базы данных и бэкапы
    "backup", "sql", "database", "dump",

    # Docker и registry
    "v2/_catalog", "api/v2/_catalog", "docker", "registry",

    # Другие уязвимости
    "cgi-bin", "shell", "cmd", "exec"
]

async def security_middleware(request: Request, call_next):
    try:
        client_ip = request.client.host if request.client else "unknown"
    except Exception:
        client_ip = "unknown"

    path = request.url.path
    method = request.method

    if any(blocked in path.lower() for blocked in BLOCKED_PATHS):
        logger.warning(f"Блокирован запрос от {client_ip}: {method} {path}")
        return JSONResponse(
            status_code=404,
            content={"status": "error", "message": "Not Found"}
        )

    if method == "GET" and path not in ["/", "/health"]:
        logger.warning(f"Блокирован GET запрос от {client_ip}: {path}")
        return JSONResponse(
            status_code=404,
            content={"status": "error", "message": "Not Found"}
        )

    if method == "POST" and path == "/webhook":
        logger.info(f"Webhook запрос от {client_ip}")

    try:
        response = await call_next(request)
        return response
    except Exception as e:
        logger.error(f"Ошибка обработки запроса от {client_ip}: {str(e)}")
        return JSONResponse(
            status_code=500,
            content={"status": "error", "message": "Internal Server Error"}
        )

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Запуск универсального обработчика сигналов...")
    init_db()
    logger.info("База данных инициализирована")
    try:
        yield
    finally:
        close_db()
        logger.info("Обработчик остановлен")

app = FastAPI(
    title="TLC Trading Bot API",
    description="API для обработки торговых сигналов",
    version="1.0.0",
    lifespan=lifespan
)

app.middleware("http")(security_middleware)

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    client_ip = request.client.host if request.client else "unknown"
    logger.error(f"Необработанное исключение от {client_ip}: {str(exc)}")

    return JSONResponse(
        status_code=500,
        content={"status": "error", "message": "Internal Server Error"}
    )


@app.get("/favicon.ico")
@app.get("/robots.txt")
@app.get("/.well-known/{rest_of_path:path}")
async def block_static_requests():
    return JSONResponse(
        status_code=404,
        content={"status": "error", "message": "Not Found"}
    )

@app.get("/v2/{rest_of_path:path}")
@app.get("/api/v2/{rest_of_path:path}")
async def block_docker_requests():
    return JSONResponse(
        status_code=404,
        content={"status": "error", "message": "Not Found"}
    )

@app.get("/")
async def root():
    return {
        "status": "success",
        "message": "TLC Trading Bot API is running",
        "endpoints": {
            "webhook": "POST /webhook",
            "health": "GET /health"
        }
    }


@app.get("/health")
async def health_check():
    from datetime import datetime
    return {
        "status": "healthy",
        "service": "TLC Trading Bot",
        "timestamp": datetime.now().isoformat()
    }


app.include_router(router)

if __name__ == "__main__":
    logger.info("Starting server on http://0.0.0.0:5000")
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=5000,
        log_config=None,
        timeout_keep_alive=5,
        limit_max_requests=1000,
    )