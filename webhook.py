# webhook.py
import json
import math
from fastapi import APIRouter, Request, HTTPException
import logging
from datetime import datetime
from database import get_cursor
from models import Signal
from utils import normalize_symbol

logger = logging.getLogger(__name__)

router = APIRouter()


def clean_json_data(data_str: str) -> dict:
    """Очищает JSON данные от NaN и других невалидных значений"""
    try:
        cleaned_str = data_str.replace(': NaN', ': null').replace(':NaN', ':null')
        return json.loads(cleaned_str)
    except json.JSONDecodeError as e:
        logger.error(f"Ошибка парсинга JSON: {str(e)}")
        raise


async def handle_move_sl_signal(data: dict):
    """Обработка сигнала MOVE_SL"""
    symbol = data.get('symbol')
    if not symbol:
        logger.error("Не указан символ для MOVE_SL")
        raise HTTPException(status_code=400, detail="Необходимо указать символ для MOVE_SL")

    cursor = get_cursor()
    cursor.execute(
        "SELECT user_id, api_key, secret_key, passphrase, exchange FROM users WHERE subscription_end > %s AND api_key IS NOT NULL AND secret_key IS NOT NULL AND subscription_type IN ('referral_approved', 'regular')",
        (datetime.now(),)
    )
    active_users = cursor.fetchall()

    if not active_users:
        logger.error("Нет пользователей с активной подпиской и API-ключами")
        raise HTTPException(status_code=400, detail="Нет пользователей с активной подпиской и API-ключами")

    results = []
    for user in active_users:
        user_id = user['user_id']
        exchange = user.get('exchange', 'bingx')
        normalized_symbol = normalize_symbol(symbol, exchange)

        try:
            if exchange == 'bingx':
                from services import process_bingx_move_sl
                result = process_bingx_move_sl(user, normalized_symbol)
            elif exchange == 'okx':
                from services import process_okx_move_sl
                result = process_okx_move_sl(user, normalized_symbol)
            else:
                logger.error(f"Неизвестная биржа: {exchange} для пользователя {user_id}")
                continue

            if result:
                results.append(result)
                logger.info(f"MOVE_SL обработан для пользователя {user_id} на бирже {exchange}")

        except Exception as e:
            logger.error(f"Ошибка обработки MOVE_SL для пользователя {user_id} на бирже {exchange}: {str(e)}")
            continue

    if not results:
        raise HTTPException(status_code=500, detail="Не удалось обработать MOVE_SL ни для одного пользователя")

    return {
        "status": "success",
        "message": "MOVE_SL сигнал обработан для активных пользователей",
        "symbol": symbol,
        "results": results
    }


@router.post("/webhook")
async def webhook(request: Request):
    """Основной webhook endpoint для торговых сигналов"""
    try:
        raw_data = await request.body()
        raw_data_str = raw_data.decode('utf-8')
        logger.info(f"Получен webhook запрос: {raw_data_str}")

        content_type = request.headers.get('Content-Type', '').lower()

        if not content_type or ('application/json' not in content_type and 'text/plain' not in content_type):
            logger.error(f"Неверный Content-Type: {content_type}")
            raise HTTPException(status_code=400, detail="Ожидается Content-Type: application/json или text/plain")

        try:
            data = clean_json_data(raw_data_str)
        except Exception as e:
            logger.error(f"Ошибка парсинга JSON: {str(e)}")
            raise HTTPException(status_code=400, detail=f"Неверный формат JSON: {str(e)}")

        if not data:
            logger.error("Получен пустой JSON")
            raise HTTPException(status_code=400, detail="Пустой JSON")

        raw_action = data.get('action', '').upper()

        if raw_action not in ['BUY', 'SELL', 'LONG', 'SHORT', 'MOVE_SL']:
            logger.error(f"Некорректное действие: {raw_action}")
            raise HTTPException(status_code=400, detail="Действие должно быть BUY, SELL, LONG, SHORT или MOVE_SL")

        # Обработка MOVE_SL (немедленно)
        if raw_action == 'MOVE_SL':
            return await handle_move_sl_signal(data)

        # Обработка торговых сигналов BUY/SELL
        action = raw_action if raw_action in ['BUY', 'SELL'] else ('BUY' if raw_action == 'LONG' else 'SELL')
        symbol = data.get('symbol')

        if not symbol:
            logger.error("Не указан символ")
            raise HTTPException(status_code=400, detail="Необходимо указать символ")

        try:
            price = float(data.get('price', 0))
            if price <= 0:
                raise ValueError("Цена должна быть положительной")
        except (TypeError, ValueError) as e:
            logger.error(f"Ошибка в цене: {str(e)}")
            raise HTTPException(status_code=400, detail="Неверный формат цены")

        stop_loss = data.get('stop_loss')
        take_profit_1 = data.get('take_profit_1')
        take_profit_2 = data.get('take_profit_2')
        take_profit_3 = data.get('take_profit_3')

        # Обрабатываем возможные NaN значения
        try:
            stop_loss = float(stop_loss) if stop_loss is not None and not math.isnan(float(stop_loss)) else None
        except (TypeError, ValueError):
            stop_loss = None

        try:
            take_profit_1 = float(take_profit_1) if take_profit_1 is not None and not math.isnan(
                float(take_profit_1)) else None
        except (TypeError, ValueError):
            take_profit_1 = None

        try:
            take_profit_2 = float(take_profit_2) if take_profit_2 is not None and not math.isnan(
                float(take_profit_2)) else None
        except (TypeError, ValueError):
            take_profit_2 = None

        try:
            take_profit_3 = float(take_profit_3) if take_profit_3 is not None and not math.isnan(
                float(take_profit_3)) else None
        except (TypeError, ValueError):
            take_profit_3 = None

        if not stop_loss or not all([take_profit_1, take_profit_2, take_profit_3]):
            logger.error(
                f"Не указаны все SL/TP: SL={stop_loss}, TP1={take_profit_1}, TP2={take_profit_2}, TP3={take_profit_3}")
            raise HTTPException(status_code=400, detail="Необходимо указать stop_loss и все три take_profit")

        # Получаем активных пользователей
        cursor = get_cursor()
        cursor.execute(
            "SELECT user_id, api_key, secret_key, passphrase, exchange FROM users WHERE subscription_end > %s AND api_key IS NOT NULL AND secret_key IS NOT NULL AND subscription_type IN ('referral_approved', 'regular')",
            (datetime.now(),)
        )
        active_users = cursor.fetchall()

        if not active_users:
            logger.error("Нет пользователей с активной подпиской и API-ключами")
            raise HTTPException(status_code=400, detail="Нет пользователей с активной подпиской и API-ключами")

        results = []
        for user in active_users:
            user_id = user['user_id']
            exchange = user.get('exchange', 'bingx')
            normalized_symbol = normalize_symbol(symbol, exchange)

            signal = {
                "action": action,
                "symbol": normalized_symbol,
                "price": price,
                "stop_loss": stop_loss,
                "take_profit_1": take_profit_1,
                "take_profit_2": take_profit_2,
                "take_profit_3": take_profit_3,
            }

            try:
                if exchange == 'bingx':
                    from services import process_bingx_signal
                    result = process_bingx_signal(user, signal)
                elif exchange == 'okx':
                    from services import process_okx_signal
                    result = process_okx_signal(user, signal)
                else:
                    logger.error(f"Неизвестная биржа: {exchange} для пользователя {user_id}")
                    continue

                if result:
                    results.append(result)
                    logger.info(f"Сигнал обработан для пользователя {user_id} на бирже {exchange}")

            except Exception as e:
                logger.error(f"Ошибка обработки сигнала для пользователя {user_id} на бирже {exchange}: {str(e)}")
                continue

        if not results:
            raise HTTPException(status_code=500, detail="Не удалось обработать сигнал ни для одного пользователя")

        return {
            "status": "success",
            "message": "Фьючерсный сигнал обработан для активных пользователей",
            "signal_id": "webhook_signal",
            "symbol": symbol,
            "results": results
        }

    except Exception as e:
        logger.error(f"Ошибка обработки webhook: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))