import json
from fastapi import APIRouter, Request, HTTPException
import logging
from datetime import datetime
from database import get_cursor
from models import Signal
from utils import normalize_symbol
from services import process_bingx_signal, process_okx_signal

logger = logging.getLogger(__name__)

router = APIRouter()

@router.post("/webhook")
async def webhook(request: Request):
    try:
        raw_data = await request.body()
        raw_data_str = raw_data.decode('utf-8')
        logger.info(f"Получен запрос: {raw_data_str}")

        content_type = request.headers.get('Content-Type', '').lower()
        if not content_type or 'application/json' not in content_type:
            logger.error(f"Неверный Content-Type: {content_type}. Ожидается application/json от TradingView с {{strategy.order.alert_message}}")
            raise HTTPException(status_code=400, detail="Ожидается Content-Type: application/json. Настройте TradingView для отправки JSON с {{strategy.order.alert_message}}")

        try:
            data = json.loads(raw_data_str)
        except Exception as e:
            logger.error(f"Ошибка парсинга JSON: {str(e)}, тело запроса: {raw_data_str}")
            raise HTTPException(status_code=400, detail=f"Неверный формат JSON: {str(e)}")

        if not data:
            logger.error("Получен пустой JSON")
            raise HTTPException(status_code=400, detail="Пустой JSON")

        raw_action = data.get('action', '').upper()
        if raw_action not in ['BUY', 'SELL', 'LONG', 'SHORT']:
            logger.error(f"Некорректное действие: {raw_action}")
            raise HTTPException(status_code=400, detail="Действие должно быть BUY, SELL, LONG или SHORT")

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

        stop_loss = float(data.get('stop_loss')) if data.get('stop_loss') else None
        take_profit_1 = float(data.get('take_profit_1')) if data.get('take_profit_1') else None
        take_profit_2 = float(data.get('take_profit_2')) if data.get('take_profit_2') else None
        take_profit_3 = float(data.get('take_profit_3')) if data.get('take_profit_3') else None

        if not stop_loss or not all([take_profit_1, take_profit_2, take_profit_3]):
            logger.error("Не указаны все необходимые параметры SL и TP")
            raise HTTPException(status_code=400, detail="Необходимо указать stop_loss и все три take_profit")

        # Signal model validation
        signal_base = Signal(
            action=action,
            symbol=symbol,
            price=price,
            stop_loss=stop_loss,
            take_profit_1=take_profit_1,
            take_profit_2=take_profit_2,
            take_profit_3=take_profit_3,
        )

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
                    result = process_bingx_signal(user, signal)
                elif exchange == 'okx':
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