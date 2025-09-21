import time
import json
import logging
import asyncio
from typing import Dict, Optional
from database import get_cursor, commit
from main import bot, send_signal_notification
from bingx_api import (
    get_balance as bingx_get_balance,
    set_leverage as bingx_set_leverage,
    calculate_quantity as bingx_calculate_quantity,
    create_main_order as bingx_create_main_order,
    create_tp_sl_orders as bingx_create_tp_sl_orders
)
from okx_api import (
    get_balance as okx_get_balance,
    set_leverage as okx_set_leverage,
    calculate_quantity as okx_calculate_quantity,
    create_main_order as okx_create_main_order
)

logger = logging.getLogger(__name__)

def process_bingx_signal(user: Dict, signal: Dict) -> Optional[Dict]:
    user_id = user['user_id']
    api_key = user['api_key']
    secret_key = user['secret_key']

    action = signal['action']
    symbol = signal['symbol']
    price = signal['price']
    stop_loss = signal['stop_loss']
    take_profits = [signal['take_profit_1'], signal['take_profit_2'], signal['take_profit_3']]
    position_side = "LONG" if action == "BUY" else "SHORT"

    try:
        balance_response = bingx_get_balance(api_key, secret_key)
        balance_data = json.loads(balance_response)
        usdt_balance = float(balance_data["data"]["balance"]["availableMargin"])

        if usdt_balance < 0.1:
            logger.error(f"Недостаточный баланс для пользователя {user_id}: {usdt_balance} USDT")
            return None

        bingx_set_leverage(symbol, leverage=10, position_side=position_side, api_key=api_key, secret_key=secret_key)

        quantity = bingx_calculate_quantity(symbol, leverage=10, risk_percent=0.10, api_key=api_key,
                                            secret_key=secret_key)

        main_order = bingx_create_main_order(symbol, action, quantity, api_key, secret_key)
        main_order_data = json.loads(main_order)

        if main_order_data.get("code") != 0:
            logger.error(f"Ошибка создания основного ордера для пользователя {user_id}: {main_order_data.get('msg')}")
            return None

        order_id = main_order_data["data"]["order"]["orderId"]
        logger.info(f"Main order for user {user_id}: {main_order}")

        cursor = get_cursor()
        cursor.execute(
            """
            INSERT INTO trades (user_id, exchange, order_id, symbol, side, position_side, quantity, entry_price, stop_loss, take_profit_1, take_profit_2, take_profit_3, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING trade_id
            """,
            (user_id, 'bingx', order_id, symbol, action, position_side, quantity, price, stop_loss,
             take_profits[0], take_profits[1], take_profits[2], 'open')
        )
        trade_id = cursor.fetchone()['trade_id']
        commit()

        time.sleep(2)

        tp_sl_results, sorted_take_profits, order_ids = bingx_create_tp_sl_orders(
            symbol=symbol,
            side=action,
            quantity=quantity,
            stop_loss=stop_loss,
            take_profits=take_profits,
            api_key=api_key,
            secret_key=secret_key
        )

        sl_order_id = order_ids[0] if order_ids else None
        tp1_order_id = order_ids[1] if len(order_ids) > 1 else None
        tp2_order_id = order_ids[2] if len(order_ids) > 2 else None
        tp3_order_id = order_ids[3] if len(order_ids) > 3 else None

        cursor.execute(
            """
            UPDATE trades SET sl_order_id = %s, tp1_order_id = %s, tp2_order_id = %s, tp3_order_id = %s
            WHERE trade_id = %s
            """,
            (sl_order_id, tp1_order_id, tp2_order_id, tp3_order_id, trade_id)
        )
        commit()

        try:
            loop = asyncio.get_event_loop()
            asyncio.run_coroutine_threadsafe(send_signal_notification(signal, user_id), loop)
            logger.info(f"Запущена отправка уведомления для пользователя {user_id}")
        except Exception as notify_error:
            logger.error(f"Ошибка отправки уведомления для user {user_id}: {notify_error}")

        return {
            "user_id": user_id,
            "exchange": "bingx",
            "trade_id": trade_id,
            "main_order": main_order_data,
            "tp_sl_orders": [json.loads(res) for res in tp_sl_results]
        }

    except Exception as e:
        logger.error(f"Ошибка обработки сигнала BingX для пользователя {user_id}: {str(e)}")
        return None

def process_okx_signal(user: Dict, signal: Dict) -> Optional[Dict]:
    user_id = user['user_id']
    api_key = user['api_key']
    secret_key = user['secret_key']
    passphrase = user['passphrase']

    action = signal['action']
    symbol = signal['symbol']
    price = signal['price']
    stop_loss = signal['stop_loss']
    take_profits = [signal['take_profit_1'], signal['take_profit_2'], signal['take_profit_3']]
    position_side = "long" if action == "BUY" else "short"

    try:
        usdt_balance = okx_get_balance(api_key, secret_key, passphrase)

        if usdt_balance < 0.1:
            logger.error(f"Недостаточный баланс для пользователя {user_id}: {usdt_balance} USDT")
            return None

        okx_set_leverage(symbol, leverage=10, tdMode="isolated", posSide=position_side,
                         api_key=api_key, secret_key=secret_key, passphrase=passphrase)

        quantity = okx_calculate_quantity(symbol, leverage=10, risk_percent=0.10,
                                          api_key=api_key, secret_key=secret_key, passphrase=passphrase)

        main_order_response, sorted_take_profits, order_id, algo_order_ids = okx_create_main_order(
            symbol=symbol,
            side=action,
            quantity=quantity,
            stop_loss=stop_loss,
            take_profits=take_profits,
            tdMode="isolated",
            api_key=api_key,
            secret_key=secret_key,
            passphrase=passphrase
        )

        sl_order_id = algo_order_ids[0] if algo_order_ids else None
        tp1_order_id = algo_order_ids[1] if len(algo_order_ids) > 1 else None
        tp2_order_id = algo_order_ids[2] if len(algo_order_ids) > 2 else None
        tp3_order_id = algo_order_ids[3] if len(algo_order_ids) > 3 else None

        cursor = get_cursor()
        cursor.execute(
            """
            INSERT INTO trades (user_id, exchange, order_id, symbol, side, position_side, quantity, entry_price, stop_loss, take_profit_1, take_profit_2, take_profit_3, sl_order_id, tp1_order_id, tp2_order_id, tp3_order_id, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING trade_id
            """,
            (user_id, 'okx', order_id, symbol, action, position_side, quantity, price, stop_loss,
             take_profits[0], take_profits[1], take_profits[2], sl_order_id, tp1_order_id, tp2_order_id, tp3_order_id,
             'open')
        )
        trade_id = cursor.fetchone()['trade_id']
        commit()

        try:
            loop = asyncio.get_event_loop()
            asyncio.run_coroutine_threadsafe(send_signal_notification(signal, user_id), loop)
            logger.info(f"Запущена отправка уведомления для пользователя {user_id}")
        except Exception as notify_error:
            logger.error(f"Ошибка отправки уведомления для user {user_id}: {notify_error}")

        return {
            "user_id": user_id,
            "exchange": "okx",
            "trade_id": trade_id,
            "main_order": main_order_response,
            "sl_order_id": sl_order_id,
            "tp1_order_id": tp1_order_id,
            "tp2_order_id": tp2_order_id,
            "tp3_order_id": tp3_order_id
        }

    except Exception as e:
        logger.error(f"Ошибка обработки сигнала OKX для пользователя {user_id}: {str(e)}")
        return None