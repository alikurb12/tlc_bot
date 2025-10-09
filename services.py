import os
import time
import json
import logging
import asyncio
from typing import Dict, Optional

from aiogram import types

from database import get_cursor, commit
from main import bot, send_signal_notification
from bingx_api import (
    get_balance as bingx_get_balance,
    set_leverage as bingx_set_leverage,
    calculate_quantity as bingx_calculate_quantity,
    create_main_order as bingx_create_main_order,
    create_tp_sl_orders as bingx_create_tp_sl_orders,
    get_open_orders as bingx_get_open_orders,
    cancel_order as bingx_cancel_order
)
from okx_api import (
    get_balance as okx_get_balance,
    set_leverage as okx_set_leverage,
    calculate_quantity as okx_calculate_quantity,
    create_main_order as okx_create_main_order,
    cancel_order as okx_cancel_order,
    get_order_status,
    close_position
)

logger = logging.getLogger(__name__)


def close_bingx_trade(user: Dict, symbol: str, current_side: str) -> bool:
    user_id = user['user_id']
    api_key = user['api_key']
    secret_key = user['secret_key']

    try:
        # Получаем открытые ордера
        open_orders_response = bingx_get_open_orders(symbol, api_key, secret_key)
        open_orders_data = open_orders_response.get("data", {}).get("orders", [])

        cursor = get_cursor()
        cursor.execute(
            """
            SELECT trade_id, order_id, sl_order_id, tp1_order_id, tp2_order_id, tp3_order_id, side
            FROM trades
            WHERE user_id = %s AND symbol = %s AND status = %s
            """,
            (user_id, symbol, 'open')
        )
        open_trades = cursor.fetchall()

        if not open_trades:
            logger.info(f"Нет открытых сделок для пользователя {user_id} по символу {symbol}")
            return False

        closed = False
        for trade in open_trades:
            if trade['side'] != current_side:  # Проверяем противоположную сторону
                order_ids = [trade['order_id'], trade['sl_order_id'], trade['tp1_order_id'],
                             trade['tp2_order_id'], trade['tp3_order_id']]
                for order_id in order_ids:
                    if order_id:
                        try:
                            bingx_cancel_order(symbol, order_id, api_key, secret_key)
                            closed = True
                        except Exception as e:
                            logger.error(f"Ошибка при отмене ордера {order_id} для {symbol}: {str(e)}")
                            continue

                cursor.execute(
                    "UPDATE trades SET status = %s WHERE trade_id = %s",
                    ('closed', trade['trade_id'])
                )
                commit()

                # Отправляем уведомление о закрытии сделки
                try:
                    notification = {
                        "action": f"CLOSE_{trade['side']}",
                        "symbol": symbol,
                        "price": 0,  # Цена закрытия неизвестна, ставим 0
                        "stop_loss": None,
                        "take_profit_1": None,
                        "take_profit_2": None,
                        "take_profit_3": None
                    }
                    loop = asyncio.get_event_loop()
                    asyncio.run_coroutine_threadsafe(
                        send_signal_notification(notification, user_id), loop
                    )
                    logger.info(f"Уведомление о закрытии сделки отправлено для пользователя {user_id}")
                except Exception as notify_error:
                    logger.error(f"Ошибка отправки уведомления о закрытии для {user_id}: {notify_error}")

        return closed

    except Exception as e:
        logger.error(f"Ошибка при закрытии сделки BingX для пользователя {user_id}: {str(e)}")
        return False


def close_okx_trade(user: Dict, symbol: str, current_side: str) -> bool:
    user_id = user['user_id']
    api_key = user['api_key']
    secret_key = user['secret_key']
    passphrase = user['passphrase']

    try:
        cursor = get_cursor()
        cursor.execute(
            """
            SELECT trade_id, order_id, sl_order_id, tp1_order_id, tp2_order_id, tp3_order_id, side
            FROM trades
            WHERE user_id = %s AND symbol = %s AND status = %s
            """,
            (user_id, symbol, 'open')
        )
        open_trades = cursor.fetchall()

        if not open_trades:
            logger.info(f"Нет открытых сделок для пользователя {user_id} по символу {symbol}")
            return False

        closed = False
        for trade in open_trades:
            if trade['side'] != current_side:  # Проверяем противоположную сторону
                order_ids = [trade['order_id'], trade['sl_order_id'], trade['tp1_order_id'],
                             trade['tp2_order_id'], trade['tp3_order_id']]
                pos_side = "long" if trade['side'] == "BUY" else "short"

                # Проверяем статус ордеров
                for order_id in order_ids:
                    if order_id:
                        try:
                            order_status = get_order_status(symbol, order_id, api_key, secret_key, passphrase)
                            if order_status['state'] in ['canceled', 'filled']:
                                logger.info(
                                    f"Ордер {order_id} для {symbol} уже закрыт (статус: {order_status['state']})")
                            else:
                                okx_cancel_order(symbol, order_id, api_key, secret_key, passphrase)
                                closed = True
                        except Exception as e:
                            logger.error(f"Ошибка при проверке/отмене ордера {order_id} для {symbol}: {str(e)}")
                            continue

                # Проверяем и закрываем позицию
                try:
                    close_position(symbol, pos_side, api_key, secret_key, passphrase)
                    closed = True
                except Exception as e:
                    logger.warning(f"Не удалось закрыть позицию {pos_side} для {symbol}: {str(e)}")

                cursor.execute(
                    "UPDATE trades SET status = %s WHERE trade_id = %s",
                    ('closed', trade['trade_id'])
                )
                commit()

                # Отправляем уведомление о закрытии сделки
                try:
                    SUPPORT_CONTACT = os.getenv("SUPPORT_CONTACT", "@SupportBot")
                    notification = {
                        "action": f"CLOSE_{trade['side']}",
                        "symbol": symbol,
                        "price": 0,  # Цена закрытия неизвестна
                        "stop_loss": None,
                        "take_profit_1": None,
                        "take_profit_2": None,
                        "take_profit_3": None
                    }
                    loop = asyncio.get_event_loop()
                    asyncio.run_coroutine_threadsafe(
                        send_signal_notification(notification, user_id), loop
                    )
                    logger.info(f"Уведомление о закрытии сделки отправлено для пользователя {user_id}")
                except Exception as notify_error:
                    logger.error(f"Ошибка отправки уведомления о закрытии для {user_id}: {notify_error}")

        return closed

    except Exception as e:
        logger.error(f"Ошибка при закрытии сделки OKX для пользователя {user_id}: {str(e)}")
        SUPPORT_CONTACT = os.getenv("SUPPORT_CONTACT", "@SupportBot")
        try:
            keyboard = types.InlineKeyboardMarkup(inline_keyboard=[
                [types.InlineKeyboardButton(text="📞 Поддержка", url=f"https://t.me/{SUPPORT_CONTACT.lstrip('@')}")]
            ])
            loop = asyncio.get_event_loop()
            asyncio.run_coroutine_threadsafe(
                bot.send_message(
                    chat_id=user_id,
                    text=f"❌ Не удалось закрыть предыдущую сделку по {symbol}. Пожалуйста, проверьте биржу и свяжитесь с поддержкой.",
                    reply_markup=keyboard
                ),
                loop
            )
        except Exception as notify_error:
            logger.error(f"Ошибка отправки уведомления об ошибке закрытия для {user_id}: {notify_error}")
        return False


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
        # Проверяем и закрываем противоположные открытые сделки
        close_bingx_trade(user, symbol, action)

        balance_response = bingx_get_balance(api_key, secret_key)
        balance_data = json.loads(balance_response)
        usdt_balance = float(balance_data["data"]["balance"]["availableMargin"])

        if usdt_balance < 0.1:
            logger.error(f"Недостаточный баланс для пользователя {user_id}: {usdt_balance} USDT")
            return None

        bingx_set_leverage(symbol, leverage=5, position_side=position_side, api_key=api_key, secret_key=secret_key)

        quantity = bingx_calculate_quantity(symbol, leverage=5, risk_percent=0.05, api_key=api_key,
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
        SUPPORT_CONTACT = os.getenv("SUPPORT_CONTACT", "@SupportBot")
        try:
            keyboard = types.InlineKeyboardMarkup(inline_keyboard=[
                [types.InlineKeyboardButton(text="📞 Поддержка", url=f"https://t.me/{SUPPORT_CONTACT.lstrip('@')}")]
            ])
            loop = asyncio.get_event_loop()
            asyncio.run_coroutine_threadsafe(
                bot.send_message(
                    chat_id=user_id,
                    text=f"❌ Ошибка обработки сигнала для {symbol}. Пожалуйста, свяжитесь с поддержкой.",
                    reply_markup=keyboard
                ),
                loop
            )
        except Exception as notify_error:
            logger.error(f"Ошибка отправки уведомления об ошибке для {user_id}: {notify_error}")
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
        # Проверяем и закрываем противоположные открытые сделки
        close_okx_trade(user, symbol, action)

        usdt_balance = okx_get_balance(api_key, secret_key, passphrase)

        if usdt_balance < 0.1:
            logger.error(f"Недостаточный баланс для пользователя {user_id}: {usdt_balance} USDT")
            return None

        okx_set_leverage(symbol, leverage=5, tdMode="isolated", posSide=position_side,
                         api_key=api_key, secret_key=secret_key, passphrase=passphrase)

        quantity = okx_calculate_quantity(symbol, leverage=5, risk_percent=0.05,
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
        SUPPORT_CONTACT = os.getenv("SUPPORT_CONTACT", "@SupportBot")
        try:
            keyboard = types.InlineKeyboardMarkup(inline_keyboard=[
                [types.InlineKeyboardButton(text="📞 Поддержка", url=f"https://t.me/{SUPPORT_CONTACT.lstrip('@')}")]
            ])
            loop = asyncio.get_event_loop()
            asyncio.run_coroutine_threadsafe(
                bot.send_message(
                    chat_id=user_id,
                    text=f"❌ Ошибка обработки сигнала для {symbol}. Пожалуйста, свяжитесь с поддержкой.",
                    reply_markup=keyboard
                ),
                loop
            )
        except Exception as notify_error:
            logger.error(f"Ошибка отправки уведомления об ошибке для {user_id}: {notify_error}")
        return None