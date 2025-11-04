import time
import json
import logging
import asyncio
from aiogram import types
import os
from typing import Dict, Optional
from database import get_cursor, commit
from utils import send_signal_notification
from main import bot
from bingx_api import (
    get_balance as bingx_get_balance,
    set_leverage as bingx_set_leverage,
    calculate_quantity as bingx_calculate_quantity,
    create_main_order as bingx_create_main_order,
    create_tp_sl_orders as bingx_create_tp_sl_orders,
    get_open_orders as bingx_get_open_orders,
    cancel_order as bingx_cancel_order,
    close_position as bingx_close_position,
    get_open_positions as bingx_get_open_positions
)
from okx_api import (
    get_balance as okx_get_balance,
    set_leverage as okx_set_leverage,
    calculate_quantity as okx_calculate_quantity,
    create_main_order as okx_create_main_order,
    cancel_order as okx_cancel_order,
    get_order_status,
    close_position as okx_close_position
)

logger = logging.getLogger(__name__)


async def close_bingx_trade(user: Dict, symbol: str, current_side: str) -> bool:
    user_id = user['user_id']
    api_key = user['api_key']
    secret_key = user['secret_key']

    try:
        cursor = get_cursor()

        # Начинаем транзакцию
        cursor.execute("BEGIN")

        # Получаем открытые сделки
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
            cursor.execute("ROLLBACK")
            logger.info(f"Нет открытых сделок для пользователя {user_id} по символу {symbol}")
            return True

        closed = False
        position_side = "LONG" if current_side == "SELL" else "SHORT"

        for trade in open_trades:
            if trade['side'] != current_side:
                # Отменяем все связанные ордера
                order_ids = [trade['order_id'], trade['sl_order_id'], trade['tp1_order_id'],
                             trade['tp2_order_id'], trade['tp3_order_id']]

                for order_id in order_ids:
                    if order_id:
                        try:
                            bingx_cancel_order(symbol, order_id, api_key, secret_key)
                            logger.info(f"Ордер {order_id} для {symbol} успешно отменён")
                            closed = True
                        except Exception as e:
                            if "order not exist" in str(e).lower():
                                logger.info(f"Ордер {order_id} для {symbol} уже не существует")
                            else:
                                logger.error(f"Ошибка при отмене ордера {order_id} для {symbol}: {str(e)}")
                                continue

                # Закрываем позицию
                try:
                    bingx_close_position(symbol, position_side, api_key, secret_key)
                    logger.info(f"Позиция {position_side} для {symbol} закрыта")
                    closed = True
                except Exception as e:
                    if "position not exist" in str(e).lower() or "order not exist" in str(e).lower():
                        logger.info(f"Позиция {position_side} для {symbol} уже не существует")
                    else:
                        logger.error(f"Ошибка при закрытии позиции {position_side} для {symbol}: {str(e)}")

                # Обновляем статус в базе данных
                cursor.execute(
                    "UPDATE trades SET status = %s WHERE trade_id = %s",
                    ('closed', trade['trade_id'])
                )

        if closed:
            commit()
            logger.info(f"Транзакция завершена для пользователя {user_id}")

            # Отправляем уведомление
            try:
                notification = {
                    "action": f"CLOSE_{'BUY' if position_side == 'LONG' else 'SELL'}",
                    "symbol": symbol,
                    "price": 0,
                    "stop_loss": None,
                    "take_profit_1": None,
                    "take_profit_2": None,
                    "take_profit_3": None
                }
                await send_signal_notification(notification, user_id, bot)
            except Exception as notify_error:
                logger.error(f"Ошибка отправки уведомления о закрытии для {user_id}: {notify_error}")
        else:
            cursor.execute("ROLLBACK")
            logger.info(f"Не было закрытых сделок для пользователя {user_id}")

        return closed

    except Exception as e:
        # Откатываем транзакцию при ошибке
        try:
            cursor.execute("ROLLBACK")
        except:
            pass
        logger.error(f"Ошибка при закрытии сделки BingX для пользователя {user_id}: {str(e)}")

        # Отправляем сообщение об ошибке
        SUPPORT_CONTACT = os.getenv("SUPPORT_CONTACT", "@SupportBot")
        try:
            keyboard = types.InlineKeyboardMarkup(inline_keyboard=[
                [types.InlineKeyboardButton(text="📞 Поддержка", url=f"https://t.me/{SUPPORT_CONTACT.lstrip('@')}")]
            ])
            await bot.send_message(
                chat_id=user_id,
                text=f"❌ Не удалось закрыть предыдущую сделку по {symbol}. Пожалуйста, проверьте биржу и свяжитесь с поддержкой.",
                reply_markup=keyboard
            )
        except Exception as notify_error:
            logger.error(f"Ошибка отправки уведомления об ошибке закрытия для {user_id}: {notify_error}")
        return False


async def close_okx_trade(user: Dict, symbol: str, current_side: str) -> bool:
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
                pos_side = "long" if trade['side'] == "BUY" else "short"
                order_ids = [trade['order_id'], trade['sl_order_id'], trade['tp1_order_id'],
                             trade['tp2_order_id'], trade['tp3_order_id']]

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

                # Закрываем позицию
                try:
                    okx_close_position(symbol, pos_side, api_key, secret_key, passphrase)
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
                        "price": 0,
                        "stop_loss": None,
                        "take_profit_1": None,
                        "take_profit_2": None,
                        "take_profit_3": None
                    }
                    await send_signal_notification(notification, user_id, bot)
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
            await bot.send_message(
                chat_id=user_id,
                text=f"❌ Не удалось закрыть предыдущую сделку по {symbol}. Пожалуйста, проверьте биржу и свяжитесь с поддержкой.",
                reply_markup=keyboard
            )
        except Exception as notify_error:
            logger.error(f"Ошибка отправки уведомления об ошибке закрытия для {user_id}: {notify_error}")
        return False


async def process_bingx_signal(user: Dict, signal: Dict) -> Optional[Dict]:
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
        await close_bingx_trade(user, symbol, action)

        # Проверяем открытые позиции
        open_positions = bingx_get_open_positions(symbol, api_key, secret_key)
        for position in open_positions:
            pos_side = position.get("positionSide")
            if pos_side and pos_side != position_side:
                try:
                    bingx_close_position(symbol, pos_side, api_key, secret_key)
                    logger.info(f"Закрыта существующая позиция {pos_side} для {symbol}")
                except Exception as e:
                    logger.error(f"Ошибка при закрытии существующей позиции {pos_side} для {symbol}: {str(e)}")

        balance_response = bingx_get_balance(api_key, secret_key)
        balance_data = json.loads(balance_response)
        usdt_balance = float(balance_data["data"]["balance"]["availableMargin"])

        if usdt_balance < 0.1:
            logger.error(f"Недостаточный баланс для пользователя {user_id}: {usdt_balance} USDT")
            return None

        bingx_set_leverage(symbol, leverage=10, position_side=position_side, api_key=api_key, secret_key=secret_key)

        quantity = bingx_calculate_quantity(symbol, leverage=10, risk_percent=0.05, api_key=api_key,
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
            await send_signal_notification(signal, user_id, bot)
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
            await bot.send_message(
                chat_id=user_id,
                text=f"❌ Ошибка обработки сигнала для {symbol}. Пожалуйста, свяжитесь с поддержкой.",
                reply_markup=keyboard
            )
        except Exception as notify_error:
            logger.error(f"Ошибка отправки уведомления об ошибке для {user_id}: {notify_error}")
        return None


async def process_okx_signal(user: Dict, signal: Dict) -> Optional[Dict]:
    user_id = user['user_id']
    api_key = user['api_key']
    secret_key = user['secret_key']
    passphrase = user['passphrase']

    action = signal['action']
    symbol = signal['symbol']
    price = signal['price']
    stop_loss = signal['stop_loss']
    take_profits = [signal['take_profit_1'], signal['take_profit_2'], signal['take_profit_3']]

    try:
        # Проверяем и закрываем противоположные открытые сделки
        await close_okx_trade(user, symbol, action)

        usdt_balance = okx_get_balance(api_key, secret_key, passphrase)

        if usdt_balance < 10:
            logger.error(f"Недостаточный баланс для пользователя {user_id}: {usdt_balance} USDT")
            return None

        # Устанавливаем плечо
        leverage_set = okx_set_leverage(symbol, leverage=10, tdMode="isolated",
                                        api_key=api_key, secret_key=secret_key, passphrase=passphrase)

        if not leverage_set:
            logger.warning(f"Не удалось установить плечо для {symbol}, продолжаем...")

        quantity = okx_calculate_quantity(symbol, leverage=10, risk_percent=0.05,
                                          api_key=api_key, secret_key=secret_key, passphrase=passphrase)

        main_order_response, sorted_take_profits, order_id, algo_order_ids, position_side = okx_create_main_order(
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
            await send_signal_notification(signal, user_id, bot)
            logger.info(f"Запущена отправка уведомления для пользователя {user_id}")
        except Exception as notify_error:
            logger.error(f"Ошибка отправки уведомления для user {user_id}: {notify_error}")

        return {
            "user_id": user_id,
            "exchange": "okx",
            "trade_id": trade_id,
            "position_side": position_side,  # Добавляем position_side в ответ
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
            await bot.send_message(
                chat_id=user_id,
                text=f"❌ Ошибка обработки сигнала для {symbol}. Пожалуйста, свяжитесь с поддержкой.",
                reply_markup=keyboard
            )
        except Exception as notify_error:
            logger.error(f"Ошибка отправки уведомления об ошибке для {user_id}: {notify_error}")
        return None

async def process_bingx_move_sl(user: Dict, symbol: str) -> Optional[Dict]:
    """Обработка MOVE_SL для BingX"""
    user_id = user['user_id']
    api_key = user['api_key']
    secret_key = user['secret_key']

    try:
        from bingx_api import move_sl_to_breakeven

        success = move_sl_to_breakeven(symbol, api_key, secret_key)

        if success:
            # Отправляем уведомление
            notification = {
                "action": "MOVE_SL",
                "symbol": symbol,
                "message": f"Стоп-лосс перемещен к цене входа для {symbol}"
            }
            await send_signal_notification(notification, user_id, bot)

            return {
                "user_id": user_id,
                "exchange": "bingx",
                "status": "success",
                "message": f"SL перемещен к цене входа для {symbol}"
            }

    except Exception as e:
        logger.error(f"Ошибка обработки MOVE_SL для пользователя {user_id}: {str(e)}")
        return None


async def process_okx_move_sl(user: Dict, symbol: str) -> Optional[Dict]:
    """Обработка MOVE_SL для OKX"""
    user_id = user['user_id']
    api_key = user['api_key']
    secret_key = user['secret_key']
    passphrase = user['passphrase']

    try:
        from okx_api import move_sl_to_breakeven

        success = move_sl_to_breakeven(symbol, api_key, secret_key, passphrase)

        if success:
            # Отправляем уведомление
            notification = {
                "action": "MOVE_SL",
                "symbol": symbol,
                "message": f"Стоп-лосс перемещен к цене входа для {symbol}"
            }
            await send_signal_notification(notification, user_id, bot)

            return {
                "user_id": user_id,
                "exchange": "okx",
                "status": "success",
                "message": f"SL перемещен к цене входа для {symbol}"
            }

    except Exception as e:
        logger.error(f"Ошибка обработки MOVE_SL для пользователя {user_id}: {str(e)}")
        return None