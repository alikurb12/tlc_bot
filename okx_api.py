import json
import logging
from okx.PublicData import PublicAPI
from okx.Trade import TradeAPI
from okx.Account import AccountAPI
from okx.MarketData import MarketAPI

from database import get_cursor, commit

logger = logging.getLogger(__name__)

APIURL = "https://www.okx.com"


def get_symbol_info(symbol: str, api_key: str, secret_key: str, passphrase: str) -> dict:
    try:
        pub_api = PublicAPI(flag ="0", domain=APIURL, debug=True)
        response = pub_api.get_instruments(instType="SWAP", instId=symbol)
        logger.info(f"Ответ API инструментов OKX: {json.dumps(response, indent=2)}")
        if response.get("code") != "0":
            raise ValueError(f"Ошибка получения информации о символе: {response.get('msg')}")
        data = response["data"][0]
        return {
            "lotSz": float(data["lotSz"]),
            "minSz": float(data["minSz"]),
            "ctVal": float(data["ctVal"]),
            "lever": int(data["lever"])
        }
    except Exception as e:
        logger.error(f"Ошибка при получении информации о символе {symbol}: {str(e)}")
        raise


def get_current_price(symbol: str, api_key: str, secret_key: str, passphrase: str) -> float:
    try:
        market_api = MarketAPI(flag="0", domain=APIURL, debug=True)
        response = market_api.get_ticker(instId=symbol)
        logger.info(f"Ответ API цены OKX: {json.dumps(response, indent=2)}")
        if response.get("code") != "0":
            raise ValueError(f"Ошибка получения цены: {response.get('msg')}")
        return float(response["data"][0]["last"])
    except Exception as e:
        logger.error(f"Ошибка при получении цены для {symbol}: {str(e)}")
        raise


def get_balance(api_key: str, secret_key: str, passphrase: str) -> float:
    try:
        account_api = AccountAPI(api_key, secret_key, passphrase, flag="0", domain=APIURL, debug=True)
        response = account_api.get_account_balance(ccy="USDT")
        logger.info(f"Ответ API баланса OKX: {json.dumps(response, indent=2)}")
        if response.get("code") != "0":
            raise ValueError(f"Ошибка получения баланса: {response.get('msg')}")

        if not response.get("data") or not response["data"][0].get("details"):
            raise ValueError("Данные баланса отсутствуют или валюта USDT не найдена")

        for detail in response["data"][0]["details"]:
            if detail.get("ccy") == "USDT":
                avail_bal = detail.get("availBal") or detail.get("availEq")
                if avail_bal is None:
                    raise ValueError("Поле availBal или availEq не найдено для USDT")
                return float(avail_bal)

        raise ValueError("Валюта USDT не найдена в ответе API")
    except Exception as e:
        logger.error(f"Ошибка при получении баланса OKX: {str(e)}")
        raise


def set_leverage(symbol: str, leverage: int = 5, tdMode: str = "isolated", api_key: str = None,
                 secret_key: str = None, passphrase: str = None) -> bool:
    try:
        account_api = AccountAPI(api_key, secret_key, passphrase, flag="0", domain=APIURL, debug=True)

        # Пробуем разные варианты установки плеча
        params_variants = [
            # Вариант 1: Без posSide и без instId (глобальное плечо)
            {"lever": str(leverage), "mgnMode": tdMode},
            # Вариант 2: Только с instId
            {"lever": str(leverage), "mgnMode": tdMode, "instId": symbol},
            # Вариант 3: С instId и ccy
            {"lever": str(leverage), "mgnMode": tdMode, "instId": symbol, "ccy": "USDT"}
        ]

        response = None
        last_error = None

        for params in params_variants:
            try:
                logger.info(f"Пробуем установить плечо с параметрами: {params}")
                response = account_api.set_leverage(**params)

                if response.get("code") == "0":
                    logger.info(f"Плечо {leverage}x успешно установлено для {symbol} ({tdMode})")
                    logger.info(f"Ответ API установки плеча OKX: {json.dumps(response, indent=2)}")
                    return True
                else:
                    last_error = response.get('msg')
                    logger.warning(f"Не удалось установить плечо: {last_error}")

            except Exception as e:
                last_error = str(e)
                logger.warning(f"Ошибка при установке плеча: {last_error}")
                continue

        # Если все варианты не сработали, пробуем с posSide как последний вариант
        try:
            params = {"lever": str(leverage), "mgnMode": tdMode, "instId": symbol, "posSide": "long"}
            logger.info(f"Пробуем установить плечо с posSide: {params}")
            response = account_api.set_leverage(**params)

            if response.get("code") == "0":
                logger.info(f"Плечо {leverage}x успешно установлено для {symbol} с posSide")
                return True
            else:
                last_error = response.get('msg')
        except Exception as e:
            last_error = str(e)

        # Если ничего не помогло, логируем и продолжаем (плечо может быть уже установлено)
        logger.warning(f"Не удалось установить плечо для {symbol}. Последняя ошибка: {last_error}")
        logger.warning("Продолжаем выполнение - возможно плечо уже установлено")
        return True

    except Exception as e:
        logger.error(f"Критическая ошибка при установке плеча для {symbol}: {str(e)}")
        # Продолжаем выполнение даже при ошибке установки плеча
        logger.warning("Продолжаем выполнение несмотря на ошибку установки плеча")
        return True


def calculate_quantity(symbol: str, leverage: int = 5, risk_percent: float = 0.10, api_key: str = None,
                       secret_key: str = None, passphrase: str = None) -> float:
    try:
        usdt_balance = get_balance(api_key, secret_key, passphrase)
        if usdt_balance <= 0:
            raise ValueError("Недостаточно USDT на балансе!")

        current_price = get_current_price(symbol, api_key, secret_key, passphrase)
        symbol_info = get_symbol_info(symbol, api_key, secret_key, passphrase)

        lot_size = symbol_info["lotSz"]
        min_size = symbol_info["minSz"]
        ct_val = symbol_info["ctVal"]

        risk_amount = usdt_balance * risk_percent
        total_trade_amount = risk_amount * leverage
        quantity = total_trade_amount / current_price / ct_val

        required_margin = total_trade_amount / leverage
        logger.info(
            f"Баланс: {usdt_balance:.4f} USDT, Требуемая маржа: {required_margin:.4f} USDT, Количество: {quantity:.4f}")

        # Проверяем минимальную сумму ордера
        min_order_amount = 5  # Минимальная сумма в USDT
        order_amount = quantity * current_price * ct_val
        if order_amount < min_order_amount:
            logger.warning(f"Сумма ордера {order_amount:.2f} USDT меньше минимальной {min_order_amount} USDT")
            # Пересчитываем количество на основе минимальной суммы
            quantity = min_order_amount / current_price / ct_val
            logger.info(f"Используем минимальную сумму, новое количество: {quantity:.4f}")

        if required_margin > usdt_balance:
            raise ValueError(
                f"Недостаточно маржи: требуется {required_margin:.4f} USDT, доступно {usdt_balance:.4f} USDT")

        quantity = round(quantity / lot_size) * lot_size
        quantity = max(min_size, quantity)

        logger.info(f"Рассчитанное количество для {symbol}: {quantity} контрактов")
        return quantity
    except Exception as e:
        logger.error(f"Ошибка при расчете количества для {symbol}: {str(e)}")
        raise

def create_main_order(symbol: str, side: str, quantity: float, stop_loss: float, take_profits: list,
                      tdMode: str = "isolated", api_key: str = None, secret_key: str = None, passphrase: str = None):
    try:
        trade_api = TradeAPI(api_key, secret_key, passphrase, flag="0", domain=APIURL, debug=True)

        # Определяем направление для SL/TP ордеров
        sl_side = "buy" if side == "SELL" else "sell"

        symbol_info = get_symbol_info(symbol, api_key, secret_key, passphrase)
        lot_size = symbol_info["lotSz"]

        quantity = round(quantity / lot_size) * lot_size
        quantity_str = f"{quantity:.2f}"
        logger.info(f"Округленное количество для ордера: {quantity_str} контрактов")

        algo_orders = []

        # SL ордер
        algo_orders.append({
            "slTriggerPx": str(round(stop_loss, 4)),
            "slOrdPx": "-1",
            "sz": quantity_str,
            "side": sl_side,
            "tpTriggerPx": "",
            "tpOrdPx": "",
            "triggerPxType": "last"
        })

        # Распределение количества для TP ордеров
        total_lots = int(quantity / lot_size)
        tp_lots_base = total_lots // 3
        tp_lots_remainder = total_lots % 3

        tp_quantities = []
        for i in range(3):
            lots = tp_lots_base + (1 if i < tp_lots_remainder else 0)
            tp_qty = lots * lot_size
            tp_quantities.append(f"{tp_qty:.2f}")

        total_tp_size = sum(float(qty) for qty in tp_quantities)
        if abs(total_tp_size - float(quantity_str)) > 0.0001:
            raise ValueError(f"Сумма размеров TP ({total_tp_size}) не равна размеру основного ордера ({quantity_str})")

        sorted_take_profits = sorted(take_profits) if side == "BUY" else sorted(take_profits, reverse=True)

        # TP ордера
        for tp_price, tp_qty in zip(sorted_take_profits, tp_quantities):
            if tp_price is not None:
                algo_orders.append({
                    "tpTriggerPx": str(round(tp_price, 4)),
                    "tpOrdPx": "-1",
                    "sz": tp_qty,
                    "side": sl_side,
                    "slTriggerPx": "",
                    "slOrdPx": "",
                    "triggerPxType": "last"
                })

        logger.info(f"TP размеры: {tp_quantities}, сумма: {total_tp_size}, основной ордер: {quantity_str}")

        # Параметры основного ордера
        order_params = {
            "instId": symbol,
            "tdMode": tdMode,
            "side": side.lower(),
            "ordType": "market",
            "sz": quantity_str,
            "attachAlgoOrds": algo_orders
            # Убираем posSide так как он вызывает ошибку для некоторых инструментов
        }

        logger.info(f"Создание ордера с параметрами: {order_params}")

        response = trade_api.place_order(**order_params)
        logger.info(f"Ответ API создания ордера OKX: {json.dumps(response, indent=2)}")

        if response.get("code") != "0":
            # Если ошибка из-за posSide, пробуем без attachAlgoOrds сначала создать основной ордер
            if "Parameter posSide error" in response.get("msg", ""):
                logger.info("Пробуем создать основной ордер без алгоритмических ордеров...")

                # Сначала создаем основной ордер
                main_order_response = trade_api.place_order(
                    instId=symbol,
                    tdMode=tdMode,
                    side=side.lower(),
                    ordType="market",
                    sz=quantity_str
                )

                if main_order_response.get("code") != "0":
                    raise ValueError(f"Ошибка создания основного ордера: {main_order_response.get('msg')}")

                order_id = main_order_response["data"][0]["ordId"]
                logger.info(f"Основной ордер создан: {order_id}")

                # Затем создаем алгоритмические ордера отдельно
                algo_order_ids = []
                for algo_order in algo_orders:
                    time.sleep(0.5)  # Задержка между запросами

                    algo_params = {
                        "instId": symbol,
                        "tdMode": tdMode,
                        "side": algo_order["side"],
                        "ordType": "conditional",
                        "sz": algo_order["sz"],
                        "triggerPxType": algo_order["triggerPxType"]
                    }

                    # Добавляем параметры в зависимости от типа ордера
                    if algo_order.get("slTriggerPx"):
                        algo_params["slTriggerPx"] = algo_order["slTriggerPx"]
                        algo_params["slOrdPx"] = algo_order["slOrdPx"]
                    else:
                        algo_params["tpTriggerPx"] = algo_order["tpTriggerPx"]
                        algo_params["tpOrdPx"] = algo_order["tpOrdPx"]

                    algo_response = trade_api.place_algo_order(**algo_params)
                    if algo_response.get("code") == "0":
                        algo_id = algo_response["data"][0]["algoId"]
                        algo_order_ids.append(algo_id)
                        logger.info(f"Алгоритмический ордер создан: {algo_id}")
                    else:
                        logger.error(f"Ошибка создания алгоритмического ордера: {algo_response.get('msg')}")

                return main_order_response, sorted_take_profits, order_id, algo_order_ids
            else:
                raise ValueError(f"Ошибка создания ордера: {response.get('msg')}")

        order_id = response["data"][0]["ordId"]
        algo_order_ids = [order.get("algoId") for order in response["data"] if order.get("algoId")]

        return response, sorted_take_profits, order_id, algo_order_ids

    except Exception as e:
        logger.error(f"Ошибка при создании основного ордера для {symbol}: {str(e)}")
        raise

def get_order_status(symbol: str, order_id: str, api_key: str, secret_key: str, passphrase: str) -> dict:
    try:
        trade_api = TradeAPI(api_key, secret_key, passphrase, flag="0", domain=APIURL, debug=True)
        response = trade_api.get_order(instId=symbol, ordId=order_id)
        logger.info(f"Ответ API статуса ордера OKX: {json.dumps(response, indent=2)}")
        if response.get("code") != "0":
            raise ValueError(f"Ошибка получения статуса ордера: {response.get('msg')}")
        return response["data"][0]
    except Exception as e:
        logger.error(f"Ошибка при получении статуса ордера {order_id} для {symbol}: {str(e)}")
        raise

def close_position(symbol: str, posSide: str, api_key: str, secret_key: str, passphrase: str) -> bool:
    try:
        trade_api = TradeAPI(api_key, secret_key, passphrase, flag="0", domain=APIURL, debug=True)
        response = trade_api.close_positions(
            instId=symbol,
            mgnMode="isolated",
            posSide=posSide
        )
        logger.info(f"Ответ API закрытия позиции OKX: {json.dumps(response, indent=2)}")
        if response.get("code") != "0":
            raise ValueError(f"Ошибка закрытия позиции: {response.get('msg')}")
        logger.info(f"Позиция {posSide} для {symbol} успешно закрыта")
        return True
    except Exception as e:
        logger.error(f"Ошибка при закрытии позиции {posSide} для {symbol}: {str(e)}")
        raise

def cancel_order(symbol: str, order_id: str, api_key: str, secret_key: str, passphrase: str) -> bool:
    try:
        trade_api = TradeAPI(api_key, secret_key, passphrase, flag="1", domain=APIURL, debug=True)
        response = trade_api.cancel_order(instId=symbol, ordId=order_id)
        logger.info(f"Ответ API отмены ордера OKX: {json.dumps(response, indent=2)}")
        if response.get("code") != "0":
            raise ValueError(f"Ошибка отмены ордера: {response.get('msg')}")
        logger.info(f"Ордер {order_id} для {symbol} успешно отменён")
        return True
    except Exception as e:
        logger.error(f"Ошибка при отмене ордера {order_id} для {symbol}: {str(e)}")
        raise


def move_sl_to_breakeven(symbol: str, api_key: str, secret_key: str, passphrase: str) -> bool:
    """
    Перемещает стоп-лосс к цене входа для OKX
    """
    try:
        trade_api = TradeAPI(api_key, secret_key, passphrase, flag="0", domain=APIURL, debug=True)
        account_api = AccountAPI(api_key, secret_key, passphrase, flag="0", domain=APIURL, debug=True)

        # Получаем открытые позиции
        response = account_api.get_positions(instType="SWAP", instId=symbol)
        if response.get("code") != "0":
            raise ValueError(f"Ошибка получения позиций: {response.get('msg')}")

        positions = response.get("data", [])

        for position in positions:
            pos_side = position.get("posSide")
            position_amt = float(position.get("pos", 0))
            avg_price = float(position.get("avgPx", 0))

            if position_amt != 0 and avg_price > 0:
                # Получаем pending ордера (включая алгоритмические)
                orders_response = trade_api.get_order_list(instType="SWAP", instId=symbol, state="live")
                if orders_response.get("code") != "0":
                    raise ValueError(f"Ошибка получения ордеров: {orders_response.get('msg')}")

                # Получаем алгоритмические ордера отдельно
                algo_response = trade_api.get_order_list(
                    ordType="conditional",
                    instId=symbol,
                    state="live"
                )

                logger.info(f"Алгоритмические ордера: {json.dumps(algo_response, indent=2)}")

                # Ищем SL ордера (conditional ордера с slTriggerPx)
                sl_orders = []
                if algo_response.get("code") == "0":
                    algo_orders = algo_response.get("data", [])
                    sl_orders = [order for order in algo_orders
                                 if order.get("slTriggerPx") and order.get("posSide") == pos_side]

                # Отменяем старые SL ордера
                for sl_order in sl_orders:
                    algo_id = sl_order.get("algoId")
                    if algo_id:
                        cancel_response = trade_api.cancel_order(
                            instId=symbol,
                            ordId=sl_order.get("ordId"),
                        )
                        logger.info(f"Ответ отмены SL ордера: {json.dumps(cancel_response, indent=2)}")
                        if cancel_response.get("code") == "0":
                            logger.info(f"Старый SL ордер {algo_id} отменен")
                        else:
                            logger.warning(f"Не удалось отменить SL ордер {algo_id}: {cancel_response.get('msg')}")

                # Создаем новый SL ордер по цене входа
                quantity = abs(position_amt)
                new_sl_price = avg_price

                # Корректируем цену SL в зависимости от направления
                if pos_side == "long":
                    new_sl_price = avg_price * 0.999  # Чуть ниже для LONG
                    side = "sell"
                else:  # short
                    new_sl_price = avg_price * 1.001  # Чуть выше для SHORT
                    side = "buy"

                # Создаем новый SL ордер
                sl_order_params = {
                    "instId": symbol,
                    "tdMode": "isolated",
                    "side": side,
                    "posSide": pos_side,
                    "ordType": "conditional",
                    "sz": str(round(quantity, 2)),
                    "slTriggerPx": str(round(new_sl_price, 4)),
                    "slOrdPx": "-1",
                    "tpTriggerPx": "",
                    "tpOrdPx": "",
                    "triggerPxType": "last"
                }

                logger.info(f"Создание нового SL ордера с параметрами: {sl_order_params}")

                create_response = trade_api.place_algo_order(**sl_order_params)
                logger.info(f"Ответ создания SL ордера: {json.dumps(create_response, indent=2)}")

                if create_response.get("code") == "0":
                    new_sl_algo_id = create_response["data"][0]["algoId"]
                    logger.info(f"Новый SL ордер {new_sl_algo_id} создан по цене {new_sl_price}")

                    # Обновляем базу данных
                    cursor = get_cursor()
                    cursor.execute(
                        """
                        UPDATE trades 
                        SET stop_loss = %s, sl_order_id = %s 
                        WHERE user_id = (SELECT user_id FROM users WHERE api_key = %s) 
                        AND symbol = %s AND status = 'open'
                        """,
                        (new_sl_price, new_sl_algo_id, api_key, symbol)
                    )
                    commit()

                    return True
                else:
                    raise ValueError(f"Ошибка создания нового SL ордера: {create_response.get('msg')}")

        logger.info(f"Нет открытых позиций для {symbol} или позиция уже закрыта")
        return True

    except Exception as e:
        logger.error(f"Ошибка при перемещении SL для {symbol} на OKX: {str(e)}")
        raise