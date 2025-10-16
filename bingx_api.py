import time
import requests
import hmac
from hashlib import sha256
import json
import logging

from database import get_cursor, commit

logger = logging.getLogger(__name__)

APIURL = "https://open-api.bingx.com"
TIME_OFFSET = 0

def get_server_time() -> int:
    try:
        url = f"{APIURL}/openApi/swap/v2/server/time"
        response = requests.get(url)
        data = response.json()
        if 'code' in data and data['code'] == 0:
            server_time = int(data['data']['serverTime'])
            local_time = int(time.time() * 1000)
            offset = server_time - local_time
            logger.info(f"Синхронизация времени: server_time={server_time}, local_time={local_time}, offset={offset} ms")
            return offset
        else:
            raise ValueError(f"Ошибка получения времени сервера: {data.get('msg')}")
    except Exception as e:
        logger.error(f"Ошибка при получении времени сервера: {str(e)}")
        return 0

def get_balance(api_key: str, secret_key: str) -> str:
    path = '/openApi/swap/v2/user/balance'
    method = "GET"
    paramsMap = {}
    paramsStr = parseParam(paramsMap)
    return send_request(method, path, paramsStr, {}, api_key, secret_key)

def get_current_price(symbol: str) -> float:
    try:
        url = f"{APIURL}/openApi/swap/v2/quote/price?symbol={symbol}"
        response = requests.get(url)
        data = response.json()
        if 'data' in data and 'price' in data['data']:
            return float(data['data']['price'])
        elif 'data' in data and 'lastPrice' in data['data']:
            return float(data['data']['lastPrice'])
        else:
            raise ValueError(f"Не удалось получить цену. Ответ API: {data}")
    except Exception as e:
        logger.error(f"Ошибка при получении цены: {e}")
        raise

def get_symbol_info(symbol: str) -> dict:
    try:
        url = f"{APIURL}/openApi/swap/v2/quote/contracts"
        response = requests.get(url)
        data = response.json()
        if 'data' in data:
            for contract in data['data']:
                if contract['symbol'] == symbol:
                    return {
                        "minQty": contract.get("minTradeVolume", 0.001),
                        "stepSize": contract.get("volumePrecision", 0.001)
                    }
        raise ValueError(f"Пара {symbol} не найдена")
    except Exception as e:
        logger.error(f"Ошибка при получении информации о паре: {symbol}")
        raise

def set_leverage(symbol: str, leverage: int=5, position_side: str="LONG", api_key: str=None, secret_key: str=None) -> bool:
    try:
        path = '/openApi/swap/v2/trade/leverage'
        method = "POST"
        paramsMap = {
            "symbol": symbol,
            "leverage": leverage,
            "side": position_side,
        }
        paramsStr = parseParam(paramsMap)
        response = send_request(method, path, paramsStr, {}, api_key, secret_key)
        response_data = json.loads(response)
        if response_data.get("code") != 0:
            raise ValueError(f"Ошибка установления плеча: {response_data.get('msg')}")
        logger.info(f"Плечо {leverage} установлено для {symbol} side = {position_side}")
        return True
    except Exception as e:
        logger.error(f"Ошибка при установке плеча для {symbol} side={position_side}: {str(e)}")
        raise

def calculate_quantity(symbol: str, leverage: int = 5, risk_percent: float = 0.05, api_key: str = None,
                       secret_key: str = None) -> float:
    try:
        balance_response = get_balance(api_key, secret_key)
        balance_data = json.loads(balance_response)
        usdt_balance = float(balance_data["data"]["balance"]["availableMargin"])

        if usdt_balance <= 0:
            raise ValueError("Недостаточно USDT на балансе!")

        current_price = get_current_price(symbol)
        symbol_info = get_symbol_info(symbol)

        min_qty = float(symbol_info["minQty"])
        step_size = float(symbol_info["stepSize"])

        risk_amount = usdt_balance * risk_percent
        total_trade_amount = risk_amount * leverage
        quantity = total_trade_amount / current_price

        required_margin = total_trade_amount / leverage * 1.001
        if required_margin > usdt_balance:
            raise ValueError(
                f"Недостаточно маржи: требуется {required_margin:.4f} USDT, доступно {usdt_balance:.4f} USDT")

        quantity = round(quantity / step_size) * step_size
        quantity = max(min_qty, quantity)

        return quantity
    except Exception as e:
        logger.error(f"Ошибка при расчете количества: {str(e)}")
        raise

def create_main_order(symbol: str, side: str, quantity: float, api_key: str, secret_key: str) -> str:
    path = '/openApi/swap/v2/trade/order'
    method = "POST"
    paramsMap = {
        "symbol": symbol,
        "side": side,
        "positionSide": "LONG" if side == "BUY" else "SHORT",
        "type": "MARKET",
        "quantity": quantity
    }
    paramsStr = parseParam(paramsMap)
    return send_request(method, path, paramsStr, {}, api_key, secret_key)

def create_tp_sl_orders(symbol: str, side: str, quantity: float, stop_loss: float, take_profits: list, api_key: str,
                        secret_key: str):
    orders = []
    stop_order = {
        "symbol": symbol,
        "side": "SELL" if side == "BUY" else "BUY",
        "positionSide": "LONG" if side == "BUY" else "SHORT",
        "type": "STOP_MARKET",
        "quantity": round(quantity, 3),
        "stopPrice": stop_loss
    }
    orders.append(stop_order)

    current_price = get_current_price(symbol)
    sorted_take_profits = sorted(take_profits) if side == "BUY" else sorted(take_profits, reverse=True)

    for tp_price in sorted_take_profits:
        if tp_price is not None:
            if side == "BUY" and tp_price <= current_price:
                logger.warning(f"Пропущен TP ордер для {symbol}: TP цена {tp_price} ниже текущей цены {current_price}")
                continue
            if side == "SELL" and tp_price >= current_price:
                logger.warning(f"Пропущен TP ордер для {symbol}: TP цена {tp_price} выше текущей цены {current_price}")
                continue
            tp_order = {
                "symbol": symbol,
                "side": "SELL" if side == "BUY" else "BUY",
                "positionSide": "LONG" if side == "BUY" else "SHORT",
                "type": "TAKE_PROFIT_MARKET",
                "quantity": round(quantity, 3),
                "stopPrice": tp_price
            }
            orders.append(tp_order)

    results = []
    order_ids = []
    for order in orders:
        time.sleep(0.5)
        paramsStr = parseParam(order)
        response = send_request("POST", '/openApi/swap/v2/trade/order', paramsStr, {}, api_key, secret_key)
        response_data = json.loads(response)
        if response_data.get("code") == 0:
            order_id = response_data["data"]["order"]["orderId"]
            order_ids.append(order_id)
        else:
            logger.error(f"Ошибка создания TP/SL ордера: {response_data.get('msg')}")
        results.append(response)
        logger.info(f"TP/SL ордер response: {response}")

    return results, sorted_take_profits, order_ids

def get_open_orders(symbol: str, api_key: str, secret_key: str) -> dict:
    try:
        path = '/openApi/swap/v2/trade/openOrders'
        method = "GET"
        paramsMap = {"symbol": symbol}
        paramsStr = parseParam(paramsMap)
        response = send_request(method, path, paramsStr, {}, api_key, secret_key)
        logger.info(f"Open orders response: {response}")
        return json.loads(response)
    except Exception as e:
        logger.error(f"Ошибка при получении открытых ордеров: {str(e)}")
        raise

def cancel_order(symbol: str, order_id: str, api_key: str, secret_key: str) -> bool:
    try:
        path = '/openApi/swap/v2/trade/order'
        method = "DELETE"
        paramsMap = {
            "symbol": symbol,
            "orderId": order_id
        }
        paramsStr = parseParam(paramsMap)
        response = send_request(method, path, paramsStr, {}, api_key, secret_key)
        response_data = json.loads(response)
        if response_data.get("code") != 0:
            raise ValueError(f"Ошибка отмены ордера: {response_data.get('msg')}")
        logger.info(f"Ордер {order_id} для {symbol} успешно отменён")
        return True
    except Exception as e:
        logger.error(f"Ошибка при отмене ордера {order_id} для {symbol}: {str(e)}")
        raise


def close_position(symbol: str, position_side: str, api_key: str, secret_key: str) -> bool:
    try:
        # Получаем открытые позиции
        open_positions = get_open_positions(symbol, api_key, secret_key)
        positionAmt = 0.0

        for position in open_positions:
            if position.get("positionSide") == position_side:
                positionAmt = float(position.get("positionAmt", 0))
                break

        if positionAmt <= 0:
            logger.info(f"Нет открытой позиции для {symbol} на стороне {position_side}")
            return True

        # В режиме хеджирования закрываем позицию противоположным ордером без reduceOnly
        path = '/openApi/swap/v2/trade/order'
        method = "POST"
        paramsMap = {
            "symbol": symbol,
            "side": "SELL" if position_side == "LONG" else "BUY",
            "positionSide": position_side,
            "type": "MARKET",
            "quantity": abs(positionAmt)  # Используем абсолютное значение
        }
        paramsStr = parseParam(paramsMap)
        response = send_request(method, path, paramsStr, {}, api_key, secret_key)
        response_data = json.loads(response)
        if response_data.get("code") != 0:
            raise ValueError(f"Ошибка закрытия позиции: {response_data.get('msg')}")
        logger.info(f"Позиция {position_side} для {symbol} успешно закрыта")
        return True
    except Exception as e:
        if "position not exist" in str(e).lower() or "order not exist" in str(e).lower():
            logger.info(f"Позиция {position_side} для {symbol} уже не существует")
            return True
        logger.error(f"Ошибка при закрытии позиции {position_side} для {symbol}: {str(e)}")
        raise

def get_open_positions(symbol: str, api_key: str, secret_key: str) -> list:
    try:
        path = '/openApi/swap/v2/user/positions'
        method = "GET"
        paramsMap = {"symbol": symbol}
        paramsStr = parseParam(paramsMap)
        response = send_request(method, path, paramsStr, {}, api_key, secret_key)
        response_data = json.loads(response)
        logger.info(f"Open positions response: {response}")
        if response_data.get("code") != 0:
            raise ValueError(f"Ошибка получения позиций: {response_data.get('msg')}")
        return response_data.get("data", [])
    except Exception as e:
        logger.error(f"Ошибка при получении открытых позиций для {symbol}: {str(e)}")
        raise

def get_sign(api_secret: str, payload: str) -> str:
    signature = hmac.new(api_secret.encode("utf-8"), payload.encode("utf-8"), digestmod=sha256).hexdigest()
    logger.info("sign=%s", signature)
    return signature

def send_request(method: str, path: str, urlpa: str, payload: dict, api_key: str, secret_key: str, retries: int = 3) -> str:
    global TIME_OFFSET
    attempt = 0
    while attempt < retries:
        try:
            url = f"{APIURL}{path}?{urlpa}&signature={get_sign(secret_key, urlpa)}"
            logger.info("Request URL: %s", url)
            headers = {'X-BX-APIKEY': api_key}
            response = requests.request(method, url, headers=headers, data=payload)
            response_data = response.json()
            if response_data.get("code") in [109414, 109500] and "timestamp is invalid" in response_data.get("msg", "").lower():
                logger.warning(f"Недопустимый timestamp, попытка {attempt + 1}/{retries}. Повторная синхронизация времени...")
                TIME_OFFSET = get_server_time()
                urlpa = urlpa.split("&timestamp=")[0] + "&timestamp=" + str(int(time.time() * 1000) + TIME_OFFSET)
                attempt += 1
                time.sleep(1)
                continue
            return response.text
        except Exception as e:
            logger.error(f"Ошибка запроса (попытка {attempt + 1}/{retries}): {str(e)}")
            if attempt < retries - 1:
                TIME_OFFSET = get_server_time()
                urlpa = urlpa.split("&timestamp=")[0] + "&timestamp=" + str(int(time.time() * 1000) + TIME_OFFSET)
                time.sleep(1)
            attempt += 1
    raise ValueError(f"Не удалось выполнить запрос после {retries} попыток")

def parseParam(paramsMap: dict) -> str:
    global TIME_OFFSET
    if not hasattr(parseParam, 'TIME_OFFSET'):
        parseParam.TIME_OFFSET = get_server_time()
        TIME_OFFSET = parseParam.TIME_OFFSET
    sortedKeys = sorted(paramsMap)
    paramsStr = "&".join(["%s=%s" % (x, paramsMap[x]) for x in sortedKeys])
    timestamp = int(time.time() * 1000) + TIME_OFFSET
    return paramsStr + "&timestamp=" + str(timestamp) if paramsStr else "timestamp=" + str(timestamp)


def move_sl_to_breakeven(symbol: str, api_key: str, secret_key: str) -> bool:
    """
    Перемещает стоп-лосс к цене входа для открытой позиции
    """
    try:
        # Получаем открытые позиции
        open_positions = get_open_positions(symbol, api_key, secret_key)

        for position in open_positions:
            position_side = position.get("positionSide")
            position_amt = float(position.get("positionAmt", 0))
            avg_price = float(position.get("avgPrice", 0))

            if position_amt != 0 and avg_price > 0:
                # Получаем открытые ордера стоп-лосса
                open_orders = get_open_orders(symbol, api_key, secret_key)
                sl_orders = [order for order in open_orders.get("data", {}).get("orders", [])
                             if order.get("type") == "STOP_MARKET"
                             and order.get("positionSide") == position_side]

                # Отменяем старые SL ордера
                for sl_order in sl_orders:
                    order_id = sl_order.get("orderId")
                    if order_id:
                        cancel_order(symbol, order_id, api_key, secret_key)
                        logger.info(f"Старый SL ордер {order_id} отменен")

                # Создаем новый SL ордер по цене входа
                quantity = abs(position_amt)
                new_sl_price = avg_price  # Перемещаем SL к цене входа

                # Для LONG позиции SL должен быть ниже цены входа, для SHORT - выше
                if position_side == "LONG":
                    # Для LONG устанавливаем SL чуть ниже цены входа (например, на 0.1%)
                    new_sl_price = avg_price * 0.999
                else:  # SHORT
                    # Для SHORT устанавливаем SL чуть выше цены входа (например, на 0.1%)
                    new_sl_price = avg_price * 1.001

                # Создаем новый SL ордер
                sl_order_params = {
                    "symbol": symbol,
                    "side": "SELL" if position_side == "LONG" else "BUY",
                    "positionSide": position_side,
                    "type": "STOP_MARKET",
                    "quantity": round(quantity, 3),
                    "stopPrice": round(new_sl_price, 4)
                }

                paramsStr = parseParam(sl_order_params)
                response = send_request("POST", '/openApi/swap/v2/trade/order', paramsStr, {}, api_key, secret_key)
                response_data = json.loads(response)

                if response_data.get("code") == 0:
                    new_sl_order_id = response_data["data"]["order"]["orderId"]
                    logger.info(f"Новый SL ордер {new_sl_order_id} создан по цене {new_sl_price}")

                    # Обновляем базу данных
                    cursor = get_cursor()
                    cursor.execute(
                        """
                        UPDATE trades 
                        SET stop_loss = %s, sl_order_id = %s 
                        WHERE user_id = (SELECT user_id FROM users WHERE api_key = %s) 
                        AND symbol = %s AND status = 'open'
                        """,
                        (new_sl_price, new_sl_order_id, api_key, symbol)
                    )
                    commit()

                    return True
                else:
                    raise ValueError(f"Ошибка создания нового SL ордера: {response_data.get('msg')}")

        logger.info(f"Нет открытых позиций для {symbol} или позиция уже закрыта")
        return True

    except Exception as e:
        logger.error(f"Ошибка при перемещении SL для {symbol}: {str(e)}")
        raise
