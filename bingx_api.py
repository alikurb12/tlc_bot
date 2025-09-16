import time
import requests
import hmac
from hashlib import sha256
import json
import logging

logger = logging.getLogger(__name__)

APIURL = "https://open-api.bingx.com"

def get_balance(api_key: str, secret_key: str) -> str:
    path = '/openApi/swap/v2/user/balance'
    method = "GET"
    paramsMap = {}
    paramsSrt = parseParam(paramsMap)
    return send_request(method, path, paramsSrt, {}, api_key, secret_key)

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
        responce = send_request(method, path, paramsStr, {}, api_key, secret_key)
        responce_data = json.loads(responce)
        if responce_data.get("code")!=0:
            raise ValueError(f"Ошибка установления плеча: {responce_data.get('msg')}")
        logger.info(f"Плечо {leverage} установлено для {symbol} side = {position_side}")
        return True
    except Exception as e:
        logger.error(f"Ошибка при установке плеча для {symbol} side={position_side}")
        raise


def calculate_quantity(symbol: str, leverage: int = 5, risk_percent: float = 0.10, api_key: str = None,
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

    tp_quantities = [quantity / 3, quantity / 3, quantity / 3]
    sorted_take_profits = sorted(take_profits) if side == "BUY" else sorted(take_profits, reverse=True)

    for i, (tp_price, tp_qty) in enumerate(zip(sorted_take_profits, tp_quantities)):
        if tp_price is not None:
            tp_order = {
                "symbol": symbol,
                "side": "SELL" if side == "BUY" else "BUY",
                "positionSide": "LONG" if side == "BUY" else "SHORT",
                "type": "TAKE_PROFIT_MARKET",
                "quantity": round(tp_qty, 3),
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

def get_sign(api_secret: str, payload: str) -> str:
    signature = hmac.new(api_secret.encode("utf-8"), payload.encode("utf-8"), digestmod=sha256).hexdigest()
    logger.info("sign=%s", signature)
    return signature

def send_request(method: str, path: str, urlpa: str, payload: dict, api_key: str, secret_key: str) -> str:
    url = f"{APIURL}{path}?{urlpa}&signature={get_sign(secret_key, urlpa)}"
    logger.info("Request URL: %s", url)
    headers = {'X-BX-APIKEY': api_key}
    response = requests.request(method, url, headers=headers, data=payload)
    return response.text

def parseParam(paramsMap: dict) -> str:
    sortedKeys = sorted(paramsMap)
    paramsStr = "&".join(["%s=%s" % (x, paramsMap[x]) for x in sortedKeys])
    return paramsStr + "&timestamp=" + str(int(time.time() * 1000)) if paramsStr else "timestamp=" + str(int(time.time() * 1000))
