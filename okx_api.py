import json
import logging
from okx.PublicData import PublicAPI
from okx.Trade import TradeAPI
from okx.Account import AccountAPI
from okx.MarketData import MarketAPI

logger = logging.getLogger(__name__)

APIURL = "https://www.okx.com"


def get_symbol_info(symbol: str, api_key: str, secret_key: str, passphrase: str) -> dict:
    try:
        pub_api = PublicAPI(flag="0", domain=APIURL, debug=True)
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


def set_leverage(symbol: str, leverage: int = 5, tdMode: str = "isolated", posSide: str = "long", api_key: str = None,
                 secret_key: str = None, passphrase: str = None) -> bool:
    try:
        account_api = AccountAPI(api_key, secret_key, passphrase, flag="0", domain=APIURL, debug=True)
        response = account_api.set_leverage(
            lever=str(leverage),
            mgnMode=tdMode,
            instId=symbol,
            posSide=posSide
        )
        logger.info(f"Ответ API установки плеча OKX: {json.dumps(response, indent=2)}")
        if response.get("code") != "0":
            raise ValueError(f"Ошибка установки плеча: {response.get('msg')}")
        logger.info(f"Плечо {leverage}x установлено для {symbol} ({posSide})")
        return True
    except Exception as e:
        logger.error(f"Ошибка при установке плеча для {symbol}: {str(e)}")
        raise


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
        posSide = "long" if side == "BUY" else "short"
        sl_side = "sell" if side == "BUY" else "buy"

        symbol_info = get_symbol_info(symbol, api_key, secret_key, passphrase)
        lot_size = symbol_info["lotSz"]

        quantity = round(quantity / lot_size) * lot_size
        quantity_str = f"{quantity:.2f}"
        logger.info(f"Округленное количество для ордера: {quantity_str} контрактов")

        algo_orders = []
        algo_orders.append({
            "slTriggerPx": str(round(stop_loss, 4)),
            "slOrdPx": "-1",
            "sz": quantity_str,
            "side": sl_side,
            "tpTriggerPx": "",
            "tpOrdPx": "",
            "triggerPxType": "last"
        })

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

        response = trade_api.place_order(
            instId=symbol,
            tdMode=tdMode,
            side=side.lower(),
            posSide=posSide,
            ordType="market",
            sz=quantity_str,
            attachAlgoOrds=algo_orders
        )
        logger.info(f"Ответ API создания ордера OKX: {json.dumps(response, indent=2)}")
        if response.get("code") != "0":
            raise ValueError(f"Ошибка создания ордера: {response.get('msg')}")

        order_id = response["data"][0]["ordId"]
        algo_order_ids = [order.get("algoId") for order in response["data"] if order.get("algoId")]

        return response, sorted_take_profits, order_id, algo_order_ids
    except Exception as e:
        logger.error(f"Ошибка при создании основного ордера для {symbol}: {str(e)}")
        raise