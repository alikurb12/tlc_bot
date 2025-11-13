import logging
from typing import Dict, List, Optional
from pybit.unified_trading import HTTP
from database import get_cursor, commit

logger = logging.getLogger(__name__)

class BybitAPI:
    def __init__(self, api_key: str, secret_key: str, testnet: bool = False):
        self.session = HTTP(
            testnet=testnet,
            api_key=api_key,
            api_secret=secret_key
        )
        self.api_key = api_key

    def get_symbol_info(self, symbol: str) -> Dict:
        try:
            response = self.session.get_instruments_info(category="linear", symbol=symbol)
            if response["retCode"] != 0:
                raise ValueError(f"Ошибка API: {response['retMsg']}")
            instrument = response["result"]["list"][0]
            return {
                "lotSizeFilter": {
                    "qtyStep": float(instrument["lotSizeFilter"]["qtyStep"]),
                    "minOrderQty": float(instrument["lotSizeFilter"]["minOrderQty"])
                },
                "leverageFilter": {
                    "maxLeverage": int(float(instrument["leverageFilter"]["maxLeverage"]))
                }
            }
        except Exception as e:
            logger.error(f"Ошибка при получении информации о символе {symbol}: {str(e)}")
            raise

    def get_current_price(self, symbol: str) -> float:
        try:
            response = self.session.get_tickers(category="linear", symbol=symbol)
            if response["retCode"] != 0:
                raise ValueError(f"Ошибка API: {response['retMsg']}")
            return float(response["result"]["list"][0]["lastPrice"])
        except Exception as e:
            logger.error(f"Ошибка при получении цены для {symbol}: {str(e)}")
            raise

    def get_balance(self) -> float:
        try:
            response = self.session.get_wallet_balance(accountType="UNIFIED")
            if response["retCode"] != 0:
                raise ValueError(f"Ошибка API: {response['retMsg']}")
            for coin in response["result"]["list"][0]["coin"]:
                if coin["coin"] == "USDT":
                    return float(coin["availableToWithdraw"])
            raise ValueError("USDT не найден в балансе")
        except Exception as e:
            logger.error(f"Ошибка при получении баланса Bybit: {str(e)}")
            raise

    def set_leverage(self, symbol: str, leverage: int = 5, tdMode: str = "isolated") -> bool:
        try:
            response = self.session.set_leverage(
                category="linear",
                symbol=symbol,
                buyLeverage=str(leverage),
                sellLeverage=str(leverage)
            )
            if response["retCode"] != 0:
                logger.warning(f"Ошибка установки плеча для {symbol}: {response['retMsg']}")
                return True  # Продолжаем, если плечо уже установлено
            logger.info(f"Плечо {leverage}x установлено для {symbol}")
            return True
        except Exception as e:
            logger.error(f"Ошибка при установке плеча для {symbol}: {str(e)}")
            return True

    def calculate_quantity(self, symbol: str, leverage: int = 5, risk_percent: float = 0.05) -> float:
        try:
            usdt_balance = self.get_balance()
            if usdt_balance <= 0:
                raise ValueError("Недостаточно USDT на балансе!")

            current_price = self.get_current_price(symbol)
            symbol_info = self.get_symbol_info(symbol)

            qty_step = symbol_info["lotSizeFilter"]["qtyStep"]
            min_qty = symbol_info["lotSizeFilter"]["minOrderQty"]

            risk_amount = usdt_balance * risk_percent
            total_trade_amount = risk_amount * leverage
            quantity = total_trade_amount / current_price

            required_margin = total_trade_amount / leverage
            if required_margin > usdt_balance:
                raise ValueError(
                    f"Недостаточно маржи: требуется {required_margin:.4f} USDT, доступно {usdt_balance:.4f} USDT"
                )

            quantity = round(quantity / qty_step) * qty_step
            quantity = max(min_qty, quantity)

            logger.info(f"Рассчитанное количество для {symbol}: {quantity}")
            return quantity
        except Exception as e:
            logger.error(f"Ошибка при расчете количества для {symbol}: {str(e)}")
            raise

    def create_main_order(
        self,
        symbol: str,
        side: str,
        quantity: float,
        stop_loss: float,
        take_profits: List[Optional[float]],
        tdMode: str = "isolated"
    ) -> tuple:
        try:
            symbol_info = self.get_symbol_info(symbol)
            qty_step = symbol_info["lotSizeFilter"]["qtyStep"]
            quantity = round(quantity / qty_step) * qty_step

            # Создаем основной ордер
            main_response = self.session.place_order(
                category="linear",
                symbol=symbol,
                side=side.capitalize(),
                orderType="Market",
                qty=str(quantity),
                timeInForce="GTC",
                positionIdx=0  # Хедж-режим, односторонняя позиция
            )
            if main_response["retCode"] != 0:
                raise ValueError(f"Ошибка создания ордера: {main_response['retMsg']}")
            order_id = main_response["result"]["orderId"]

            algo_orders = []
            sl_side = "Sell" if side == "BUY" else "Buy"
            sorted_take_profits = sorted(take_profits) if side == "BUY" else sorted(take_profits, reverse=True)
            valid_take_profits = [tp for tp in sorted_take_profits if tp is not None]

            total_lots = int(quantity / qty_step)
            tp_lots_base = total_lots // 3
            tp_lots_remainder = total_lots % 3
            tp_quantities = []
            for i in range(3):
                lots = tp_lots_base + (1 if i < tp_lots_remainder else 0)
                tp_qty = lots * qty_step
                tp_quantities.append(tp_qty)

            total_tp_size = sum(tp_quantities)
            if abs(total_tp_size - quantity) > 0.0001:
                correction = quantity - total_tp_size
                tp_quantities[-1] = tp_quantities[-1] + correction

            algo_orders.append(self.session.place_order(
                category="linear",
                symbol=symbol,
                side=sl_side,
                orderType="Market",
                qty=str(quantity),
                stopLoss=str(round(stop_loss, 4)),
                timeInForce="GTC",
                positionIdx=0,
                triggerDirection=1 if side == "BUY" else 2
            ))

            algo_order_ids = [order_id]
            for tp_price, tp_qty in zip(valid_take_profits, tp_quantities):
                if tp_price is not None:
                    tp_response = self.session.place_order(
                        category="linear",
                        symbol=symbol,
                        side=sl_side,
                        orderType="Limit",
                        qty=str(tp_qty),
                        price=str(round(tp_price, 4)),
                        timeInForce="GTC",
                        positionIdx=0
                    )
                    if tp_response["retCode"] != 0:
                        logger.error(f"Ошибка создания TP ордера: {tp_response['retMsg']}")
                        continue
                    algo_order_ids.append(tp_response["result"]["orderId"])

            logger.info(f"Основной ордер создан: {order_id}, TP/SL ордера: {algo_order_ids[1:]}")
            return main_response, valid_take_profits, order_id, algo_order_ids[1:], "net"

        except Exception as e:
            logger.error(f"Ошибка при создании основного ордера для {symbol}: {str(e)}")
            raise

    def get_order_status(self, symbol: str, order_id: str) -> Dict:
        try:
            response = self.session.get_order_history(category="linear", symbol=symbol, orderId=order_id)
            if response["retCode"] != 0:
                raise ValueError(f"Ошибка API: {response['retMsg']}")
            return response["result"]["list"][0]
        except Exception as e:
            logger.error(f"Ошибка при получении статуса ордера {order_id} для {symbol}: {str(e)}")
            raise

    def close_position(self, symbol: str, posSide: str) -> bool:
        try:
            response = self.session.get_positions(category="linear", symbol=symbol)
            if response["retCode"] != 0:
                raise ValueError(f"Ошибка API: {response['retMsg']}")
            positions = response["result"]["list"]
            for pos in positions:
                qty = float(pos["size"])
                side = "Sell" if pos["side"] == "Buy" else "Buy"
                if qty > 0:
                    self.session.place_order(
                        category="linear",
                        symbol=symbol,
                        side=side,
                        orderType="Market",
                        qty=str(qty),
                        timeInForce="GTC",
                        positionIdx=0
                    )
                    logger.info(f"Позиция для {symbol} закрыта")
            return True
        except Exception as e:
            logger.error(f"Ошибка при закрытии позиции для {symbol}: {str(e)}")
            raise

    def cancel_order(self, symbol: str, order_id: str) -> bool:
        try:
            response = self.session.cancel_order(category="linear", symbol=symbol, orderId=order_id)
            if response["retCode"] != 0:
                raise ValueError(f"Ошибка API: {response['retMsg']}")
            logger.info(f"Ордер {order_id} для {symbol} отменён")
            return True
        except Exception as e:
            logger.error(f"Ошибка при отмене ордера {order_id} для {symbol}: {str(e)}")
            raise

    def move_sl_to_breakeven(self, symbol: str) -> bool:
        try:
            response = self.session.get_positions(category="linear", symbol=symbol)
            if response["retCode"] != 0:
                raise ValueError(f"Ошибка API: {response['retMsg']}")
            positions = response["result"]["list"]

            for position in positions:
                side = position["side"]
                qty = float(position["size"])
                avg_price = float(position["avgPrice"])

                if qty > 0 and avg_price > 0:
                    orders_response = self.session.get_open_orders(category="linear", symbol=symbol)
                    if orders_response["retCode"] != 0:
                        raise ValueError(f"Ошибка API: {orders_response['retMsg']}")
                    sl_orders = [order for order in orders_response["result"]["list"]
                                 if order.get("stopLoss")]

                    for order in sl_orders:
                        self.cancel_order(symbol, order["orderId"])

                    new_sl_price = avg_price * (0.999 if side == "Buy" else 1.001)
                    sl_response = self.session.place_order(
                        category="linear",
                        symbol=symbol,
                        side="Sell" if side == "Buy" else "Buy",
                        orderType="Market",
                        qty=str(qty),
                        stopLoss=str(round(new_sl_price, 4)),
                        timeInForce="GTC",
                        positionIdx=0,
                        triggerDirection=1 if side == "Buy" else 2
                    )
                    if sl_response["retCode"] != 0:
                        raise ValueError(f"Ошибка создания SL: {sl_response['retMsg']}")
                    new_sl_order_id = sl_response["result"]["orderId"]

                    cursor = get_cursor()
                    cursor.execute(
                        """
                        UPDATE trades 
                        SET stop_loss = %s, sl_order_id = %s 
                        WHERE user_id = (SELECT user_id FROM users WHERE api_key = %s) 
                        AND symbol = %s AND status = 'open'
                        """,
                        (new_sl_price, new_sl_order_id, self.api_key, symbol)
                    )
                    commit()
                    logger.info(f"SL перемещён к {new_sl_price} для {symbol}")
                    return True

            logger.info(f"Нет открытых позиций для {symbol}")
            return True
        except Exception as e:
            logger.error(f"Ошибка при перемещении SL для {symbol} на Bybit: {str(e)}")
            raise

def get_symbol_info(symbol: str, api_key: str, secret_key: str, passphrase: str = None) -> Dict:
    return BybitAPI(api_key, secret_key).get_symbol_info(symbol)

def get_current_price(symbol: str, api_key: str, secret_key: str, passphrase: str = None) -> float:
    return BybitAPI(api_key, secret_key).get_current_price(symbol)

def get_balance(api_key: str, secret_key: str, passphrase: str = None) -> float:
    return BybitAPI(api_key, secret_key).get_balance()

def set_leverage(symbol: str, leverage: int = 5, tdMode: str = "isolated", api_key: str = None,
                 secret_key: str = None, passphrase: str = None) -> bool:
    return BybitAPI(api_key, secret_key).set_leverage(symbol, leverage, tdMode)

def calculate_quantity(symbol: str, leverage: int = 5, risk_percent: float = 0.05, api_key: str = None,
                       secret_key: str = None, passphrase: str = None) -> float:
    return BybitAPI(api_key, secret_key).calculate_quantity(symbol, leverage, risk_percent)

def create_main_order(symbol: str, side: str, quantity: float, stop_loss: float, take_profits: List[Optional[float]],
                     tdMode: str = "isolated", api_key: str = None, secret_key: str = None, passphrase: str = None):
    return BybitAPI(api_key, secret_key).create_main_order(symbol, side, quantity, stop_loss, take_profits, tdMode)

def get_order_status(symbol: str, order_id: str, api_key: str, secret_key: str, passphrase: str = None) -> Dict:
    return BybitAPI(api_key, secret_key).get_order_status(symbol, order_id)

def close_position(symbol: str, posSide: str, api_key: str, secret_key: str, passphrase: str = None) -> bool:
    return BybitAPI(api_key, secret_key).close_position(symbol, posSide)

def cancel_order(symbol: str, order_id: str, api_key: str, secret_key: str, passphrase: str = None) -> bool:
    return BybitAPI(api_key, secret_key).cancel_order(symbol, order_id)

def move_sl_to_breakeven(symbol: str, api_key: str, secret_key: str, passphrase: str = None) -> bool:
    return BybitAPI(api_key, secret_key).move_sl_to_breakeven(symbol)