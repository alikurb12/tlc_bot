import logging
import time
from typing import Dict, List, Optional
import pybitget
from database import get_cursor, commit

logger = logging.getLogger(__name__)


class BitgetAPI:
    def __init__(self, api_key: str, secret_key: str, passphrase: str, testnet: bool = False):
        self.client = pybitget.Bitget(
            api_key=api_key,
            api_secret=secret_key,
            passphrase=passphrase,
            base_url="https://api.bitget.com" if not testnet else "https://capi.bitget.com"
        )
        self.api_key = api_key

    def get_symbol_info(self, symbol: str) -> Dict:
        """Получает информацию о торговой паре"""
        try:
            response = self.client.mix_get_symbols("umcbl")  # umcbl для USDT-M фьючерсов
            if response.get("code") != "00000":
                raise ValueError(f"Ошибка API: {response.get('msg')}")

            for contract in response["data"]:
                if contract["symbol"] == symbol:
                    return {
                        "minQty": float(contract.get("minTradeAmount", 0.001)),
                        "qtyStep": float(contract.get("volumePlace", 0.001)),
                        "maxLeverage": int(float(contract.get("maxLeverage", 125)))
                    }
            raise ValueError(f"Пара {symbol} не найдена")
        except Exception as e:
            logger.error(f"Ошибка при получении информации о символе {symbol}: {str(e)}")
            raise

    def get_current_price(self, symbol: str) -> float:
        """Получает текущую рыночную цену"""
        try:
            response = self.client.mix_get_ticker(symbol)
            if response.get("code") != "00000":
                raise ValueError(f"Ошибка API: {response.get('msg')}")
            return float(response["data"][0]["last"])
        except Exception as e:
            logger.error(f"Ошибка при получении цены для {symbol}: {str(e)}")
            raise

    def get_balance(self) -> float:
        """Получает доступный баланс в USDT"""
        try:
            response = self.client.mix_get_account("umcbl", "USDT")
            if response.get("code") != "00000":
                raise ValueError(f"Ошибка API: {response.get('msg')}")
            return float(response["data"].get("available", 0))
        except Exception as e:
            logger.error(f"Ошибка при получении баланса Bitget: {str(e)}")
            raise

    def set_leverage(self, symbol: str, leverage: int = 5, tdMode: str = "isolated") -> bool:
        """Устанавливает плечо для торговой пары"""
        try:
            margin_mode = "isolated" if tdMode == "isolated" else "cross"
            response = self.client.mix_set_leverage(
                symbol=symbol,
                marginCoin="USDT",
                leverage=leverage,
                marginMode=margin_mode,
                holdSide="long"  # Для хедж-режима
            )
            if response.get("code") != "00000":
                logger.warning(f"Ошибка установки плеча для {symbol}: {response.get('msg')}")
                return True  # Продолжаем, если плечо уже установлено
            logger.info(f"Плечо {leverage}x установлено для {symbol} ({tdMode})")
            return True
        except Exception as e:
            logger.error(f"Ошибка при установке плеча для {symbol}: {str(e)}")
            return True

    def calculate_quantity(self, symbol: str, leverage: int = 5, risk_percent: float = 0.05) -> float:
        """Рассчитывает количество контрактов для ордера"""
        try:
            usdt_balance = self.get_balance()
            if usdt_balance <= 0:
                raise ValueError("Недостаточно USDT на балансе!")

            current_price = self.get_current_price(symbol)
            symbol_info = self.get_symbol_info(symbol)

            min_qty = symbol_info["minQty"]
            qty_step = symbol_info["qtyStep"]

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
        """Создает основной ордер с SL/TP"""
        try:
            symbol_info = self.get_symbol_info(symbol)
            qty_step = symbol_info["qtyStep"]
            quantity = round(quantity / qty_step) * qty_step

            side = side.upper()
            pos_side = "long" if side == "BUY" else "short"
            sl_side = "sell" if side == "BUY" else "buy"

            # Создаем основной ордер
            main_response = self.client.mix_place_order(
                symbol=symbol,
                marginCoin="USDT",
                size=quantity,
                side=side.lower(),
                orderType="market",
                posSide=pos_side
            )
            if main_response.get("code") != "00000":
                raise ValueError(f"Ошибка создания ордера: {main_response.get('msg')}")
            order_id = main_response["data"]["orderId"]

            # Распределяем количество для TP ордеров
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

            sorted_take_profits = sorted(take_profits) if side == "BUY" else sorted(take_profits, reverse=True)
            valid_take_profits = [tp for tp in sorted_take_profits if tp is not None]

            algo_order_ids = []

            # Создаем SL ордер
            sl_response = self.client.mix_place_plan_order(
                symbol=symbol,
                marginCoin="USDT",
                size=quantity,
                triggerPrice=stop_loss,
                side=sl_side,
                orderType="market",
                triggerType="fill_price",
                posSide=pos_side
            )
            if sl_response.get("code") != "00000":
                logger.error(f"Ошибка создания SL ордера: {sl_response.get('msg')}")
            else:
                algo_order_ids.append(sl_response["data"]["orderId"])

            # Создаем TP ордера
            for tp_price, tp_qty in zip(valid_take_profits, tp_quantities):
                if tp_price is not None:
                    tp_response = self.client.mix_place_plan_order(
                        symbol=symbol,
                        marginCoin="USDT",
                        size=tp_qty,
                        triggerPrice=tp_price,
                        side=sl_side,
                        orderType="market",
                        triggerType="fill_price",
                        posSide=pos_side
                    )
                    if tp_response.get("code") != "00000":
                        logger.error(f"Ошибка создания TP ордера: {tp_response.get('msg')}")
                    else:
                        algo_order_ids.append(tp_response["data"]["orderId"])

            logger.info(f"Основной ордер создан: {order_id}, TP/SL ордера: {algo_order_ids}")
            return main_response, valid_take_profits, order_id, algo_order_ids, pos_side

        except Exception as e:
            logger.error(f"Ошибка при создании основного ордера для {symbol}: {str(e)}")
            raise

    def get_order_status(self, symbol: str, order_id: str) -> Dict:
        """Получает статус ордера"""
        try:
            response = self.client.mix_get_order_details(symbol, order_id)
            if response.get("code") != "00000":
                raise ValueError(f"Ошибка API: {response.get('msg')}")
            return response["data"]
        except Exception as e:
            logger.error(f"Ошибка при получении статуса ордера {order_id} для {symbol}: {str(e)}")
            raise

    def close_position(self, symbol: str, posSide: str) -> bool:
        """Закрывает позицию"""
        try:
            response = self.client.mix_get_position(symbol, "USDT")
            if response.get("code") != "00000":
                raise ValueError(f"Ошибка API: {response.get('msg')}")

            for pos in response["data"]:
                if pos["holdSide"] == posSide.lower():
                    qty = float(pos["total"])
                    side = "sell" if posSide.lower() == "long" else "buy"
                    close_response = self.client.mix_place_order(
                        symbol=symbol,
                        marginCoin="USDT",
                        size=qty,
                        side=side,
                        orderType="market",
                        posSide=posSide.lower()
                    )
                    if close_response.get("code") != "00000":
                        raise ValueError(f"Ошибка закрытия позиции: {close_response.get('msg')}")
                    logger.info(f"Позиция {posSide} для {symbol} закрыта")
                    return True
            logger.info(f"Нет открытых позиций для {symbol} на стороне {posSide}")
            return True
        except Exception as e:
            logger.error(f"Ошибка при закрытии позиции для {symbol}: {str(e)}")
            raise

    def cancel_order(self, symbol: str, order_id: str) -> bool:
        """Отменяет ордер"""
        try:
            response = self.client.mix_cancel_order(symbol, order_id, "USDT")
            if response.get("code") != "00000":
                raise ValueError(f"Ошибка API: {response.get('msg')}")
            logger.info(f"Ордер {order_id} для {symbol} отменён")
            return True
        except Exception as e:
            logger.error(f"Ошибка при отмене ордера {order_id} для {symbol}: {str(e)}")
            raise

    def move_sl_to_breakeven(self, symbol: str) -> bool:
        """Перемещает стоп-лосс к цене входа"""
        try:
            response = self.client.mix_get_position(symbol, "USDT")
            if response.get("code") != "00000":
                raise ValueError(f"Ошибка API: {response.get('msg')}")

            positions = response["data"]
            for position in positions:
                pos_side = position["holdSide"]
                qty = float(position["total"])
                avg_price = float(position["avgPrice"])

                if qty > 0 and avg_price > 0:
                    orders_response = self.client.mix_get_plan_orders(symbol)
                    if orders_response.get("code") != "00000":
                        raise ValueError(f"Ошибка API: {orders_response.get('msg')}")

                    sl_orders = [order for order in orders_response["data"]
                                 if order.get("triggerType") == "fill_price" and order.get("posSide") == pos_side]

                    for order in sl_orders:
                        self.cancel_order(symbol, order["orderId"])

                    new_sl_price = avg_price * (0.999 if pos_side == "long" else 1.001)
                    sl_side = "sell" if pos_side == "long" else "buy"

                    sl_response = self.client.mix_place_plan_order(
                        symbol=symbol,
                        marginCoin="USDT",
                        size=qty,
                        triggerPrice=new_sl_price,
                        side=sl_side,
                        orderType="market",
                        triggerType="fill_price",
                        posSide=pos_side
                    )
                    if sl_response.get("code") != "00000":
                        raise ValueError(f"Ошибка создания SL: {sl_response.get('msg')}")
                    new_sl_order_id = sl_response["data"]["orderId"]

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
            logger.error(f"Ошибка при перемещении SL для {symbol} на Bitget: {str(e)}")
            raise


# Совместимость с другими модулями
def get_symbol_info(symbol: str, api_key: str, secret_key: str, passphrase: str = None) -> Dict:
    return BitgetAPI(api_key, secret_key, passphrase).get_symbol_info(symbol)


def get_current_price(symbol: str, api_key: str, secret_key: str, passphrase: str = None) -> float:
    return BitgetAPI(api_key, secret_key, passphrase).get_current_price(symbol)


def get_balance(api_key: str, secret_key: str, passphrase: str = None) -> float:
    return BitgetAPI(api_key, secret_key, passphrase).get_balance()


def set_leverage(symbol: str, leverage: int = 5, tdMode: str = "isolated", api_key: str = None,
                 secret_key: str = None, passphrase: str = None) -> bool:
    return BitgetAPI(api_key, secret_key, passphrase).set_leverage(symbol, leverage, tdMode)


def calculate_quantity(symbol: str, leverage: int = 5, risk_percent: float = 0.05, api_key: str = None,
                       secret_key: str = None, passphrase: str = None) -> float:
    return BitgetAPI(api_key, secret_key, passphrase).calculate_quantity(symbol, leverage, risk_percent)


def create_main_order(symbol: str, side: str, quantity: float, stop_loss: float, take_profits: List[Optional[float]],
                      tdMode: str = "isolated", api_key: str = None, secret_key: str = None, passphrase: str = None):
    return BitgetAPI(api_key, secret_key, passphrase).create_main_order(symbol, side, quantity, stop_loss, take_profits,
                                                                        tdMode)


def get_order_status(symbol: str, order_id: str, api_key: str, secret_key: str, passphrase: str = None) -> Dict:
    return BitgetAPI(api_key, secret_key, passphrase).get_order_status(symbol, order_id)


def close_position(symbol: str, posSide: str, api_key: str, secret_key: str, passphrase: str = None) -> bool:
    return BitgetAPI(api_key, secret_key, passphrase).close_position(symbol, posSide)


def cancel_order(symbol: str, order_id: str, api_key: str, secret_key: str, passphrase: str = None) -> bool:
    return BitgetAPI(api_key, secret_key, passphrase).cancel_order(symbol, order_id)


def move_sl_to_breakeven(symbol: str, api_key: str, secret_key: str, passphrase: str = None) -> bool:
    return BitgetAPI(api_key, secret_key, passphrase).move_sl_to_breakeven(symbol)