import logging
from typing import Dict, List, Optional
import ccxt
from database import get_cursor, commit

logger = logging.getLogger(__name__)


class BybitAPI:
    def __init__(self, api_key: str, secret_key: str, broker_id: str = "Gh000942", testnet: bool = False):
        self.exchange = ccxt.bybit({
            "apiKey": api_key,
            "secret": secret_key,
            "sandbox": testnet,
            "headers": {"Referer": broker_id},
            "options": {
                "defaultType": "linear",
                "adjustForTimeDifference": True,
                "createMarketBuyOrderRequiresPrice": False,
                "defaultPositionIdx": 1,  # Устанавливаем по умолчанию для long позиций
            },
            "recvWindow": 15000,
        })
        self.api_key = api_key
        self.broker_id = broker_id

        # Принудительно загружаем markets при инициализации
        try:
            self.exchange.load_markets()
            logger.info(
                f"Bybit API initialized with broker_id: {broker_id}, loaded {len(self.exchange.markets)} markets")
        except Exception as e:
            logger.warning(f"Could not load markets on init: {e}")

    def _ensure_markets_loaded(self):
        """Убедиться, что markets загружены"""
        if not self.exchange.markets:
            try:
                self.exchange.load_markets()
                logger.info(f"Markets loaded successfully, total: {len(self.exchange.markets)}")
            except Exception as e:
                logger.error(f"Failed to load markets: {e}")
                raise

    def _get_position_idx(self, side: str) -> int:
        """Получить правильный positionIdx для стороны позиции"""
        # Для Bybit в режиме One-Way:
        # 1 - для long позиций
        # 2 - для short позиций
        return 1 if side.lower() == 'buy' else 2

    def get_symbol_info(self, symbol: str) -> Dict:
        try:
            self._ensure_markets_loaded()

            market = self.exchange.market(symbol)

            return {
                "lotSizeFilter": {
                    "qtyStep": market['precision']['amount'],
                    "minOrderQty": market['limits']['amount']['min'],
                },
                "leverageFilter": {
                    "maxLeverage": 10
                }
            }
        except Exception as e:
            logger.error(f"Error getting symbol info {symbol}: {e}")
            raise

    def get_current_price(self, symbol: str) -> float:
        try:
            self._ensure_markets_loaded()
            ticker = self.exchange.fetch_ticker(symbol)
            return float(ticker['last'])
        except Exception as e:
            logger.error(f"Error getting price {symbol}: {e}")
            raise

    def get_balance(self) -> float:
        try:
            balance = self.exchange.fetch_balance()
            return float(balance['USDT']['free'])
        except Exception as e:
            logger.error(f"Error getting balance: {e}")
            return 0.0

    def set_leverage(self, symbol: str, leverage: int = 10) -> bool:
        try:
            self._ensure_markets_loaded()

            # Для Bybit используем специальный метод установки плеча
            response = self.exchange.set_leverage(leverage, symbol)
            logger.info(f"Leverage set to {leverage} for {symbol}")
            return True
        except Exception as e:
            logger.debug(f"Leverage not changed: {e}")
            return False

    def calculate_quantity(self, symbol: str, leverage: int = 10, risk_percent: float = 0.05) -> float:
        try:
            self._ensure_markets_loaded()

            balance = self.get_balance()
            if balance <= 1:
                raise ValueError("Not enough balance")

            price = self.get_current_price(symbol)
            info = self.get_symbol_info(symbol)
            qty_step = info["lotSizeFilter"]["qtyStep"]
            min_qty = info["lotSizeFilter"]["minOrderQty"]

            risk_amount = balance * risk_percent
            raw_qty = (risk_amount * leverage) / price

            quantity = round(raw_qty / qty_step) * qty_step
            quantity = max(min_qty, quantity)

            order_value = quantity * price
            min_order_amt = 5.0
            if order_value < min_order_amt:
                min_quantity = (min_order_amt / price) * 1.01
                quantity = round(min_quantity / qty_step) * qty_step
                quantity = max(min_qty, quantity)

                order_value = quantity * price
                if order_value < min_order_amt:
                    quantity = min_qty
                    order_value = quantity * price
                    if order_value < min_order_amt:
                        raise ValueError(
                            f"Min order amount {min_order_amt} USDT not reached. Current: {order_value:.2f} USDT")

            logger.info(f"Calculated quantity {symbol}: {quantity}")
            return quantity

        except Exception as e:
            logger.error(f"Error calculating quantity {symbol}: {e}")
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
            self._ensure_markets_loaded()

            info = self.get_symbol_info(symbol)
            qty_step = info["lotSizeFilter"]["qtyStep"]
            price = self.get_current_price(symbol)
            position_idx = self._get_position_idx(side)

            order_value = quantity * price
            min_order_amt = 5.0

            if order_value < min_order_amt:
                raise ValueError(f"Order amount {order_value:.2f} USDT less than minimum {min_order_amt} USDT")

            quantity = round(quantity / qty_step) * qty_step
            if quantity <= 0:
                raise ValueError("Quantity <= 0")

            logger.info(f"Opening position {symbol} {side} {quantity} with positionIdx: {position_idx}")

            # Устанавливаем плечо
            self.set_leverage(symbol, 10)

            # Основной рыночный ордер с правильным positionIdx
            main_order = self.exchange.create_order(
                symbol=symbol,
                type='market',
                side=side.lower(),
                amount=quantity,
                params={
                    'timeInForce': 'GTC',
                    'positionIdx': position_idx,
                }
            )

            main_order_id = main_order['id']

            # Ждем немного чтобы позиция открылась
            import time
            time.sleep(2)

            # Установка стоп-лосса
            sl_order_id = None
            try:
                sl_order = self.exchange.create_order(
                    symbol=symbol,
                    type='stop',
                    side='sell' if side.lower() == 'buy' else 'buy',
                    amount=quantity,
                    price=None,
                    params={
                        'stopPrice': stop_loss,
                        'reduceOnly': True,
                        'timeInForce': 'GTC',
                        'positionIdx': position_idx,
                        'triggerDirection': 1 if side.lower() == 'buy' else 2,
                    }
                )
                sl_order_id = sl_order['id']
                logger.info(f"SL set at {stop_loss}")
            except Exception as e:
                logger.warning(f"SL not set via create_order: {e}")
                # Альтернативный метод через private API
                try:
                    sl_response = self.exchange.private_post_private_linear_stop_order_create({
                        'symbol': self.exchange.market_id(symbol),
                        'side': 'Sell' if side.lower() == 'buy' else 'Buy',
                        'order_type': 'Market',
                        'qty': str(quantity),
                        'base_price': str(price),
                        'stop_px': str(stop_loss),
                        'time_in_force': 'GoodTillCancel',
                        'reduce_only': True,
                        'close_on_trigger': False,
                        'position_idx': position_idx,
                    })
                    sl_order_id = sl_response['result']['stop_order_id']
                    logger.info(f"SL set via private API at {stop_loss}")
                except Exception as e2:
                    logger.warning(f"SL also failed via private API: {e2}")

            # Установка тейк-профитов
            tp_ids = []
            valid_tps = [tp for tp in take_profits if tp is not None][:3]

            if valid_tps:
                tp_qty = round((quantity / len(valid_tps)) / qty_step) * qty_step

                for i, tp_price in enumerate(valid_tps):
                    try:
                        tp_order = self.exchange.create_order(
                            symbol=symbol,
                            type='limit',
                            side='sell' if side.lower() == 'buy' else 'buy',
                            amount=tp_qty,
                            price=tp_price,
                            params={
                                'reduceOnly': True,
                                'timeInForce': 'GTC',
                                'positionIdx': position_idx
                            }
                        )
                        tp_ids.append(tp_order['id'])
                        logger.info(f"TP{i + 1} set at {tp_price}")
                    except Exception as e:
                        logger.warning(f"TP{i + 1} not set via create_order: {e}")
                        # Альтернативный метод для TP
                        try:
                            tp_response = self.exchange.private_post_private_linear_order_create({
                                'symbol': self.exchange.market_id(symbol),
                                'side': 'Sell' if side.lower() == 'buy' else 'Buy',
                                'order_type': 'Limit',
                                'qty': str(tp_qty),
                                'price': str(tp_price),
                                'time_in_force': 'GoodTillCancel',
                                'reduce_only': True,
                                'close_on_trigger': False,
                                'position_idx': position_idx,
                            })
                            tp_ids.append(tp_response['result']['order_id'])
                            logger.info(f"TP{i + 1} set via private API at {tp_price}")
                        except Exception as e2:
                            logger.warning(f"TP{i + 1} also failed via private API: {e2}")

            logger.info(f"Position opened: {symbol} {side} {quantity}")
            return {"result": {"orderId": main_order_id}}, valid_tps, main_order_id, tp_ids, "net", sl_order_id

        except Exception as e:
            logger.error(f"Error opening position {symbol}: {e}")
            raise

    def close_position(self, symbol: str) -> bool:
        try:
            self._ensure_markets_loaded()

            # Получаем открытые позиции
            positions = self.exchange.fetch_positions([symbol])
            closed = False

            for pos in positions:
                if pos['symbol'] == symbol and abs(pos['contracts']) > 0:
                    side = 'sell' if pos['side'] == 'long' else 'buy'
                    position_idx = 1 if pos['side'] == 'long' else 2

                    logger.info(f"Closing position {symbol}: {pos['side']} {abs(pos['contracts'])}")

                    close_order = self.exchange.create_order(
                        symbol=symbol,
                        type='market',
                        side=side,
                        amount=abs(pos['contracts']),
                        params={
                            'reduceOnly': True,
                            'positionIdx': position_idx
                        }
                    )

                    if close_order['id']:
                        logger.info(f"Position closed: {symbol} {pos['side']} {abs(pos['contracts'])}")
                        closed = True

            if not closed:
                logger.info(f"No open positions for {symbol}")

            return closed

        except Exception as e:
            logger.error(f"Error closing position {symbol}: {e}")
            return False

    def cancel_order(self, symbol: str, order_id: str) -> bool:
        try:
            self._ensure_markets_loaded()

            self.exchange.cancel_order(order_id, symbol)
            logger.info(f"Order cancelled: {order_id}")
            return True
        except Exception as e:
            if "Order not found" in str(e) or "order not exists" in str(e).lower():
                logger.info(f"Order already doesn't exist: {order_id}")
                return True
            else:
                logger.error(f"Error cancelling order {order_id}: {e}")
                return False

    def move_sl_to_breakeven(self, symbol: str) -> bool:
        try:
            self._ensure_markets_loaded()

            positions = self.exchange.fetch_positions([symbol])
            if not positions:
                return False

            pos = positions[0]
            if abs(pos['contracts']) <= 0:
                return False

            avg_price = float(pos['entryPrice'])
            side = pos['side']
            position_idx = 1 if side == 'long' else 2

            # Вычисляем новую цену стоп-лосса (безубыток)
            if side == 'long':
                new_sl = avg_price * 0.999
            else:
                new_sl = avg_price * 1.001

            new_sl = round(new_sl, 6)

            logger.info(f"SL moved to breakeven for {symbol}: {new_sl}")

            cursor = get_cursor()
            cursor.execute(
                """UPDATE trades 
                   SET stop_loss = %s, sl_order_id = NULL 
                   WHERE user_id = (SELECT user_id FROM users WHERE api_key = %s) 
                     AND symbol = %s AND status = 'open'""",
                (new_sl, self.api_key, symbol)
            )
            commit()
            return True

        except Exception as e:
            logger.error(f"Error moving SL {symbol}: {e}")
            return False


# Обновленные функции с правильной нормализацией символов:

def normalize_bybit_symbol(symbol: str) -> str:
    """Нормализация символа для Bybit"""
    if symbol.endswith('.P'):
        # Преобразуем HBARUSDT.P в HBAR/USDT
        base = symbol.replace('.P', '').replace('USDT', '')
        return f"{base}/USDT"
    return symbol


def get_symbol_info(symbol: str, api_key: str, secret_key: str, **kwargs) -> Dict:
    client = BybitAPI(api_key, secret_key)
    client._ensure_markets_loaded()
    normalized_symbol = normalize_bybit_symbol(symbol)
    return client.get_symbol_info(normalized_symbol)


def get_current_price(symbol: str, api_key: str, secret_key: str, **kwargs) -> float:
    client = BybitAPI(api_key, secret_key)
    client._ensure_markets_loaded()
    normalized_symbol = normalize_bybit_symbol(symbol)
    return client.get_current_price(normalized_symbol)


def get_balance(api_key: str, secret_key: str, **kwargs) -> float:
    return BybitAPI(api_key, secret_key).get_balance()


def set_leverage(symbol: str, leverage: int = 10, **kwargs) -> bool:
    client = BybitAPI(kwargs["api_key"], kwargs["secret_key"])
    client._ensure_markets_loaded()
    normalized_symbol = normalize_bybit_symbol(symbol)
    return client.set_leverage(normalized_symbol, leverage)


def calculate_quantity(symbol: str, leverage: int = 10, risk_percent: float = 0.05, **kwargs) -> float:
    client = BybitAPI(kwargs["api_key"], kwargs["secret_key"])
    client._ensure_markets_loaded()
    normalized_symbol = normalize_bybit_symbol(symbol)
    return client.calculate_quantity(normalized_symbol, leverage, risk_percent)


def create_main_order(symbol: str, side: str, quantity: float, stop_loss: float, take_profits: List[Optional[float]],
                      **kwargs):
    client = BybitAPI(kwargs["api_key"], kwargs["secret_key"])
    client._ensure_markets_loaded()
    normalized_symbol = normalize_bybit_symbol(symbol)
    return client.create_main_order(normalized_symbol, side, quantity, stop_loss, take_profits)


def close_position(symbol: str, **kwargs) -> bool:
    client = BybitAPI(kwargs["api_key"], kwargs["secret_key"])
    client._ensure_markets_loaded()
    normalized_symbol = normalize_bybit_symbol(symbol)
    return client.close_position(normalized_symbol)


def cancel_order(symbol: str, order_id: str, **kwargs) -> bool:
    client = BybitAPI(kwargs["api_key"], kwargs["secret_key"])
    client._ensure_markets_loaded()
    normalized_symbol = normalize_bybit_symbol(symbol)
    return client.cancel_order(normalized_symbol, order_id)


def move_sl_to_breakeven(symbol: str, **kwargs) -> bool:
    client = BybitAPI(kwargs["api_key"], kwargs["secret_key"])
    client._ensure_markets_loaded()
    normalized_symbol = normalize_bybit_symbol(symbol)
    return client.move_sl_to_breakeven(normalized_symbol)


def get_order_status(symbol: str, order_id: str, api_key: str, secret_key: str, **kwargs) -> Dict:
    try:
        client = BybitAPI(api_key, secret_key)
        client._ensure_markets_loaded()
        normalized_symbol = normalize_bybit_symbol(symbol)
        order = client.exchange.fetch_order(order_id, normalized_symbol)
        return {
            "orderStatus": order['status'],
            "orderId": order_id
        }
    except Exception as e:
        logger.error(f"get_order_status error: {e}")
        return {"orderStatus": "NotFound", "orderId": order_id}