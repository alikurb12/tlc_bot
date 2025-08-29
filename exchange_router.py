import time
import logging
from flask import Flask, request, jsonify
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import os
import datetime
import threading
import schedule
import re
import json

from bingx_signals import (
    get_balance as bingx_get_balance,
    get_current_price as bingx_get_current_price,
    get_symbol_info as bingx_get_symbol_info,
    set_leverage as bingx_set_leverage,
    calculate_quantity as bingx_calculate_quantity,
    create_main_order as bingx_create_main_order,
    create_tp_sl_orders as bingx_create_tp_sl_orders,
    update_stop_loss as bingx_update_stop_loss,
    get_open_orders as bingx_get_open_orders
)

from okx.PublicData import PublicAPI
from okx.Trade import TradeAPI
from okx.Account import AccountAPI
from okx.MarketData import MarketAPI

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s', 
    handlers=[
        logging.FileHandler('exchange_router.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

load_dotenv()
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = os.getenv("DB_PORT", "5432")
APIURL = "https://www.okx.com"

try:
    conn = psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT,
        cursor_factory=RealDictCursor
    )
    cursor = conn.cursor()
    logger.info("Database connection established successfully.")
except Exception as e:
    logger.error(f"Database connection error: {e}")
    raise

cursor.execute("""
    CREATE TABLE IF NOT EXISTS trades (
        trade_id SERIAL PRIMARY KEY,
        user_id BIGINT NOT NULL,
        exchange VARCHAR(20) NOT NULL,
        order_id VARCHAR(255) NOT NULL,
        symbol TEXT NOT NULL,
        side TEXT NOT NULL,
        position_side TEXT NOT NULL,
        quantity REAL NOT NULL,
        entry_price REAL NOT NULL,
        stop_loss REAL,
        take_profit_1 REAL,
        take_profit_2 REAL,
        take_profit_3 REAL,
        sl_order_id VARCHAR(255),
        tp1_order_id VARCHAR(255),
        tp2_order_id VARCHAR(255),
        tp3_order_id VARCHAR(255),
        status TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (user_id) REFERENCES users (user_id) ON DELETE CASCADE
    )
""")
conn.commit()

def okx_get_symbol_info(symbol, api_key, secret_key, passphrase):
    try:
        pub_api = PublicAPI(flag="1", domain=APIURL, debug=True)
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

def okx_get_current_price(symbol, api_key, secret_key, passphrase):
    try:
        market_api = MarketAPI(flag="1", domain=APIURL, debug=True)
        response = market_api.get_ticker(instId=symbol)
        logger.info(f"Ответ API цены OKX: {json.dumps(response, indent=2)}")
        if response.get("code") != "0":
            raise ValueError(f"Ошибка получения цены: {response.get('msg')}")
        return float(response["data"][0]["last"])
    except Exception as e:
        logger.error(f"Ошибка при получении цены для {symbol}: {str(e)}")
        raise

def okx_get_balance(api_key, secret_key, passphrase):
    try:
        account_api = AccountAPI(api_key, secret_key, passphrase, flag="1", domain=APIURL, debug=True)
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

def okx_set_leverage(symbol, leverage=10, tdMode="isolated", posSide="long", api_key=None, secret_key=None, passphrase=None):
    try:
        account_api = AccountAPI(api_key, secret_key, passphrase, flag="1", domain=APIURL, debug=True)
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

def okx_calculate_quantity(symbol, leverage=10, risk_percent=0.10, api_key=None, secret_key=None, passphrase=None):
    try:
        usdt_balance = okx_get_balance(api_key, secret_key, passphrase)
        if usdt_balance <= 0:
            raise ValueError("Недостаточно USDT на балансе!")
        
        current_price = okx_get_current_price(symbol, api_key, secret_key, passphrase)
        symbol_info = okx_get_symbol_info(symbol, api_key, secret_key, passphrase)
        
        lot_size = symbol_info["lotSz"]
        min_size = symbol_info["minSz"]
        ct_val = symbol_info["ctVal"]
        
        risk_amount = usdt_balance * risk_percent
        total_trade_amount = risk_amount * leverage
        quantity = total_trade_amount / current_price / ct_val
        
        required_margin = total_trade_amount / leverage
        logger.info(f"Баланс: {usdt_balance:.4f} USDT, Требуемая маржа: {required_margin:.4f} USDT, Количество: {quantity:.4f}")
        if required_margin > usdt_balance:
            raise ValueError(f"Недостаточно маржи: требуется {required_margin:.4f} USDT, доступно {usdt_balance:.4f} USDT")
        
        quantity = round(quantity / lot_size) * lot_size
        quantity = max(min_size, quantity)
        
        logger.info(f"Рассчитанное количество для {symbol}: {quantity} контрактов")
        return quantity
    except Exception as e:
        logger.error(f"Ошибка при расчете количества для {symbol}: {str(e)}")
        raise

def okx_create_main_order(symbol, side, quantity, stop_loss, take_profits, tdMode="isolated", api_key=None, secret_key=None, passphrase=None):
    try:
        trade_api = TradeAPI(api_key, secret_key, passphrase, flag="1", domain=APIURL, debug=True)
        posSide = "long" if side == "BUY" else "short"
        sl_side = "sell" if side == "BUY" else "buy"
        
        symbol_info = okx_get_symbol_info(symbol, api_key, secret_key, passphrase)
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

def process_bingx_signal(user, signal):
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

        quantity = bingx_calculate_quantity(symbol, leverage=10, risk_percent=0.10, api_key=api_key, secret_key=secret_key)

        main_order = bingx_create_main_order(symbol, action, quantity, api_key, secret_key)
        main_order_data = json.loads(main_order)
        
        if main_order_data.get("code") != 0:
            logger.error(f"Ошибка создания основного ордера для пользователя {user_id}: {main_order_data.get('msg')}")
            return None
        
        order_id = main_order_data["data"]["order"]["orderId"]
        logger.info(f"Main order for user {user_id}: {main_order}")

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
        conn.commit()

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
        conn.commit()

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

def process_okx_signal(user, signal):
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

        cursor.execute(
            """
            INSERT INTO trades (user_id, exchange, order_id, symbol, side, position_side, quantity, entry_price, stop_loss, take_profit_1, take_profit_2, take_profit_3, sl_order_id, tp1_order_id, tp2_order_id, tp3_order_id, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING trade_id
            """,
            (user_id, 'okx', order_id, symbol, action, position_side, quantity, price, stop_loss, 
             take_profits[0], take_profits[1], take_profits[2], sl_order_id, tp1_order_id, tp2_order_id, tp3_order_id, 'open')
        )
        trade_id = cursor.fetchone()['trade_id']
        conn.commit()

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

def normalize_symbol(symbol, exchange):
    symbol = symbol.upper()
    logger.info(f"Нормализация символа: входной символ={symbol}, биржа={exchange}")
    
    if exchange == 'bingx':
        symbol = symbol.replace(':', '/').replace('-', '/')
        symbol = re.sub(r'\.P$', '', symbol)
        if '/' in symbol:
            base, quote = symbol.split('/')
            normalized = f"{base}-{quote}"
        else:
            normalized = symbol.replace("USDT", "-USDT")
        logger.info(f"Нормализованный символ для BingX: {normalized}")
        return normalized
    
    elif exchange == 'okx':
        symbol = re.sub(r'\.P$', '', symbol)
        symbol = symbol.replace(':', '-').replace('/', '-')
        if '-' not in symbol:
            symbol = re.sub(r'USDT$', '-USDT', symbol)
        if not symbol.endswith('-SWAP'):
            symbol = f"{symbol}-SWAP"
        normalized = symbol
        logger.info(f"Нормализованный символ для OKX: {normalized}")
        return normalized
    
    logger.warning(f"Неизвестная биржа: {exchange}, возвращаем исходный символ: {symbol}")
    return symbol

@app.route('/webhook', methods=['POST'])
def webhook():
    try:
        raw_data = request.get_data(as_text=True)
        logger.info(f"Получен запрос: {raw_data}")
        
        content_type = request.headers.get('Content-Type')
        if not content_type or 'application/json' not in content_type.lower():
            logger.error(f"Неверный Content-Type: {content_type}")
            return jsonify({'status': 'error', 'message': 'Ожидается Content-Type: application/json'}), 400

        try:
            data = request.get_json(force=True)
        except Exception as e:
            logger.error(f"Ошибка парсинга JSON: {str(e)}, тело запроса: {raw_data}")
            return jsonify({'status': 'error', 'message': f'Неверный формат JSON: {str(e)}'}), 400

        if not data:
            logger.error("Получен пустой JSON")
            return jsonify({'status': 'error', 'message': 'Пустой JSON'}), 400

        raw_action = data.get('action', '').upper()
        if raw_action not in ['BUY', 'SELL', 'LONG', 'SHORT']:
            logger.error(f"Некорректное действие: {raw_action}")
            return jsonify({'status': 'error', 'message': 'Действие должно быть BUY, SELL, LONG или SHORT'}), 400

        action = raw_action if raw_action in ['BUY', 'SELL'] else ('BUY' if raw_action == 'LONG' else 'SELL')

        symbol = data.get('symbol')
        if not symbol:
            logger.error("Не указан символ")
            return jsonify({'status': 'error', 'message': 'Необходимо указать символ'}), 400

        try:
            price = float(data.get('price', 0))
            if price <= 0:
                raise ValueError("Цена должна быть положительной")
        except (TypeError, ValueError) as e:
            logger.error(f"Ошибка в цене: {str(e)}")
            return jsonify({'status': 'error', 'message': 'Неверный формат цены'}), 400

        stop_loss = float(data.get('stop_loss')) if data.get('stop_loss') else None
        take_profit_1 = float(data.get('take_profit_1')) if data.get('take_profit_1') else None
        take_profit_2 = float(data.get('take_profit_2')) if data.get('take_profit_2') else None
        take_profit_3 = float(data.get('take_profit_3')) if data.get('take_profit_3') else None

        if not stop_loss or not all([take_profit_1, take_profit_2, take_profit_3]):
            logger.error("Не указаны все необходимые параметры SL и TP")
            return jsonify({'status': 'error', 'message': 'Необходимо указать stop_loss и все три take_profit'}), 400

        cursor.execute(
            "SELECT user_id, api_key, secret_key, passphrase, exchange FROM users WHERE subscription_end > %s AND api_key IS NOT NULL AND secret_key IS NOT NULL AND subscription_type IN ('referral_approved', 'regular')",
            (datetime.datetime.now(),)
        )
        active_users = cursor.fetchall()
        
        if not active_users:
            logger.error("Нет пользователей с активной подпиской и API-ключами")
            return jsonify({'status': 'error', 'message': 'Нет пользователей с активной подпиской и API-ключами'}), 400

        results = []
        for user in active_users:
            user_id = user['user_id']
            exchange = user['exchange'] or 'bingx'
            normalized_symbol = normalize_symbol(symbol, exchange)
            
            signal = {
                "action": action,
                "symbol": normalized_symbol,
                "price": price,
                "stop_loss": stop_loss,
                "take_profit_1": take_profit_1,
                "take_profit_2": take_profit_2,
                "take_profit_3": take_profit_3
            }

            try:
                if exchange == 'bingx':
                    result = process_bingx_signal(user, signal)
                elif exchange == 'okx':
                    result = process_okx_signal(user, signal)
                else:
                    logger.error(f"Неизвестная биржа: {exchange} для пользователя {user_id}")
                    continue
                
                if result:
                    results.append(result)
                    logger.info(f"Сигнал обработан для пользователя {user_id} на бирже {exchange}")
                    
            except Exception as e:
                logger.error(f"Ошибка обработки сигнала для пользователя {user_id} на бирже {exchange}: {str(e)}")
                continue

        if not results:
            return jsonify({'status': 'error', 'message': 'Не удалось обработать сигнал ни для одного пользователя'}), 500

        return jsonify({
            'status': 'success',
            'message': 'Фьючерсный сигнал обработан для активных пользователей',
            'signal_id': 'webhook_signal',
            'symbol': symbol,
            'results': results
        }), 200

    except Exception as e:
        logger.error(f"Ошибка обработки webhook: {str(e)}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

if __name__ == "__main__":
    logger.info("Запуск универсального обработчика сигналов...")
    app.run(host='0.0.0.0', port=5000, debug=False)
    logger.info("Обработчик запущен и слушает на порту 5000")