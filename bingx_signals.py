import time
import requests
import hmac
import json
from hashlib import sha256
import logging
from flask import Flask, request, jsonify
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import os
import datetime
import threading
import schedule

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s', 
    handlers=[
        logging.FileHandler('signal_receiver.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Конфигурация
load_dotenv()
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = os.getenv("DB_PORT", "5432")
APIURL = "https://open-api.bingx.com"

# Подключение к базе данных PostgreSQL
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

# Создание таблицы trades
cursor.execute("""
    CREATE TABLE IF NOT EXISTS trades (
        trade_id SERIAL PRIMARY KEY,
        user_id BIGINT NOT NULL,
        order_id BIGINT NOT NULL,
        symbol TEXT NOT NULL,
        side TEXT NOT NULL,
        position_side TEXT NOT NULL,
        quantity REAL NOT NULL,
        entry_price REAL NOT NULL,
        stop_loss REAL,
        take_profit_1 REAL,
        take_profit_2 REAL,
        take_profit_3 REAL,
        sl_order_id BIGINT,
        tp1_order_id BIGINT,
        tp2_order_id BIGINT,
        tp3_order_id BIGINT,
        status TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (user_id) REFERENCES users (user_id) ON DELETE CASCADE
    )
""")
conn.commit()

def get_balance(api_key, secret_key):
    path = '/openApi/swap/v2/user/balance'
    method = "GET"
    paramsMap = {}
    paramsStr = parseParam(paramsMap)
    response = send_request(method, path, paramsStr, {}, api_key, secret_key)
    return response

def get_current_price(symbol="NOT-USDT"):
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
        logger.error(f"Ошибка при получении цены: {str(e)}")
        raise

def get_symbol_info(symbol="NOT-USDT"):
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
        logger.error(f"Ошибка при получении информации о паре: {str(e)}")
        raise

def set_leverage(symbol, leverage=10, position_side="LONG", api_key=None, secret_key=None):
    try:
        path = '/openApi/swap/v2/trade/leverage'
        method = "POST"
        paramsMap = {
            "symbol": symbol,
            "leverage": leverage,
            "side": position_side
        }
        paramsStr = parseParam(paramsMap)
        response = send_request(method, path, paramsStr, {}, api_key, secret_key)
        response_data = json.loads(response)
        if response_data.get("code") != 0:
            raise ValueError(f"Ошибка установки плеча: {response_data.get('msg')}")
        logger.info(f"Плечо {leverage}x установлено для {symbol} (side: {position_side})")
        return True
    except Exception as e:
        logger.error(f"Ошибка при установке плеча для {symbol}: {str(e)}")
        raise

def calculate_quantity(symbol, leverage=10, risk_percent=0.10, api_key=None, secret_key=None):
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
            raise ValueError(f"Недостаточно маржи: требуется {required_margin:.4f} USDT, доступно {usdt_balance:.4f} USDT")
        
        quantity = round(quantity / step_size) * step_size
        quantity = max(min_qty, quantity)
        
        return quantity
    except Exception as e:
        logger.error(f"Ошибка при расчете количества: {str(e)}")
        raise

def create_main_order(symbol, side, quantity, api_key, secret_key):
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
    response = send_request(method, path, paramsStr, {}, api_key, secret_key)
    return response

def create_tp_sl_orders(symbol, side, quantity, stop_loss, take_profits, api_key, secret_key):
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

def update_stop_loss(symbol, sl_order_id, new_stop_price, quantity, side, position_side, api_key, secret_key):
    try:
        # Отменяем текущий стоп-лосс ордер
        path = '/openApi/swap/v2/trade/order'
        method = "DELETE"
        paramsMap = {"symbol": symbol, "orderId": sl_order_id}
        paramsStr = parseParam(paramsMap)
        response = send_request(method, path, paramsStr, {}, api_key, secret_key)
        response_data = json.loads(response)
        if response_data.get("code") != 0:
            raise ValueError(f"Ошибка отмены стоп-лосса: {response_data.get('msg')}")
        
        # Создаём новый стоп-лосс ордер
        new_stop_order = {
            "symbol": symbol,
            "side": "SELL" if side == "BUY" else "BUY",
            "positionSide": position_side,
            "type": "STOP_MARKET",
            "quantity": round(quantity, 3),
            "stopPrice": new_stop_price
        }
        paramsStr = parseParam(new_stop_order)
        response = send_request("POST", '/openApi/swap/v2/trade/order', paramsStr, {}, api_key, secret_key)
        response_data = json.loads(response)
        if response_data.get("code") != 0:
            raise ValueError(f"Ошибка создания нового стоп-лосса: {response_data.get('msg')}")
        
        logger.info(f"Стоп-лосс обновлён для символа {symbol}, новый stopPrice: {new_stop_price}")
        return response_data["data"]["order"]["orderId"]
    except Exception as e:
        logger.error(f"Ошибка при обновлении стоп-лосса для {symbol}: {str(e)}")
        raise

def get_open_orders(symbol, api_key, secret_key):
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

def get_sign(api_secret, payload):
    signature = hmac.new(api_secret.encode("utf-8"), payload.encode("utf-8"), digestmod=sha256).hexdigest()
    logger.info("sign=%s", signature)
    return signature

def send_request(method, path, urlpa, payload, api_key, secret_key):
    url = f"{APIURL}{path}?{urlpa}&signature={get_sign(secret_key, urlpa)}"
    logger.info("Request URL: %s", url)
    headers = {'X-BX-APIKEY': api_key}
    response = requests.request(method, url, headers=headers, data=payload)
    return response.text

def parseParam(paramsMap):
    sortedKeys = sorted(paramsMap)
    paramsStr = "&".join(["%s=%s" % (x, paramsMap[x]) for x in sortedKeys])
    return paramsStr + "&timestamp=" + str(int(time.time() * 1000)) if paramsStr else "timestamp=" + str(int(time.time() * 1000))

def check_and_update_stop_loss():
    while True:
        try:
            cursor.execute(
                """
                SELECT t.trade_id, t.user_id, t.order_id, t.symbol, t.side, t.position_side, t.quantity, 
                       t.entry_price, t.take_profit_1, t.status, u.api_key, u.secret_key, t.sl_order_id, t.tp1_order_id 
                FROM trades t 
                JOIN users u ON t.user_id = u.user_id 
                WHERE t.status = 'open' AND u.subscription_end > %s
                """,
                (datetime.datetime.now(),)
            )
            open_trades = cursor.fetchall()
            
            for trade in open_trades:
                trade_id = trade['trade_id']
                user_id = trade['user_id']
                order_id = trade['order_id']
                symbol = trade['symbol']
                side = trade['side']
                position_side = trade['position_side']
                quantity = trade['quantity']
                entry_price = trade['entry_price']
                take_profit_1 = trade['take_profit_1']
                sl_order_id = trade['sl_order_id']
                tp1_order_id = trade['tp1_order_id']
                api_key = trade['api_key']
                secret_key = trade['secret_key']
                
                try:
                    open_orders = get_open_orders(symbol, api_key, secret_key)
                    tp1_filled = False
                    for order in open_orders.get("data", []):
                        if order['orderId'] == tp1_order_id and order['status'] == "FILLED":
                            tp1_filled = True
                            break
                        if order['orderId'] == tp1_order_id and order['status'] != "FILLED":
                            tp1_filled = False
                            break
                    
                    if tp1_filled:
                        # Переносим стоп-лосс в безубыток
                        new_sl_order_id = update_stop_loss(
                            symbol=symbol,
                            sl_order_id=sl_order_id,
                            new_stop_price=entry_price,
                            quantity=quantity,
                            side=side,
                            position_side=position_side,
                            api_key=api_key,
                            secret_key=secret_key
                        )
                        cursor.execute(
                            "UPDATE trades SET stop_loss = %s, sl_order_id = %s, status = 'breakeven' WHERE trade_id = %s",
                            (entry_price, new_sl_order_id, trade_id)
                        )
                        conn.commit()
                        logger.info(f"Стоп-лосс перенесён в безубыток для trade_id {trade_id}, user_id {user_id}")
                except Exception as e:
                    logger.error(f"Ошибка проверки TP/SL для trade_id {trade_id}, user_id {user_id}: {str(e)}")
                    continue
        except Exception as e:
            logger.error(f"Ошибка в задаче проверки стоп-лосса: {str(e)}")
        time.sleep(60)  # Проверяем каждые 60 секунд

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
        position_side = "LONG" if action == "BUY" else "SHORT"

        symbol = data.get('symbol')
        if not symbol:
            logger.error("Не указан символ")
            return jsonify({'status': 'error', 'message': 'Необходимо указать символ'}), 400

        symbol = symbol.upper().replace(':', '/').replace('-', '/')
        if '/' in symbol:
            base, quote = symbol.split('/')
            normalized_symbol = f"{base}-{quote}"
        else:
            normalized_symbol = symbol.replace("USDT.P", "-USDT")

        try:
            symbol_info = get_symbol_info(normalized_symbol)
        except ValueError as e:
            logger.error(f"Фьючерсный символ {normalized_symbol} не найден")
            return jsonify({'status': 'error', 'message': f'Фьючерсный символ {normalized_symbol} не найден'}), 400

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
        quantity_percent = 10

        if not stop_loss or not all([take_profit_1, take_profit_2, take_profit_3]):
            logger.error("Не указаны все необходимые параметры SL и TP")
            return jsonify({'status': 'error', 'message': 'Необходимо указать stop_loss и все три take_profit'}), 400

        signal = {
            "action": action,
            "symbol": normalized_symbol,
            "price": price,
            "stop_loss": stop_loss,
            "take_profit_1": take_profit_1,
            "take_profit_2": take_profit_2,
            "take_profit_3": take_profit_3,
            "quantity_percent": quantity_percent,
            "inst_type": "SWAP",
            "status": "received"
        }

        cursor.execute(
            "SELECT user_id, api_key, secret_key FROM users WHERE subscription_end > %s AND api_key IS NOT NULL AND secret_key IS NOT NULL AND subscription_type IN ('referral_approved', 'regular')",
            (datetime.datetime.now(),)
        )
        active_users = cursor.fetchall()
        if not active_users:
            logger.error("Нет пользователей с активной подпиской и API-ключами")
            return jsonify({'status': 'error', 'message': 'Нет пользователей с активной подпиской и API-ключами'}), 400

        results = []
        for user in active_users:
            user_id = user['user_id']
            api_key = user['api_key']
            secret_key = user['secret_key']
            logger.info(f"Processing signal for user {user_id}")

            try:
                balance_response = get_balance(api_key, secret_key)
                balance_data = json.loads(balance_response)
                usdt_balance = float(balance_data["data"]["balance"]["availableMargin"])
                if usdt_balance < 0.1:
                    logger.error(f"Недостаточный баланс для пользователя {user_id}: {usdt_balance} USDT")
                    continue

                try:
                    set_leverage(normalized_symbol, leverage=10, position_side=position_side, api_key=api_key, secret_key=secret_key)
                except Exception as e:
                    logger.error(f"Не удалось установить плечо для пользователя {user_id}: {str(e)}")
                    continue

                try:
                    quantity = calculate_quantity(normalized_symbol, leverage=10, risk_percent=0.10, api_key=api_key, secret_key=secret_key)
                    logger.info(f"Calculated quantity for user {user_id}: {quantity} {normalized_symbol.split('-')[0]}")
                except Exception as e:
                    logger.error(f"Ошибка расчета количества для пользователя {user_id}: {str(e)}")
                    continue

                main_order = create_main_order(normalized_symbol, action, quantity, api_key, secret_key)
                main_order_data = json.loads(main_order)
                if main_order_data.get("code") != 0:
                    logger.error(f"Ошибка создания основного ордера для пользователя {user_id}: {main_order_data.get('msg')}")
                    continue
                order_id = main_order_data["data"]["order"]["orderId"]
                logger.info(f"Main order for user {user_id}: {main_order}")

                # Сохранение сделки в базе данных
                cursor.execute(
                    """
                    INSERT INTO trades (user_id, order_id, symbol, side, position_side, quantity, entry_price, stop_loss, take_profit_1, take_profit_2, take_profit_3, status)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING trade_id
                    """,
                    (user_id, order_id, normalized_symbol, action, position_side, quantity, price, stop_loss, take_profit_1, take_profit_2, take_profit_3, 'open')
                )
                trade_id = cursor.fetchone()['trade_id']
                conn.commit()
                logger.info(f"Trade saved for user {user_id}, trade_id: {trade_id}")

                time.sleep(2)

                take_profits = [take_profit_1, take_profit_2, take_profit_3]
                tp_sl_results, sorted_take_profits, order_ids = create_tp_sl_orders(
                    symbol=normalized_symbol,
                    side=action,
                    quantity=quantity,
                    stop_loss=stop_loss,
                    take_profits=take_profits,
                    api_key=api_key,
                    secret_key=secret_key
                )

                # Сохранение orderId для SL и TP
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

                user_result = {
                    "user_id": user_id,
                    "trade_id": trade_id,
                    "main_order": main_order_data,
                    "tp_sl_orders": [json.loads(res) for res in tp_sl_results]
                }

                for i, res in enumerate(user_result["tp_sl_orders"]):
                    if res["code"] != 0:
                        logger.error(f"Ошибка в TP/SL ордере #{i+1} для пользователя {user_id}: {res['msg']}")
                    else:
                        logger.info(f"TP/SL ордер #{i+1} для пользователя {user_id}: {res}")

                try:
                    open_orders = get_open_orders(normalized_symbol, api_key, secret_key)
                    for order in open_orders.get("data", []):
                        logger.info(f"Проверенный ордер для пользователя {user_id}: {order}")
                except Exception as e:
                    logger.error(f"Не удалось проверить открытые ордера для пользователя {user_id}: {str(e)}")

                signal["status"] = "executed"
                signal["order_id"] = order_id
                results.append(user_result)

                tp_response = [
                    {'price': sorted_take_profits[0], 'quantity_percent': 33.33},
                    {'price': sorted_take_profits[1], 'quantity_percent': 33.33},
                    {'price': sorted_take_profits[2], 'quantity_percent': 33.34}
                ] if action == "BUY" else [
                    {'price': take_profits[0], 'quantity_percent': 33.33},
                    {'price': take_profits[1], 'quantity_percent': 33.33},
                    {'price': take_profits[2], 'quantity_percent': 33.34}
                ]

                logger.info(f"Сигнал обработан для пользователя {user_id}: order_id={order_id}, symbol={normalized_symbol}")
            except Exception as e:
                logger.error(f"Ошибка обработки сигнала для пользователя {user_id}: {str(e)}")
                continue

        if not results:
            return jsonify({'status': 'error', 'message': 'Не удалось обработать сигнал ни для одного пользователя'}), 500

        return jsonify({
            'status': 'success',
            'message': 'Фьючерсный сигнал обработан для активных пользователей',
            'signal_id': 'webhook_signal',
            'symbol': normalized_symbol,
            'results': results
        }), 200

    except Exception as e:
        logger.error(f"Ошибка обработки webhook: {str(e)}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

# Запуск проверки стоп-лоссов в отдельном потоке
def run_scheduler():
    schedule.every(60).seconds.do(check_and_update_stop_loss)
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    logger.info("Запуск приемника фьючерсных сигналов...")
    # Запускаем проверку стоп-лоссов в отдельном потоке
    scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
    scheduler_thread.start()
    app.run(host='0.0.0.0', port=5000, debug=False)
    logger.info("Приемник запущен и слушает на порту 5000")