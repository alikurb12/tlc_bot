# utils.py
import re
import logging
from aiogram import Bot

logger = logging.getLogger(__name__)


def normalize_symbol(symbol: str, exchange: str) -> str:
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
    elif exchange == "bitget":
        if not symbol.endswith("_UMCBL"):
            symbol = f"{symbol}_UMCBL"
            normalized = symbol
            return normalized
    return symbol

    logger.warning(f"Неизвестная биржа: {exchange}, возвращаем исходный символ: {symbol}")
    return symbol

async def send_signal_notification(signal: dict, user_id: int, bot: Bot) -> None:
    try:
        action = signal.get('action', 'N/A')
        symbol = signal.get('symbol', 'N/A')
        price = signal.get('price', 'N/A')
        stop_loss = signal.get('stop_loss', 'N/A')
        take_profit_1 = signal.get('take_profit_1', 'N/A')
        take_profit_2 = signal.get('take_profit_2', 'N/A')
        take_profit_3 = signal.get('take_profit_3', 'N/A')
        message = signal.get('message', None)

        if action == "MOVE_SL":
            text = message or f"Стоп-лосс для {symbol} перемещён к цене входа"
        else:
            text = (
                f"📈 Новый сигнал: {action} {symbol}\n"
                f"💰 Цена: {price}\n"
                f"🛑 Стоп-лосс: {stop_loss}\n"
                f"🎯 Тейк-профит 1: {take_profit_1}\n"
                f"🎯 Тейк-профит 2: {take_profit_2}\n"
                f"🎯 Тейк-профит 3: {take_profit_3}"
            )

        await bot.send_message(chat_id=user_id, text=text, parse_mode="Markdown")
        logging.info(f"Уведомление отправлено пользователю {user_id} для сигнала {action} {symbol}")
    except Exception as e:
        logging.error(f"Ошибка отправки уведомления пользователю {user_id}: {str(e)}")