# utils.py
import re
import logging

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

    logger.warning(f"Неизвестная биржа: {exchange}, возвращаем исходный символ: {symbol}")
    return symbol