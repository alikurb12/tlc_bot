import logging
import asyncio
import datetime
from dotenv import load_dotenv
import os
import psycopg2
from psycopg2.extras import RealDictCursor
from aiogram import Bot, Dispatcher, types, Router, F
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.state import State, StatesGroup
from aiogram.filters import Command
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError
from aiogram.enums.chat_member_status import ChatMemberStatus
import aiohttp
import requests

logging.basicConfig(level=logging.INFO)
load_dotenv()

# ------------------- Настройки из .env -------------------
BOT_TOKEN = os.getenv("BOT_TOKEN")
YOOMONEY_ACCESS_TOKEN = os.getenv("YOOMONEY_ACCESS_TOKEN")
YOOMONEY_RECEIVER = os.getenv("YOOMONEY_RECEIVER")
GROUP_ID = int(os.getenv("GROUP_ID"))
MODERATOR_GROUP_ID = int(os.getenv("MODERATOR_GROUP_ID"))
SUPPORT_CONTACT = os.getenv("SUPPORT_CONTACT", "@vextrsupport")
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = os.getenv("DB_PORT", "5432")

bot = Bot(token=BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)
router = Router()
dp.include_router(router)

# ------------------- База данных -------------------
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
    print("Database connection established successfully.")
except Exception as e:
    logging.error(f"Database connection error: {e}")
    raise

# Создание таблиц
cursor.execute("""
    CREATE TABLE IF NOT EXISTS users (
        user_id BIGINT PRIMARY KEY,
        chat_id BIGINT,
        subscription_end TIMESTAMP,
        subscription_type TEXT,
        referral_uuid TEXT,
        api_key TEXT,
        secret_key TEXT,
        passphrase TEXT,
        exchange TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
""")
cursor.execute("""
    CREATE TABLE IF NOT EXISTS payments (
        invoice_id TEXT PRIMARY KEY,
        user_id BIGINT,
        amount REAL,
        currency TEXT,
        status TEXT,
        tariff_id TEXT,
        payment_method TEXT DEFAULT 'yoomoney',
        yoomoney_label TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (user_id) REFERENCES users (user_id)
    )
""")

# Проверки и добавление столбцов
cursor.execute("""
    SELECT column_name 
    FROM information_schema.columns 
    WHERE table_name = 'users' AND column_name = 'chat_id';
""")
if not cursor.fetchone():
    cursor.execute("ALTER TABLE users ADD COLUMN chat_id BIGINT;")
    conn.commit()

cursor.execute("""
    SELECT column_name 
    FROM information_schema.columns 
    WHERE table_name = 'users' AND column_name = 'passphrase';
""")
if not cursor.fetchone():
    cursor.execute("ALTER TABLE users ADD COLUMN passphrase TEXT;")
    conn.commit()

cursor.execute("""
    SELECT column_name 
    FROM information_schema.columns 
    WHERE table_name = 'payments' AND column_name = 'payment_method';
""")
if not cursor.fetchone():
    cursor.execute("ALTER TABLE payments ADD COLUMN payment_method TEXT DEFAULT 'yoomoney';")
    conn.commit()

cursor.execute("""
    SELECT column_name 
    FROM information_schema.columns 
    WHERE table_name = 'payments' AND column_name = 'yoomoney_label';
""")
if not cursor.fetchone():
    cursor.execute("ALTER TABLE payments ADD COLUMN yoomoney_label TEXT;")
    conn.commit()

conn.commit()

# ------------------- Тарифы -------------------
TARIFFS = {
    '1month': {'days': 30, 'price': 500, 'name': '1 месяц', 'currency': 'RUB'},
    '3months': {'days': 90, 'price': 1200, 'name': '3 месяца', 'currency': 'RUB'},
}


# ------------------- Состояния -------------------
class PaymentStates(StatesGroup):
    waiting_for_subscription_type = State()
    waiting_for_exchange = State()
    waiting_for_referral_uuid = State()
    waiting_for_payment = State()
    waiting_for_api_key = State()
    waiting_for_secret_key = State()
    waiting_for_passphrase = State()


# ------------------- YooMoney -------------------
def create_yoomoney_payment(user_id: int, amount: float, description: str):
    """
    Генерация QuickPay ссылки YooMoney.
    """
    label = f"user_{user_id}"
    pay_url = (
        f"https://yoomoney.ru/quickpay/confirm.xml?"
        f"receiver={YOOMONEY_RECEIVER}&"
        f"quickpay-form=donate&"
        f"targets={description}&"
        f"paymentType=SB&"  # SB - Сбербанк, AC - карта
        f"sum={amount}&"
        f"label={label}"
    )
    return {"status": "success", "pay_url": pay_url, "label": label}


def check_yoomoney_payment(label: str):
    """
    Проверка входящих платежей по метке через YooMoney API.
    """
    url = "https://yoomoney.ru/api/operation-history"
    headers = {
        "Authorization": f"Bearer {YOOMONEY_ACCESS_TOKEN}",
        "Content-Type": "application/x-www-form-urlencoded"
    }
    data = {
        "type": "deposition",
        "label": label,
        "records": 10
    }
    try:
        response = requests.post(url, headers=headers, data=data)
        response.raise_for_status()
        operations = response.json().get("operations", [])
        for op in operations:
            if op.get("label") == label and op.get("status") == "success":
                return True
        return False
    except requests.exceptions.RequestException as e:
        logging.error(f"Ошибка проверки YooMoney: {e}")
        return False


# ------------------- Клавиатуры -------------------
def get_subscription_type_keyboard():
    keyboard = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="Обычная подписка", callback_data="subscription:regular")],
        [types.InlineKeyboardButton(text="Реферальная подписка", callback_data="subscription:referral")],
        [types.InlineKeyboardButton(text="📞 Поддержка", url=f"https://t.me/{SUPPORT_CONTACT.lstrip('@')}")]
    ])
    return keyboard


def get_tariffs_keyboard():
    keyboard = types.InlineKeyboardMarkup(inline_keyboard=[])
    for tariff_id, tariff in TARIFFS.items():
        keyboard.inline_keyboard.append([types.InlineKeyboardButton(
            callback_data=f"tariff:{tariff_id}",
            text=f"{tariff['name']} - {tariff['price']}₽"
        )])
    keyboard.inline_keyboard.append(
        [types.InlineKeyboardButton(text="📞 Поддержка", url=f"https://t.me/{SUPPORT_CONTACT.lstrip('@')}")])
    return keyboard


def get_exchange_keyboard():
    keyboard = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="BingX", callback_data="exchange:bingx"),
         types.InlineKeyboardButton(text="OKX", callback_data="exchange:okx")],
        [types.InlineKeyboardButton(text="📞 Поддержка", url=f"https://t.me/{SUPPORT_CONTACT.lstrip('@')}")]
    ])
    return keyboard


def get_main_menu(user_id):
    buttons = [[types.KeyboardButton(text="Подключить API")]]
    cursor.execute("SELECT subscription_end FROM users WHERE user_id = %s", (user_id,))
    result = cursor.fetchone()
    if result and result['subscription_end'] is not None and result['subscription_end'] > datetime.datetime.now():
        buttons.append([types.KeyboardButton(text="Информация о подписке")])
    buttons.append([types.KeyboardButton(text="📞 Поддержка")])
    keyboard = types.ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True, one_time_keyboard=False)
    return keyboard


# ------------------- Вспомогательные функции -------------------
async def is_bot_in_group():
    try:
        print(f"Checking bot in group: {GROUP_ID}")
        member = await bot.get_chat_member(chat_id=GROUP_ID, user_id=bot.id)
        return member.status in [ChatMemberStatus.MEMBER, ChatMemberStatus.ADMINISTRATOR]
    except TelegramForbiddenError:
        logging.error("Бот не добавлен в группу или не имеет доступа.")
        return False
    except Exception as e:
        logging.error(f"Ошибка при проверке нахождения бота в группе: {e}")
        return False


VIDEO_INSTRUCTIONS = {
    'bingx': 'videos/bingx.mp4',
    'okx': 'videos/okx.mp4'
}


# ------------------- Обработчики команд -------------------
@router.message(Command("start"))
async def cmd_start(message: types.Message, state: FSMContext):
    is_in_group = await is_bot_in_group()
    if not is_in_group:
        keyboard = types.InlineKeyboardMarkup(inline_keyboard=[
            [types.InlineKeyboardButton(text="📞 Поддержка", url=f"https://t.me/{SUPPORT_CONTACT.lstrip('@')}")]
        ])
        await message.answer(
            "❌ Бот не состоит в группе или не имеет доступа. Пожалуйста, добавьте бота в группу и назначьте его администратором.",
            reply_markup=keyboard
        )
        return

    user_id = message.from_user.id
    cursor.execute("SELECT subscription_end, subscription_type FROM users WHERE user_id = %s", (user_id,))
    result = cursor.fetchone()

    await message.answer(
        "Данный бот предоставляет вам возможность пользоваться автоматизированной версией нашей торговой стратегии без надобности выходить за пределы Telegram.\n"
        "Вам остается лишь один раз провести небольшую настройку, после чего вы сможете пользоваться стратегией и с помощью этого бота.\n"
        "Для начала вам нужно выбрать тип вашей подписки",
        reply_markup=get_subscription_type_keyboard()
    )
    await state.set_state(PaymentStates.waiting_for_subscription_type)


@router.callback_query(F.data.startswith("subscription:"))
async def process_subscription_type(callback_query: types.CallbackQuery, state: FSMContext):
    subscription_type = callback_query.data.split(":")[1]
    user_id = callback_query.from_user.id
    await callback_query.answer()

    cursor.execute("SELECT subscription_type FROM users WHERE user_id = %s", (user_id,))
    result = cursor.fetchone()

    if subscription_type == "referral":
        if result and result['subscription_type'] == "referral_approved":
            await callback_query.message.edit_text(
                "✅ У вас уже есть подтверждённая реферальная подписка. Выберите биржу для подключения API:",
                reply_markup=get_exchange_keyboard()
            )
            await state.set_state(PaymentStates.waiting_for_api_key)
            return
        elif result and result['subscription_type'] == "referral_pending":
            keyboard = types.InlineKeyboardMarkup(inline_keyboard=[
                [types.InlineKeyboardButton(text="📞 Поддержка", url=f"https://t.me/{SUPPORT_CONTACT.lstrip('@')}")]
            ])
            await callback_query.message.edit_text(
                "⏳ Ваш предыдущий UUID ещё на модерации. Пожалуйста, дождитесь ответа.",
                reply_markup=keyboard
            )
            return
        await callback_query.message.edit_text(
            "Выберите биржу для реферальной подписки:",
            reply_markup=get_exchange_keyboard()
        )
        await state.update_data(subscription_type=subscription_type, user_id=user_id)
        await state.set_state(PaymentStates.waiting_for_exchange)
    else:
        cursor.execute(
            "INSERT INTO users (user_id, chat_id, subscription_type) VALUES (%s, %s, %s) ON CONFLICT (user_id) DO UPDATE SET chat_id = %s, subscription_type = %s",
            (user_id, user_id, "regular", user_id, "regular")
        )
        conn.commit()
        await callback_query.message.edit_text(
            "Выберите тариф для подписки:",
            reply_markup=get_tariffs_keyboard()
        )
        await state.update_data(subscription_type="regular")
        await state.set_state(PaymentStates.waiting_for_payment)


@router.callback_query(F.data.startswith("tariff:"))
async def process_tariff_selection(callback_query: types.CallbackQuery, state: FSMContext):
    tariff_id = callback_query.data.split(":")[1]
    user_id = callback_query.from_user.id
    await callback_query.answer()

    if tariff_id not in TARIFFS:
        keyboard = types.InlineKeyboardMarkup(inline_keyboard=[
            [types.InlineKeyboardButton(text="📞 Поддержка", url=f"https://t.me/{SUPPORT_CONTACT.lstrip('@')}")]
        ])
        await callback_query.message.edit_text("❌ Неверный тариф. Пожалуйста, выберите снова:", reply_markup=keyboard)
        return

    tariff = TARIFFS[tariff_id]
    description = f"Подписка {tariff['name']}"

    # Создаем платеж в YooMoney
    payment_result = create_yoomoney_payment(user_id, tariff['price'], description)

    if payment_result["status"] != "success":
        keyboard = types.InlineKeyboardMarkup(inline_keyboard=[
            [types.InlineKeyboardButton(text="📞 Поддержка", url=f"https://t.me/{SUPPORT_CONTACT.lstrip('@')}")]
        ])
        await callback_query.message.edit_text(
            "❌ Ошибка при создании платежа. Попробуйте позже или свяжитесь с поддержкой.",
            reply_markup=keyboard
        )
        return

    # Сохраняем в базу данных
    invoice_id = f"yoomoney_{payment_result['label']}"
    cursor.execute(
        "INSERT INTO payments (invoice_id, user_id, amount, currency, status, tariff_id, payment_method, yoomoney_label) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
        (invoice_id, user_id, tariff['price'], "RUB", "pending", tariff_id, "yoomoney", payment_result['label'])
    )
    conn.commit()

    # Отправляем пользователю ссылку для оплаты
    keyboard = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="💳 Оплатить", url=payment_result["pay_url"])],
        [types.InlineKeyboardButton(text="✅ Я оплатил", callback_data=f"check_payment:{payment_result['label']}")],
        [types.InlineKeyboardButton(text="Отмена", callback_data="cancel")],
        [types.InlineKeyboardButton(text="📞 Поддержка", url=f"https://t.me/{SUPPORT_CONTACT.lstrip('@')}")]
    ])

    await callback_query.message.edit_text(
        f"💳 Оплатите <b>{tariff['price']}₽</b> за <b>{tariff['name']}</b>\n"
        f"🔗 <a href='{payment_result['pay_url']}'>Ссылка для оплаты</a>\n\n"
        f"После оплаты нажмите кнопку ниже 👇",
        parse_mode="HTML",
        reply_markup=keyboard
    )
    await state.update_data(
        tariff_id=tariff_id,
        yoomoney_label=payment_result['label'],
        invoice_id=invoice_id
    )
    await state.set_state(PaymentStates.waiting_for_payment)


@router.callback_query(F.data.startswith("check_payment:"))
async def check_payment_callback(callback_query: types.CallbackQuery, state: FSMContext):
    label = callback_query.data.split(":")[1]
    user_id = callback_query.from_user.id
    await callback_query.answer("🔄 Проверяем оплату, подождите...")

    # Проверяем статус платежа
    paid = check_yoomoney_payment(label)

    if paid:
        # Находим данные платежа
        cursor.execute(
            "SELECT user_id, tariff_id FROM payments WHERE yoomoney_label = %s",
            (label,)
        )
        payment = cursor.fetchone()

        if payment:
            tariff = TARIFFS.get(payment['tariff_id'])
            if tariff:
                # Обновляем подписку
                subscription_end = datetime.datetime.now() + datetime.timedelta(days=tariff['days'])
                cursor.execute(
                    "UPDATE users SET subscription_end = %s, subscription_type = %s WHERE user_id = %s",
                    (subscription_end, "regular", payment['user_id'])
                )
                cursor.execute(
                    "UPDATE payments SET status = %s WHERE yoomoney_label = %s",
                    ("completed", label)
                )
                conn.commit()

                await callback_query.message.edit_text(
                    f"✅ Оплата подтверждена!\n"
                    f"🔓 Подписка активна до <b>{subscription_end.strftime('%d.%m.%Y %H:%M')}</b>\n\n"
                    f"Теперь вы можете подключить API для автоматической торговли.",
                    parse_mode="HTML",
                    reply_markup=get_main_menu(user_id)
                )
                await state.clear()
                return

    # Если платеж еще не прошел
    keyboard = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="🔄 Проверить снова", callback_data=f"check_payment:{label}")],
        [types.InlineKeyboardButton(text="📞 Поддержка", url=f"https://t.me/{SUPPORT_CONTACT.lstrip('@')}")]
    ])
    await callback_query.message.edit_text(
        "⏳ Платёж ещё не подтверждён. Попробуйте проверить статус через несколько секунд.",
        reply_markup=keyboard
    )


@router.message(Command("status"))
async def cmd_status(message: types.Message):
    user_id = message.from_user.id
    cursor.execute("SELECT subscription_end, subscription_type FROM users WHERE user_id = %s", (user_id,))
    result = cursor.fetchone()

    if not result or not result['subscription_end']:
        await message.answer("❌ У вас нет активной подписки.")
        return

    end_date = result['subscription_end']
    now = datetime.datetime.now()

    if end_date < now:
        await message.answer("⚠️ Ваша подписка истекла.")
    else:
        remain = end_date - now
        await message.answer(
            f"✅ Ваша подписка активна до <b>{end_date.strftime('%d.%m.%Y %H:%M')}</b>\n"
            f"🕓 Осталось примерно <b>{remain.days}</b> дней.\n"
            f"📊 Тип подписки: <b>{result['subscription_type']}</b>",
            parse_mode="HTML",
            reply_markup=get_main_menu(user_id)
        )


# ------------------- Обработчики для реферальной системы -------------------
@router.callback_query(F.data.startswith("exchange:"))
async def process_exchange(callback_query: types.CallbackQuery, state: FSMContext):
    exchange = callback_query.data.split(":")[1]
    user_id = callback_query.from_user.id
    video_path = VIDEO_INSTRUCTIONS.get(exchange)

    if not video_path or not os.path.exists(video_path):
        logging.error(f"Video file for {exchange} not found at {video_path}")
        keyboard = types.InlineKeyboardMarkup(inline_keyboard=[
            [types.InlineKeyboardButton(text="📞 Поддержка", url=f"https://t.me/{SUPPORT_CONTACT.lstrip('@')}")]
        ])
        try:
            await callback_query.message.edit_text(
                "❌ Ошибка: Видеоинструкция для выбранной биржи недоступна. Пожалуйста, свяжитесь с поддержкой.",
                reply_markup=keyboard
            )
        except TelegramBadRequest:
            await bot.send_message(
                chat_id=user_id,
                text="❌ Ошибка: Видеоинструкция для выбранной биржи недоступна. Пожалуйста, свяжитесь с поддержкой.",
                reply_markup=keyboard
            )
        return

    try:
        for attempt in range(3):
            try:
                await bot.send_video(
                    chat_id=user_id,
                    video=types.FSInputFile(video_path),
                    caption=f"📹 Ознакомьтесь с видеоинструкцией по созданию API-ключа для {exchange.upper()}:",
                    request_timeout=100
                )
                break
            except aiohttp.ClientError as e:
                logging.warning(f"Attempt {attempt + 1} failed for user {user_id}: {e}")
                if attempt == 2:
                    logging.error(f"All attempts to send video for {exchange} to user {user_id} failed")
                    keyboard = types.InlineKeyboardMarkup(inline_keyboard=[
                        [types.InlineKeyboardButton(text="📞 Поддержка",
                                                    url=f"https://t.me/{SUPPORT_CONTACT.lstrip('@')}")]
                    ])
                    await bot.send_message(
                        chat_id=user_id,
                        text="❌ Ошибка при отправке видеоинструкции из-за сетевых проблем. Пожалуйста, попробуйте снова позже.",
                        reply_markup=keyboard
                    )
                    return
                await asyncio.sleep(2)

        keyboard = types.InlineKeyboardMarkup(inline_keyboard=[
            [types.InlineKeyboardButton(text="Отмена", callback_data="cancel")],
            [types.InlineKeyboardButton(text="📞 Поддержка", url=f"https://t.me/{SUPPORT_CONTACT.lstrip('@')}")]
        ])
        await bot.send_message(
            chat_id=user_id,
            text="📎 Пожалуйста, введите ваш UUID с биржи для модерации:",
            reply_markup=keyboard
        )
        await state.update_data(exchange=exchange)
        await state.set_state(PaymentStates.waiting_for_referral_uuid)
        logging.info(f"Exchange selected: {exchange} for user {user_id}, video sent: {video_path}")

    except TelegramForbiddenError:
        logging.error(f"User {user_id} blocked the bot")
        try:
            await bot.send_message(
                chat_id=MODERATOR_GROUP_ID,
                text=f"Пользователь {user_id} заблокировал бота при попытке отправки видео для {exchange}."
            )
        except Exception as mod_error:
            logging.error(f"Failed to notify moderator group about user {user_id} blocking bot: {mod_error}")
        try:
            await callback_query.message.edit_text(
                "❌ Вы заблокировали бота. Разблокируйте, чтобы продолжить."
            )
        except TelegramBadRequest:
            pass
    except Exception as e:
        logging.error(f"Unexpected error sending video for {exchange} to user {user_id}: {e}")
        keyboard = types.InlineKeyboardMarkup(inline_keyboard=[
            [types.InlineKeyboardButton(text="📞 Поддержка", url=f"https://t.me/{SUPPORT_CONTACT.lstrip('@')}")]
        ])
        try:
            await bot.send_message(
                chat_id=user_id,
                text="❌ Произошла ошибка. Пожалуйста, попробуйте снова или свяжитесь с поддержкой.",
                reply_markup=keyboard
            )
        except TelegramBadRequest:
            await bot.send_message(
                chat_id=MODERATOR_GROUP_ID,
                text=f"Не удалось отправить сообщение пользователю {user_id} из-за ошибки: {e}"
            )


@router.message(PaymentStates.waiting_for_referral_uuid)
async def process_referral_uuid(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    referral_uuid = message.text.strip()
    data = await state.get_data()
    exchange = data.get('exchange')
    logging.info(f"Processing UUID {referral_uuid} for user {user_id}, exchange: {exchange}")

    if len(referral_uuid) < 8:
        keyboard = types.InlineKeyboardMarkup(inline_keyboard=[
            [types.InlineKeyboardButton(text="📞 Поддержка", url=f"https://t.me/{SUPPORT_CONTACT.lstrip('@')}")]
        ])
        await message.answer("❌ Неверный формат UUID. Пожалуйста, попробуйте снова:", reply_markup=keyboard)
        return

    cursor.execute("SELECT subscription_type, subscription_end, api_key FROM users WHERE user_id = %s", (user_id,))
    result = cursor.fetchone()

    if result and result['subscription_type'] == "referral_pending":
        keyboard = types.InlineKeyboardMarkup(inline_keyboard=[
            [types.InlineKeyboardButton(text="📞 Поддержка", url=f"https://t.me/{SUPPORT_CONTACT.lstrip('@')}")]
        ])
        await message.answer("⏳ Ваш предыдущий UUID ещё на модерации. Пожалуйста, дождитесь ответа.",
                             reply_markup=keyboard)
        return
    elif result and result['subscription_type'] == "referral_approved" and result[
        'subscription_end'] > datetime.datetime.now():
        if result['api_key']:
            await message.answer(
                "✅ У вас уже есть активная реферальная подписка и подключённая автоторговля. Выберите действие:",
                reply_markup=get_main_menu(user_id)
            )
        else:
            await message.answer(
                "✅ У вас уже есть активная реферальная подписка. Введите ваш API-ключ:",
                reply_markup=types.ReplyKeyboardRemove()
            )
            await state.set_state(PaymentStates.waiting_for_api_key)
        return

    cursor.execute(
        "INSERT INTO users (user_id, chat_id, subscription_type, referral_uuid, exchange) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (user_id) DO UPDATE SET chat_id = %s, subscription_type = %s, referral_uuid = %s, exchange = %s",
        (user_id, user_id, "referral_pending", referral_uuid, exchange, user_id, "referral_pending", referral_uuid,
         exchange)
    )
    conn.commit()

    try:
        await bot.send_message(
            MODERATOR_GROUP_ID,
            f"Новый запрос на реферальную подписку:\n"
            f"Пользователь: {user_id}\n"
            f"Биржа: {exchange.upper()}\n"
            f"UUID: {referral_uuid}\n"
            f"Пожалуйста, подтвердите или отклоните запрос.",
            reply_markup=types.InlineKeyboardMarkup(inline_keyboard=[
                [types.InlineKeyboardButton(text="✅ Подтвердить", callback_data=f"approve_uuid:{user_id}")],
                [types.InlineKeyboardButton(text="❌ Отклонить", callback_data=f"reject_uuid:{user_id}")]
            ])
        )
        keyboard = types.InlineKeyboardMarkup(inline_keyboard=[
            [types.InlineKeyboardButton(text="📞 Поддержка", url=f"https://t.me/{SUPPORT_CONTACT.lstrip('@')}")]
        ])
        await message.answer(
            "✅ UUID отправлен на модерацию. Ожидайте подтверждения от модератора.",
            reply_markup=keyboard
        )
        await state.update_data(referral_uuid=referral_uuid)
    except Exception as e:
        logging.error(f"Ошибка отправки UUID модератору: {e}")
        keyboard = types.InlineKeyboardMarkup(inline_keyboard=[
            [types.InlineKeyboardButton(text="📞 Поддержка", url=f"https://t.me/{SUPPORT_CONTACT.lstrip('@')}")]
        ])
        await message.answer("❌ Ошибка при отправке UUID. Попробуйте позже.", reply_markup=keyboard)
        await state.clear()


@router.callback_query(F.data.startswith("approve_uuid:") | F.data.startswith("reject_uuid:"))
async def process_moderator_decision(callback_query: types.CallbackQuery, state: FSMContext):
    try:
        logging.info(f"Processing callback: {callback_query.data}")
        action, user_id = callback_query.data.split(":")
        user_id = int(user_id)
        await callback_query.answer()

        if action == "approve_uuid":
            logging.info(f"Approving UUID for user {user_id}")
            subscription_end = datetime.datetime.now() + datetime.timedelta(days=365)
            cursor.execute(
                "UPDATE users SET subscription_type = %s, subscription_end = %s WHERE user_id = %s",
                ("referral_approved", subscription_end, user_id)
            )
            conn.commit()
            try:
                cursor.execute("SELECT api_key, exchange FROM users WHERE user_id = %s", (user_id,))
                result = cursor.fetchone()
                current_state = await state.get_state()
                logging.info(f"Current state for user {user_id} after UUID approval: {current_state}")
                if result['api_key']:
                    await bot.send_message(
                        user_id,
                        "✅ Ваш UUID подтверждён модератором! API-ключ уже подключён. Выберите действие:",
                        reply_markup=get_main_menu(user_id)
                    )
                    await state.clear()
                else:
                    await bot.send_message(
                        user_id,
                        '''
Для успешной автоматизации вам нужно будет предоставить API-ключ и Secret Key с вашей биржи. 
ВАЖНО! Мы не используем ваши данные в личных целях и не передаем их третьим лицам! 
Все данные хранятся в защищенной базе данных и используются только для отправки команд на биржу.
Пожалуйста, введите ваш API-ключ:
''',
                        reply_markup=types.ReplyKeyboardRemove()
                    )
                    await state.update_data(exchange=result['exchange'])
                    await state.set_state(PaymentStates.waiting_for_api_key)
                await callback_query.message.edit_text(
                    f"Решение по UUID для пользователя {user_id}: Подтверждено"
                )
            except TelegramForbiddenError:
                logging.error(f"Cannot send message to user {user_id}: Bot is blocked")
                await callback_query.message.edit_text(
                    f"Решение по UUID для пользователя {user_id}: Подтверждено, но пользователь заблокировал бота"
                )
            except TelegramBadRequest as e:
                logging.error(f"Telegram error for user {user_id}: {e}")
                await callback_query.message.edit_text(
                    f"Решение по UUID для пользователя {user_id}: Подтверждено, но не удалось отправить сообщение"
                )
        else:  # reject_uuid
            logging.info(f"Rejecting UUID for user {user_id}")
            cursor.execute(
                "UPDATE users SET subscription_type = %s, referral_uuid = NULL WHERE user_id = %s",
                ("rejected", user_id)
            )
            conn.commit()
            try:
                await bot.send_message(
                    user_id,
                    "❌ Ваш UUID отклонён модератором. Вы можете выбрать обычную подписку или попробовать снова:",
                    reply_markup=get_subscription_type_keyboard()
                )
                await state.set_state(PaymentStates.waiting_for_subscription_type)
                await callback_query.message.edit_text(
                    f"Решение по UUID для пользователя {user_id}: Отклонено"
                )
            except TelegramForbiddenError:
                logging.error(f"Cannot send message to user {user_id}: Bot is blocked")
                await callback_query.message.edit_text(
                    f"Решение по UUID для пользователя {user_id}: Отклонено, но пользователь заблокировал бота"
                )
            except TelegramBadRequest as e:
                logging.error(f"Telegram error for user {user_id}: {e}")
                await callback_query.message.edit_text(
                    f"Решение по UUID для пользователя {user_id}: Отклонено, но не удалось отправить сообщение"
                )
    except Exception as e:
        logging.error(f"Error processing moderator decision for callback {callback_query.data}: {e}")
        keyboard = types.InlineKeyboardMarkup(inline_keyboard=[
            [types.InlineKeyboardButton(text="📞 Поддержка", url=f"https://t.me/{SUPPORT_CONTACT.lstrip('@')}")]
        ])
        await callback_query.message.edit_text("❌ Ошибка при обработке решения. Попробуйте снова.",
                                               reply_markup=keyboard)


# ------------------- Обработчики для API подключения -------------------
@router.message(F.text == "Подключить API")
async def connect_api(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    current_state = await state.get_state()
    logging.info(f"Processing 'Подключить API' for user {user_id}, current state: {current_state}")

    cursor.execute("SELECT subscription_type, subscription_end, api_key FROM users WHERE user_id = %s", (user_id,))
    result = cursor.fetchone()

    if result and result['subscription_type'] == "referral_pending":
        keyboard = types.InlineKeyboardMarkup(inline_keyboard=[
            [types.InlineKeyboardButton(text="📞 Поддержка", url=f"https://t.me/{SUPPORT_CONTACT.lstrip('@')}")]
        ])
        await message.answer("⏳ Ваш UUID на модерации. Пожалуйста, дождитесь подтверждения.", reply_markup=keyboard)
        return

    if result and result['subscription_type'] == "referral_approved" and result[
        'subscription_end'] > datetime.datetime.now():
        if result['api_key']:
            await message.answer(
                "✅ У вас уже подключенна автоматизация. Вы можете проверить информацию о подписке или связаться с поддержкой для изменения ключей.",
                reply_markup=get_main_menu(user_id)
            )
            await state.clear()
            return
        if current_state in [PaymentStates.waiting_for_api_key, PaymentStates.waiting_for_secret_key,
                             PaymentStates.waiting_for_passphrase]:
            if current_state == PaymentStates.waiting_for_api_key:
                await message.answer('''
Для того, чтобы успешно провести автоматизацию, вам нужно будет прислать api ключ и secret key с вашей биржи. 
ВАЖНО!
Мы не используем ваши данные в личных целях и не передаем их третьим лицам! Все данные хранятся в защищенной базе данных, мы используем их только для того, чтобы иметь возможность прямого запроса команд на биржу.
Пожалуйста, напишите ваш API ключ:
''')
            elif current_state == PaymentStates.waiting_for_secret_key:
                await message.answer("Пожалуйста, введите ваш Secret Key:")
            elif current_state == PaymentStates.waiting_for_passphrase:
                await message.answer("Пожалуйста, введите ваш Passphrase:")
            return
        await state.clear()
        await message.answer("Выберите биржу:", reply_markup=get_exchange_keyboard())
        await state.set_state(PaymentStates.waiting_for_api_key)
    else:
        await message.answer(
            "❗️ У вас нет активной подписки для подключения автоматизации. Пожалуйста, выберите тип подписки:",
            reply_markup=get_subscription_type_keyboard()
        )
        await state.set_state(PaymentStates.waiting_for_subscription_type)


@router.message(F.text == "Информация о подписке")
async def subscription_info(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    current_state = await state.get_state()
    logging.info(f"Processing 'Информация о подписке' for user {user_id}, current state: {current_state}")

    cursor.execute("SELECT subscription_end, subscription_type, api_key, exchange FROM users WHERE user_id = %s",
                   (user_id,))
    result = cursor.fetchone()

    if result and result['subscription_type'] == "referral_pending":
        keyboard = types.InlineKeyboardMarkup(inline_keyboard=[
            [types.InlineKeyboardButton(text="📞 Поддержка", url=f"https://t.me/{SUPPORT_CONTACT.lstrip('@')}")]
        ])
        await message.answer("⏳ Ваш UUID на модерации. Пожалуйста, дождитесь подтверждения.", reply_markup=keyboard)
        return

    if result and result['subscription_type'] == "referral_approved" and result['subscription_end'] is not None and \
            result['subscription_end'] > datetime.datetime.now():
        subscription_end = result['subscription_end']
        subscription_type = result['subscription_type']
        api_status = "Подключен" if result['api_key'] else "Не подключен"
        exchange = result['exchange'] or "Не выбрана"
        await message.answer(
            f"📋 Информация о подписке:\n"
            f"Тип подписки: Реферальная (подтверждена)\n"
            f"Активна до: {subscription_end.strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"Биржа: {exchange}\n"
            f"API: {api_status}",
            parse_mode="HTML",
            reply_markup=get_main_menu(user_id)
        )
        await state.clear()
    else:
        await message.answer(
            "❗️ У вас нет активной подписки.",
            reply_markup=get_main_menu(user_id)
        )
        await state.set_state(PaymentStates.waiting_for_subscription_type)


@router.message(F.text == "📞 Поддержка")
async def contact_support(message: types.Message, state: FSMContext):
    await message.answer(
        f"📞 Свяжитесь с поддержкой: {SUPPORT_CONTACT}",
        reply_markup=get_main_menu(message.from_user.id)
    )


@router.message(PaymentStates.waiting_for_api_key)
async def process_api_key(message: types.Message, state: FSMContext):
    api_key = message.text.strip()
    user_id = message.from_user.id
    data = await state.get_data()
    exchange = data.get('exchange')

    if not exchange:
        logging.error(f"No exchange selected for user {user_id}")
        keyboard = types.InlineKeyboardMarkup(inline_keyboard=[
            [types.InlineKeyboardButton(text="📞 Поддержка", url=f"https://t.me/{SUPPORT_CONTACT.lstrip('@')}")]
        ])
        await message.answer("Ошибка: биржа не выбрана. Пожалуйста, выберите биржу:",
                             reply_markup=get_exchange_keyboard())
        return

    if len(api_key) < 10:
        logging.warning(f"Invalid API key length for user {user_id}: {api_key}")
        keyboard = types.InlineKeyboardMarkup(inline_keyboard=[
            [types.InlineKeyboardButton(text="📞 Поддержка", url=f"https://t.me/{SUPPORT_CONTACT.lstrip('@')}")]
        ])
        await message.answer("❌ Неверный формат API-ключа. Пожалуйста, введите корректный API-ключ:",
                             reply_markup=keyboard)
        return

    logging.info(f"API key received for user {user_id}: {api_key}")
    await state.update_data(api_key=api_key)
    await message.answer("Введите ваш Secret Key:")
    await state.set_state(PaymentStates.waiting_for_secret_key)


@router.message(PaymentStates.waiting_for_secret_key)
async def process_secret_key(message: types.Message, state: FSMContext):
    secret_key = message.text.strip()
    data = await state.get_data()
    user_id = message.from_user.id
    exchange = data.get('exchange')
    api_key = data.get('api_key')

    if len(secret_key) < 10:
        logging.warning(f"Invalid Secret Key length for user {user_id}: {secret_key}")
        keyboard = types.InlineKeyboardMarkup(inline_keyboard=[
            [types.InlineKeyboardButton(text="📞 Поддержка", url=f"https://t.me/{SUPPORT_CONTACT.lstrip('@')}")]
        ])
        await message.answer("❌ Неверный формат Secret Key. Пожалуйста, введите корректный Secret Key:",
                             reply_markup=keyboard)
        return

    logging.info(f"Secret key received for user {user_id}: {secret_key}")
    await state.update_data(secret_key=secret_key)

    if exchange == 'okx':
        await message.answer("Введите ваш Passphrase:")
        await state.set_state(PaymentStates.waiting_for_passphrase)
    else:
        cursor.execute(
            "UPDATE users SET api_key = %s, secret_key = %s, passphrase = NULL, exchange = %s, chat_id = %s WHERE user_id = %s",
            (api_key, secret_key, exchange, user_id, user_id)
        )
        conn.commit()
        await message.answer(
            "✅ Вы успешно подключили автоторговлю!",
            reply_markup=get_main_menu(user_id)
        )
        await state.clear()
        logging.info(f"API keys successfully saved for user {user_id}, exchange: {exchange}")


@router.message(PaymentStates.waiting_for_passphrase)
async def process_passphrase(message: types.Message, state: FSMContext):
    passphrase = message.text.strip()
    data = await state.get_data()
    user_id = message.from_user.id
    exchange = data.get('exchange')
    api_key = data.get('api_key')
    secret_key = data.get('secret_key')

    if len(passphrase) < 8:
        logging.warning(f"Invalid Passphrase length for user {user_id}: {passphrase}")
        keyboard = types.InlineKeyboardMarkup(inline_keyboard=[
            [types.InlineKeyboardButton(text="📞 Поддержка", url=f"https://t.me/{SUPPORT_CONTACT.lstrip('@')}")]
        ])
        await message.answer(
            "❌ Неверный формат Passphrase. Пожалуйста, введите корректный Passphrase (минимум 8 символов):",
            reply_markup=keyboard
        )
        return

    logging.info(f"Passphrase received for user {user_id}: {passphrase}")
    cursor.execute(
        "UPDATE users SET api_key = %s, secret_key = %s, passphrase = %s, exchange = %s, chat_id = %s WHERE user_id = %s",
        (api_key, secret_key, passphrase, exchange, user_id, user_id)
    )
    conn.commit()

    await message.answer(
        "✅ API-ключ и Passphrase успешно добавлены! Автоматизация подключена.",
        reply_markup=get_main_menu(user_id)
    )
    await state.clear()
    logging.info(f"API keys and passphrase successfully saved for user {user_id}, exchange: {exchange}")


@router.callback_query(F.data == "cancel")
async def cancel_action(callback_query: types.CallbackQuery, state: FSMContext):
    await callback_query.message.delete()
    await bot.send_message(
        callback_query.from_user.id,
        "Действие отменено.",
        reply_markup=get_main_menu(callback_query.from_user.id)
    )
    await state.clear()


@router.message(lambda message: message.text not in ["Подключить API", "Информация о подписке", "📞 Поддержка"])
async def handle_invalid_input(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    current_state = await state.get_state()
    logging.info(f"Invalid input from user {user_id}, state: {current_state}, input: {message.text}")

    cursor.execute("SELECT subscription_type, subscription_end FROM users WHERE user_id = %s", (user_id,))
    result = cursor.fetchone()

    if current_state == PaymentStates.waiting_for_api_key:
        await process_api_key(message, state)
        return
    elif current_state == PaymentStates.waiting_for_secret_key:
        await process_secret_key(message, state)
        return
    elif current_state == PaymentStates.waiting_for_passphrase:
        await process_passphrase(message, state)
        return
    elif current_state == PaymentStates.waiting_for_referral_uuid:
        await process_referral_uuid(message, state)
        return
    elif result and result['subscription_type'] == "referral_approved" and result[
        'subscription_end'] > datetime.datetime.now():
        await message.answer(
            "✅ У вас есть активная подписка. Выберите действие:",
            reply_markup=get_main_menu(user_id)
        )
        await state.clear()
    else:
        keyboard = types.InlineKeyboardMarkup(inline_keyboard=[
            [types.InlineKeyboardButton(text="📞 Поддержка", url=f"https://t.me/{SUPPORT_CONTACT.lstrip('@')}")]
        ])
        await message.answer(
            "❗️ Пожалуйста, используйте кнопки меню для взаимодействия с ботом.",
            reply_markup=keyboard
        )
        await state.clear()


# ------------------- Фоновые задачи -------------------
async def check_yoomoney_payments():
    """Периодическая проверка статусов платежей YooMoney"""
    while True:
        try:
            cursor.execute(
                "SELECT yoomoney_label, user_id, tariff_id FROM payments WHERE payment_method = 'yoomoney' AND status = 'pending'"
            )
            pending_payments = cursor.fetchall()

            for payment in pending_payments:
                label = payment['yoomoney_label']
                user_id = payment['user_id']
                tariff_id = payment['tariff_id']

                paid = check_yoomoney_payment(label)

                if paid:
                    tariff = TARIFFS.get(tariff_id)
                    if tariff:
                        subscription_end = datetime.datetime.now() + datetime.timedelta(days=tariff['days'])
                        cursor.execute(
                            "UPDATE users SET subscription_end = %s, subscription_type = %s WHERE user_id = %s",
                            (subscription_end, "regular", user_id)
                        )
                        cursor.execute(
                            "UPDATE payments SET status = %s WHERE yoomoney_label = %s",
                            ("completed", label)
                        )
                        conn.commit()

                        try:
                            await bot.send_message(
                                user_id,
                                f"✅ Оплата подтверждена!\n"
                                f"🔓 Подписка активна до <b>{subscription_end.strftime('%d.%m.%Y %H:%M')}</b>\n\n"
                                f"Теперь вы можете подключить API для автоматической торговли.",
                                parse_mode="HTML",
                                reply_markup=get_main_menu(user_id)
                            )
                        except TelegramBadRequest as e:
                            logging.error(f"Failed to notify user {user_id} about payment: {e}")

        except Exception as e:
            logging.error(f"Error in check_yoomoney_payments: {e}")

        await asyncio.sleep(30)


async def check_subscriptions():
    """Проверка истекших подписок"""
    while True:
        now = datetime.datetime.now()
        cursor.execute("SELECT user_id FROM users WHERE subscription_end < %s", (now,))
        expired_users = cursor.fetchall()

        for user in expired_users:
            user_id = user['user_id']
            try:
                member = await bot.get_chat_member(chat_id=GROUP_ID, user_id=user_id)
                if member.status not in [ChatMemberStatus.LEFT, ChatMemberStatus.KICKED]:
                    await bot.ban_chat_member(chat_id=GROUP_ID, user_id=user_id)
                cursor.execute("DELETE FROM users WHERE user_id = %s", (user_id,))
                conn.commit()
                try:
                    keyboard = types.InlineKeyboardMarkup(inline_keyboard=[
                        [types.InlineKeyboardButton(text="📞 Поддержка",
                                                    url=f"https://t.me/{SUPPORT_CONTACT.lstrip('@')}")]
                    ])
                    await bot.send_message(user_id, "Ваша подписка истекла. Пожалуйста, продлите её.",
                                           reply_markup=keyboard)
                except TelegramBadRequest as send_error:
                    logging.warning(f"Could not send expiration message to user {user_id}: {send_error}")
            except TelegramForbiddenError:
                logging.warning(f"Bot was blocked by user {user_id}. Removing from DB.")
                cursor.execute("DELETE FROM users WHERE user_id = %s", (user_id,))
                conn.commit()
            except Exception as processing_error:
                logging.error(f"Error processing expired subscription for user {user_id}: {processing_error}")
        await asyncio.sleep(3600)


# ------------------- Запуск -------------------
async def main():
    print("🤖 Бот VEXTR с YooMoney запущен...")
    asyncio.create_task(check_subscriptions())
    asyncio.create_task(check_yoomoney_payments())
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())