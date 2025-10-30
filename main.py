import logging
import asyncio
import datetime
import uuid
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
from yoomoney import Client, Quickpay

logging.basicConfig(level=logging.INFO)
load_dotenv()

# ------------------- Настройки -------------------
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

# ------------------- YooMoney -------------------
yoomoney_client = Client(YOOMONEY_ACCESS_TOKEN)

# ------------------- База данных -------------------
try:
    conn = psycopg2.connect(
        host=DB_HOST, database=DB_NAME, user=DB_USER,
        password=DB_PASSWORD, port=DB_PORT, cursor_factory=RealDictCursor
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
        email TEXT,
        terms_accepted BOOLEAN DEFAULT FALSE,
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

# Добавление недостающих полей
for col, sql in [
    ("email", "ALTER TABLE users ADD COLUMN email TEXT;"),
    ("terms_accepted", "ALTER TABLE users ADD COLUMN terms_accepted BOOLEAN DEFAULT FALSE;")
]:
    cursor.execute(f"""
        SELECT column_name FROM information_schema.columns 
        WHERE table_name = 'users' AND column_name = %s;
    """, (col,))
    if not cursor.fetchone():
        cursor.execute(sql)
        conn.commit()

conn.commit()

# ------------------- Тарифы -------------------
TARIFFS = {
    '1month': {'days': 30, 'price': 500, 'name': '1 месяц', 'currency': 'RUB'},
    '3months': {'days': 90, 'price': 1200, 'name': '3 месяца', 'currency': 'RUB'},
}

# ------------------- Состояния -------------------
class PaymentStates(StatesGroup):
    waiting_for_terms = State()
    waiting_for_subscription_type = State()
    waiting_for_exchange = State()
    waiting_for_referral_uuid = State()
    waiting_for_payment = State()
    waiting_for_email = State()
    waiting_for_api_key = State()
    waiting_for_secret_key = State()
    waiting_for_passphrase = State()

# ------------------- YooMoney функции -------------------
def create_yoomoney_payment(user_id: int, amount: float, description: str):
    label = f"user_{user_id}_{uuid.uuid4().hex[:8]}"
    quickpay = Quickpay(
        receiver=YOOMONEY_RECEIVER,
        quickpay_form="shop",
        targets=description,
        paymentType="SB",
        sum=amount,
        label=label
    )
    return {"status": "success", "pay_url": quickpay.redirected_url, "label": label}

def check_yoomoney_payment(label: str) -> bool:
    try:
        history = yoomoney_client.operation_history(label=label)
        for op in history.operations:
            if op.label == label and op.status == "success":
                return True
        return False
    except Exception as e:
        logging.error(f"YooMoney check error: {e}")
        return False

# ------------------- Клавиатуры -------------------
def get_terms_keyboard():
    return types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="Подтвердить", callback_data="terms:accept")],
        [types.InlineKeyboardButton(text="Отклонить", callback_data="terms:decline")],
        [types.InlineKeyboardButton(text="Поддержка", url=f"https://t.me/{SUPPORT_CONTACT.lstrip('@')}")]
    ])

def get_subscription_type_keyboard():
    return types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="Обычная подписка", callback_data="subscription:regular")],
        [types.InlineKeyboardButton(text="Реферальная подписка", callback_data="subscription:referral")],
        [types.InlineKeyboardButton(text="Поддержка", url=f"https://t.me/{SUPPORT_CONTACT.lstrip('@')}")]
    ])

def get_tariffs_keyboard():
    kb = types.InlineKeyboardMarkup(inline_keyboard=[])
    for tariff_id, tariff in TARIFFS.items():
        kb.inline_keyboard.append([types.InlineKeyboardButton(
            text=f"{tariff['name']} – {tariff['price']}₽",
            callback_data=f"tariff:{tariff_id}"
        )])
    kb.inline_keyboard.append([types.InlineKeyboardButton(text="Поддержка", url=f"https://t.me/{SUPPORT_CONTACT.lstrip('@')}")])
    return kb

def get_exchange_keyboard():
    return types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="BingX", callback_data="exchange:bingx"),
         types.InlineKeyboardButton(text="OKX", callback_data="exchange:okx")],
        [types.InlineKeyboardButton(text="Поддержка", url=f"https://t.me/{SUPPORT_CONTACT.lstrip('@')}")]
    ])

def get_main_menu(user_id):
    buttons = [[types.KeyboardButton(text="Подключить API")]]
    cursor.execute("SELECT subscription_end FROM users WHERE user_id = %s", (user_id,))
    res = cursor.fetchone()
    if res and res['subscription_end'] and res['subscription_end'] > datetime.datetime.now():
        buttons.append([types.KeyboardButton(text="Информация о подписке")])
    buttons.append([types.KeyboardButton(text="Поддержка")])
    return types.ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)

def get_support_kb():
    return types.InlineKeyboardMarkup(inline_keyboard=[[
        types.InlineKeyboardButton(text="Поддержка", url=f"https://t.me/{SUPPORT_CONTACT.lstrip('@')}")
    ]])

# ------------------- Вспомогательные -------------------
async def is_bot_in_group():
    try:
        member = await bot.get_chat_member(chat_id=GROUP_ID, user_id=bot.id)
        return member.status in [ChatMemberStatus.MEMBER, ChatMemberStatus.ADMINISTRATOR]
    except TelegramForbiddenError:
        return False
    except Exception as e:
        logging.error(f"Ошибка проверки бота в группе: {e}")
        return False

VIDEO_INSTRUCTIONS = {
    'bingx': 'videos/bingx.mp4',
    'okx': 'videos/okx.mp4'
}

# ------------------- Обработчики -------------------
@router.message(Command("start"))
async def cmd_start(message: types.Message, state: FSMContext):
    if not await is_bot_in_group():
        await message.answer("Бот не в группе или без прав админа.", reply_markup=get_support_kb())
        return

    cursor.execute("SELECT terms_accepted FROM users WHERE user_id = %s", (message.from_user.id,))
    res = cursor.fetchone()

    if res and res['terms_accepted']:
        await message.answer("Выберите тип подписки:", reply_markup=get_subscription_type_keyboard())
        await state.set_state(PaymentStates.waiting_for_subscription_type)
        return

    await message.answer(
        "Добро пожаловать в торгового бота VEXTR!\n\n"
        "Торговля на финансовых рынках связана с рисками.\n\n"
        "Продолжая, вы подтверждаете, что ознакомились и согласны с\n"
        "<a href='https://www.vextr.ru/privacy'>Политикой конфиденциальности</a> и "
        "<a href='https://www.vextr.ru/terms'>Условиями использования</a>.",
        parse_mode="HTML",
        reply_markup=get_terms_keyboard()
    )
    await state.set_state(PaymentStates.waiting_for_terms)

@router.callback_query(F.data.startswith("terms:"))
async def process_terms(callback_query: types.CallbackQuery, state: FSMContext):
    action = callback_query.data.split(":")[1]
    user_id = callback_query.from_user.id
    await callback_query.answer()

    if action == "accept":
        cursor.execute(
            "INSERT INTO users (user_id, terms_accepted) VALUES (%s, TRUE) "
            "ON CONFLICT (user_id) DO UPDATE SET terms_accepted = TRUE",
            (user_id,)
        )
        conn.commit()
        await callback_query.message.edit_text(
            "Спасибо за подтверждение! Выберите тип подписки:",
            reply_markup=get_subscription_type_keyboard()
        )
        await state.set_state(PaymentStates.waiting_for_subscription_type)
    else:
        await callback_query.message.edit_text(
            "Вы отклонили условия. Доступ к боту запрещён.",
            reply_markup=get_support_kb()
        )
        await state.clear()

@router.callback_query(F.data.startswith("subscription:"))
async def process_subscription_type(callback_query: types.CallbackQuery, state: FSMContext):
    sub_type = callback_query.data.split(":")[1]
    user_id = callback_query.from_user.id
    await callback_query.answer()

    if sub_type == "referral":
        cursor.execute("SELECT subscription_type FROM users WHERE user_id = %s", (user_id,))
        cur = cursor.fetchone()
        if cur and cur['subscription_type'] == "referral_approved":
            await callback_query.message.edit_text("Реферальная подписка активна.\nВыберите биржу:", reply_markup=get_exchange_keyboard())
            await state.set_state(PaymentStates.waiting_for_api_key)
            return
        await callback_query.message.edit_text("Выберите биржу для реферала:", reply_markup=get_exchange_keyboard())
        await state.update_data(subscription_type="referral")
        await state.set_state(PaymentStates.waiting_for_exchange)
    else:
        cursor.execute(
            "INSERT INTO users (user_id, chat_id, subscription_type) VALUES (%s, %s, %s) "
            "ON CONFLICT (user_id) DO UPDATE SET chat_id = %s, subscription_type = %s",
            (user_id, user_id, "regular", user_id, "regular")
        )
        conn.commit()
        await callback_query.message.edit_text("Выберите тариф:", reply_markup=get_tariffs_keyboard())
        await state.update_data(subscription_type="regular")
        await state.set_state(PaymentStates.waiting_for_payment)

@router.callback_query(F.data.startswith("tariff:"))
async def process_tariff_selection(callback_query: types.CallbackQuery, state: FSMContext):
    tariff_id = callback_query.data.split(":")[1]
    user_id = callback_query.from_user.id
    await callback_query.answer()

    if tariff_id not in TARIFFS:
        await callback_query.message.edit_text("Неверный тариф.", reply_markup=get_support_kb())
        return

    tariff = TARIFFS[tariff_id]
    await state.update_data(tariff_id=tariff_id, tariff_name=tariff['name'], tariff_price=tariff['price'])

    await callback_query.message.edit_text(
        "Напишите ваш e-mail.\n"
        "Отправляя e-mail, вы соглашаетесь с\n"
        "<a href='https://www.vextr.ru/privacy'>Политикой конфиденциальности</a>\n"
        "и <a href='https://www.vextr.ru/docs'>Политикой обработки персональных данных</a>",
        parse_mode="HTML",
        reply_markup=types.InlineKeyboardMarkup(inline_keyboard=[
            [types.InlineKeyboardButton(text="Отмена", callback_data="cancel")],
            [types.InlineKeyboardButton(text="Поддержка", url=f"https://t.me/{SUPPORT_CONTACT.lstrip('@')}")]
        ])
    )
    await state.set_state(PaymentStates.waiting_for_email)

@router.message(PaymentStates.waiting_for_email)
async def process_email(message: types.Message, state: FSMContext):
    email = message.text.strip()
    user_id = message.from_user.id

    if "@" not in email or "." not in email or len(email) < 5:
        await message.answer("Некорректный email. Попробуйте снова:")
        return

    data = await state.get_data()
    tariff_id = data['tariff_id']
    tariff = TARIFFS[tariff_id]
    description = f"Подписка {tariff['name']}"

    cursor.execute(
        "INSERT INTO users (user_id, email) VALUES (%s, %s) ON CONFLICT (user_id) DO UPDATE SET email = %s",
        (user_id, email, email)
    )
    conn.commit()

    payment = create_yoomoney_payment(user_id, tariff['price'], description)
    if payment["status"] != "success":
        await message.answer("Ошибка создания платежа.", reply_markup=get_support_kb())
        return

    invoice_id = f"yoomoney_{payment['label']}"
    cursor.execute(
        "INSERT INTO payments (invoice_id, user_id, amount, currency, status, tariff_id, payment_method, yoomoney_label) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
        (invoice_id, user_id, tariff['price'], "RUB", "pending", tariff_id, "yoomoney", payment['label'])
    )
    conn.commit()

    await state.update_data(yoomoney_label=payment['label'], invoice_id=invoice_id, email=email)

    kb = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="Оплатить", url=payment["pay_url"])],
        [types.InlineKeyboardButton(text="Я оплатил", callback_data=f"check_payment:{payment['label']}")],
        [types.InlineKeyboardButton(text="Отмена", callback_data="cancel")],
        [types.InlineKeyboardButton(text="Поддержка", url=f"https://t.me/{SUPPORT_CONTACT.lstrip('@')}")]
    ])

    await message.answer(
        f"Оплатите <b>{tariff['price']}₽</b> за <b>{tariff['name']}</b>\n"
        f"Email: <code>{email}</code>\n\n"
        f"<a href='{payment['pay_url']}'>Ссылка для оплаты</a>\n\n"
        f"После оплаты нажмите кнопку ниже",
        parse_mode="HTML",
        reply_markup=kb
    )
    await state.set_state(PaymentStates.waiting_for_payment)

@router.callback_query(F.data.startswith("check_payment:"))
async def check_payment_callback(callback_query: types.CallbackQuery, state: FSMContext):
    label = callback_query.data.split(":")[1]
    user_id = callback_query.from_user.id
    await callback_query.answer("Проверяем…")

    if check_yoomoney_payment(label):
        cursor.execute("SELECT tariff_id FROM payments WHERE yoomoney_label = %s", (label,))
        payment = cursor.fetchone()
        if not payment:
            await callback_query.message.edit_text("Платёж не найден.")
            return

        tariff = TARIFFS.get(payment['tariff_id'])
        if not tariff:
            await callback_query.message.edit_text("Тариф не найден.")
            return

        end = datetime.datetime.now() + datetime.timedelta(days=tariff['days'])
        cursor.execute(
            "UPDATE users SET subscription_end = %s, subscription_type = %s WHERE user_id = %s",
            (end, "regular", user_id)
        )
        cursor.execute("UPDATE payments SET status = %s WHERE yoomoney_label = %s", ("completed", label))
        conn.commit()

        data = await state.get_data()
        email = data.get('email', 'Не указан')
        tariff_name = data.get('tariff_name', tariff['name'])

        try:
            await bot.send_message(
                MODERATOR_GROUP_ID,
                f"ОПЛАТА ПОДТВЕРЖДЕНА\n\n"
                f"Пользователь: <a href='tg://user?id={user_id}'>{user_id}</a>\n"
                f"Email: <code>{email}</code>\n"
                f"Тариф: <b>{tariff_name}</b> ({tariff['price']}₽)\n"
                f"Активна до: <b>{end.strftime('%d.%m.%Y %H:%M')}</b>",
                parse_mode="HTML"
            )
        except Exception as e:
            logging.error(f"Ошибка отправки в модераторку: {e}")

        await callback_query.message.edit_text(
            f"Оплата подтверждена!\n"
            f"Подписка активна до <b>{end.strftime('%d.%m.%Y %H:%M')}</b>\n\n"
            f"Теперь подключите API.",
            parse_mode="HTML"
        )
        await bot.send_message(user_id, "Выберите действие:", reply_markup=get_main_menu(user_id))
        await state.clear()
    else:
        kb = types.InlineKeyboardMarkup(inline_keyboard=[
            [types.InlineKeyboardButton(text="Проверить снова", callback_data=f"check_payment:{label}")],
            [types.InlineKeyboardButton(text="Поддержка", url=f"https://t.me/{SUPPORT_CONTACT.lstrip('@')}")]
        ])
        await callback_query.message.edit_text("Платёж не подтверждён. Подождите и попробуйте снова.", reply_markup=kb)

# ------------------- Реферальная система -------------------
@router.callback_query(F.data.startswith("exchange:"))
async def process_exchange(callback_query: types.CallbackQuery, state: FSMContext):
    user_id = callback_query.from_user.id
    exchange = callback_query.data.split(":")[1]

    cursor.execute("SELECT subscription_type FROM users WHERE user_id = %s", (user_id,))
    res = cursor.fetchone()

    if res['subscription_type'] == "regular":
        cursor.execute("UPDATE users SET exchange = %s WHERE user_id = %s", (exchange, user_id))
        conn.commit()

        await callback_query.message.edit_text(f"Биржа {exchange.upper()} выбрана.\n\nВведите ваш API-ключ:")
        await bot.send_message(user_id, "Введите ваш API-ключ:", reply_markup=types.ReplyKeyboardRemove())
        await state.update_data(exchange=exchange)
        await state.set_state(PaymentStates.waiting_for_api_key)

    elif res['subscription_type'] == "referral_approved":
        video_path = VIDEO_INSTRUCTIONS.get(exchange)
        if not video_path or not os.path.exists(video_path):
            await callback_query.message.edit_text("Видео недоступно.", reply_markup=get_support_kb())
            return
        await bot.send_video(user_id, types.FSInputFile(video_path), caption=f"Инструкция для {exchange.upper()}")
        await bot.send_message(user_id, "Введите UUID:", reply_markup=types.InlineKeyboardMarkup(inline_keyboard=[
            [types.InlineKeyboardButton(text="Отмена", callback_data="cancel")]
        ]))
        await state.update_data(exchange=exchange)
        await state.set_state(PaymentStates.waiting_for_referral_uuid)
    else:
        await callback_query.answer("Ошибка доступа.", show_alert=True)

@router.message(PaymentStates.waiting_for_referral_uuid)
async def process_referral_uuid(message: types.Message, state: FSMContext):
    uuid_text = message.text.strip()
    user_id = message.from_user.id
    data = await state.get_data()
    exchange = data.get('exchange')

    cursor.execute(
        "INSERT INTO users (user_id, subscription_type, referral_uuid, exchange) VALUES (%s, %s, %s, %s) "
        "ON CONFLICT (user_id) DO UPDATE SET subscription_type = %s, referral_uuid = %s, exchange = %s",
        (user_id, "referral_pending", uuid_text, exchange, "referral_pending", uuid_text, exchange)
    )
    conn.commit()

    await bot.send_message(
        MODERATOR_GROUP_ID,
        f"Новый реферал:\nID: {user_id}\nБиржа: {exchange.upper()}\nUUID: {uuid_text}",
        reply_markup=types.InlineKeyboardMarkup(inline_keyboard=[
            [types.InlineKeyboardButton(text="Подтвердить", callback_data=f"approve_uuid:{user_id}")],
            [types.InlineKeyboardButton(text="Отклонить", callback_data=f"reject_uuid:{user_id}")]
        ])
    )
    await message.answer("UUID отправлен. Ожидайте.")
    await state.clear()

# ------------------- Подключение API -------------------
@router.message(F.text == "Подключить API")
async def connect_api(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    cursor.execute("SELECT subscription_type, subscription_end, api_key, exchange FROM users WHERE user_id = %s", (user_id,))
    res = cursor.fetchone()

    if not res or not res['subscription_end'] or res['subscription_end'] <= datetime.datetime.now():
        await message.answer("Нет активной подписки.", reply_markup=get_subscription_type_keyboard())
        await state.set_state(PaymentStates.waiting_for_subscription_type)
        return

    if res['api_key']:
        await message.answer("API уже подключён.", reply_markup=get_main_menu(user_id))
        return

    if res['subscription_type'] == "regular":
        if res['exchange']:
            await message.answer(f"Биржа: {res['exchange'].upper()}\nВведите API-ключ:", reply_markup=types.ReplyKeyboardRemove())
            await state.update_data(exchange=res['exchange'])
        else:
            await message.answer("Выберите биржу:", reply_markup=get_exchange_keyboard())
        await state.set_state(PaymentStates.waiting_for_api_key)

    elif res['subscription_type'] == "referral_approved":
        if res['exchange']:
            await message.answer("Введите API-ключ:", reply_markup=types.ReplyKeyboardRemove())
            await state.update_data(exchange=res['exchange'])
            await state.set_state(PaymentStates.waiting_for_api_key)
        else:
            await message.answer("Выберите биржу:", reply_markup=get_exchange_keyboard())
            await state.set_state(PaymentStates.waiting_for_api_key)

@router.message(F.text == "Информация о подписке")
async def subscription_info(message: types.Message):
    user_id = message.from_user.id
    cursor.execute("SELECT subscription_end, subscription_type, api_key, exchange FROM users WHERE user_id = %s", (user_id,))
    res = cursor.fetchone()

    if not res or not res['subscription_end'] or res['subscription_end'] <= datetime.datetime.now():
        await message.answer("Нет активной подписки.", reply_markup=get_main_menu(user_id))
        return

    sub_type = res['subscription_type']
    sub_name = {"regular": "Обычная (оплачена)", "referral_approved": "Реферальная"}.get(sub_type, sub_type)
    end_date = res['subscription_end'].strftime('%d.%m.%Y %H:%M')
    api_status = "Подключён" if res['api_key'] else "Не подключён"
    exchange_name = res['exchange'].upper() if res['exchange'] else "Не выбрана"

    await message.answer(
        f"**Информация о подписке**\n\n"
        f"**Тип:** {sub_name}\n"
        f"**Активна до:** {end_date}\n"
        f"**Биржа:** {exchange_name}\n"
        f"**API:** {api_status}",
        parse_mode="Markdown",
        reply_markup=get_main_menu(user_id)
    )

@router.message(F.text == "Поддержка")
async def contact_support(message: types.Message):
    await message.answer(f"Поддержка: {SUPPORT_CONTACT}", reply_markup=get_main_menu(message.from_user.id))

@router.message(PaymentStates.waiting_for_api_key)
async def process_api_key(message: types.Message, state: FSMContext):
    key = message.text.strip()
    if len(key) < 10:
        await message.answer("API-ключ короткий.")
        return
    data = await state.get_data()
    exchange = data.get('exchange')
    if not exchange:
        await message.answer("Биржа не выбрана. Начните заново.")
        await state.clear()
        return
    await state.update_data(api_key=key)
    await message.answer("Введите Secret Key:")
    await state.set_state(PaymentStates.waiting_for_secret_key)

@router.message(PaymentStates.waiting_for_secret_key)
async def process_secret_key(message: types.Message, state: FSMContext):
    secret = message.text.strip()
    if len(secret) < 10:
        await message.answer("Secret Key короткий.")
        return
    data = await state.get_data()
    exchange = data['exchange']
    api_key = data['api_key']
    await state.update_data(secret_key=secret)

    if exchange == 'okx':
        await message.answer("Введите Passphrase:")
        await state.set_state(PaymentStates.waiting_for_passphrase)
    else:
        cursor.execute(
            "UPDATE users SET api_key = %s, secret_key = %s, exchange = %s WHERE user_id = %s",
            (api_key, secret, exchange, message.from_user.id)
        )
        conn.commit()
        await message.answer("API подключён!", reply_markup=get_main_menu(message.from_user.id))
        await state.clear()

@router.message(PaymentStates.waiting_for_passphrase)
async def process_passphrase(message: types.Message, state: FSMContext):
    passphrase = message.text.strip()
    if len(passphrase) < 8:
        await message.answer("Passphrase короткий.")
        return
    data = await state.get_data()
    cursor.execute(
        "UPDATE users SET api_key = %s, secret_key = %s, passphrase = %s, exchange = %s WHERE user_id = %s",
        (data['api_key'], data['secret_key'], passphrase, data['exchange'], message.from_user.id)
    )
    conn.commit()
    await message.answer("API и Passphrase сохранены!", reply_markup=get_main_menu(message.from_user.id))
    await state.clear()

@router.callback_query(F.data == "cancel")
async def cancel_action(callback_query: types.CallbackQuery, state: FSMContext):
    await callback_query.message.delete()
    await bot.send_message(callback_query.from_user.id, "Отменено.", reply_markup=get_main_menu(callback_query.from_user.id))
    await state.clear()

# ------------------- Фоновые задачи -------------------
async def check_subscriptions():
    while True:
        now = datetime.datetime.now()
        cursor.execute("SELECT user_id FROM users WHERE subscription_end < %s", (now,))
        for row in cursor.fetchall():
            uid = row['user_id']
            try:
                member = await bot.get_chat_member(GROUP_ID, uid)
                if member.status not in [ChatMemberStatus.LEFT, ChatMemberStatus.KICKED]:
                    await bot.ban_chat_member(GROUP_ID, uid)
                cursor.execute("DELETE FROM users WHERE user_id = %s", (uid,))
                conn.commit()
                await bot.send_message(uid, "Подписка истекла.")
            except TelegramForbiddenError:
                cursor.execute("DELETE FROM users WHERE user_id = %s", (uid,))
                conn.commit()
            except Exception as e:
                logging.error(f"Ошибка: {e}")
        await asyncio.sleep(3600)

# ------------------- Запуск -------------------
async def main():
    print("Бот запущен...")
    asyncio.create_task(check_subscriptions())
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())