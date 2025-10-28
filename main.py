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

# ------------------- YooMoney клиент -------------------
yoomoney_client = Client(YOOMONEY_ACCESS_TOKEN)

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

# Добавление недостающих столбцов (если их нет)
for col, sql in [
    ("chat_id", "ALTER TABLE users ADD COLUMN chat_id BIGINT;"),
    ("passphrase", "ALTER TABLE users ADD COLUMN passphrase TEXT;"),
    ("payment_method", "ALTER TABLE payments ADD COLUMN payment_method TEXT DEFAULT 'yoomoney';"),
    ("yoomoney_label", "ALTER TABLE payments ADD COLUMN yoomoney_label TEXT;")
]:
    cursor.execute(f"""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = 'users' AND column_name = %s
        UNION
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = 'payments' AND column_name = %s;
    """, (col, col))
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
    waiting_for_subscription_type = State()
    waiting_for_exchange = State()
    waiting_for_referral_uuid = State()
    waiting_for_payment = State()
    waiting_for_api_key = State()
    waiting_for_secret_key = State()
    waiting_for_passphrase = State()

# ------------------- YooMoney функции -------------------
def create_yoomoney_payment(user_id: int, amount: float, description: str):
    """Создаёт QuickPay-платёж и возвращает URL и уникальный label."""
    label = f"user_{user_id}_{uuid.uuid4().hex[:8]}"
    quickpay = Quickpay(
        receiver=YOOMONEY_RECEIVER,
        quickpay_form="shop",
        targets=description,
        paymentType="SB",          # SB – карта Сбербанка, AC – любая карта
        sum=amount,
        label=label
    )
    return {
        "status": "success",
        "pay_url": quickpay.redirected_url,
        "label": label
    }

def check_yoomoney_payment(label: str) -> bool:
    """Проверяет, прошёл ли платёж с указанным label."""
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
    kb.inline_keyboard.append([types.InlineKeyboardButton(
        text="Поддержка", url=f"https://t.me/{SUPPORT_CONTACT.lstrip('@')}"
    )])
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

# ------------------- Вспомогательные функции -------------------
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

# ------------------- Обработчики команд -------------------
@router.message(Command("start"))
async def cmd_start(message: types.Message, state: FSMContext):
    if not await is_bot_in_group():
        await message.answer(
            "Бот не состоит в группе или не имеет прав администратора.\n"
            "Добавьте бота в группу и дайте ему права администратора.",
            reply_markup=types.InlineKeyboardMarkup(inline_keyboard=[[
                types.InlineKeyboardButton(text="Поддержка", url=f"https://t.me/{SUPPORT_CONTACT.lstrip('@')}")
            ]])
        )
        return

    await message.answer(
        "Добро пожаловать! Выберите тип подписки:",
        reply_markup=get_subscription_type_keyboard()
    )
    await state.set_state(PaymentStates.waiting_for_subscription_type)

@router.callback_query(F.data.startswith("subscription:"))
async def process_subscription_type(callback_query: types.CallbackQuery, state: FSMContext):
    sub_type = callback_query.data.split(":")[1]
    user_id = callback_query.from_user.id
    await callback_query.answer()

    cursor.execute("SELECT subscription_type FROM users WHERE user_id = %s", (user_id,))
    cur = cursor.fetchone()

    if sub_type == "referral":
        if cur and cur['subscription_type'] == "referral_approved":
            await callback_query.message.edit_text(
                "У вас уже подтверждённая реферальная подписка.\nВыберите биржу:",
                reply_markup=get_exchange_keyboard()
            )
            await state.set_state(PaymentStates.waiting_for_api_key)
            return
        if cur and cur['subscription_type'] == "referral_pending":
            await callback_query.message.edit_text(
                "Ваш UUID уже на модерации. Дождитесь ответа.",
                reply_markup=types.InlineKeyboardMarkup(inline_keyboard=[[
                    types.InlineKeyboardButton(text="Поддержка", url=f"https://t.me/{SUPPORT_CONTACT.lstrip('@')}")
                ]])
            )
            return

        await callback_query.message.edit_text(
            "Выберите биржу для реферальной подписки:",
            reply_markup=get_exchange_keyboard()
        )
        await state.update_data(subscription_type="referral")
        await state.set_state(PaymentStates.waiting_for_exchange)
    else:
        cursor.execute(
            """INSERT INTO users (user_id, chat_id, subscription_type)
               VALUES (%s, %s, %s)
               ON CONFLICT (user_id) DO UPDATE
               SET chat_id = %s, subscription_type = %s""",
            (user_id, user_id, "regular", user_id, "regular")
        )
        conn.commit()
        await callback_query.message.edit_text(
            "Выберите тариф:",
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
        await callback_query.message.edit_text(
            "Неверный тариф. Выберите снова:",
            reply_markup=types.InlineKeyboardMarkup(inline_keyboard=[[
                types.InlineKeyboardButton(text="Поддержка", url=f"https://t.me/{SUPPORT_CONTACT.lstrip('@')}")
            ]])
        )
        return

    tariff = TARIFFS[tariff_id]
    description = f"Подписка {tariff['name']}"

    payment = create_yoomoney_payment(user_id, tariff['price'], description)
    if payment["status"] != "success":
        await callback_query.message.edit_text(
            "Ошибка создания платежа. Попробуйте позже.",
            reply_markup=types.InlineKeyboardMarkup(inline_keyboard=[[
                types.InlineKeyboardButton(text="Поддержка", url=f"https://t.me/{SUPPORT_CONTACT.lstrip('@')}")
            ]])
        )
        return

    invoice_id = f"yoomoney_{payment['label']}"
    cursor.execute(
        """INSERT INTO payments
           (invoice_id, user_id, amount, currency, status, tariff_id, payment_method, yoomoney_label)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
        (invoice_id, user_id, tariff['price'], "RUB", "pending", tariff_id, "yoomoney", payment['label'])
    )
    conn.commit()

    kb = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="Оплатить", url=payment["pay_url"])],
        [types.InlineKeyboardButton(text="Я оплатил", callback_data=f"check_payment:{payment['label']}")],
        [types.InlineKeyboardButton(text="Отмена", callback_data="cancel")],
        [types.InlineKeyboardButton(text="Поддержка", url=f"https://t.me/{SUPPORT_CONTACT.lstrip('@')}")]
    ])

    await callback_query.message.edit_text(
        f"Оплатите <b>{tariff['price']}₽</b> за <b>{tariff['name']}</b>\n"
        f"<a href='{payment['pay_url']}'>Ссылка для оплаты</a>\n\n"
        f"После оплаты нажмите кнопку ниже",
        parse_mode="HTML",
        reply_markup=kb
    )
    await state.update_data(tariff_id=tariff_id, yoomoney_label=payment['label'], invoice_id=invoice_id)
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
            await callback_query.message.edit_text("Платёж не найден в базе.")
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

        # УБИРАЕМ reply_markup из edit_text
        await callback_query.message.edit_text(
            f"Оплата подтверждена!\n"
            f"Подписка активна до <b>{end.strftime('%d.%m.%Y %H:%M')}</b>\n\n"
            f"Теперь подключите API.",
            parse_mode="HTML"
        )

        # Отправляем обычную клавиатуру отдельно
        await bot.send_message(
            user_id,
            "Выберите действие:",
            reply_markup=get_main_menu(user_id)
        )
        await state.clear()
    else:
        kb = types.InlineKeyboardMarkup(inline_keyboard=[
            [types.InlineKeyboardButton(text="Проверить снова", callback_data=f"check_payment:{label}")],
            [types.InlineKeyboardButton(text="Поддержка", url=f"https://t.me/{SUPPORT_CONTACT.lstrip('@')}")]
        ])
        await callback_query.message.edit_text(
            "Платёж ещё не подтверждён. Подождите 10-30 сек. и попробуйте снова.",
            reply_markup=kb
        )

@router.message(Command("status"))
async def cmd_status(message: types.Message):
    user_id = message.from_user.id
    cursor.execute("SELECT subscription_end, subscription_type FROM users WHERE user_id = %s", (user_id,))
    res = cursor.fetchone()
    if not res or not res['subscription_end']:
        await message.answer("У вас нет активной подписки.")
        return
    end = res['subscription_end']
    now = datetime.datetime.now()
    if end < now:
        await message.answer("Подписка истекла.")
    else:
        await message.answer(
            f"Подписка активна до <b>{end.strftime('%d.%m.%Y %H:%M')}</b>\n"
            f"Осталось примерно <b>{(end - now).days}</b> дней.\n"
            f"Тип: <b>{res['subscription_type']}</b>",
            parse_mode="HTML",
            reply_markup=get_main_menu(user_id)
        )

# ------------------- Реферальная система -------------------
@router.callback_query(F.data.startswith("exchange:"))
async def process_exchange(callback_query: types.CallbackQuery, state: FSMContext):
    user_id = callback_query.from_user.id
    exchange = callback_query.data.split(":")[1]

    cursor.execute("SELECT subscription_type FROM users WHERE user_id = %s", (user_id,))
    res = cursor.fetchone()

    # === ДЛЯ ОБЫЧНЫХ ПОЛЬЗОВАТЕЛЕЙ (regular) ===
    if res['subscription_type'] == "regular":
        cursor.execute("UPDATE users SET exchange = %s WHERE user_id = %s", (exchange, user_id))
        conn.commit()

        # Редактируем сообщение — без клавиатуры
        await callback_query.message.edit_text(
            f"Биржа {exchange.upper()} выбрана.\n\nВведите ваш API-ключ:"
        )

        # Отправляем новое сообщение + убираем клавиатуру
        await bot.send_message(
            user_id,
            "Введите ваш API-ключ:",
            reply_markup=types.ReplyKeyboardRemove()
        )

        await state.update_data(exchange=exchange)
        await state.set_state(PaymentStates.waiting_for_api_key)

    # === ДЛЯ РЕФЕРАЛОВ (referral_approved) ===
    elif res['subscription_type'] == "referral_approved":
        video_path = VIDEO_INSTRUCTIONS.get(exchange)
        if not video_path or not os.path.exists(video_path):
            await callback_query.message.edit_text(
                "Видеоинструкция недоступна. Обратитесь в поддержку.",
                reply_markup=types.InlineKeyboardMarkup(inline_keyboard=[[
                    types.InlineKeyboardButton(text="Поддержка", url=f"https://t.me/{SUPPORT_CONTACT.lstrip('@')}")
                ]])
            )
            return

        await bot.send_video(
            chat_id=user_id,
            video=types.FSInputFile(video_path),
            caption=f"Инструкция по созданию API-ключа на {exchange.upper()}:"
        )

        await bot.send_message(
            user_id,
            "Введите ваш UUID с биржи:",
            reply_markup=types.InlineKeyboardMarkup(inline_keyboard=[
                [types.InlineKeyboardButton(text="Отмена", callback_data="cancel")],
                [types.InlineKeyboardButton(text="Поддержка", url=f"https://t.me/{SUPPORT_CONTACT.lstrip('@')}")]
            ])
        )
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

    if len(uuid_text) < 8:
        await message.answer("UUID слишком короткий. Попробуйте снова.")
        return

    cursor.execute("SELECT subscription_type FROM users WHERE user_id = %s", (user_id,))
    cur = cursor.fetchone()

    if cur and cur['subscription_type'] == "referral_pending":
        await message.answer("Ваш предыдущий UUID уже на модерации.")
        return
    if cur and cur['subscription_type'] == "referral_approved":
        await message.answer(
            "У вас уже подтверждённая реферальная подписка.",
            reply_markup=get_main_menu(user_id)
        )
        await state.clear()
        return

    cursor.execute(
        """INSERT INTO users (user_id, chat_id, subscription_type, referral_uuid, exchange)
           VALUES (%s, %s, %s, %s, %s)
           ON CONFLICT (user_id) DO UPDATE
           SET subscription_type = %s, referral_uuid = %s, exchange = %s""",
        (user_id, user_id, "referral_pending", uuid_text, exchange,
         "referral_pending", uuid_text, exchange)
    )
    conn.commit()

    await bot.send_message(
        MODERATOR_GROUP_ID,
        f"Новый запрос реферальной подписки:\n"
        f"Пользователь: {user_id}\n"
        f"Биржа: {exchange.upper()}\n"
        f"UUID: {uuid_text}",
        reply_markup=types.InlineKeyboardMarkup(inline_keyboard=[
            [types.InlineKeyboardButton(text="Подтвердить", callback_data=f"approve_uuid:{user_id}")],
            [types.InlineKeyboardButton(text="Отклонить", callback_data=f"reject_uuid:{user_id}")]
        ])
    )
    await message.answer(
        "UUID отправлен модератору. Ожидайте подтверждения.",
        reply_markup=types.InlineKeyboardMarkup(inline_keyboard=[[
            types.InlineKeyboardButton(text="Поддержка", url=f"https://t.me/{SUPPORT_CONTACT.lstrip('@')}")
        ]])
    )
    await state.update_data(referral_uuid=uuid_text)

@router.callback_query(F.data.startswith("approve_uuid:") | F.data.startswith("reject_uuid:"))
async def moderator_decision(callback_query: types.CallbackQuery, state: FSMContext):
    action, uid = callback_query.data.split(":")
    user_id = int(uid)
    await callback_query.answer()

    if action == "approve_uuid":
        end = datetime.datetime.now() + datetime.timedelta(days=365)
        cursor.execute(
            "UPDATE users SET subscription_type = %s, subscription_end = %s WHERE user_id = %s",
            ("referral_approved", end, user_id)
        )
        conn.commit()
        try:
            cursor.execute("SELECT api_key, exchange FROM users WHERE user_id = %s", (user_id,))
            res = cursor.fetchone()
            if res['api_key']:
                await bot.send_message(
                    user_id,
                    "UUID подтверждён! API уже подключён.",
                    reply_markup=get_main_menu(user_id)
                )
            else:
                await bot.send_message(
                    user_id,
                    "UUID подтверждён! Введите API-ключ:",
                    reply_markup=types.ReplyKeyboardRemove()
                )
                await state.update_data(exchange=res['exchange'])
                await state.set_state(PaymentStates.waiting_for_api_key)
            await callback_query.message.edit_text(f"Подтверждено для {user_id}")
        except TelegramForbiddenError:
            await callback_query.message.edit_text(f"Подтверждено, но пользователь заблокировал бота")
    else:
        cursor.execute(
            "UPDATE users SET subscription_type = %s, referral_uuid = NULL WHERE user_id = %s",
            ("rejected", user_id)
        )
        conn.commit()
        try:
            await bot.send_message(
                user_id,
                "UUID отклонён. Выберите обычную подписку или попробуйте снова:",
                reply_markup=get_subscription_type_keyboard()
            )
            await state.set_state(PaymentStates.waiting_for_subscription_type)
            await callback_query.message.edit_text(f"Отклонено для {user_id}")
        except TelegramForbiddenError:
            await callback_query.message.edit_text(f"Отклонено, пользователь заблокировал бота")

@router.message(F.text == "Подключить API")
async def connect_api(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    cursor.execute("SELECT subscription_type, subscription_end, api_key, exchange FROM users WHERE user_id = %s", (user_id,))
    res = cursor.fetchone()

    if not res or not res['subscription_end'] or res['subscription_end'] <= datetime.datetime.now():
        await message.answer(
            "Нет активной подписки. Выберите тип:",
            reply_markup=get_subscription_type_keyboard()
        )
        await state.set_state(PaymentStates.waiting_for_subscription_type)
        return

    if res['api_key']:
        await message.answer("API уже подключён.", reply_markup=get_main_menu(user_id))
        await state.clear()
        return

    # === ДЛЯ ОБЫЧНОЙ ПОДПИСКИ (regular) ===
    if res['subscription_type'] == "regular":
        # Если биржа уже выбрана — продолжаем
        if res['exchange']:
            await message.answer(
                f"Биржа уже выбрана: {res['exchange'].upper()}\n"
                "Введите ваш API-ключ:",
                reply_markup=types.ReplyKeyboardRemove()
            )
            await state.update_data(exchange=res['exchange'])
            await state.set_state(PaymentStates.waiting_for_api_key)
        else:
            # Просим выбрать биржу
            await message.answer("Выберите биржу для подключения API:", reply_markup=get_exchange_keyboard())
            await state.set_state(PaymentStates.waiting_for_api_key)

    # === ДЛЯ РЕФЕРАЛЬНОЙ (referral_approved) ===
    elif res['subscription_type'] == "referral_approved":
        if res['exchange']:
            await message.answer(
                f"Реферальная подписка. Биржа: {res['exchange'].upper()}\n"
                "Введите API-ключ:",
                reply_markup=types.ReplyKeyboardRemove()
            )
            await state.update_data(exchange=res['exchange'])
            await state.set_state(PaymentStates.waiting_for_api_key)
        else:
            await message.answer("Выберите биржу:", reply_markup=get_exchange_keyboard())
            await state.set_state(PaymentStates.waiting_for_api_key)

    else:
        await message.answer("Подписка не подтверждена. Дождитесь модерации.")

@router.message(F.text == "Информация о подписке")
async def subscription_info(message: types.Message):
    user_id = message.from_user.id
    cursor.execute(
        "SELECT subscription_end, subscription_type, api_key, exchange FROM users WHERE user_id = %s",
        (user_id,)
    )
    res = cursor.fetchone()

    if not res or not res['subscription_end'] or res['subscription_end'] <= datetime.datetime.now():
        await message.answer("Нет активной подписки.", reply_markup=get_main_menu(user_id))
        return

    # Определяем тип подписки
    sub_type = res['subscription_type']
    if sub_type == "regular":
        sub_name = "Обычная (оплачена)"
    elif sub_type == "referral_approved":
        sub_name = "Реферальная (подтверждена)"
    elif sub_type == "referral_pending":
        sub_name = "Реферальная (на модерации)"
    else:
        sub_name = sub_type.capitalize()

    # Формируем текст
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
    api_key = message.text.strip()
    if len(api_key) < 10:
        await message.answer("API-ключ слишком короткий.")
        return

    data = await state.get_data()
    exchange = data.get('exchange')

    if not exchange:
        await message.answer("Ошибка: биржа не выбрана. Нажмите 'Подключить API' заново.")
        await state.clear()
        return

    await state.update_data(api_key=api_key)
    await message.answer("Введите Secret Key:")
    await state.set_state(PaymentStates.waiting_for_secret_key)

@router.message(PaymentStates.waiting_for_secret_key)
async def process_secret_key(message: types.Message, state: FSMContext):
    secret_key = message.text.strip()
    if len(secret_key) < 10:
        await message.answer("Secret Key слишком короткий.")
        return

    data = await state.get_data()
    exchange = data['exchange']
    api_key = data['api_key']

    await state.update_data(secret_key=secret_key)

    if exchange == 'okx':
        await message.answer("Введите Passphrase:")
        await state.set_state(PaymentStates.waiting_for_passphrase)
    else:
        # BingX — сохраняем сразу
        cursor.execute(
            "UPDATE users SET api_key = %s, secret_key = %s, exchange = %s WHERE user_id = %s",
            (api_key, secret_key, exchange, message.from_user.id)
        )
        conn.commit()
        await message.answer("API подключён!", reply_markup=get_main_menu(message.from_user.id))
        await state.clear()

@router.message(PaymentStates.waiting_for_passphrase)
async def process_passphrase(message: types.Message, state: FSMContext):
    passphrase = message.text.strip()
    if len(passphrase) < 8:
        await message.answer("Passphrase слишком короткий (мин. 8).")
        return
    data = await state.get_data()
    cursor.execute(
        """UPDATE users SET api_key = %s, secret_key = %s, passphrase = %s, exchange = %s, chat_id = %s
           WHERE user_id = %s""",
        (data['api_key'], data['secret_key'], passphrase, data['exchange'], message.from_user.id, message.from_user.id)
    )
    conn.commit()
    await message.answer("API и Passphrase сохранены!", reply_markup=get_main_menu(message.from_user.id))
    await state.clear()

@router.callback_query(F.data == "cancel")
async def cancel_action(callback_query: types.CallbackQuery, state: FSMContext):
    await callback_query.message.delete()
    await bot.send_message(
        callback_query.from_user.id,
        "Действие отменено.",
        reply_markup=get_main_menu(callback_query.from_user.id)
    )
    await state.clear()

@router.message(lambda m: m.text not in ["Подключить API", "Информация о подписке", "Поддержка"])
async def handle_invalid(message: types.Message, state: FSMContext):
    cur = await state.get_state()
    if cur == PaymentStates.waiting_for_api_key:
        await process_api_key(message, state)
    elif cur == PaymentStates.waiting_for_secret_key:
        await process_secret_key(message, state)
    elif cur == PaymentStates.waiting_for_passphrase:
        await process_passphrase(message, state)
    elif cur == PaymentStates.waiting_for_referral_uuid:
        await process_referral_uuid(message, state)
    else:
        await message.answer("Используйте кнопки меню.", reply_markup=get_main_menu(message.from_user.id))
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
                await bot.send_message(uid, "Подписка истекла. Продлите её.")
            except TelegramForbiddenError:
                cursor.execute("DELETE FROM users WHERE user_id = %s", (uid,))
                conn.commit()
            except Exception as e:
                logging.error(f"Ошибка при обработке истёкшей подписки {uid}: {e}")
        await asyncio.sleep(3600)

# (Фоновая проверка платежей больше не нужна – пользователь сам жмёт «Я оплатил»)

# ------------------- Запуск -------------------
async def main():
    print("Бот запущен...")
    asyncio.create_task(check_subscriptions())
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())