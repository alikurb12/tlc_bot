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
import requests
import aiohttp

logging.basicConfig(level=logging.INFO)
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
CRYPTO_BOT_TOKEN = os.getenv("CRYPTO_BOT_TOKEN")
GROUP_ID = int(os.getenv("GROUP_ID"))
MODERATOR_GROUP_ID = int(os.getenv("MODERATOR_GROUP_ID"))
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
        invoice_id BIGINT PRIMARY KEY,
        user_id BIGINT,
        amount REAL,
        currency TEXT,
        status TEXT,
        FOREIGN KEY (user_id) REFERENCES users (user_id)
    )
""")
cursor.execute("""
    SELECT column_name 
    FROM information_schema.columns 
    WHERE table_name = 'users' AND column_name = 'chat_id';
""")
if not cursor.fetchone():
    logging.info("Adding chat_id column to users table...")
    cursor.execute("ALTER TABLE users ADD COLUMN chat_id BIGINT;")
    conn.commit()
    logging.info("Column chat_id added to users table.")

cursor.execute("""
    SELECT column_name 
    FROM information_schema.columns 
    WHERE table_name = 'users' AND column_name = 'passphrase';
""")
if not cursor.fetchone():
    logging.info("Adding passphrase column to users table...")
    cursor.execute("ALTER TABLE users ADD COLUMN passphrase TEXT;")
    conn.commit()
    logging.info("Column passphrase added to users table.")

conn.commit()

TARIFFS = {
    'test': {'days': 1, 'price': 1, 'name': 'Тестовый (1 день)'},
    '1month': {'days': 30, 'price': 30, 'name': '1 месяц'},
    '3months': {'days': 90, 'price': 70, 'name': '3 месяца'},
}


class PaymentStates(StatesGroup):
    waiting_for_subscription_type = State()
    waiting_for_exchange = State()
    waiting_for_referral_uuid = State()
    waiting_for_payment = State()
    waiting_for_api_key = State()
    waiting_for_secret_key = State()
    waiting_for_passphrase = State()


def create_invoice(user_id, amount, description):
    url = "https://pay.crypt.bot/api/createInvoice"
    headers = {"Crypto-Pay-API-Token": CRYPTO_BOT_TOKEN}
    params = {
        "asset": "USDT",
        "amount": amount,
        "description": description,
        "hidden_message": "Спасибо за оплату!",
        "payload": f"user_{user_id}"
    }
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as request_error:
        logging.error(f"Error creating CryptoBot invoice: {request_error}")
        return None


def get_subscription_type_keyboard():
    keyboard = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="Обычная подписка", callback_data="subscription:regular")],
        [types.InlineKeyboardButton(text="Реферальная подписка", callback_data="subscription:referral")]
    ])
    return keyboard


def get_tariffs_keyboard():
    keyboard = types.InlineKeyboardMarkup(inline_keyboard=[])
    for tariff_id, tariff in TARIFFS.items():
        keyboard.inline_keyboard.append([types.InlineKeyboardButton(
            callback_data=f"tariff:{tariff_id}",
            text=f"{tariff['name']} - {tariff['price']}$"
        )])
    return keyboard


def get_exchange_keyboard():
    keyboard = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="BingX", callback_data="exchange:bingx"),
         types.InlineKeyboardButton(text="OKX", callback_data="exchange:okx")]
    ])
    return keyboard


def get_main_menu(user_id):
    buttons = [[types.KeyboardButton(text="Подключить API")]]
    cursor.execute("SELECT subscription_end FROM users WHERE user_id = %s", (user_id,))
    result = cursor.fetchone()
    if result and result['subscription_end'] is not None and result['subscription_end'] > datetime.datetime.now():
        buttons.append([types.KeyboardButton(text="Информация о подписке")])
    keyboard = types.ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True, one_time_keyboard=False)
    return keyboard


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
    'bingx': 'videos/bingx_instruction.mp4',
    'okx': 'videos/okx_instruction.mp4'
}


@router.message(Command("start"))
async def cmd_start(message: types.Message, state: FSMContext):
    is_in_group = await is_bot_in_group()
    if not is_in_group:
        await message.answer(
            "❌ Бот не состоит в группе или не имеет доступа. Пожалуйста, добавьте бота в группу и назначьте его администратором."
        )
        return

    user_id = message.from_user.id
    cursor.execute("SELECT subscription_end, subscription_type FROM users WHERE user_id = %s", (user_id,))
    result = cursor.fetchone()

    if not result:
        await message.answer(
            "Данный бот предоставляет вам возможность пользоваться автоматизированной версией нашей торговой стратегии без надобности выходить за пределы Telegram."
            "Вам остается лишь один раз провести небольшую настройку, после чего вы сможете пользоваться стратегией и с помощью этого бота."
            "Для начала вам нужно выбрать тип вашей подписки",
            reply_markup=get_subscription_type_keyboard()
        )
        await state.set_state(PaymentStates.waiting_for_subscription_type)
    else:
        subscription_end = result['subscription_end']
        subscription_type = result['subscription_type']
        if subscription_type == "referral_approved" and subscription_end is not None and subscription_end > datetime.datetime.now():
            await message.answer(
                f"✅ Ваша подписка активна до <b>{subscription_end.strftime('%Y-%m-%d %H:%M:%S')}</b>",
                parse_mode="HTML",
                reply_markup=get_main_menu(user_id)
            )
            await state.clear()
        elif subscription_type == "referral_pending":
            await message.answer(
                "⏳ Ваш UUID на модерации. Пожалуйста, дождитесь подтверждения."
            )
        else:
            await message.answer(
                "❗️Ваша подписка истекла или отсутствует. Пожалуйста, выберите тариф:",
                reply_markup=get_tariffs_keyboard()
            )
            await state.set_state(PaymentStates.waiting_for_payment)


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
            await callback_query.message.edit_text(
                "⏳ Ваш предыдущий UUID ещё на модерации. Пожалуйста, дождитесь ответа."
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


@router.callback_query(F.data.startswith("exchange:"))
async def process_exchange(callback_query: types.CallbackQuery, state: FSMContext):
    exchange = callback_query.data.split(":")[1]
    user_id = callback_query.from_user.id
    video_path = VIDEO_INSTRUCTIONS.get(exchange)

    if not video_path or not os.path.exists(video_path):
        logging.error(f"Video file for {exchange} not found at {video_path}")
        try:
            await callback_query.message.edit_text(
                "❌ Ошибка: Видеоинструкция для выбранной биржи недоступна. Пожалуйста, свяжитесь с поддержкой."
            )
        except TelegramBadRequest:
            await bot.send_message(
                chat_id=user_id,
                text="❌ Ошибка: Видеоинструкция для выбранной биржи недоступна. Пожалуйста, свяжитесь с поддержкой."
            )
        return

    try:
        file_size = os.path.getsize(video_path) / (1024 * 1024)
        if file_size > 50:
            logging.error(f"Video file {video_path} too large: {file_size} MB")
            await callback_query.message.edit_text(
                "❌ Ошибка: Видео слишком большое для отправки. Пожалуйста, свяжитесь с поддержкой для получения инструкции."
            )
            return

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
                    await bot.send_message(
                        chat_id=user_id,
                        text="❌ Ошибка при отправке видеоинструкции из-за сетевых проблем. Пожалуйста, попробуйте снова позже."
                    )
                    return
                await asyncio.sleep(2)

        await bot.send_message(
            chat_id=user_id,
            text="📎 Пожалуйста, введите ваш UUID с биржи для модерации:",
            reply_markup=types.InlineKeyboardMarkup(inline_keyboard=[
                [types.InlineKeyboardButton(text="Отмена", callback_data="cancel")]
            ])
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
    except TelegramBadRequest as e:
        logging.error(f"Telegram error sending video for {exchange} to user {user_id}: {e}")
        try:
            await bot.send_message(
                chat_id=user_id,
                text="❌ Ошибка при отправке видеоинструкции. Пожалуйста, попробуйте снова или свяжитесь с поддержкой."
            )
        except TelegramBadRequest:
            await bot.send_message(
                chat_id=MODERATOR_GROUP_ID,
                text=f"Не удалось отправить сообщение пользователю {user_id} из-за ошибки: {e}"
            )
    except Exception as e:
        logging.error(f"Unexpected error sending video for {exchange} to user {user_id}: {e}")
        try:
            await bot.send_message(
                chat_id=user_id,
                text="❌ Произошла ошибка. Пожалуйста, попробуйте снова или свяжитесь с поддержкой."
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
        await message.answer("❌ Неверный формат UUID. Пожалуйста, попробуйте снова:")
        return

    cursor.execute("SELECT subscription_type, subscription_end, api_key FROM users WHERE user_id = %s", (user_id,))
    result = cursor.fetchone()
    if result and result['subscription_type'] == "referral_pending":
        await message.answer("⏳ Ваш предыдущий UUID ещё на модерации. Пожалуйста, дождитесь ответа.")
        return
    elif result and result['subscription_type'] == "referral_approved" and result[
        'subscription_end'] > datetime.datetime.now():
        if result['api_key']:
            await message.answer(
                "✅ У вас уже есть активная реферальная подписка и подключённый API. Выберите действие:",
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
        await message.answer(
            "✅ UUID отправлен на модерацию. Ожидайте подтверждения от модератора."
        )
        await state.update_data(referral_uuid=referral_uuid)
    except Exception as e:
        logging.error(f"Ошибка отправки UUID модератору: {e}")
        await message.answer("❌ Ошибка при отправке UUID. Попробуйте позже.")
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
        await callback_query.message.edit_text("❌ Ошибка при обработке решения. Попробуйте снова.")


@router.message(F.text == "Подключить API")
async def connect_api(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    current_state = await state.get_state()
    logging.info(f"Processing 'Подключить API' for user {user_id}, current state: {current_state}")

    cursor.execute("SELECT subscription_type, subscription_end, api_key FROM users WHERE user_id = %s", (user_id,))
    result = cursor.fetchone()

    if result and result['subscription_type'] == "referral_pending":
        await message.answer("⏳ Ваш UUID на модерации. Пожалуйста, дождитесь подтверждения.")
        return

    if result and result['subscription_type'] == "referral_approved" and result[
        'subscription_end'] > datetime.datetime.now():
        if result['api_key']:
            await message.answer(
                "✅ У вас уже подключен API. Вы можете проверить информацию о подписке или связаться с поддержкой для изменения ключей.",
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
            "❗️ У вас нет активной подписки для подключения API. Пожалуйста, выберите тип подписки:",
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
        await message.answer("⏳ Ваш UUID на модерации. Пожалуйста, дождитесь подтверждения.")
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


@router.message(PaymentStates.waiting_for_api_key)
async def process_api_key(message: types.Message, state: FSMContext):
    api_key = message.text.strip()
    user_id = message.from_user.id
    data = await state.get_data()
    exchange = data.get('exchange')

    if not exchange:
        logging.error(f"No exchange selected for user {user_id}")
        await message.answer("Ошибка: биржа не выбрана. Пожалуйста, выберите биржу:",
                             reply_markup=get_exchange_keyboard())
        return

    if len(api_key) < 10:
        logging.warning(f"Invalid API key length for user {user_id}: {api_key}")
        await message.answer("❌ Неверный формат API-ключа. Пожалуйста, введите корректный API-ключ:")
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
        await message.answer("❌ Неверный формат Secret Key. Пожалуйста, введите корректный Secret Key:")
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
            "✅ API-ключ успешно добавлен!",
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
        await message.answer(
            "❌ Неверный формат Passphrase. Пожалуйста, введите корректный Passphrase (минимум 8 символов):")
        return

    logging.info(f"Passphrase received for user {user_id}: {passphrase}")
    cursor.execute(
        "UPDATE users SET api_key = %s, secret_key = %s, passphrase = %s, exchange = %s, chat_id = %s WHERE user_id = %s",
        (api_key, secret_key, passphrase, exchange, user_id, user_id)
    )
    conn.commit()

    await message.answer(
        "✅ API-ключ и Passphrase успешно добавлены!",
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


@router.message(lambda message: message.text not in ["Подключить API", "Информация о подписке"])
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
        await message.answer(
            "❗️ Пожалуйста, используйте кнопки меню для взаимодействия с ботом.",
            reply_markup=get_main_menu(user_id)
        )
        await state.clear()


async def send_signal_notification(signal: dict, user_id: int):
    """Отправляет уведомление о новом сигнале пользователю."""
    action = signal['action']
    symbol = signal['symbol']
    price = signal['price']
    stop_loss = signal['stop_loss']
    take_profits = [signal.get('take_profit_1'), signal.get('take_profit_2'), signal.get('take_profit_3')]

    tp1, tp2, tp3 = take_profits
    message = (
        f"🔔 <b>Открыт сигнал</b>\n"
        f"📊 Пара: {symbol}\n"
        f"💰 Цена входа: {price}\n"
        f"🎯 Тейк-профит 1: {tp1}\n"
        f"🎯 Тейк-профит 2: {tp2}\n"
        f"🎯 Тейк-профит 3: {tp3}\n"
        f"🛑 Стоп-лосс: {stop_loss}\n\n"
        f"Пожалуйста, проверьте, все ли открыто на бирже. Если возникли проблемы с открытием, напишите в поддержку!"
    )

    try:
        await bot.send_message(
            chat_id=user_id,
            text=message,
            parse_mode="HTML"
        )
        logging.info(f"Уведомление о сигнале отправлено пользователю {user_id}")
    except Exception as e:
        logging.error(f"Ошибка отправки уведомления пользователю {user_id}: {e}")


async def check_subscriptions():
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
                    await bot.send_message(user_id, "Ваша подписка истекла. Пожалуйста, продлите её.")
                except TelegramBadRequest as send_error:
                    logging.warning(f"Could not send expiration message to user {user_id}: {send_error}")
            except TelegramForbiddenError:
                logging.warning(f"Bot was blocked by user {user_id}. Removing from DB.")
                cursor.execute("DELETE FROM users WHERE user_id = %s", (user_id,))
                conn.commit()
            except Exception as processing_error:
                logging.error(f"Error processing expired subscription for user {user_id}: {processing_error}")
        await asyncio.sleep(3600)


async def main():
    is_in_group = await is_bot_in_group()
    if not is_in_group:
        logging.error("Бот не состоит в группе или не имеет доступа.")
        print("❌ Бот не состоит в группе или не имеет доступа. Добавьте бота в группу и назначьте его администратором.")
        return

    asyncio.create_task(check_subscriptions())
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())