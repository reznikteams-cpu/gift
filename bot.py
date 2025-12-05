import logging
import os
from datetime import datetime

import psycopg2
from psycopg2 import pool

from telegram import (
    Update,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
    CallbackQueryHandler,
)

# ----------------- ЛОГИРОВАНИЕ -----------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# ----------------- НАСТРОЙКИ -----------------
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "<VSTAV_SVOI_TOKEN>")
CHANNEL_USERNAME = os.getenv("CHANNEL_USERNAME", "@your_channel_username")
DATABASE_URL = os.getenv("DATABASE_URL")

DB_POOL: pool.SimpleConnectionPool | None = None


# ----------------- ИНИЦИАЛИЗАЦИЯ БД -----------------
def init_db():
    """
    Создаём пул соединений и таблицу users, если её ещё нет.
    DATABASE_URL берём из Railway (Postgres).
    """
    global DB_POOL

    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL env var is not set")

    DB_POOL = pool.SimpleConnectionPool(1, 5, DATABASE_URL)

    conn = DB_POOL.getconn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS users (
                        user_id BIGINT PRIMARY KEY,
                        username TEXT,
                        first_name TEXT,
                        last_name TEXT,
                        is_gift_given BOOLEAN DEFAULT FALSE,
                        created_at TIMESTAMPTZ,
                        updated_at TIMESTAMPTZ
                    );
                    """
                )
        logger.info("Database initialized")
    finally:
        DB_POOL.putconn(conn)


# ----------------- ХЕЛПЕРЫ ДЛЯ РАБОТЫ С ПОЛЬЗОВАТЕЛЕМ -----------------
def upsert_user(user):
    """
    Добавляем / обновляем пользователя в таблице users.
    """
    if not user:
        return
    if DB_POOL is None:
        raise RuntimeError("DB_POOL is not initialized")

    conn = DB_POOL.getconn()
    try:
        now = datetime.utcnow()
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO users (user_id, username, first_name, last_name, is_gift_given, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, FALSE, %s, %s)
                    ON CONFLICT (user_id) DO UPDATE SET
                        username   = EXCLUDED.username,
                        first_name = EXCLUDED.first_name,
                        last_name  = EXCLUDED.last_name,
                        updated_at = EXCLUDED.updated_at;
                    """,
                    (
                        user.id,
                        user.username,
                        user.first_name,
                        user.last_name,
                        now,
                        now,
                    ),
                )
    finally:
        DB_POOL.putconn(conn)


def has_gift(user_id: int) -> bool:
    """
    Проверяем, получал ли уже пользователь подарок.
    """
    if DB_POOL is None:
        raise RuntimeError("DB_POOL is not initialized")

    conn = DB_POOL.getconn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT is_gift_given FROM users WHERE user_id = %s;",
                    (user_id,),
                )
                row = cur.fetchone()
                if row is None:
                    return False
                return bool(row[0])
    finally:
        DB_POOL.putconn(conn)


def mark_gift_given(user_id: int):
    """
    Помечаем, что подарок выдан.
    """
    if DB_POOL is None:
        raise RuntimeError("DB_POOL is not initialized")

    conn = DB_POOL.getconn()
    try:
        now = datetime.utcnow()
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE users
                    SET is_gift_given = TRUE, updated_at = %s
                    WHERE user_id = %s;
                    """,
                    (now, user_id),
                )
    finally:
        DB_POOL.putconn(conn)


# ----------------- ПРОВЕРКА ПОДПИСКИ НА КАНАЛ -----------------
async def check_subscription(user_id: int, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """
    Проверяем статус участника канала. Бот должен быть админом в канале.
    """
    try:
        member = await context.bot.get_chat_member(
            chat_id=CHANNEL_USERNAME,
            user_id=user_id,
        )
        status = member.status
        logger.info("User %s status in channel: %s", user_id, status)

        return status in ("member", "administrator", "creator")
    except Exception as e:
        logger.error("Error in check_subscription: %s", e)
        # Если не смогли проверить — считаем, что не подписан
        return False


# ----------------- ЛОГИКА ВЫДАЧИ ПОДАРКА -----------------
async def process_gift_flow(chat_id: int, user, context: ContextTypes.DEFAULT_TYPE):
    """
    Общая точка:
    - если подарок уже выдавали — сообщаем об этом
    - если нет — выдаём и отмечаем в БД
    """
    if has_gift(user.id):
        await context.bot.send_message(
            chat_id=chat_id,
            text=(
                "Ты уже получал(а) свой подарок 🎁\n\n"
                "Если нужно ещё раз — напиши нам, и мы поможем."
            ),
        )
    else:
        await send_gift(chat_id, context)
        mark_gift_given(user.id)


async def send_gift(chat_id: int, context: ContextTypes.DEFAULT_TYPE):
    """
    Сам «подарок»: здесь можно отправить файл, промокод, ссылку и т.п.
    """
    await context.bot.send_message(
        chat_id=chat_id,
        text=(
            "🎁 Вот твой подарок!\n\n"
            "Спасибо за подписку. Держи бонус: промокод *GIFT2025* на специальное предложение."
        ),
        parse_mode="Markdown",
    )

    # Пример: можно отправить файл
    # with open("gift.pdf", "rb") as f:
    #     await context.bot.send_document(
    #         chat_id=chat_id,
    #         document=f,
    #         caption="Твой бонусный материал",
    #     )


# ----------------- ХЕНДЛЕРЫ БОТА -----------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /start — сохраняем пользователя, проверяем подписку и выдаём подарок/инструкцию.
    """
    user = update.effective_user
    chat_id = update.effective_chat.id

    if not user:
        return

    upsert_user(user)

    is_subscribed = await check_subscription(user.id, context)

    if is_subscribed:
        await process_gift_flow(chat_id, user, context)
    else:
        keyboard = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        "Перейти в канал",
                        url=f"https://t.me/{CHANNEL_USERNAME.lstrip('@')}",
                    )
                ],
                [
                    InlineKeyboardButton(
                        "✅ Я подписался",
                        callback_data="check_sub_again",
                    )
                ],
            ]
        )
        await context.bot.send_message(
            chat_id=chat_id,
            text=(
                "Чтобы получить подарок, нужно быть подписанным на наш канал.\n\n"
                "1️⃣ Подпишись на канал\n"
                "2️⃣ Нажми кнопку «✅ Я подписался»"
            ),
            reply_markup=keyboard,
        )


async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Обработка кнопки «✅ Я подписался».
    """
    query = update.callback_query
    if not query:
        return

    await query.answer()

    if query.data == "check_sub_again":
        user = query.from_user
        chat_id = query.message.chat.id

        upsert_user(user)

        is_subscribed = await check_subscription(user.id, context)

        if is_subscribed:
            try:
                await query.message.delete()
            except Exception:
                pass

            await process_gift_flow(chat_id, user, context)
        else:
            await query.edit_message_text(
                "Похоже, подписка ещё не оформлена 🤍\n\n"
                "Проверь, что ты подписан(а) на канал, и нажми кнопку ещё раз."
            )


async def gift(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /gift — отдельная команда, если хочешь, чтобы человек сам запросил подарок.
    """
    user = update.effective_user
    chat_id = update.effective_chat.id

    if not user:
        return

    upsert_user(user)

    is_subscribed = await check_subscription(user.id, context)

    if is_subscribed:
        await process_gift_flow(chat_id, user, context)
    else:
        await context.bot.send_message(
            chat_id=chat_id,
            text=(
                "Чтобы получить подарок, нужно быть подписанным на канал.\n\n"
                "Подпишись и снова отправь команду /gift."
            ),
        )


# ----------------- ТОЧКА ВХОДА -----------------
def main():
    if not TELEGRAM_TOKEN or TELEGRAM_TOKEN.startswith("<"):
        raise RuntimeError("Задай TELEGRAM_BOT_TOKEN в переменных окружения")

    init_db()

    app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("gift", gift))
    app.add_handler(CallbackQueryHandler(button_handler))

    logger.info("Bot started")
    app.run_polling()


if __name__ == "__main__":
    main()
