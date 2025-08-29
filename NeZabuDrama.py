#!/usr/bin/env python
# coding: utf-8

# In[ ]:

# ИМПОРТЫ
# Стандартные библиотеки
import logging
import os
import signal
import sys
import time
import traceback
from functools import partial
import asyncio
import re
import unicodedata
import requests
from datetime import datetime

# Внешние библиотеки
import aiosqlite
import nest_asyncio
import urllib.parse 

# Telegram API и связанные библиотеки
import telegram
from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    ReplyKeyboardMarkup,
    Bot
)
from telegram.error import BadRequest
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    CallbackQueryHandler,
    CallbackContext,
    filters,
    JobQueue
)
from telegram.constants import ParseMode

# КОНФИГУРАЦИЯ ЛОГИРОВАНИЯ
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


# КОНФИГУРАЦИЯ БОТА И БД
DB_PATH = 'doramas.db'
DB_PATH_2 = 'doramas_users.db'
from config import TOKEN, ADMINS

# ======== Константы ==========
COUNTRIES = ["Южная Корея", "Китай", "Япония"]
COUNTRY_FLAGS = {
    "Южная Корея": "🇰🇷",
    "Китай": "🇨🇳",
    "Япония": "🇯🇵",
}
PAGE_SIZE = 10
DEFAULT_MESSAGE = "😔 Пожалуйста, используйте кнопки для навигации. Ввод текста не поддерживается."
ACTION_TYPE_COMMAND = "command"
ACTION_TYPE_MESSAGE = "message"
ACTION_TYPE_CALLBACK = "callback"
# ======== Состояния ConversationHandler ==========
(
    ADDING_TITLE_RU,
    ADDING_TITLE_EN,
    ADDING_COUNTRY,
    ADDING_YEAR,
    ADDING_DIRECTOR,
    ADDING_LEAD_ACTRESS,
    ADDING_LEAD_ACTOR,
    ADDING_PERSONAL_RATING,
    ADDING_COMMENT,
    ADDING_PLOT,
    ADDING_POSTER_URL, 
    DELETING_DORAMA,
    GETTING_DORAMA_ID,
    LIST_DORAMAS_RATING,
    SEARCH_COUNTRY,
    SEARCH_TITLE,
    HANDLE_PAGINATION,
    SEARCH_ACTOR,
    CHOOSE_ACTOR,   
    SEARCH_ACTRESS,     
    CHOOSE_ACTRESS,      
    SEARCH_DIRECTOR, 
    CHOOSE_DIRECTOR,
    SHOW_MENU,
    PAGE
) = range(25)

# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ

# ======== Разделяем слишком длинные сообщения ==========
def split_message(message: str, max_length=4096):
    return [message[i:i + max_length] for i in range(0, len(message), max_length)]

# ======== Обрезаем текст (для кнопок в поиске по названию) ==========
def truncate_text(text, max_length=10):
    if len(text) > max_length:
        return text[:max_length] + "..."
    return text

# ======== Экранируем специальные символы ==========
def prevent_hashtag_linking(text: str) -> str:
    return text.replace("#", "#\u200b")

def escape_markdown(text: str) -> str:
    # Определите набор символов для экранирования
    escape_chars = "*[]()~`>#+-=|{}.!"
    # Экранируйте найденные символы
    for char in escape_chars:
        text = text.replace(char, "\\" + char)
    return text

def escape_markdown_v2(text: str) -> str:
    escape_chars = r"_*[]()~`>#+-=|{}.!"
    return "".join("\\" + char if char in escape_chars else char for char in text)

def remove_extra_escape(text: str) -> str:
    # Список символов для удаления экранирования
    chars_to_unescape = ".!~\-"
    
    # Удалите лишние экранирования
    for char in chars_to_unescape:
        text = text.replace("\\" + char, char)
    
    return text

# ======== Создаём клавиатуру ==========
def create_keyboard(buttons: list[list[InlineKeyboardButton]], resize=True) -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(buttons, resize_keyboard=True)

# ======== Создаём кнопки с текстом и callback_data ==========
def create_button(text: str, callback_data: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton(text, callback_data=callback_data)]])

# ======== Создаём клавиатуру для главного меню ==========
def create_main_menu_keyboard() -> InlineKeyboardMarkup:
    keyboard = [
        [InlineKeyboardButton("Список всех дорам 📚", callback_data="list_doramas_menu")],           
        [InlineKeyboardButton("Поиск по стране 🌍", callback_data="search_by_country")],    
        [InlineKeyboardButton("Поиск по актеру 🤴🏻", callback_data="search_by_actor")],
        [InlineKeyboardButton("Поиск по актрисе 👸🏻", callback_data="search_by_actress")],
        [InlineKeyboardButton("Поиск по режиссеру 🎬", callback_data="search_by_director")],
        [InlineKeyboardButton("Добавить (Катя)", callback_data="add_dorama"),
         InlineKeyboardButton("Удалить (Катя)", callback_data="delete_dorama")],
        [InlineKeyboardButton("В главное меню 🌸", callback_data="show_menu")]
    ]

    return InlineKeyboardMarkup(keyboard)

# ======== Создаём клавиатуру с кнопкой отмена ==========
def create_cancel_keyboard() -> InlineKeyboardMarkup:
    return create_button("Отмена", "cancel")

# ======== Определяем кнопку назад ==========
back_button = InlineKeyboardMarkup([
    [InlineKeyboardButton("Вернуться в главное меню 🌸", callback_data="show_menu")]
])

# ======== Создаём клавиатуру с кнопкой "В главное меню" ==========
def return_to_main_menu() -> InlineKeyboardMarkup:
    return create_button("В главное меню 🌸", "show_menu")


# ======== Создаем клавиатуру с кнопками про странам ==========
def create_country_buttons():
    return [
        [InlineKeyboardButton(f"{COUNTRY_FLAGS.get(country, '')} {country}", callback_data=f"select_country:{country}")]
        for country in COUNTRIES
    ]

# РАБОТА С БАЗОЙ ДАННЫХ  
# ======== Создаем БД и индексы для поиска ==========
async def init_db():
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            # Создаем таблицу если она не существует
            await db.execute('''
                CREATE TABLE IF NOT EXISTS doramas (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    title_ru TEXT NOT NULL,
                    title_en TEXT NOT NULL,
                    country TEXT NOT NULL,
                    year INTEGER NOT NULL,
                    director TEXT NOT NULL,
                    lead_actress TEXT NOT NULL,
                    lead_actor TEXT NOT NULL,
                    personal_rating INTEGER NOT NULL,
                    comment TEXT NOT NULL,
                    plot TEXT NOT NULL,
                    poster_url TEXT
                )
            ''')

            # Список индексов для создания
            indexes = [
                ('idx_title_ru', 'title_ru'),
                ('idx_title_en', 'title_en'),
                ('idx_country', 'country'),
                ('idx_lead_actor', 'lead_actor'),
                ('idx_lead_actress', 'lead_actress'),
                ('idx_director', 'director'),
            ]

            # Создаем индексы
            for index_name, column_name in indexes:
                await db.execute(f'CREATE INDEX IF NOT EXISTS {index_name} ON doramas ({column_name})')
            
            await db.commit()
            logger.info("✅ База данных успешно инициализирована или уже существует.")
    except aiosqlite.Error as e:
        logger.error(f"⚠️ Ошибка при инициализации базы данных: {e}", exc_info=True)
        sys.exit(1)

# ======== Получаем общее количество дорам ==========
async def get_total_doramas_count():
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute('SELECT COUNT(*) FROM doramas') as cursor:
                count = await cursor.fetchone()
                return count[0] if count else 0
    except aiosqlite.Error as e:
        logger.error(f"⚠️ Ошибка при получении количества дорам: {e}")
        return 0
    
# ======== Преобразует ссылку Яндекс.Диска в прямую ссылку ==========    
def get_yandex_disk_direct_link(yandex_url):
    try:
        base_api_url = "https://cloud-api.yandex.net/v1/disk/public/resources/download"
        response = requests.get(base_api_url, params={"public_key": yandex_url})

        if response.status_code == 200:
            return response.json().get("href", "")
        else:
            return ""
    except Exception as e:
        logger.error(f"Ошибка при получении прямой ссылки: {e}")
        return ""

# ======== Унифицированная функция для отправки ответов пользователю ==========    
async def send_reply(update: Update, text: str):
    if update.message:
        await update.message.reply_text(text)
    elif update.callback_query:
        await update.callback_query.message.reply_text(text)
    else:
        logger.warning("Не могу отправить сообщение: неизвестный тип update.")

# ОСНОВНЫЕ ФУНКЦИИ БОТА
# ======== Прописываем старт ==========
def get_start_menu() -> InlineKeyboardMarkup:
    """Создаёт стартовую клавиатуру с кнопкой 'Начать'."""
    keyboard = [[InlineKeyboardButton("Начать", callback_data="show_menu")]]
    return InlineKeyboardMarkup(keyboard)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обрабатывает команду /start, логирует пользователя и показывает стартовое меню."""
    try:
        user = update.effective_user
        if not user:
            logger.warning("⚠️ Нет информации о пользователе в update.")
            return

        user_id = user.id
        username = user.username or "Не указан"
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # Текущая временная метка

        # Логируем запуск команды /start и добавляем пользователя в базу, если его там нет
        async with aiosqlite.connect(DB_PATH_2) as db:
            await db.execute('''
                INSERT INTO users (user_id, username, first_seen, last_seen)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(user_id) DO UPDATE SET
                    username = excluded.username,
                    last_seen = excluded.last_seen;
            ''', (user_id, username, now, now))

            # Исправленный вызов функции update_last_actions
            message_id = update.message.message_id if update.message else None
            await update_last_actions(db, user_id, "command", "/start", message_id, None, now)
            await db.commit()

        # Получаем общее количество дорам
        total_doramas = await get_total_doramas_count()
        reply_markup = get_start_menu()

        welcome_text = (
            "👋 *Привет!*\nЯ сериальный бот 🌸*НеЗабудрама!*🌸\n\n"
            "Моя задача — собирать информацию \nо дорамах, которые Катя уже успела посмотреть. 📚 \n\n"
            f"Сейчас в моей библиотеке *{total_doramas}* дорам!\n📖 (И мы продолжаем её пополнять!)\n\n"
            "Нажми «Начать», чтобы узнать,\nчто я могу тебе показать! ✨"
        )

        # Отправляем приветственное сообщение
        if update.message:
            await update.message.reply_text(
                welcome_text, reply_markup=reply_markup, parse_mode='Markdown'
            )
        elif update.callback_query:
            query = update.callback_query
            await query.answer()
            await query.edit_message_text(
                welcome_text, reply_markup=reply_markup, parse_mode='Markdown'
            )
        else:
            logger.warning("⚠️ Не удалось определить, как отправить сообщение.")

        return SHOW_MENU

    except Exception as e:
        logger.error(f"⚠️ Ошибка при обработке команды /start: {e}", exc_info=True)

        if update.message:
            await update.message.reply_text("Произошла ошибка при обработке команды /start.")
        elif update.callback_query:
            await update.callback_query.message.reply_text("Произошла ошибка при обработке команды /start.")
        elif update.effective_chat:
            await context.bot.send_message(chat_id=update.effective_chat.id, text="Произошла непредвиденная ошибка. Попробуйте позже.")
        else:
            logger.warning("Не удалось отправить сообщение об ошибке: update.effective_chat отсутствует.")

# ========  Главное меню ==========
async def show_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    logger.info("Функция show_menu вызвана!")  
    query = update.callback_query
    reply_markup = create_main_menu_keyboard()


    try:
        if query:
            await query.answer()
            try:
                await query.message.delete() # Попытка удалить старое сообщение
            except Exception as e:
                logger.warning(f"Не удалось удалить старое сообщение: {e}", exc_info=True)

            await context.bot.send_message( # Отправляем новое сообщение
                chat_id=query.message.chat_id,
                text="Выберите действие:",
                reply_markup=reply_markup
            )    
        elif update.message:
            await update.message.reply_text(
                text="Выберите действие:",
                reply_markup=reply_markup
            )
        else:
            logger.warning(f"Неизвестный тип обновления в show_menu: {update}", exc_info=True) # Логируем тип обновления

            chat_id = None # Получаем chat_id
            if update.effective_chat:
                chat_id = update.effective_chat.id
            elif update.message:
                chat_id = update.message.chat_id
            elif update.callback_query:
                chat_id = update.callback_query.message.chat_id

            if chat_id:
                await context.bot.send_message(chat_id=chat_id, text="Произошла непредвиденная ошибка. Попробуйте позже.",
                reply_markup=reply_markup)
            else:
                logger.warning("Не удалось отправить сообщение об ошибке: не удалось определить chat_id.")
            return SHOW_MENU

    except Exception as e:
        logger.error(f"Ошибка при отображении главного меню: {e}", exc_info=True)

        chat_id = None  # Получаем chat_id
        if update.message:
            chat_id = update.message.chat_id
        elif update.callback_query:
            chat_id = update.callback_query.message.chat_id
        elif update.effective_chat:
            chat_id = update.effective_chat.id

        if chat_id:
            await context.bot.send_message(chat_id=chat_id, text="Произошла ошибка при отображении главного меню.",
                reply_markup=reply_markup)
        else:
            logger.warning("Не удалось отправить сообщение об ошибке: не удалось определить chat_id.")

    return SHOW_MENU
        

# ========   Обработчик нажатия кнопки "Главное меню" ==========
async def handle_back_to_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    
    context.user_data.clear()

    logger.info("Процесс поиска сброшен. Переход в главное меню.")
    
    reply_markup = create_main_menu_keyboard()

    try:
        try:

            await query.edit_message_text("🏠 Главное меню:", reply_markup=reply_markup)

        except telegram.error.BadRequest as e:

            if 'Message is not modified' not in str(e):

                raise
    except Exception as e:
        logger.error(f"Ошибка при возврате в главное меню: {e}")
                     
    return ConversationHandler.END

# ФУНКЦИЯ ПЕРЕЗАПУСКА БОТА
# ======== Очищает состояние и возвращает в главное меню ==========
async def restart(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data.clear()
    logger.info("Процесс перезапуска. Все данные пользователя очищены.")
    
    reply_markup = create_main_menu_keyboard()
    
    await update.message.reply_text("🔄 *Бот перезапущен.* \n\n🌸Пожалуйста, выберите действие:", reply_markup=reply_markup, parse_mode='Markdown')

    return SHOW_MENU

# ФУНКЦИЯ ОТМЕНЫ
# ======== Обрабатывает нажатие кнопки отмены и возвращает в главное меню ==========
async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query

    # Получаем клавиатуру с кнопкой "В главное меню"
    reply_markup = create_main_menu_keyboard()

    try:
        if query:
            await query.answer()
            try:

                await query.edit_message_text("❌ Действие отменено.")

            except telegram.error.BadRequest as e:

                if 'Message is not modified' not in str(e):

                    raise

            # Отправляем сообщение с кнопкой "В главное меню"
            if query.message:
                await query.message.reply_text(
                    "Вы можете вернуться в главное меню:", 
                    reply_markup=reply_markup
                )

                # Удаляем сообщение через 1 секунду
                await asyncio.sleep(1)

                try:
                    await query.message.delete()
                except Exception as delete_error:
                    logger.warning(f"Ошибка при удалении сообщения: {delete_error}")

        # Очищаем данные пользователя, если они есть
        context.user_data.clear()  

        return ConversationHandler.END  # Завершаем диалог

    except Exception as e:
        logger.error(f"Ошибка при возврате в главное меню: {e}", exc_info=True)
        return ConversationHandler.END


# ДОБАВЛЕНИЕ ДОРАМЫ
def get_main_menu():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🌸 Главное меню", callback_data="return_to_main_menu")],
    ])

# ======== Создаем клавиатуру с кнопками для рейтинга ==========
def create_rating_keyboard():
    keyboard = []
    row = []
    for i in range(1, 11):
        row.append(InlineKeyboardButton(str(i), callback_data=f"rating:{i}"))
        if i % 5 == 0:  # Разбиваем на строки по 5 кнопок
            keyboard.append(row)
            row = []
    if row:  # Если осталась неполная строка, добавляем ее
        keyboard.append(row)
    
    # Добавляем кнопку отмены
    keyboard.append([InlineKeyboardButton("Отмена", callback_data="cancel")])
    
    return InlineKeyboardMarkup(keyboard)

# ======== Запрос данных от пользователя и сохранение в базу данных ==========
async def add_dorama(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = None
    
    reply_markup = create_main_menu_keyboard()

    if update.message:
        user_id = update.message.from_user.id
    elif update.callback_query:
        user_id = update.callback_query.from_user.id
        await update.callback_query.answer()  # Ответ на callback query
    else:
        logger.warning("Получено обновление без message и callback_query")
        return ConversationHandler.END

    if user_id not in ADMINS:
        if update.message:
            await update.message.reply_text("❌ У вас нет прав для добавления дорам.",
                reply_markup=reply_markup)
        elif update.callback_query:
            await update.callback_query.edit_message_text("❌ У вас нет прав для добавления дорам.",
                reply_markup=reply_markup)
        return ConversationHandler.END

    reply_markup = create_cancel_keyboard()

    if update.message:
        await update.message.reply_text("🇷🇺 Введите название дорамы на русском языке:", reply_markup=reply_markup)
    elif update.callback_query:
        await update.callback_query.edit_message_text("🇷🇺 Введите название дорамы на русском языке:", reply_markup=reply_markup)

    return ADDING_TITLE_RU

# Шаг 1: Название на русском
async def receive_title_ru(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message and update.message.text:
        title_ru = update.message.text.strip()
    elif update.callback_query and update.callback_query.data:
        title_ru = update.callback_query.data.strip()
    else:
        await update.message.reply_text("⚠️ Не удалось получить название дорамы. Попробуйте еще раз.")
        return ADDING_TITLE_RU

    if not title_ru:
        await update.message.reply_text("💡Название на русском не может быть пустым. Попробуйте еще раз.")
        return ADDING_TITLE_RU
    
    context.user_data['title_ru'] = title_ru

    reply_markup = create_cancel_keyboard()
    
    await update.message.reply_text("🇬🇧 Введите название дорамы на английском языке:", reply_markup=reply_markup)
    return ADDING_TITLE_EN

# Шаг 2: Название на английском
async def receive_title_en(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message and update.message.text:
        title_en = update.message.text.strip()  # Получаем текст сообщения
    elif update.callback_query and update.callback_query.data:
        title_en = update.callback_query.data.strip()  # Для кнопок, если вдруг.
    else:
        await update.message.reply_text("⚠️ Не удалось получить название дорамы. Попробуйте еще раз.")
        return ADDING_TITLE_EN
        
    if not title_en:
        await update.message.reply_text("💡Название на английском не может быть пустым. Попробуйте еще раз.")
        return ADDING_TITLE_EN
    
    context.user_data['title_en'] = title_en

    # Отправляем сообщение с клавиатурой выбора страны
    reply_markup = InlineKeyboardMarkup(create_country_buttons())
    await update.message.reply_text("🌏 Выберите страну производства дорамы:", reply_markup=reply_markup)
    return ADDING_COUNTRY

# Шаг 3: Страна производства (с кнопками выбора)
# ======== Обработчик для выбора страны при добавлении дорамы ========
async def receive_country_for_add(update: Update, context: ContextTypes.DEFAULT_TYPE):
    country_data = update.callback_query.data
    if not country_data.startswith("select_country:"):
        await update.callback_query.answer("❌ Ошибка. Выберите страну с помощью кнопок.")
        return ADDING_COUNTRY

    # Извлекаем страну из callback_data
    country = country_data.replace("select_country:", "").strip()

    # Проверяем, что страна существует в списке
    if country not in COUNTRIES:
        await update.callback_query.answer("❌ Неверный выбор страны. Попробуйте еще раз.")
        return ADDING_COUNTRY

    # Сохраняем выбранную страну в контексте пользователя
    context.user_data['country'] = country

    # Создаем клавиатуру с кнопкой "Отмена"
    reply_markup = create_cancel_keyboard()
    
    # Отвечаем на callback и редактируем сообщение с новой клавиатурой
    await update.callback_query.answer()
    await update.callback_query.edit_message_text(
        f"Вы выбрали страну: {COUNTRY_FLAGS.get(country, '')} {country}\n📅 Теперь введите год выхода дорамы:",
        reply_markup=reply_markup  # Передаем клавиатуру с кнопкой "Отмена"
    )
    return ADDING_YEAR

# Шаг 4: Год выпуска
async def receive_year(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message and update.message.text:
        year_str = update.message.text.strip()
    elif update.callback_query and update.callback_query.data:
        year_str = update.callback_query.data.strip()
    else:
        await update.message.reply_text("⚠️ Не удалось получить год. Попробуйте еще раз.")
        return ADDING_YEAR
        
    if not year_str.isdigit():
        await update.message.reply_text("📅 Пожалуйста, введите год цифрами.")
        return ADDING_YEAR
        
    year = int(year_str)
    if not (1900 <= year <= 2100):
        await update.message.reply_text("📅 Пожалуйста, введите корректный год (1900-2100).")
        return ADDING_YEAR

    context.user_data['year'] = year

    reply_markup = create_cancel_keyboard()

    await update.message.reply_text("🎬 Введите имя режиссера:", reply_markup=reply_markup)
    return ADDING_DIRECTOR

# Шаг 5: Имя режиссера
async def receive_director(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Проверяем, что пришел текст из сообщения
    if update.message and update.message.text:
        director = update.message.text.strip()
    elif update.callback_query and update.callback_query.data:
        director = update.callback_query.data.strip()
    else:
        await update.message.reply_text("⚠️ Не удалось получить имя режиссера. Попробуйте еще раз.")
        return ADDING_DIRECTOR

    if not director:
        await update.message.reply_text("💡Имя режиссера не может быть пустым. Попробуйте еще раз.")
        return ADDING_DIRECTOR
    
    # Сохраняем выбранного режиссера в контексте пользователя
    context.user_data['director'] = director
    
    reply_markup = create_cancel_keyboard()
    
    await update.message.reply_text("👸🏻 Введите имя актрисы:", reply_markup=reply_markup)
    return ADDING_LEAD_ACTRESS

# Шаг 6: Имя актрисы
async def receive_lead_actress(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Проверяем, что пришел текст из сообщения
    if update.message and update.message.text:
        lead_actress = update.message.text.strip()
    elif update.callback_query and update.callback_query.data:
        lead_actress = update.callback_query.data.strip()
    else:
        await update.message.reply_text("⚠️ Не удалось получить имя актрисы. Попробуйте еще раз.")
        return ADDING_LEAD_ACTRESS
        
    if not lead_actress:
        await update.message.reply_text("💡Имя актрисы не может быть пустым. Попробуйте еще раз.")
        return ADDING_LEAD_ACTRESS
        
    context.user_data['lead_actress'] = lead_actress
    
    reply_markup = create_cancel_keyboard()
    
    await update.message.reply_text("🤴🏻 Введите имя актера:", reply_markup=reply_markup)
    return ADDING_LEAD_ACTOR

# Шаг 7: Имя актёра
async def receive_lead_actor(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Проверяем, что пришел текст из сообщения
    if update.message and update.message.text:
        lead_actor = update.message.text.strip()
    elif update.callback_query and update.callback_query.data:
        lead_actor = update.callback_query.data.strip()
    else:
        await update.message.reply_text("⚠️ Не удалось получить имя актера. Попробуйте еще раз.")
        return ADDING_LEAD_ACTOR
    
    if not lead_actor:
        await update.message.reply_text("💡Имя актера не может быть пустым. Попробуйте еще раз.")
        return ADDING_LEAD_ACTOR
    
    context.user_data['lead_actor'] = lead_actor
    
    reply_markup = create_cancel_keyboard()
    
    await update.message.reply_text("🎞️ Введите сюжет дорамы:", reply_markup=reply_markup)
    return ADDING_PLOT

# Шаг 8: Сюжет дорамы
async def receive_plot(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Проверяем, что пришел текст из сообщения
    if update.message and update.message.text:
        plot = update.message.text.strip()
    elif update.callback_query and update.callback_query.data:
        plot = update.callback_query.data.strip()
    else:
        await update.message.reply_text("⚠️ Не удалось получить сюжет дорамы. Попробуйте еще раз.")
        return ADDING_PLOT
        
    if not plot:
        await update.message.reply_text("💡Сюжет не может быть пустым. Попробуйте еще раз.")
        return ADDING_PLOT
        
    context.user_data['plot'] = plot
    
    # Отправляем клавиатуру с оценками
    reply_markup = create_rating_keyboard()
    await update.message.reply_text("⭐ Оценка дорамы (от 1 до 10):", reply_markup=reply_markup)
    return ADDING_PERSONAL_RATING

# Шаг 9: Обработка выбора личной оценки
async def receive_personal_rating(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    if query.data.startswith("rating:"):
        personal_rating = int(query.data.split(":")[1])

        if 1 <= personal_rating <= 10:
            context.user_data['personal_rating'] = personal_rating
            reply_markup = create_cancel_keyboard()
            try:

                await query.edit_message_text(f"Вы выбрали оценку: {personal_rating}\n💬 Введите комментарий к дораме:", reply_markup=reply_markup)

            except telegram.error.BadRequest as e:

                if 'Message is not modified' not in str(e):

                    raise
            return ADDING_COMMENT
        else:
            try:

                await query.edit_message_text("⚠️ Пожалуйста, выберите оценку от 1 до 10.")

            except telegram.error.BadRequest as e:

                if 'Message is not modified' not in str(e):

                    raise
            return ADDING_PERSONAL_RATING
    else:
        try:

            await query.edit_message_text("⚠️ Пожалуйста, выберите оценку с помощью кнопок.")

        except telegram.error.BadRequest as e:

            if 'Message is not modified' not in str(e):

                raise
        return ADDING_PERSONAL_RATING

    context.user_data['personal_rating'] = personal_rating
    
    reply_markup = create_cancel_keyboard()
    
    await update.message.reply_text("💬 Введите комментарий к дораме:", reply_markup=reply_markup)
    return ADDING_COMMENT

# Шаг 10: Личный комментарий
async def receive_comment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    comment = update.message.text.strip() if update.message and update.message.text else None

    if not comment:
        await update.message.reply_text("💡Комментарий не может быть пустым. Попробуйте еще раз.")
        return ADDING_COMMENT

    context.user_data['comment'] = comment

    await update.message.reply_text("📷 Введите URL постера с Яндекс.Диска:")
    return ADDING_POSTER_URL

# Шаг 11: Получаем постер
async def receive_poster_url(update: Update, context: ContextTypes.DEFAULT_TYPE):
    poster_url = update.message.text.strip()

    # Проверяем, что ссылка действительно с Яндекс.Диска
    if not poster_url.startswith("https://disk.yandex.ru/"):
        await update.message.reply_text("⚠️ Неверный URL! Введите ссылку с Яндекс.Диска.")
        return ADDING_POSTER_URL
    
    # Преобразуем ссылку Яндекс.Диска в прямую ссылку
    direct_link = get_yandex_disk_direct_link(poster_url)
    if not direct_link:
        await update.message.reply_text("⚠️ Не удалось получить прямую ссылку на постер. Проверьте ссылку.")
        return ADDING_POSTER_URL

    context.user_data['poster_url'] = direct_link  # Сохраняем прямую ссылку

    try:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                '''
                INSERT INTO doramas (title_ru, title_en, country, year, director, lead_actress, lead_actor, personal_rating, comment, plot, poster_url)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                (
                    context.user_data['title_ru'],
                    context.user_data['title_en'],
                    context.user_data['country'],
                    context.user_data['year'],
                    context.user_data['director'],
                    context.user_data['lead_actress'],
                    context.user_data['lead_actor'],
                    context.user_data['personal_rating'],
                    context.user_data['comment'],
                    context.user_data['plot'],
                    context.user_data['poster_url'],  
                ),
            )
            await db.commit()

        await update.message.reply_text("🎉 Дорама успешно добавлена!")
        # Очищаем user_data после успешного добавления
        context.user_data.clear()  # Очищаем все данные

        return ConversationHandler.END  

    except aiosqlite.Error as e:
        logger.error(f"❌ Ошибка при добавлении дорамы в БД: {e}", exc_info=True)
        await update.message.reply_text(f"❌ Ошибка при добавлении дорамы в БД: {e}")
        return ADDING_POSTER_URL # Или ConversationHandler.END, в зависимости от желаемого поведения
    except Exception as e:
        logger.error(f"❌ Непредвиденная ошибка: {e}", exc_info=True)
        await update.message.reply_text(f"❌ Произошла непредвиденная ошибка: {e}")
        return ADDING_POSTER_URL # Или ConversationHandler.END, в зависимости от желаемого поведения
        
# ФУНКЦИЯ УДАЛЕНИЯ ДОРАМЫ
# -- Инициирует процесс удаления дорамы
async def delete_dorama(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()

    user_id = update.effective_user.id
    if user_id not in ADMINS:
        try:

            await query.edit_message_text("❌ У вас нет прав для удаления дорам.")

        except telegram.error.BadRequest as e:

            if 'Message is not modified' not in str(e):

                raise
        return ConversationHandler.END

    keyboard = [
        [InlineKeyboardButton("Отмена", callback_data="cancel")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await context.bot.send_message(chat_id=update.effective_chat.id,
                                   text="Введите ID дорамы, которую хотите удалить:",
                                   reply_markup=reply_markup)
    
    return DELETING_DORAMA 

# -- Обрабатывает ввод ID дорамы для удаления.
async def handle_delete_dorama(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    dorama_id = update.message.text.strip()
    
    # Сохраняем ID дорамы в контексте пользователя
    context.user_data['dorama_id_to_delete'] = dorama_id

    keyboard = [
        [InlineKeyboardButton("Да, удалить", callback_data="confirm_delete")],
        [InlineKeyboardButton("Нет, отменить", callback_data="cancel")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text(f"Вы уверены, что хотите удалить дораму с ID {dorama_id}?", reply_markup=reply_markup)
    
    return DELETING_DORAMA

# --Подтверждает удаление дорамы и удаляет ее из базы данных.
async def confirm_delete_dorama(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()

    if query.data == "confirm_delete":
        dorama_id = context.user_data.get('dorama_id_to_delete')
        if not dorama_id:
            try:

                await query.edit_message_text("ID дорамы не найден. Операция отменена.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("В главное меню 🌸", callback_data="return_to_main_menu")]]))

            except telegram.error.BadRequest as e:

                if 'Message is not modified' not in str(e):

                    raise                         
            return ConversationHandler.END

        try:
            dorama_id_int = int(dorama_id)  # Преобразуем в целое число
        except ValueError:
            try:

                await query.edit_message_text("⚠️ ID должен быть числом. Операция отменена.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("В главное меню 🌸", callback_data="return_to_main_menu")]]))

            except telegram.error.BadRequest as e:

                if 'Message is not modified' not in str(e):

                    raise
            return ConversationHandler.END

        try:
            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute('DELETE FROM doramas WHERE id = ?', (dorama_id_int,))  
                await db.commit()
            try:

                await query.edit_message_text(f"Дорама с ID {dorama_id} успешно удалена!", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("В главное меню 🌸", callback_data="return_to_main_menu")]]))

            except telegram.error.BadRequest as e:

                if 'Message is not modified' not in str(e):

                    raise
        except aiosqlite.Error as e:
            logger.error(f"Ошибка при удалении дорамы: {e}")
            try:

                await query.edit_message_text(f"Произошла ошибка при удалении дорамы: {e}", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("В главное меню 🌸", callback_data="return_to_main_menu")]]))

            except telegram.error.BadRequest as e:

                if 'Message is not modified' not in str(e):

                    raise
    else:
        try:

            await query.edit_message_text("Удаление отменено.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("В главное меню 🌸", callback_data="return_to_main_menu")]]))

        except telegram.error.BadRequest as e:

            if 'Message is not modified' not in str(e):

                raise

    return ConversationHandler.END

# ФУНКЦИЯ ДЛЯ ПОЛУЧЕНИЯ ИНФОРМАЦИИ О ДОРАМЕ ПО ID
async def get_dorama_details(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()

    reply_markup = create_cancel_keyboard()
    
    try:
        try:

            await query.edit_message_text("*🔎 Введите ID дорамы, которую хотите посмотреть:*", reply_markup=reply_markup, parse_mode='Markdown')

        except telegram.error.BadRequest as e:

            if 'Message is not modified' not in str(e):

                raise
    except Exception as e:
        logger.error(f"⚠️ Ошибка при запросе ID дорамы: {e}", exc_info=True)
        
    return GETTING_DORAMA_ID

# Возвращает безопасное значение с экранированием.
def safe_get(value):
    logger.debug(f"Value received in safe_get: {value} (type: {type(value)})")
    if value is None:
        return "Не указано"
    if isinstance(value, str):
        escaped_text = escape_markdown(prevent_hashtag_linking(value))
        return remove_extra_escape(escaped_text)
    return "Не указано"

# Возвращает строку с подробной информацией о дораме.
async def get_dorama_details_text(row: aiosqlite.Row) -> str:
    title_ru = safe_get(row['title_ru'])
    title_en = safe_get(row['title_en'])
    plot = safe_get(row['plot'])  
    comment = safe_get(row['comment'])  

    return (
        f"*🇷🇺 {title_ru}*\n"
        f"*🇬🇧 {title_en}*\n\n"
        f"*🌏Страна:* {safe_get(row['country'])}\n"
        f"*📅Год:* {safe_get(str(row['year']))}\n\n"
        f"*🎬Режиссер:* {safe_get(row['director'])}\n"
        f"*👸🏻Главная актриса:* {safe_get(row['lead_actress'])}\n"
        f"*🤴🏻Главный актер:* {safe_get(row['lead_actor'])}\n\n"
        f"*🎞️Сюжет:* {plot}\n\n"  
        f"*⭐Личная оценка:* {safe_get(str(row['personal_rating']))}\n"
        f"*💬Комментарий:* {comment}\n"  
        f"*ID* {(row[0])}\n"
        
    )

# Универсальная функция для отправки сообщений (текст или фото).
async def _send_message(update: Update, text: str, photo: str = None, reply_markup: InlineKeyboardMarkup = None):
    try:
        if update.callback_query:
            if photo:
                await update.callback_query.message.reply_photo(photo=photo, caption=text, parse_mode="Markdown", reply_markup=reply_markup)
            else:
                await update.callback_query.message.reply_text(text, parse_mode="Markdown", reply_markup=reply_markup)
        elif update.message:
            if photo:
                await update.message.reply_photo(photo=photo, caption=text, parse_mode="Markdown", reply_markup=reply_markup)
            else:
                await update.message.reply_text(text, parse_mode="Markdown", reply_markup=reply_markup)
    except Exception as e:
        logger.error(f"Ошибка при отправке сообщения: {e}", exc_info=True)

# Функция для отправки информации о дораме.
async def send_dorama_details(update: Update, row: aiosqlite.Row, context: ContextTypes.DEFAULT_TYPE):
    details = await get_dorama_details_text(row)
    poster_url = safe_get(row['poster_url'])  # Получаем ссылку

    if poster_url.startswith("https://disk.yandex.ru/"):
        poster_download_url = get_yandex_disk_direct_link(poster_url)
        await _send_message(update, details, photo=poster_download_url, reply_markup=back_button)
    else:
        await _send_message(update, details, reply_markup=back_button)



# Получает ID дорамы и выводит информацию
async def receive_dorama_id(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    dorama_id_str = update.message.text.strip()
    try:
        dorama_id = int(dorama_id_str)
    except ValueError:
        await update.message.reply_text("⚠️ ID должен быть числом. Попробуйте еще раз.")
        return GETTING_DORAMA_ID

    try:
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                """
                SELECT id, title_ru, title_en, country, year, director, lead_actress, lead_actor, personal_rating, comment, plot, poster_url
                FROM doramas WHERE id = ?
                """, 
                (dorama_id,)
            ) as cursor:
                row = await cursor.fetchone()

        if row is None:
            await update.message.reply_text(f"🚫 Дорама с ID {dorama_id} не найдена.")
            return ConversationHandler.END


        await send_dorama_details(update, row, context)
        return ConversationHandler.END

    except aiosqlite.Error as e:
        logger.error(f"⚠️ Ошибка при работе с базой данных: {e}", exc_info=True)
        await update.message.reply_text("⚠️ Произошла ошибка при работе с базой данных.", reply_markup=back_button)
        return ConversationHandler.END

                 
# ПОКАЗАТЬ ДОРАМУ 
# Обрабатывает нажатие на кнопку с информацией о дораме по ID.
async def handle_show_dorama(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()

    _, _, dorama_id = query.data.partition(":")
    if not dorama_id or not dorama_id.isdigit():
        await _send_message(update, "⚠️ Ошибка: Не удалось найти информацию о дораме.", reply_markup=back_button)
        return ConversationHandler.END

    try:
        start_time = asyncio.get_event_loop().time()
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                """
                SELECT id AS id,
                       title_ru AS title_ru,
                       title_en AS title_en,
                       country AS country,
                       year AS year,
                       director AS director,
                       lead_actress AS lead_actress,
                       lead_actor AS lead_actor,
                       personal_rating AS personal_rating,
                       comment AS comment,
                       plot AS plot,
                       poster_url AS poster_url
                FROM doramas WHERE id = ?
                """,
                (dorama_id,)
            ) as cursor:
                row = await cursor.fetchone()

        end_time = asyncio.get_event_loop().time()
        logger.info(f"SQL query execution time: {end_time - start_time:.4f} seconds")

        if row:
            await send_dorama_details(update, row, context)
        else:
            await _send_message(update, "🚫 Информация о дораме не найдена.", reply_markup=back_button)

        return ConversationHandler.END

    except Exception as e:
        logger.error(f"⚠️ Ошибка при получении информации о дораме: {e}", exc_info=True)
        await _send_message(update, "⚠️ Произошла ошибка при получении информации о дораме. Попробуйте снова.", reply_markup=back_button)
        return ConversationHandler.END


# ФУНКЦИИ ДЛЯ ПОИСКА ДОРАМЫ ПО СТРАНЕ (с кнопочками)
# Функция для поиска по стране
async def search_by_country(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    
    # Создаем кнопки для выбора страны
    keyboard = create_country_buttons()

    # Добавляем кнопку возврата в главное меню
    keyboard.append([InlineKeyboardButton("В главное меню 🌸", callback_data="return_to_main_menu")])

    reply_markup = InlineKeyboardMarkup(keyboard)

    try:
        # Отправляем сообщение с выбором страны
        try:

            await query.edit_message_text("*🚩 Выберите страну для поиска:*", reply_markup=reply_markup, parse_mode='Markdown')

        except telegram.error.BadRequest as e:

            if 'Message is not modified' not in str(e):

                raise
    except BadRequest:
        # Если сообщение нельзя редактировать, отправляем новое
        await update.message.reply_text("*🚩 Выберите страну для поиска:*", reply_markup=reply_markup, parse_mode='Markdown')
    except Exception as e:
        # Логируем все другие ошибки
        logger.error(f"Ошибка при редактировании сообщения: {e}")
        await update.message.reply_text("❌ Произошла ошибка при обработке запроса.",
                                        reply_markup=InlineKeyboardMarkup([
                                            [InlineKeyboardButton("🔍 Новый поиск", callback_data="search_by_actor")],
                                            [InlineKeyboardButton("В главное меню 🌸", callback_data="return_to_main_menu")]
                                        ])
        )
                                       
    return SEARCH_COUNTRY

# Обработчик выбора страны для поиска
async def handle_search_by_country(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()

    # Разделяем данные из callback_data
    try:
        if query.data.startswith("select_country:"):
            country = query.data.split(":")[1]
            page = 0  # Начинаем с первой страницы
        else:
            raise ValueError(f"Неизвестный формат callback_data: {query.data}")
    except ValueError as ve:
        await query.edit_message_text("⚠️ Произошла ошибка. Попробуйте снова.",
                                      reply_markup=InlineKeyboardMarkup([
                                          [InlineKeyboardButton("🔍 Новый поиск", callback_data="search_by_country")],
                                          [InlineKeyboardButton("В главное меню 🌸", callback_data="return_to_main_menu")]
                                      ])
        )
        return ConversationHandler.END

    # Сохраняем выбранную страну в user_data
    context.user_data['country'] = country

    # Загружаем первую страницу дорам
    return await fetch_doramas_page(update, context, country, page)

# Функция для поиска дорам по стране
async def fetch_doramas_page(update: Update, context: ContextTypes.DEFAULT_TYPE, country: str, page: int) -> int:
    query = update.callback_query

    try:
        # Подключаемся к базе данных
        async with aiosqlite.connect(DB_PATH) as db:
            # Получаем общее количество дорам для указанной страны
            async with db.execute('SELECT COUNT(*) FROM doramas WHERE country LIKE ?', (f"%{country}%",)) as cursor:
                total_results_country = (await cursor.fetchone())[0]

            # Сохраняем количество результатов в контексте
            context.user_data['total_results_country'] = total_results_country

            if total_results_country == 0:
                keyboard = [
                    [InlineKeyboardButton("🔍 Новый поиск", callback_data="search_by_country")],
                    [InlineKeyboardButton("🌸 Главное меню", callback_data="return_to_main_menu")]
                ]
                reply_markup = InlineKeyboardMarkup(keyboard)
                try:

                    await query.edit_message_text(f"🚫 Дорамы из страны '{country}' не найдены.", reply_markup=reply_markup)

                except telegram.error.BadRequest as e:

                    if 'Message is not modified' not in str(e):

                        raise
                return ConversationHandler.END

            # Вычисляем смещение для текущей страницы
            offset = page * PAGE_SIZE

            # Выполняем запрос с пагинацией и сортировкой по названию (по алфавиту)
            async with db.execute(
                'SELECT id, title_ru, year FROM doramas WHERE country LIKE ? ORDER BY title_ru ASC LIMIT ? OFFSET ?',
                (f"%{country}%", PAGE_SIZE, offset)
            ) as cursor:
                results = await cursor.fetchall()

            # Создание кнопок с названиями дорам
            dorama_buttons = [
                [InlineKeyboardButton(f"🎬 {title_ru} ({year})", callback_data=f"show_dorama:{dorama_id}")]
                for dorama_id, title_ru, year in results
            ]

            pagination_keyboard = create_pagination_buttons("country", page=page, total_results=total_results_country).inline_keyboard
            keyboard = dorama_buttons + list(pagination_keyboard)
            keyboard.append([InlineKeyboardButton("🌸 В главное меню", callback_data="return_to_main_menu")])

            reply_markup = InlineKeyboardMarkup(keyboard)

            # Отправляем сообщение с кнопками
            await query.edit_message_text(
                f"*🚩 Найдено {total_results_country} дорам из страны {country}:*\n📄 Страница {page + 1} из {(total_results_country // PAGE_SIZE) + (1 if total_results_country % PAGE_SIZE else 0)}",
                reply_markup=reply_markup, parse_mode="Markdown"
            )

        return ConversationHandler.END

    except aiosqlite.Error as e:
        logger.error(f"Ошибка базы данных: {e} | Пользователь: {update.effective_user.id}")
        await query.edit_message_text("⚠️ Произошла внутренняя ошибка при поиске дорам.",
                                      reply_markup=InlineKeyboardMarkup([
                                          [InlineKeyboardButton("🔍 Новый поиск", callback_data="search_by_country")],
                                          [InlineKeyboardButton("В главное меню 🌸", callback_data="return_to_main_menu")]
                                      ])
        )
        return ConversationHandler.END

# --- Нормализация текста ---
def normalize_text(text):
    if not text:
        return ""
    
    text = text.strip()
    text = unicodedata.normalize("NFKC", text)  # Нормализуем текст
    text = re.sub(r"\s+", " ", text)  # Убираем лишние пробелы
    return text.lower()  # Приводим к нижнему регистру без лишней обработки для кириллицы


# --- Поиск по названию ---
# Хэндлер для поиска по названию
async def start_search_by_title(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    try:
        if update.callback_query:  # Если это обратный вызов
            query = update.callback_query
            await query.answer()
        else:  # Если это текстовое сообщение
            query = None

        # Клавиатура с кнопкой "Отмена"
        reply_markup = create_cancel_keyboard()

        # Просим пользователя ввести название дорамы
        if query:
            await query.edit_message_text(
                "*🔎 Введите название дорамы или слово на русском или английском языке:*", 
                reply_markup=reply_markup, 
                parse_mode='Markdown'
            )
        else:
            await update.message.reply_text(
                "*🔎 Введите название дорамы или слово на русском или английском языке:*", 
                reply_markup=reply_markup, 
                parse_mode='Markdown'
            )

        # Очистка данных поиска из контекста (если это необходимо для нового поиска)
        context.user_data.clear()  # Сброс данных поиска
        logger.info("Контекст очищен. Начинаем новый поиск по названию.")
        
        # Устанавливаем тип поиска
        context.user_data['search_type'] = 'title'
        return SEARCH_TITLE  # Переходим в ожидание текста
    
    except Exception as e:
        logger.error(f"Ошибка в start_search_by_title: {e}")
        if update.callback_query:
            await update.callback_query.answer()
            await update.callback_query.edit_message_text("⚠️ Произошла ошибка при обработке поиска.")
        elif update.message:
            await update.message.reply_text("⚠️ Произошла ошибка при обработке поиска.")
        return ConversationHandler.END

    
# Обработчик поиска по введенному названию
async def handle_search_by_title(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    try:
        if update.message:
            title = update.message.text.strip()  # Получаем текст сообщения
            normalized_title = normalize_text(title)  # Нормализуем и очищаем название
            logger.info(f"Нормализованный заголовок: {normalized_title}")  # Логируем нормализованный заголовок
            context.user_data['normalized_title'] = normalized_title  # Сохраняем нормализованный заголовок в контексте

            # Проверка на пустой ввод
            if not normalized_title:
                await update.message.reply_text(
                    "⚠️ Ошибка: Вы не ввели название для поиска. Попробуйте снова.",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("🔍 Новый поиск", callback_data="search_by_title")],
                        [InlineKeyboardButton("В главное меню 🌸", callback_data="return_to_main_menu")]
                    ])  
                )
                return SEARCH_TITLE  # Ожидаем текст заново
            
            # Загружаем первую страницу с результатами
            logger.info(f"Ищем по названию: {normalized_title}")
            return await fetch_doramas_by_title_page(update, context, normalized_title, 0)

    except Exception as e:
        logger.error(f"Ошибка в handle_search_by_title: {e}")
        await update.message.reply_text("⚠️ Произошла ошибка при обработке запроса. Попробуйте снова.", 
                                        reply_markup=InlineKeyboardMarkup([
                                        [InlineKeyboardButton("🔍 Новый поиск", callback_data="search_by_title")],
                                        [InlineKeyboardButton("В главное меню 🌸", callback_data="return_to_main_menu")]
                                       ])
        )
        return SEARCH_TITLE  # Возвращаемся в состояние поиска


# Функция для поиска дорам по названию
async def fetch_doramas_by_title_page(update: Update, context: ContextTypes.DEFAULT_TYPE, normalized_title: str, page: int) -> int:
    logger.info(f"Запрос на страницы: {page}, с нормализованным названием: {normalized_title}")
    query = update.callback_query

    try:
        async with aiosqlite.connect(DB_PATH) as db:
            # Получаем общее количество дорам для указанного названия
            async with db.execute(
                'SELECT COUNT(*) FROM doramas WHERE LOWER(title_ru) LIKE ? OR LOWER(title_en) LIKE ?', 
                (f"%{normalized_title}%", f"%{normalized_title}%")
            ) as cursor:
                total_results_title = (await cursor.fetchone())[0] or 0  # Проверка на None
            logger.info(f"Найдено результатов: {total_results_title}")

            # Сохраняем количество результатов в контексте
            context.user_data['total_results_title'] = total_results_title
            
            # Если результатов нет, показываем сообщение и завершаем диалог
            if total_results_title == 0:
                keyboard = [
                    [InlineKeyboardButton("🔍 Новый поиск", callback_data="search_by_title")],
                    [InlineKeyboardButton("🌸 Главное меню", callback_data="return_to_main_menu")]
                ]
                reply_markup = InlineKeyboardMarkup(keyboard)

                if query:
                    try:

                        await query.edit_message_text(f"🚫 Дорамы с названием '{normalized_title}' не найдены.", reply_markup=reply_markup)

                    except telegram.error.BadRequest as e:

                        if 'Message is not modified' not in str(e):

                            raise
                else:
                    await update.message.reply_text(f"🚫 Дорамы с названием '{normalized_title}' не найдены.", reply_markup=reply_markup)

                return ConversationHandler.END  # Завершаем диалог            
            
            # Вычисляем параметры пагинации
            offset = page * PAGE_SIZE
            max_pages = (total_results_title + PAGE_SIZE - 1) // PAGE_SIZE              
            
            logger.info(f"SQL-запрос: SELECT id, title_ru, title_en FROM doramas WHERE LOWER(title_ru) LIKE '%{normalized_title}%' COLLATE NOCASE")

            # Выполняем запрос с пагинацией и сортировкой по названию (по алфавиту)
            async with db.execute(
                'SELECT id, title_ru, title_en, country, year FROM doramas '
                'WHERE LOWER(title_ru) LIKE LOWER(?) COLLATE NOCASE ' 
                'OR LOWER(title_en) LIKE LOWER(?) COLLATE NOCASE '
                'ORDER BY title_ru ASC LIMIT ? OFFSET ?',
                (f"%{normalized_title}%", f"%{normalized_title}%", PAGE_SIZE, offset)
            ) as cursor:
                results_title = await cursor.fetchall()
                      
            if not results_title:
                # Формируем клавиатуру с кнопками "Новый поиск" и "Главное меню"
                keyboard = [
                    [InlineKeyboardButton("🔍 Новый поиск", callback_data="search_by_title")],
                    [InlineKeyboardButton("🌸 Главное меню", callback_data="return_to_main_menu")]
                ]
                reply_markup = InlineKeyboardMarkup(keyboard)

        # Формируем ответ с результатами
        response = f"*🌸 Найдено {total_results_title} дорам по запросу '{normalized_title}':*\n\n"
        response += f"📄 Страница {page + 1} из {max_pages}\n\n" # Добавляем информацию о количестве страниц

        keyboard = []
        seen_ids = set() # Множество для хранения уникальных идентификаторов
        
        # Определяем язык поиска
        is_russian_search = any(char in normalized_title for char in "абвгдежзийклмнопрстуфхцчшщъыьэюя") 

        # Формируем кнопки для каждой найденной дорамы
        for row in results_title:
            dorama_id, title_ru, title_en, country, _ = row
            
            # Нормализация заголовка, но без изменения регистра
            if title_ru:
                normalized_title_ru = unicodedata.normalize("NFKC", title_ru)
                
            if dorama_id not in seen_ids: # Проверяем, был ли этот ID уже добавлен
                seen_ids.add(dorama_id) # Добавляем ID в множество
                country_flag = COUNTRY_FLAGS.get(country, country)

                # Ограничиваем длину названий
                truncated_title_ru = truncate_text(normalized_title_ru)
                truncated_title_en = truncate_text(title_en)    
                
                # Формируем текст кнопки в зависимости от языка поиска
                button_text = f"🎬 {normalized_title_ru if is_russian_search else title_en} ({truncated_title_ru}) {country_flag}"
                # **Создаем кнопку и добавляем её в список**
                button = InlineKeyboardButton(button_text, callback_data=f"show_dorama:{dorama_id}")
                keyboard.append([button])  # Добавляем кнопку в список

                logger.info(f"dorama_id: {dorama_id}, title_ru: {normalized_title_ru}, title_en: {title_en}, button_text: {button_text}")

        # Добавляем кнопки пагинации
        keyboard.extend(create_pagination_buttons("title", page, total_results_title).inline_keyboard)
        logger.info(f"Сформированная клавиатура перед отправкой: {keyboard}")
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        logger.info(f"Текст сообщения: {response}")
        logger.info(f"Клавиатура: {reply_markup}")                
        
        # Отправляем результат
        if query:
            try:

                await query.edit_message_text(response, reply_markup=reply_markup, parse_mode="Markdown")

            except telegram.error.BadRequest as e:

                if 'Message is not modified' not in str(e):

                    raise
        elif update.message:
            await update.message.reply_text(response, reply_markup=reply_markup, parse_mode="Markdown")
        else:
            logger.error("Ошибка: Нет данных для обновления сообщения")
            return ConversationHandler.END  
        
        return HANDLE_PAGINATION  # Продолжаем обработку пагинации
    
    except aiosqlite.Error as e:
        logger.error(f"Ошибка базы данных: {e}")
        logger.exception(e)  

        error_message = "⚠️ Произошла ошибка при поиске дорамы."
        if query:
            try:

                await query.edit_message_text(error_message)

            except telegram.error.BadRequest as e:

                if 'Message is not modified' not in str(e):

                    raise
        elif update.message:
            await update.message.reply_text(error_message)

        return ConversationHandler.END  


# ФУНКЦИЯ ПОИСКА ПО АКТЁРУ
# Сразу создадим клавиатуру
def create_actor_keyboard(actors, actor_names_with_flags, total_actors, page=0):
    keyboard = [
        [InlineKeyboardButton(name, callback_data=f"choose_actor:{actor[0]}")]
        for actor, name in zip(actors, actor_names_with_flags)
    ]
    
    pagination_buttons = create_pagination_buttons("actor", page, total_actors)
    keyboard.extend(pagination_buttons.inline_keyboard)
    
    keyboard.append([InlineKeyboardButton("Отмена", callback_data="cancel")])
    
    return InlineKeyboardMarkup(keyboard)

# Функция поиска по актеру
async def search_by_actor(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    try:
        query = update.callback_query
        await query.answer()    
    
        logger.info("Обработчик search_by_actor вызван!")
    
        reply_markup = create_cancel_keyboard()
    
        await query.edit_message_text(
            "*🔎 Введите имя или фамилию актёра с заглавной буквы на русском языке:*", 
            reply_markup=reply_markup, 
            parse_mode='Markdown'
        )

        # Сбрасываем текущий поиск перед новым вводом
        context.user_data.clear()  # Очистка перед новым поиском    
        logger.info("Контекст очищен. Начинаем новый поиск по названию.")
    
        # Устанавливаем тип поиска
        context.user_data['search_type'] = 'actor'
        return SEARCH_ACTOR
    
    except Exception as e:
        logger.error(f"Ошибка в search_by_title: {e}")
        await update.callback_query.answer()
        await update.callback_query.edit_message_text("⚠️ Произошла ошибка при обработке поиска.", reply_markup=back_button)
        return ConversationHandler.END


# Функция для обработки поиска актёра и отображения списка дорам
async def handle_search_by_actor(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if update.message:
        actor_name = update.message.text.strip()
        logger.info(f"🔍 handle_search_by_actor вызван! Пользователь ввёл: {actor_name}")
        
        if not actor_name:
            await update.message.reply_text(
                "⚠️ Пожалуйста, введите имя актёра.", 
                reply_markup=back_button
            )
            return SEARCH_ACTOR
        
        context.user_data['search_actor_name'] = actor_name
        
        try:
            actors = await fetch_actors_from_db(actor_name, 0)
            total_actors = await get_total_actors(actor_name)
            
            if actors:
                actor_names_with_flags = [
                    f"{actor[0]} {COUNTRY_FLAGS.get(actor[1], '🌍')}"
                    for actor in actors
                ]
                
                keyboard = create_actor_keyboard(actors, actor_names_with_flags, total_actors)
                await update.message.reply_text(
                    "*❔Выберите нужный вариант:*", 
                    reply_markup=keyboard, 
                    parse_mode='Markdown'
                )
                return CHOOSE_ACTOR
            
            # Если найдено только одно имя, показываем его дорамы
            actor_name_without_flags = re.sub(r'[^\w\s]', '', actor_name)
            await show_doramas_by_actor(update, context, actor_name_without_flags)
        
        except Exception as e:
            logger.error(f"⚠️ Ошибка при поиске: {e}", exc_info=True)
            await update.message.reply_text(
            f"⚠️ Произошла ошибка. Попробуйте позже. Детали: {e}", 
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔍 Новый поиск", callback_data="search_by_actor")],
                    [InlineKeyboardButton("В главное меню 🌸", callback_data="return_to_main_menu")]
                ])
            )
    
        return ConversationHandler.END

#  Получаем список актёров по имени актёра с пагинацией
async def fetch_actors_from_db(actor_name: str, page: int) -> list:
    start_index = page * PAGE_SIZE
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT DISTINCT lead_actor, country FROM doramas WHERE lead_actor LIKE ? LIMIT ?, ?",
            (f"%{actor_name}%", start_index, PAGE_SIZE)
        ) as cursor:
            return await cursor.fetchall() 

# Получаем общее количество актёров для пагинации
async def get_total_actors(actor_name: str) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT COUNT(DISTINCT lead_actor) FROM doramas WHERE lead_actor LIKE ?",
            (f"%{actor_name}%",)
        ) as cursor:
            return (await cursor.fetchone())[0]

async def show_actors_list(update: Update, context: ContextTypes.DEFAULT_TYPE, actor_name: str, page: int) -> int:
    try:
        start_index = page * PAGE_SIZE  # Начальный индекс для пагинации
        
        # Извлекаем актёров с пагинацией
        actors = await fetch_actors_from_db(actor_name, page)
        total_actors = await get_total_actors(actor_name)
        
        if actors:
            actor_names_with_flags = [
                f"{actor[0]} {COUNTRY_FLAGS.get(actor[1], '🌍')}"  # Используем actor[1] как страну
                for actor in actors
            ]
            
            # Формируем клавиатуру для актёров
            keyboard = create_actor_keyboard(actors, actor_names_with_flags, total_actors, page)
            
            response_text = f"Актёры по имени '{actor_name}' (страница {page + 1}):"
            
            await update.callback_query.message.edit_text(response_text, reply_markup=keyboard, parse_mode='Markdown')
            return CHOOSE_ACTOR
        
        else:
            await update.callback_query.message.edit_text("Актёры не найдены.")
    
    except Exception as e:
        logger.error(f"Ошибка при получении списка актёров: {e}")
        await update.callback_query.message.edit_text("Произошла ошибка при поиске актёров.")
    
    return CHOOSE_ACTOR        
        
# Обрабатываем выбор из списка актёров
async def handle_choose_actor(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()

    _, _, actor_name = query.data.partition(":")
    if not actor_name:
        await query.message.reply_text(
            "⚠️ Ошибка: Не удалось определить имя актёра.", 
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔍 Новый поиск", callback_data="search_by_actor")],
                [InlineKeyboardButton("В главное меню 🌸", callback_data="return_to_main_menu")]
            ])
        )
        return ConversationHandler.END

    # Удаляем флаг из имени актёра
    actor_name_without_flags = re.sub(r'[^\w\s]', '', actor_name)

    # Показываем дорамы выбранного актёра с пагинацией
    return await show_doramas_by_actor(update, context, actor_name_without_flags)

# Выводим список дорам по актёру
async def show_doramas_by_actor(update: Update, context: ContextTypes.DEFAULT_TYPE, actor_name: str, page: int = 0) -> int:
    query = update.callback_query  
    start_index = page * PAGE_SIZE 
    
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT id, title_ru, country, year FROM doramas WHERE LOWER(lead_actor) LIKE LOWER(?) LIMIT ?, ?", 
                ('%' + actor_name.strip() + '%', start_index, PAGE_SIZE)
            ) as cursor:
                results = await cursor.fetchall()
                
            async with db.execute(
                "SELECT COUNT(*) FROM doramas WHERE LOWER(lead_actor) LIKE LOWER(?)", 
                ('%' + actor_name.strip() + '%',)
            ) as cursor:
                total_doramas = (await cursor.fetchone())[0]
                
        # Сохраняем имя актёра и общее число дорам для пагинации
        context.user_data['search_actor_name'] = actor_name
        context.user_data['total_results_actor'] = total_doramas        
        if results:
            country = results[0][2]  # Берем страну из первой найденной дорамы
            country_flag = COUNTRY_FLAGS.get(country, "🌍")
            total_pages = (total_doramas + PAGE_SIZE - 1) // PAGE_SIZE
            
            response = (
                f"🤴 Актёр: *{actor_name}* {country_flag}\n"
                f"📄 Страница {page + 1} из {total_pages}\n"
                f"Всего дорам: *{total_doramas}*\n\n"
            )
            
            keyboard = [[InlineKeyboardButton(f"🎬 {row[1]} ({row[3]})", callback_data=f"show_dorama:{row[0]}")] for row in results]

            # Добавляем кнопки пагинации
            keyboard.extend(create_pagination_buttons("actor", page, total_doramas).inline_keyboard)

            keyboard.append([InlineKeyboardButton("🔍 Новый поиск", callback_data="search_by_actor")])
            keyboard.append([InlineKeyboardButton("В главное меню 🌸", callback_data="return_to_main_menu")])

            reply_markup = InlineKeyboardMarkup(keyboard)

            if query and query.message:
                try:

                    await query.message.edit_text(text=response, reply_markup=reply_markup, parse_mode='Markdown')

                except telegram.error.BadRequest as e:

                    if 'Message is not modified' not in str(e):

                        raise
            else:
                await update.message.reply_text(text=response, reply_markup=reply_markup, parse_mode='Markdown')
               
        else:
            await query.message.edit_text(
                f"🚫 Дорамы с актёром '{actor_name}' не найдены.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔍 Начать новый поиск", callback_data="search_by_actor")],
                    [InlineKeyboardButton("В главное меню 🌸", callback_data="return_to_main_menu")]
                ])
            )                
        
        return ConversationHandler.END

    except Exception as e:
        logger.error(
            f"⚠️ Ошибка при поиске дорам по актёру: {e}", 
            exc_info=True
        )
        await update.message.reply_text(
            "⚠️ Произошла ошибка при поиске дорам. Попробуйте позже.", 
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔍 Новый поиск", callback_data="search_by_actor")],
                [InlineKeyboardButton("В главное меню 🌸", callback_data="return_to_main_menu")]
            ])
        )
        return ConversationHandler.END
        

# ФУНКЦИЯ ПОИСКА ПО АКТРИСЕ
# Сразу создадим клавиатуру
def create_actress_keyboard(actress, actress_names_with_flags, total_actress, page=0):
    keyboard = [
        [InlineKeyboardButton(name, callback_data=f"choose_actress:{actress[0]}")]
        for actress, name in zip(actress, actress_names_with_flags)
    ]
    
    pagination_buttons = create_pagination_buttons("actress", page, total_actress)
    keyboard.extend(pagination_buttons.inline_keyboard)
    
    keyboard.append([InlineKeyboardButton("Отмена", callback_data="cancel")])
    
    return InlineKeyboardMarkup(keyboard)

# Функция поиска по актрисе
async def search_by_actress(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    try:
        query = update.callback_query
        await query.answer()    
    
        logger.info("Обработчик search_by_actress вызван!")
    
        reply_markup = create_cancel_keyboard()
    
        await query.edit_message_text(
            "*🔎 Введите имя или фамилию актрисы с заглавной буквы на русском языке:*", 
            reply_markup=reply_markup, 
            parse_mode='Markdown'
        )

        # Сбрасываем текущий поиск перед новым вводом
        context.user_data.clear()  # Очистка перед новым поиском    
        logger.info("Контекст очищен. Начинаем новый поиск по названию.")
    
        # Устанавливаем тип поиска
        context.user_data['search_type'] = 'actress'
        return SEARCH_ACTRESS
    
    except Exception as e:
        logger.error(f"Ошибка в search_by_title: {e}")
        await update.callback_query.answer()
        await update.callback_query.edit_message_text(
            "⚠️ Произошла ошибка при обработке поиска.", 
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔍 Новый поиск", callback_data="search_by_actress")],
                [InlineKeyboardButton("В главное меню 🌸", callback_data="return_to_main_menu")]
            ])
        )
        return ConversationHandler.END

# Функция для обработки поиска актрисы и отображения списка дорам
async def handle_search_by_actress(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if update.message:
        actress_name = update.message.text.strip()
        logger.info(f"🔍 handle_search_by_actress вызван! Пользователь ввёл: {actress_name}")
        
        if not actress_name:
            await update.message.reply_text(
                "⚠️ Пожалуйста, введите имя актрисы.", 
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔍 Новый поиск", callback_data="search_by_actress")],
                    [InlineKeyboardButton("В главное меню 🌸", callback_data="return_to_main_menu")]
                ])
            )
            return SEARCH_ACTRESS
        
        context.user_data['search_actress_name'] = actress_name
        
        try:
            actresses = await fetch_actresses_from_db(actress_name, 0)
            total_actresses = await get_total_actresses(actress_name)
            
            if actresses:
                actress_names_with_flags = [
                    f"{actress[0]} {COUNTRY_FLAGS.get(actress[1], '🌍')}"
                    for actress in actresses
                ]
                
                keyboard = create_actress_keyboard(actresses, actress_names_with_flags, total_actresses)
                await update.message.reply_text(
                    "*❔Выберите нужный вариант:*", 
                    reply_markup=keyboard, 
                    parse_mode='Markdown'
                )
                return CHOOSE_ACTRESS
            
            # Если найдено только одно имя, показываем его дорамы
            actress_name_without_flags = re.sub(r'[^\w\s]', '', actress_name)
            await show_doramas_by_actress(update, context, actress_name_without_flags)
        
        except Exception as e:
            logger.error(
                f"⚠️ Ошибка при поиске: {e}", 
                exc_info=True
            )
            await update.message.reply_text(
            f"⚠️ Произошла ошибка. Попробуйте позже. Детали: {e}", 
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔍 Новый поиск", callback_data="search_by_actress")],
                    [InlineKeyboardButton("В главное меню 🌸", callback_data="return_to_main_menu")]
                ])
            )
    
    return ConversationHandler.END

# Получаем список актрис по имени актрисы с пагинацией
async def fetch_actresses_from_db(actress_name: str, page: int) -> list:
    start_index = page * PAGE_SIZE
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT DISTINCT lead_actress, country FROM doramas WHERE lead_actress LIKE ? LIMIT ?, ?",
            (f"%{actress_name}%", start_index, PAGE_SIZE)
        ) as cursor:
            return await cursor.fetchall() 

# Получаем общее количество актрис для пагинации
async def get_total_actresses(actress_name: str) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT COUNT(DISTINCT lead_actress) FROM doramas WHERE lead_actress LIKE ?",
            (f"%{actress_name}%",)
        ) as cursor:
            return (await cursor.fetchone())[0]

#  Функция для отображения списка актрис с пагинацией
async def show_actresses_list(update: Update, context: ContextTypes.DEFAULT_TYPE, actress_name: str, page: int) -> int:
    try:
        start_index = page * PAGE_SIZE  # Начальный индекс для пагинации
        
        # Извлекаем актрис с пагинацией
        actresses = await fetch_actresses_from_db(actress_name, page)
        total_actresses = await get_total_actresses(actress_name)
        
        if actresses:
            actress_names_with_flags = [
                f"{actress[0]} {COUNTRY_FLAGS.get(actress[1], '🌍')}"  # Используем actress[1] как страну
                for actress in actresses
            ]
            
            # Формируем клавиатуру для актрис
            keyboard = create_actress_keyboard(actresses, actress_names_with_flags, total_actresses, page)
            
            response_text = f"Актрисы по имени '{actress_name}' (страница {page + 1}):"
            
            await update.callback_query.message.edit_text(response_text, reply_markup=keyboard, parse_mode='Markdown')
            return CHOOSE_ACTRESS
        
        else:
            await update.callback_query.message.edit_text("Актрисы не найдены.")
    
    except Exception as e:
        logger.error(f"Ошибка при получении списка актрис: {e}")
        await update.callback_query.message.edit_text("Произошла ошибка при поиске актрис.")
    
    return CHOOSE_ACTRESS

# Обрабатывает выбор из списка актрис
async def handle_choose_actress(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()

    _, _, actress_name = query.data.partition(":")
    if not actress_name:
        await query.message.reply_text(
            "⚠️ Ошибка: Не удалось определить имя актрисы.", 
            reply_markup=back_button
        )
        return ConversationHandler.END

    # Удаляем флаг из имени актрисы
    actress_name_without_flags = re.sub(r'[^\w\s]', '', actress_name)

    # Показываем дорамы выбранной актрисы с пагинацией
    return await show_doramas_by_actress(update, context, actress_name_without_flags)

# Выводит список дорам по актрисе
async def show_doramas_by_actress(update: Update, context: ContextTypes.DEFAULT_TYPE, actress_name: str, page: int = 0) -> int:
    query = update.callback_query  
    start_index = page * PAGE_SIZE 
    
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT id, title_ru, country, year FROM doramas WHERE LOWER(lead_actress) LIKE LOWER(?) LIMIT ?, ?", 
                ('%' + actress_name.strip() + '%', start_index, PAGE_SIZE)
            ) as cursor:
                results = await cursor.fetchall()
                
            async with db.execute(
                "SELECT COUNT(*) FROM doramas WHERE LOWER(lead_actress) LIKE LOWER(?)", 
                ('%' + actress_name.strip() + '%',)
            ) as cursor:
                total_doramas = (await cursor.fetchone())[0]
                
        # Сохраняем имя актрисы и общее число дорам для пагинации
        context.user_data['search_actress_name'] = actress_name
        context.user_data['total_results_actress'] = total_doramas        
        
        if results:
            country = results[0][2]  # Берем страну из первой найденной дорамы
            country_flag = COUNTRY_FLAGS.get(country, "🌍")
            total_pages = (total_doramas + PAGE_SIZE - 1) // PAGE_SIZE
            
            response = (
                f"🤴 Актриса: *{actress_name}* {country_flag}\n"
                f"📄 Страница {page + 1} из {total_pages}\n"
                f"Всего дорам: *{total_doramas}*\n\n"
            )
            
            keyboard = [[InlineKeyboardButton(f"🎬 {row[1]} ({row[3]})", callback_data=f"show_dorama:{row[0]}")] for row in results]

            # Добавляем кнопки пагинации
            keyboard.extend(create_pagination_buttons("actress", page, total_doramas).inline_keyboard)

            keyboard.append([InlineKeyboardButton("🔍 Новый поиск", callback_data="search_by_actress")])
            keyboard.append([InlineKeyboardButton("В главное меню 🌸", callback_data="return_to_main_menu")])

            reply_markup = InlineKeyboardMarkup(keyboard)

            if query and query.message:
                try:

                    await query.message.edit_text(text=response, reply_markup=reply_markup, parse_mode='Markdown')

                except telegram.error.BadRequest as e:

                    if 'Message is not modified' not in str(e):

                        raise
            else:
                await update.message.reply_text(text=response, reply_markup=reply_markup, parse_mode='Markdown')
               
        else:
            await query.message.edit_text(
                f"🚫 Дорамы с актрисой '{actress_name}' не найдены.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔍 Начать новый поиск", callback_data="search_by_actress")],
                    [InlineKeyboardButton("В главное меню 🌸", callback_data="return_to_main_menu")]
                ])
            )
                
        return ConversationHandler.END

    except Exception as e:
        logger.error(
            f"⚠️ Ошибка при поиске дорам по актрисе: {e}", 
            exc_info=True
        )
        await update.message.reply_text(
            "⚠️ Произошла ошибка при поиске дорам. Попробуйте позже.",
            reply_markup=back_button
        )
        return ConversationHandler.END

# ФУНКЦИЯ ПОИСКА ПО РЕЖИССЁРУ
# Функция для формирования клавиатуры режиссёров
def create_director_keyboard(directors, director_names_with_flags, total_directors, page=0):
    keyboard = [
        [InlineKeyboardButton(name, callback_data=f"choose_director:{director[0]}")]
        for director, name in zip(directors, director_names_with_flags)
    ]
    
    pagination_buttons = create_pagination_buttons("director", page, total_directors)
    keyboard.extend(pagination_buttons.inline_keyboard)
    
    keyboard.append([InlineKeyboardButton("Отмена", callback_data="cancel")])
    
    return InlineKeyboardMarkup(keyboard)

# Функция поиска по режиссёру
async def search_by_director(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    try:
        query = update.callback_query
        await query.answer()    
    
        logger.info("Обработчик search_by_director вызван!")
    
        reply_markup = create_cancel_keyboard()
    
        await query.edit_message_text(
            "*🔎 Введите имя или фамилию режиссёра с заглавной буквы на русском языке:*", 
            reply_markup=reply_markup, 
            parse_mode='Markdown'
        )

        # Сбрасываем текущий поиск перед новым вводом
        context.user_data.clear()  # Очистка перед новым поиском    
        logger.info("Контекст очищен. Начинаем новый поиск по названию.")
    
        # Устанавливаем тип поиска
        context.user_data['search_type'] = 'director'
        return SEARCH_DIRECTOR
    
    except Exception as e:
        logger.error(f"Ошибка в search_by_title: {e}")
        await update.callback_query.answer()
        await update.callback_query.edit_message_text(
            "⚠️ Произошла ошибка при обработке поиска.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔍 Новый поиск", callback_data="search_by_director")],
                [InlineKeyboardButton("В главное меню 🌸", callback_data="return_to_main_menu")]
            ])
        )
        return ConversationHandler.END


# Функция для обработки поиска режиссёра и отображения списка дорам
async def handle_search_by_director(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if update.message:
        director_name = update.message.text.strip()
        logger.info(f"🔍 handle_search_by_director вызван! Пользователь ввёл: {director_name}")
        
        if not director_name:
            await update.message.reply_text(
                "⚠️ Пожалуйста, введите имя режиссёра.", 
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔍 Новый поиск", callback_data="search_by_director")],
                    [InlineKeyboardButton("В главное меню 🌸", callback_data="return_to_main_menu")]
                ])
            )
            return SEARCH_DIRECTOR
        
        context.user_data['search_director_name'] = director_name
        
        try:
            directors = await fetch_directors_from_db(director_name, 0)
            total_directors = await get_total_directors(director_name)
            
            if directors:
                director_names_with_flags = [
                    f"{director[0]} {COUNTRY_FLAGS.get(director[1], '🌍')}"
                    for director in directors
                ]
                
                keyboard = create_director_keyboard(directors, director_names_with_flags, total_directors)
                await update.message.reply_text(
                    "*❔Выберите нужный вариант:*", 
                    reply_markup=keyboard, 
                    parse_mode='Markdown'
                )
                return CHOOSE_DIRECTOR
            
            # Если найдено только одно имя, показываем его дорамы
            director_name_without_flags = re.sub(r'[^\w\s]', '', director_name)
            await show_doramas_by_director(update, context, director_name_without_flags)
        
        except Exception as e:
            logger.error(f"⚠️ Ошибка при поиске: {e}", exc_info=True)
            await update.message.reply_text(
                f"⚠️ Произошла ошибка. Попробуйте позже. Детали: {e}", 
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔍 Новый поиск", callback_data="search_by_director")],
                    [InlineKeyboardButton("В главное меню 🌸", callback_data="return_to_main_menu")]
                ])
            )
    
    return ConversationHandler.END

# Получаем список режиссёров по имени режиссёра с пагинацией
async def fetch_directors_from_db(director_name: str, page: int) -> list:
    start_index = page * PAGE_SIZE
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT DISTINCT director, country FROM doramas WHERE director LIKE ? LIMIT ?, ?",
            (f"%{director_name}%", start_index, PAGE_SIZE)
        ) as cursor:
            return await cursor.fetchall() 

# Получаем общее количество режиссёров для пагинации
async def get_total_directors(director_name: str) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT COUNT(DISTINCT director) FROM doramas WHERE director LIKE ?",
            (f"%{director_name}%",)
        ) as cursor:
            return (await cursor.fetchone())[0]
        
# Функция для отображения списка режиссёров с пагинацией
async def show_directors_list(update: Update, context: ContextTypes.DEFAULT_TYPE, director_name: str, page: int) -> int:
    try:
        start_index = page * PAGE_SIZE  # Начальный индекс для пагинации
        
        # Извлекаем режиссёров с пагинацией
        directors = await fetch_directors_from_db(director_name, page)
        total_directors = await get_total_directors(director_name)
        
        if directors:
            director_names_with_flags = [
                f"{director[0]} {COUNTRY_FLAGS.get(director[1], '🌍')}"  # Используем director[1] как страну
                for director in directors
            ]
            
            # Формируем клавиатуру для режиссёров
            keyboard = create_director_keyboard(directors, director_names_with_flags, total_directors, page)
            
            response_text = f"Режиссёры по имени '{director_name}' (страница {page + 1}):"
            
            await update.callback_query.message.edit_text(response_text, reply_markup=keyboard, parse_mode='Markdown')
            return CHOOSE_DIRECTOR
        
        else:
            await update.callback_query.message.edit_text("Режиссёры не найдены.")
    
    except Exception as e:
        logger.error(f"Ошибка при получении списка режиссёров: {e}")
        await update.callback_query.message.edit_text("Произошла ошибка при поиске режиссёров.")
    
    return CHOOSE_DIRECTOR

# Обрабатывает выбор из списка режиссёров
async def handle_choose_director(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()

    _, _, director_name = query.data.partition(":")
    if not director_name:
        await query.message.reply_text(
            "⚠️ Ошибка: Не удалось определить имя режиссёра.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔍 Новый поиск", callback_data="search_by_director")],
                [InlineKeyboardButton("В главное меню 🌸", callback_data="return_to_main_menu")]
            ])
        )
        return ConversationHandler.END

    # Удаляем флаг из имени режиссёра
    director_name_without_flags = re.sub(r'[^\w\s]', '', director_name)

    # Показываем дорамы выбранного режиссёра с пагинацией
    return await show_doramas_by_director(update, context, director_name_without_flags)


# Выводит список дорам по режиссёру
async def show_doramas_by_director(update: Update, context: ContextTypes.DEFAULT_TYPE, director_name: str, page: int = 0) -> int:
    query = update.callback_query  
    start_index = page * PAGE_SIZE 
    
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT id, title_ru, country, year FROM doramas WHERE LOWER(director) LIKE LOWER(?) LIMIT ?, ?", 
                ('%' + director_name.strip() + '%', start_index, PAGE_SIZE)
            ) as cursor:
                results = await cursor.fetchall()
                
            async with db.execute(
                "SELECT COUNT(*) FROM doramas WHERE LOWER(director) LIKE LOWER(?)", 
                ('%' + director_name.strip() + '%',)
            ) as cursor:
                total_doramas = (await cursor.fetchone())[0]
                
        # Сохраняем имя режиссёра и общее число дорам для пагинации
        context.user_data['search_director_name'] = director_name
        context.user_data['total_results_director'] = total_doramas        
        
        if results:
            country = results[0][2]  # Берем страну из первой найденной дорамы
            country_flag = COUNTRY_FLAGS.get(country, "🌍")
            total_pages = (total_doramas + PAGE_SIZE - 1) // PAGE_SIZE
            
            response = (
                f"🤴 Режиссёр: *{director_name}* {country_flag}\n"
                f"📄 Страница {page + 1} из {total_pages}\n"
                f"Всего дорам: *{total_doramas}*\n\n"
            )
            
            keyboard = [[InlineKeyboardButton(f"🎬 {row[1]} ({row[3]})", callback_data=f"show_dorama:{row[0]}")] for row in results]

            # Добавляем кнопки пагинации
            keyboard.extend(create_pagination_buttons("director", page, total_doramas).inline_keyboard)

            keyboard.append([InlineKeyboardButton("🔍 Новый поиск", callback_data="search_by_director")])
            keyboard.append([InlineKeyboardButton("В главное меню 🌸", callback_data="return_to_main_menu")])

            reply_markup = InlineKeyboardMarkup(keyboard)

            if query and query.message:
                try:

                    await query.message.edit_text(text=response, reply_markup=reply_markup, parse_mode='Markdown')

                except telegram.error.BadRequest as e:

                    if 'Message is not modified' not in str(e):

                        raise
            else:
                await update.message.reply_text(text=response, reply_markup=reply_markup, parse_mode='Markdown')
               
        else:
            await query.message.edit_text(
                f"🚫 Дорамы с режиссёром '{director_name}' не найдены.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔍 Начать новый поиск", callback_data="search_by_director")],
                    [InlineKeyboardButton("В главное меню 🌸", callback_data="return_to_main_menu")]
                ])
            )
                
        return ConversationHandler.END

    except Exception as e:
        logger.error(f"⚠️ Ошибка при поиске дорам по режиссёру: {e}", exc_info=True)
        await update.message.reply_text(
            "⚠️ Произошла ошибка при поиске дорам. Попробуйте позже.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔍 Новый поиск", callback_data="search_by_director")],
                [InlineKeyboardButton("В главное меню 🌸", callback_data="return_to_main_menu")]
            ])
        )
        return ConversationHandler.END



# СПИСОК ВСЕХ ДОРАМ
# Функция для инициализации пагинации и данных пользователя
def initialize_page(context, key):
    if key not in context.user_data:
        context.user_data[key] = 0

# Обработчик выбора языка
# Меню для выбора параметров дорам
async def list_doramas_menu(update, context):
    query = update.callback_query
    await query.answer()

    keyboard = [
        [InlineKeyboardButton("🔠 По алфавиту", callback_data="list_by_letter")],
        [InlineKeyboardButton("⭐ По рейтингу", callback_data="list_doramas_by_rating")],
        [InlineKeyboardButton("📅 Поиск по году", callback_data="list_years")],
        [InlineKeyboardButton("🌸 В главное меню", callback_data="return_to_main_menu")],
    ]
    
    try:

    
        await query.edit_message_text("Выберите опцию:", reply_markup=InlineKeyboardMarkup(keyboard))

    
    except telegram.error.BadRequest as e:

    
        if 'Message is not modified' not in str(e):

    
            raise

# ========  Функция для получения общего количества дорам ======== 
async def get_total_doramas_count():
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT COUNT(*) FROM doramas") as cursor:
            return (await cursor.fetchone())[0]

# ========  Функция для выбора языка ======== 
async def handle_language_choice(update, context): 
    query = update.callback_query
    await query.answer()
    
    if query.data == "language_ru":
        context.user_data["language"] = "ru"
    elif query.data == "language_en":
        context.user_data["language"] = "en"
        
    context.user_data["language"] = "ru" if query.data == "language_ru" else "en"
    await list_doramas_by_letter(update, context)

# ========  Функция для отображения меню выбора языка ======== 
async def choose_language(update, context):
    query = update.callback_query
    await query.answer()

    keyboard = [
        [InlineKeyboardButton("🇷🇺 Русский", callback_data="language_ru")],
        [InlineKeyboardButton("🇬🇧 English", callback_data="language_en")],
        [InlineKeyboardButton("🌸 В главное меню", callback_data="return_to_main_menu")]
    ]
    try:

        await query.edit_message_text("*Выберите язык:*", parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(keyboard))

    except telegram.error.BadRequest as e:

        if 'Message is not modified' not in str(e):

            raise
    
# ========   Функция для отображения списка букв дорам ======== 
async def list_doramas_by_letter(update, context):
    query = update.callback_query
    await query.answer()

    # Инициализация текущей страницы
    initialize_page(context, "letter_page")

    language = context.user_data.get("language", "ru")
    column = "title_ru" if language == "ru" else "title_en"
    prompt = "*Выберите первую букву названия: 🇷🇺*" if language == "ru" else "*Выберите первую букву названия: 🇬🇧*"
    
    # Подключение к базе данных
    async with aiosqlite.connect(DB_PATH) as db:
        # Получение доступных букв
        async with db.execute(f"SELECT DISTINCT substr({column}, 1, 1) FROM doramas WHERE {column} IS NOT NULL") as cursor:
            available_letters = [row[0] for row in await cursor.fetchall()]

    # Приведение к нижнему регистру для правильного сравнения
    available_letters = list(set(letter.upper() for letter in available_letters if letter.strip()))

    if language == "ru":
        russian_alphabet = "АБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯ"
        letters = sorted([letter for letter in available_letters if letter in russian_alphabet])
        non_letters = sorted([letter for letter in available_letters if letter not in russian_alphabet])
    else:
        letters = sorted([letter for letter in available_letters if letter.isalpha()])
        non_letters = sorted([letter for letter in available_letters if not letter.isalpha()])

    # Создание клавиатуры с 5 кнопками в каждом ряду
    letter_rows = [letters[i:i + 5] for i in range(0, len(letters), 5)]
    non_letter_rows = [non_letters[i:i + 5] for i in range(0, len(non_letters), 5)]

    keyboard = (
        [[InlineKeyboardButton(letter, callback_data=f"filter_by_letter_{letter}") for letter in row] for row in letter_rows] 
        + [[InlineKeyboardButton(letter, callback_data=f"filter_by_letter_{letter}") for letter in row] for row in non_letter_rows]
    )

    # Добавление кнопок возврата
    keyboard.append([InlineKeyboardButton("🌸 В главное меню", callback_data="return_to_main_menu")])

    # Отображение
    total_doramas = await get_total_doramas_count()
    try:

        await query.edit_message_text(f"*{prompt}*\nВсего дорам: {total_doramas}", parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(keyboard))

    except telegram.error.BadRequest as e:

        if 'Message is not modified' not in str(e):

            raise


# ========  Функция для отображения дорам по выбранной букве ========
async def show_doramas_by_letter(update, context):
    query = update.callback_query
    await query.answer()

    # Получение выбранной буквы
    if query.data.startswith("filter_by_letter_"):
        letter = query.data.split("_")[-1]
        context.user_data["selected_letter"] = letter
    else:
        letter = context.user_data["selected_letter"]

    # Подключение к базе данных
    column = "title_ru" if context.user_data.get("language", "ru") == "ru" else "title_en"
    
    async with aiosqlite.connect(DB_PATH) as db:
        if letter.upper() in ['T', 'A']:
            async with db.execute(f"""
                SELECT id, {column}, country 
                FROM doramas 
                WHERE 
                    ({column} LIKE ? AND {column} NOT LIKE 'The %' AND {column} NOT LIKE 'A %') 
                    OR 
                    ({column} LIKE 'The %' AND SUBSTR({column}, 5, 1) = ?) 
                    OR 
                    ({column} LIKE 'A %' AND SUBSTR({column}, 2, 1) = ?)
                ORDER BY {column}
            """, (letter + "%", letter.upper(), letter.upper())) as cursor:
                rows = await cursor.fetchall()
        else:
            async with db.execute(f"""
                SELECT id, {column}, country 
                FROM doramas 
                WHERE 
                    ({column} LIKE ?) 
                    OR 
                    ({column} LIKE 'The %' AND SUBSTR({column}, 5, 1) = ?) 
                    OR 
                    ({column} LIKE 'A %' AND SUBSTR({column}, 2, 1) = ?)
                ORDER BY {column}
            """, (letter + "%", letter.upper(), letter.upper())) as cursor:
                rows = await cursor.fetchall()

    if not rows:
        try:

            await query.edit_message_text(f"Нет дорам, начинающихся на {letter}.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="list_doramas_by_letter")]]))

        except telegram.error.BadRequest as e:

            if 'Message is not modified' not in str(e):

                raise
        return


    # Пагинация дорам по букве
    initialize_page(context, "letter_doramas_page")

    start, end = context.user_data["letter_doramas_page"] * PAGE_SIZE, (context.user_data["letter_doramas_page"] + 1) * PAGE_SIZE
    rows_to_show = rows[start:end]

    keyboard = [
        [InlineKeyboardButton(f"{row[1]} {COUNTRY_FLAGS.get(row[2], '🌍')}", callback_data=f"show_dorama:{row[0]}")] 
        for row in rows_to_show
    ]

    pagination_buttons = []
    if context.user_data["letter_doramas_page"] > 0:
        pagination_buttons.append(InlineKeyboardButton("⬅️ Назад", callback_data="letter_doramas_page_back"))
    if len(rows) > (context.user_data["letter_doramas_page"] + 1) * PAGE_SIZE:
        pagination_buttons.append(InlineKeyboardButton("➡️ Вперед", callback_data="letter_doramas_page_next"))

    if pagination_buttons:
        pagination_buttons = [pagination_buttons]
    else:
        pagination_buttons = []
    

    # Кнопка возврата
    keyboard.extend(pagination_buttons)
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="list_doramas_by_letter")])

    try:


        await query.edit_message_text(f"Дорамы на букву {letter} (Всего: {len(rows)}):", reply_markup=InlineKeyboardMarkup(keyboard))


    except telegram.error.BadRequest as e:


        if 'Message is not modified' not in str(e):


            raise

# Функция для обработки пагинации дорам по букве
async def handle_letter_doramas_pagination(update, context):
    query = update.callback_query
    await query.answer()

    page_change = -1 if query.data == "letter_doramas_page_back" else 1
    context.user_data["letter_doramas_page"] = max(0, context.user_data["letter_doramas_page"] + page_change)

    await show_doramas_by_letter(update, context)


# ========  Функция для отображения списка дорам по рейтингу ========== 
async def list_doramas_by_rating(update, context):
    query = update.callback_query
    await query.answer()

    initialize_page(context, "rating_page")

    # Получение списка рейтингов
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT DISTINCT personal_rating FROM doramas ORDER BY personal_rating DESC") as cursor:
        
            rows = await cursor.fetchall()
            
    ratings = [str(row[0]) for row in rows if row[0] is not None]

    # Формируем клавиатуру для рейтингов, добавляем звездочку к каждому рейтингу
    keyboard = [
        [InlineKeyboardButton(f"⭐ {rating}", callback_data=f"filter_by_rating_{rating}")]
        for rating in ratings
    ]
    
    # Добавляем кнопку "В главное меню"
    keyboard.append([InlineKeyboardButton("🌸 В главное меню", callback_data="return_to_main_menu")])
    
    # Отображение
    total_doramas = await get_total_doramas_count()
    try:

        await query.edit_message_text(f"*Выберите оценку* (Всего дорам: {total_doramas}):", parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(keyboard))

    except telegram.error.BadRequest as e:

        if 'Message is not modified' not in str(e):

            raise


# ======== Функция для отображения дорам по выбранному рейтингу  ========== 
async def show_doramas_by_rating(update, context):
    query = update.callback_query
    await query.answer()

    # Получение выбранного рейтинга
    if query.data.startswith("filter_by_rating_"):
        rating = query.data.split("_")[-1]
        context.user_data["selected_rating"] = rating
    else:
        rating = context.user_data["selected_rating"]

    # Получаем количество дорам с этим рейтингом
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT COUNT(*) FROM doramas WHERE personal_rating = ?", (rating,)) as cursor:
            count_row = await cursor.fetchone()
            count = count_row[0] if count_row else 0

    # Получаем дорамы с этим рейтингом
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT id, title_ru, country FROM doramas WHERE personal_rating = ? ORDER BY title_ru", (rating,)) as cursor:
            rows = await cursor.fetchall()
            
    if not rows:
        await query.edit_message_text(f"Нет дорам с рейтингом {rating}.", 
                                      reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="list_doramas_by_rating")]]))
        return            

    # Пагинация дорам по рейтингу
    initialize_page(context, "rating_doramas_page")

    start, end = context.user_data["rating_doramas_page"] * PAGE_SIZE, (context.user_data["rating_doramas_page"] + 1) * PAGE_SIZE
    rows_to_show = rows[start:end]
    
    keyboard = [
        [InlineKeyboardButton(f"{row[1]} {COUNTRY_FLAGS.get(row[2], '🌍')}", callback_data=f"show_dorama:{row[0]}")] 
        for row in rows_to_show
    ]

    pagination_buttons = []
    if context.user_data["rating_doramas_page"] > 0:
        pagination_buttons.append(InlineKeyboardButton("⬅️ Назад", callback_data="rating_doramas_page_back"))
    if len(rows) > (context.user_data["rating_doramas_page"] + 1) * PAGE_SIZE:
        pagination_buttons.append(InlineKeyboardButton("➡️ Вперед", callback_data="rating_doramas_page_next"))

    if pagination_buttons:
        pagination_buttons = [pagination_buttons]
    else:
        pagination_buttons = []
        
    # Кнопка возврата
    keyboard.extend(pagination_buttons)
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="list_doramas_by_rating")])

    try:


        await query.edit_message_text(f"Дорамы с рейтингом {rating} (Всего: {len(rows)}):", reply_markup=InlineKeyboardMarkup(keyboard))


    except telegram.error.BadRequest as e:


        if 'Message is not modified' not in str(e):


            raise

# ======== Функция для обработки пагинации дорам  ========== 
async def handle_rating_doramas_pagination(update, context):
    query = update.callback_query
    await query.answer()

    page_change = -1 if query.data == "rating_doramas_page_back" else 1
    context.user_data["rating_doramas_page"] = max(0, context.user_data["rating_doramas_page"] + page_change)

    await show_doramas_by_rating(update, context)


# ======== Обработчик текстовых сообщений ==========
async def handle_text_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.info("Обработчик текстовых сообщений вызван.")
    
    await log_user_activity(update, context)
    logger.info("Функция log_user_activity вызвана.")  # Добавьте этот лог

    if update.effective_message.text:
        await update.message.reply_text(DEFAULT_MESSAGE, reply_markup=back_button)
        logger.info(f"Пользователь {update.effective_user.id} ввел текстовое сообщение.")
        
        

    
# ======== Общий обработчик Callback-запросов ==========
async def handle_callback_query(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    if query.data:
        user = update.effective_user
        if user:
            user_id = user.id
            button_text = query.data
            
            # Логируем данные перед сохранением
            logger.info(f"Попытка сохранения callback_data: {button_text} для пользователя {user_id}")
            
        # Обработка callback-запросов
        if query.data == "list_by_letter":
            await log_user_activity(update, context)  # Логируем действие пользователя
            await choose_language(update, context)

        elif query.data in ["language_ru", "language_en"]:
            await log_user_activity(update, context)  # Логируем действие пользователя
            context.user_data["language"] = query.data.split("_")[1]
            await list_doramas_by_letter(update, context)

        elif query.data == "list_doramas_by_letter":
            await log_user_activity(update, context)  # Логируем действие пользователя
            await list_doramas_by_letter(update, context)

        elif query.data.startswith("filter_by_letter_"):
            await log_user_activity(update, context)  # Логируем действие пользователя
            context.user_data["selected_letter"] = query.data.split("_")[-1]
            await show_doramas_by_letter(update, context)

        elif query.data.startswith("filter_by_rating_"):
            await log_user_activity(update, context)  # Логируем действие пользователя
            await show_doramas_by_rating(update, context)

        elif query.data in ["rating_doramas_page_back", "rating_doramas_page_next"]:
            await log_user_activity(update, context)  # Логируем действие пользователя
            await handle_rating_doramas_pagination(update, context)

        elif query.data.startswith("show_dorama:"):
            await log_user_activity(update, context)  # Логируем действие пользователя
            dorama_id = query.data.split(":")[1]
            await show_dorama_details(update, context, dorama_id)

        elif query.data == "return_to_main_menu":
            await log_user_activity(update, context)  # Логируем действие пользователя
            await list_doramas_menu(update, context)

        elif query.data in ["letter_doramas_page_back", "letter_doramas_page_next"]:
            await log_user_activity(update, context)  # Логируем действие пользователя
            await handle_letter_doramas_pagination(update, context)

        elif query.data.startswith("select_country:"):
            await log_user_activity(update, context)  # Логируем действие пользователя
            country = query.data.split(":")[1]
            logger.info(f"Выбрана страна: {country}")
            await show_doramas_by_country(update, context, country)

        elif query.data == "search_by_actor":
            logger.info("Нажата кнопка 'актер'")
            await log_user_activity(update, context)  # Логируем действие пользователя
            await query.message.reply_text("Пожалуйста, введите фамилию или имя актера:")

        elif query.data == "search_by_actress":
            logger.info("Нажата кнопка 'актриса'")
            await log_user_activity(update, context)  # Логируем действие пользователя
            await query.message.reply_text("Пожалуйста, введите фамилию или имя актрисы:")

        elif query.data == "search_by_director":
            logger.info("Нажата кнопка 'режиссер'")
            await log_user_activity(update, context)  # Логируем действие пользователя
            await query.message.reply_text("Пожалуйста, введите фамилию или имя режиссёра:")

        elif query.data == "back":
            logger.info("Нажата кнопка назад")
            await log_user_activity(update, context)  # Логируем действие пользователя
            await update.callback_query.answer()
            await update.callback_query.edit_message_text("Вы вернулись назад.")

        else:  
            await log_user_activity(update, context)  # Логируем действие пользователя
            await query.edit_message_text(
                text="Неизвестная кнопка. Пожалуйста, попробуйте еще раз.", 
                reply_markup=query.message.reply_markup
            )

    else:  # Если callback_data пустой
        logger.warning("⚠️ callback_data не передан!")
        await query.message.reply_text("Произошла ошибка. Попробуйте снова.")

# СПИСОК ВСЕХ ДОРАМ ПО ГОДУ
# ======== Функция для вывода списка ========== 
async def list_years(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    try:
        # Извлекаем данные из callback_data
        data_parts = query.data.split(":")
        if len(data_parts) > 1:
            page = int(data_parts[-1])  # Страница
        else:
            page = 0  # Страница по умолчанию

        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("SELECT COUNT(DISTINCT year) FROM doramas") as cursor:
                total_years = (await cursor.fetchone())[0]
                
            if total_years is None:
                total_years = 0  # Если по какой-то причине данных нет

            # Подсчёт страниц
            max_pages = (total_years + PAGE_SIZE - 1) // PAGE_SIZE
            page = min(page, max_pages - 1)  # Убедитесь, что страница не выходит за пределы
            offset = page * PAGE_SIZE


            async with db.execute(
                "SELECT DISTINCT year FROM doramas ORDER BY year DESC LIMIT ? OFFSET ?",
                (PAGE_SIZE, offset)
            ) as cursor:
                years = await cursor.fetchall()

        if not years:
            try:

                await query.edit_message_text("🚫 На этой странице нет годов.", reply_markup=back_button)

            except telegram.error.BadRequest as e:

                if 'Message is not modified' not in str(e):

                    raise
            return

        # Создаем кнопки для каждого года
        keyboard = []
        for year in years:
            keyboard.append([InlineKeyboardButton(str(year[0]), callback_data=f"list_doramas_year_{year[0]}")])

        # Кнопки пагинации
        pagination_keyboard = create_pagination_buttons("list_years", page, total_years)

        # Добавляем кнопки пагинации
        keyboard.extend(pagination_keyboard.inline_keyboard)

        try:
            reply_markup = InlineKeyboardMarkup(keyboard)
            try:

                await query.edit_message_text("📅 *Выберите год:*", reply_markup=reply_markup, parse_mode="Markdown")

            except telegram.error.BadRequest as e:

                if 'Message is not modified' not in str(e):

                    raise
        except BadRequest as e:
            logger.error(f"Ошибка при редактировании сообщения: {e}")
            if "message is not modified" in str(e):
                logger.info("Сообщение не было изменено.")
            else:
                # Логируем дополнительные детали ошибки
                logger.exception("Детали ошибки: ", exc_info=True)
    
    except Exception as e:
        logger.error(f"Неизвестная ошибка: {e}")
        try:

            await query.edit_message_text("🚨 Произошла ошибка при загрузке годов. Попробуйте снова позже.")

        except telegram.error.BadRequest as e:

            if 'Message is not modified' not in str(e):

                raise
            
# ======== Функция для отображения списка дорам по году ========== 
async def list_doramas_by_year(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    try:
        # Извлекаем данные из callback_data
        data_parts = query.data.split("_")
        if len(data_parts) < 4:
            try:

                await query.edit_message_text("⚠️ Ошибка: Неверный формат данных.", reply_markup=back_button)

            except telegram.error.BadRequest as e:

                if 'Message is not modified' not in str(e):

                    raise
            return
        
        year = data_parts[3]  # Год — четвёртый элемент в data_parts
        page = int(data_parts[4]) if len(data_parts) > 4 else 0  # Страница — пятый элемент (если есть)

        if not year.isdigit():
            try:

                await query.edit_message_text("⚠️ Ошибка: Год должен быть числом.", reply_markup=back_button)

            except telegram.error.BadRequest as e:

                if 'Message is not modified' not in str(e):

                    raise
            return
        
        year = int(year)  # Преобразуем в число
        
        # Подключаемся к базе данных
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("SELECT COUNT(*) FROM doramas WHERE year = ?", (year,)) as cursor:
                total_doramas = (await cursor.fetchone())[0]

            if total_doramas == 0:
                try:

                    await query.edit_message_text(f"🚫 В {year} году дорам нет.", reply_markup=back_button)

                except telegram.error.BadRequest as e:

                    if 'Message is not modified' not in str(e):

                        raise
                return

            # Подсчёт страниц
            total_pages = (total_doramas // PAGE_SIZE) + (1 if total_doramas % PAGE_SIZE else 0)
            offset = page * PAGE_SIZE

            # Запрос списка дорам с пагинацией
            async with db.execute(
                "SELECT id, title_ru, country FROM doramas WHERE year = ? ORDER BY title_ru LIMIT ? OFFSET ?",
                (year, PAGE_SIZE, offset)
            ) as cursor:
                doramas = await cursor.fetchall()

        # Формируем заголовок
        response = f"📅 *Дорамы {year} года ({total_doramas} всего):*\n\n"
        response += f"📄 Страница {page + 1} из {total_pages}\n\n"

        # Кнопки навигации по страницам
        pagination_keyboard = create_pagination_buttons(None, page, total_doramas, year=year)
        
        # Создаём кнопки с дорамами
        keyboard = []
        for dorama_id, title, country in doramas:
            country_flag = COUNTRY_FLAGS.get(country, "🌍")  # Получаем флаг страны или ставим 🌍
            title = escape_markdown(prevent_hashtag_linking(title))  # Экранируем спецсимволы
            keyboard.append([InlineKeyboardButton(f"{title} {country_flag} ", callback_data=f"show_dorama:{dorama_id}")])

        # Добавляем кнопки пагинации
        keyboard.extend(pagination_keyboard.inline_keyboard)
            
        # Кнопки возврата
        keyboard.append([InlineKeyboardButton("📅 Список по годам", callback_data="list_years")])
        keyboard.append([InlineKeyboardButton("🌸 В главное меню", callback_data="return_to_main_menu")])

        reply_markup = InlineKeyboardMarkup(keyboard)
        try:

            await query.edit_message_text(response, reply_markup=reply_markup, parse_mode="Markdown")

        except telegram.error.BadRequest as e:

            if 'Message is not modified' not in str(e):

                raise

    except Exception as e:
        logger.error(f"Ошибка при получении списка дорам за {year}: {e}")
        try:

            await query.edit_message_text("⚠️ Ошибка при загрузке списка дорам.", reply_markup=back_button)

        except telegram.error.BadRequest as e:

            if 'Message is not modified' not in str(e):

                raise

              
# УНИВАРСАЛЬНЫЙ ХЭНДЛЕР ПАГИНАЦИИ        
# ========  Универсальная функция для создания кнопок пагинации  ==========
def create_pagination_buttons(prefix, page, total_results, year=None):
    max_pages = (total_results + PAGE_SIZE - 1) // PAGE_SIZE  # Количество страниц

    keyboard = []
    row = []   
    
    # Кнопка "Предыдущая"
    if page > 0:
        if year:
            row.append(InlineKeyboardButton("⬅️ Назад", callback_data=f"list_doramas_year_{year}_{page - 1}"))            
        elif prefix == "list_years":
            row.append(InlineKeyboardButton("⬅️ Назад", callback_data=f"{prefix}:{page - 1}"))
        else:
            row.append(InlineKeyboardButton("⬅️ Назад", callback_data=f"{prefix}:{page - 1}"))
    
    # Кнопка "Следующая"
    if (page + 1) * PAGE_SIZE < total_results:
        next_page = page + 1
        if year:
            row.append(InlineKeyboardButton("➡️ Вперёд", callback_data=f"list_doramas_year_{year}_{next_page}"))
        elif prefix == "list_years":
            row.append(InlineKeyboardButton("➡️ Вперёд", callback_data=f"{prefix}:{next_page}"))
        else:
            row.append(InlineKeyboardButton("➡️ Вперёд", callback_data=f"{prefix}:{next_page}"))

    if row:
        keyboard.append(row)
        
    # Кнопки для дополнительных действий
    if prefix == "title":
        keyboard.append([InlineKeyboardButton("🔍 Новый поиск", callback_data="search_by_title")])
        keyboard.append([InlineKeyboardButton("🌸 Главное меню", callback_data="return_to_main_menu")])
        
    elif prefix == "country":
        keyboard.append([InlineKeyboardButton("🌍 Вернуться в список стран", callback_data="search_by_country")])
        
    elif prefix == "list_years":
        keyboard.append([InlineKeyboardButton("🌸 Главное меню", callback_data="return_to_main_menu")])

    return InlineKeyboardMarkup(keyboard)

        
# ========  Общий хэндлер пагинации ==========
async def handle_pagination(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()

    try:
        # Разбираем callback_data
        logger.info(f"Received callback_data: {query.data}")
        data_parts = query.data.split(":")
        
        if len(data_parts) < 2:
            try:

                await query.edit_message_text("⚠️ Ошибка: Неверный формат данных.", reply_markup=back_button)

            except telegram.error.BadRequest as e:

                if 'Message is not modified' not in str(e):

                    raise
            return ConversationHandler.END

        prefix, page_str = data_parts[0], data_parts[-1]  # Берем последние данные как страницу
        page = int(page_str)
        logger.info(f"Получен запрос с callback_data: {query.data} (prefix: {prefix}, page: {page})")  # Логируем запрос

            # Обработка пагинации в зависимости от префикса
        if prefix == "country":
            country = context.user_data['country']
            total_results_country = context.user_data.get('total_results_country', 0)
            logger.info(f"Количество результатов для страны {country}: {total_results_country}")
            return await fetch_doramas_page(update, context, country, page)
            
        elif prefix == "title":
            normalized_title = context.user_data.get('normalized_title')  
            total_results_title = context.user_data.get('total_results_title', 0)
            logger.info(f"Количество результатов для {normalized_title}: {total_results_title}")
            return await fetch_doramas_by_title_page(update, context, normalized_title, page)
            
        elif prefix == "actor":
            actor_name = context.user_data.get('search_actor_name', '')
            logger.info(f"Пагинация по актёру: {actor_name}, страница {page}")
            if not actor_name:
                logger.error("❌ Ошибка: `search_actor_name` отсутствует в `context.user_data`!")
                try:

                    await query.edit_message_text("⚠️ Ошибка: данные поиска потеряны. Попробуйте заново.", reply_markup=back_button)

                except telegram.error.BadRequest as e:

                    if 'Message is not modified' not in str(e):

                        raise
                return ConversationHandler.END
            context.user_data['actor_page'] = page
            return await show_actors_list(update, context, actor_name, page)

        elif prefix == "actress":
            actress_name = context.user_data.get('search_actress_name', '')
            logger.info(f"Пагинация по актёру: {actress_name}, страница {page}")
            if not actress_name:
                logger.error("❌ Ошибка: `search_actress_name` отсутствует в `context.user_data`!")
                try:

                    await query.edit_message_text("⚠️ Ошибка: данные поиска потеряны. Попробуйте заново.", reply_markup=back_button)

                except telegram.error.BadRequest as e:

                    if 'Message is not modified' not in str(e):

                        raise
                return ConversationHandler.END
            context.user_data['actress_page'] = page            
            return await show_actresses_list(update, context, actress_name, page)

        elif prefix == "director":
            director_name = context.user_data.get('search_director_name', '')
            logger.info(f"Пагинация по актёру: {director_name}, страница {page}")
            if not director_name:
                logger.error("❌ Ошибка: `search_director_name` отсутствует в `context.user_data`!")
                try:

                    await query.edit_message_text("⚠️ Ошибка: данные поиска потеряны. Попробуйте заново.", reply_markup=back_button)

                except telegram.error.BadRequest as e:

                    if 'Message is not modified' not in str(e):

                        raise
                return ConversationHandler.END
            context.user_data['director_page'] = page            
            return await show_directors_list(update, context, director_name, page)
            
      # Дополнительные обработки пагинации для других вариантов
        elif "list_doramas_year" in query.data:
            data_parts = query.data.split("_")
            if len(data_parts) < 5 or not data_parts[4].isdigit():
                try:

                    await query.edit_message_text("⚠️ Ошибка: неверный формат данных.", reply_markup=back_button)

                except telegram.error.BadRequest as e:

                    if 'Message is not modified' not in str(e):

                        raise
                return ConversationHandler.END

            year = data_parts[3]  # Год
            page = int(data_parts[4])  # Страница

            context.user_data['current_page'] = page

            await list_doramas_by_year(update, context)
            return

        elif query.data == "list_years" or "list_years:" in query.data:
            data_parts = query.data.split(":")
            if len(data_parts) > 1:
                page = int(data_parts[-1])  # Страница
            else:
                page = 0  # Страница по умолчанию

            await list_years(update, context)
            return
        
                # Обработка пагинации в зависимости от префикса
        if prefix == "list_by_letter":
            context.user_data['page'] = page
            await list_doramas_by_letter(update, context)
            return

        elif prefix == "list_by_rating":
            context.user_data['rating_page'] = page
            await list_doramas_by_rating(update, context)
            return

    except (ValueError, IndexError) as ve:
        logger.error(f"Ошибка обработки callback: {ve}")
        try:

            await query.edit_message_text(f"⚠️ Произошла ошибка: {ve}", reply_markup=back_button)

        except telegram.error.BadRequest as e:

            if 'Message is not modified' not in str(e):

                raise
        return ConversationHandler.END

# ========   Обрабатывает текстовые сообщения вне контекста команд  ==========
async def handle_button_press(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("🧐 Пожалуйста, используйте кнопки меню.")  

# ======== Обработка неизвестных команд ==========
async def unknown(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Отправка сообщения пользователю
    await update.effective_message.reply_text("😔 Пожалуйста, используйте кнопки для навигации. Ввод команд словами не поддерживается.", reply_markup=back_button)
    logger.info(f"Пользователь {update.effective_user.id} ввел неизвестную команду.")

# ======== Хэндлер ошибок ==========
async def error_handler(update: object | None, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update is not None and context.error is not None:
        logger.error(msg="Exception while handling an update:", exc_info=context.error)

        # Traceback информация
        tb_list = traceback.format_exception(None, context.error, context.error.__traceback__)
        tb_string = ''.join(tb_list)

        # Сообщение об ошибке для администраторов
        error_message = (
            f"⚠️ Произошла ошибка при обработке запроса.\n"
            f"<pre>update = {str(update)}</pre>\n"
            f"<pre>context.chat_data = {str(context.chat_data)}</pre>\n"
            f"<pre>context.user_data = {str(context.user_data)}</pre>\n"
            f"<pre>{tb_string}</pre>"
        )
        
        keyboard = [
            [
                InlineKeyboardButton("🌸 Вернуться", callback_data="return_to_main_menu")
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        for admin_id in ADMINS:
            try:
                await context.bot.send_message(chat_id=admin_id, text=error_message)
                logger.info(f"Сообщение об ошибке отправлено администратору с ID {admin_id}")
            except Exception as e:
                logger.warning(f"Не удалось отправить сообщение об ошибке администратору с ID {admin_id}: {e}")


# ======== Инициализация БД ==========
async def init_user_db():
    async with aiosqlite.connect(DB_PATH_2) as db:
        await db.executescript('''
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_seen TEXT DEFAULT CURRENT_TIMESTAMP,
                last_seen TEXT DEFAULT NULL,
                last_message TEXT DEFAULT NULL,
                last_callback_data TEXT DEFAULT NULL
            );
            CREATE TABLE IF NOT EXISTS user_actions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                action_type TEXT, 
                action_data TEXT, 
                message_id INTEGER, 
                callback_query_id TEXT,
                timestamp TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS idx_user_actions_user_id ON user_actions (user_id);
        ''')
        await db.commit()
        logger.info("✅ База данных пользователей успешно инициализирована.")

# ======== Логирование действий пользователей ==========
async def log_user_activity(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Логирует активность пользователя."""
    user = update.effective_user
    if not user:
        logger.warning("⚠️ Нет информации о пользователе в update.")
        return
    
    user_id = user.id
    username = user.username or "Без имени"
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    action_type = None
    action_data = None
    message_id = None
    callback_query_id = None

    if update.message:
        message_id = update.message.message_id
        if update.message.text:
            if update.message.text.startswith('/'):
                action_type = "command"
                action_data = update.message.text
            else:
                action_type = "message"
                action_data = update.message.text
    elif update.callback_query:
        callback_query_id = update.callback_query.id
        action_type = "callback"
        action_data = update.callback_query.data

    if not action_type:
        return
    
    try:
        async with aiosqlite.connect(DB_PATH_2) as db:
            # Запись действия в таблицу user_actions
            await db.execute("""
                INSERT INTO user_actions (user_id, action_type, action_data, message_id, callback_query_id, timestamp)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (user_id, action_type, action_data, message_id, callback_query_id, now))
            
            # Обновление данных в таблице users
            await db.execute("""
                INSERT INTO users (user_id, username, first_seen, last_seen, last_message, last_callback_data)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(user_id) DO UPDATE SET
                    username = excluded.username,
                    last_seen = excluded.last_seen,
                    last_message = excluded.last_message,
                    last_callback_data = excluded.last_callback_data;
            """, (user_id, username, now, now,
                  action_data if action_type == "message" else None,
                  action_data if action_type == "callback" else None))
            
            await db.commit()
            
        logger.info(f"💾 Данные пользователя {user_id} обновлены! Тип действия: {action_type}, Данные: {action_data}")

    except aiosqlite.Error as e:
        logger.error(f"⚠️ Ошибка при логировании активности пользователя {user_id}: {e}", exc_info=True)

async def update_last_actions(db: aiosqlite.Connection, user_id: int, action_type: str, action_data: str, message_id: int, callback_query_id: str, timestamp: str) -> None:
    """Записывает действие пользователя в таблицу user_actions."""
    try:
        sql_query = """
            INSERT INTO user_actions (user_id, action_type, action_data, message_id, callback_query_id, timestamp)
            VALUES (?, ?, ?, ?, ?, ?)
        """
        log_data = (user_id, action_type, action_data, message_id, callback_query_id, timestamp)
        logger.info(f"SQL-запрос: {sql_query}, Данные: {log_data}")  # Добавьте этот лог
        await db.execute(sql_query, log_data)
        logger.info(f"✅ {timestamp} Действие для пользователя {user_id} записано: {action_type} - {action_data}")
    except aiosqlite.Error as e:
        logger.error(f"⚠️ {timestamp} Ошибка при логировании действия для пользователя {user_id}: {e}", exc_info=True)

# ======== Получение списка пользователей ==========
async def get_users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        async with aiosqlite.connect(DB_PATH_2) as db:
            async with db.execute("""
                SELECT u.user_id, u.username, u.first_seen, u.last_seen
                FROM users u
            """) as cursor:
                users = await cursor.fetchall()

        if not users:
            await update.message.reply_text("📭 Нет зарегистрированных пользователей.")
            return

        user_info = "\n".join([
            f"🆔 {u[0]} | 👤 {u[1]} | 🕓 {u[2]} | ⏳ {u[3]}"
            for u in users
        ])
        
        safe_user_info = escape_markdown_v2(user_info)
        await update.message.reply_text(f"👥 *Список пользователей:*\n{safe_user_info}", parse_mode='MarkdownV2')

    except aiosqlite.Error as e:
        logger.error(f"⚠️ Ошибка при получении списка пользователей: {e}", exc_info=True)

# ======== Получение истории действий ==========
async def get_user_actions(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not user:
        return
    
    user_id = user.id
    try:
        async with aiosqlite.connect(DB_PATH_2) as db:
            sql_query = "SELECT action_type, action_data, timestamp FROM user_actions WHERE user_id = ? ORDER BY timestamp DESC LIMIT 10"
            logger.info(f"Выполняемый SQL-запрос: {sql_query}, user_id: {user_id}")  # Добавьте этот лог
            async with db.execute(sql_query, (user_id,)) as cursor:
                actions = await cursor.fetchall()
        
        logger.info(f"Результаты запроса: {actions}")  # Добавьте этот лог

        if not actions:
            await send_reply(update, "📭 Нет последних действий.")
            return
        
        actions_text = "\n".join([f"{a[2]} - {a[0]}: {a[1]}" for a in actions])
        await send_reply(update, f"📝 Последние действия:\n{actions_text}")

    except aiosqlite.Error as e:
        logger.error(f"⚠️ Ошибка при получении истории действий пользователя {user_id}: {e}", exc_info=True)


    
# Хэндлер для получения информации о дораме по ID
get_dorama_handler = ConversationHandler(
    entry_points=[CallbackQueryHandler(get_dorama_details, pattern="^get_dorama_details$")],
    states={
        GETTING_DORAMA_ID: [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_dorama_id)],
    },
    fallbacks=[
        CallbackQueryHandler(cancel, pattern="^cancel$"),
        CallbackQueryHandler(handle_back_to_menu, pattern="^return_to_main_menu$"),
    ],
    name="get_dorama",
)        
        
# Хэндлер для поиска по стране
search_country_handler = ConversationHandler(
    entry_points=[CallbackQueryHandler(search_by_country, pattern="^search_by_country$")],
    states={
        SEARCH_COUNTRY: [
            CallbackQueryHandler(handle_search_by_country, pattern="^select_country:.*$"),
            CallbackQueryHandler(handle_pagination, pattern="country:"),
            CallbackQueryHandler(handle_pagination, pattern="country_page:") ,
        ],
        HANDLE_PAGINATION: [
            CallbackQueryHandler(search_by_country, pattern="^search_by_country$"),
            CallbackQueryHandler(handle_back_to_menu, pattern="^return_to_main_menu$"),
            CallbackQueryHandler(cancel, pattern="^cancel$"),
            
        ],
    },
    fallbacks=[
        CallbackQueryHandler(cancel, pattern="^cancel$"),
        CallbackQueryHandler(handle_back_to_menu, pattern="^return_to_main_menu$"),
    ],
    name="search_by_country",
)


# Хэндлер для поиска по названию
search_title_handler = ConversationHandler(
    entry_points=[
        CallbackQueryHandler(start_search_by_title, pattern="^search_by_title$"),
        CommandHandler("search_by_title", start_search_by_title),  # Добавьте этот обработчик команды         
    ],
    states={
        SEARCH_TITLE: [
            MessageHandler(filters.TEXT & ~filters.COMMAND, handle_search_by_title),
        ],
        HANDLE_PAGINATION: [
            CallbackQueryHandler(handle_pagination, pattern=r"^title:\d+$"),
            CallbackQueryHandler(start_search_by_title, pattern="^search_by_title$"), 
            CallbackQueryHandler(handle_back_to_menu, pattern="^return_to_main_menu$"),
        ],
    },
    fallbacks=[
        CallbackQueryHandler(cancel, pattern="^cancel$"),
        CallbackQueryHandler(handle_back_to_menu, pattern="^return_to_main_menu$"),
    ],
    name="search_by_title",
)

# Хэндлер поиска по актеру
search_actor_handler = ConversationHandler(
    entry_points=[CallbackQueryHandler(search_by_actor, pattern="^search_by_actor$")],
    states={
        SEARCH_ACTOR: [
            MessageHandler(filters.TEXT & ~filters.COMMAND, handle_search_by_actor),
            MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_message),
            CallbackQueryHandler(handle_pagination, pattern="^actor:\d+$")
        ],
        CHOOSE_ACTOR: [CallbackQueryHandler(handle_choose_actor, pattern="^choose_actor:.*$")],
    },
    fallbacks=[
        CallbackQueryHandler(handle_back_to_menu, pattern="^return_to_main_menu$"), 
        CallbackQueryHandler(cancel, pattern="^cancel$")
    ],
    name="search_by_actor",
)


# Хэндлер поиска по актрисе
search_actress_handler = ConversationHandler(
    entry_points=[CallbackQueryHandler(search_by_actress, pattern="^search_by_actress$")],
    states={
        SEARCH_ACTRESS: [
            MessageHandler(filters.TEXT & ~filters.COMMAND, handle_search_by_actress),
            MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_message),
            CallbackQueryHandler(handle_pagination, pattern="^actress:\d+$")
        ],
        CHOOSE_ACTRESS: [CallbackQueryHandler(handle_choose_actress, pattern="^choose_actress:.*$")],
    },
    fallbacks=[
        CallbackQueryHandler(handle_back_to_menu, pattern="^return_to_main_menu$"), 
        CallbackQueryHandler(cancel, pattern="^cancel$"),
    ],
    name="search_by_actress",
)

# Хэндлер поиска по режиссеру
search_director_handler = ConversationHandler(
    entry_points=[CallbackQueryHandler(search_by_director, pattern="^search_by_director$")],
    states={
        SEARCH_DIRECTOR: [
            MessageHandler(filters.TEXT & ~filters.COMMAND, handle_search_by_director),
            MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_message),
            CallbackQueryHandler(handle_pagination, pattern="^director:\d+$")
        ],
        CHOOSE_DIRECTOR: [CallbackQueryHandler(handle_choose_director, pattern=r"^choose_director:.*$")],
    },
    fallbacks=[
        CallbackQueryHandler(handle_back_to_menu, pattern="^return_to_main_menu$"), 
        CallbackQueryHandler(cancel, pattern="^cancel$"),
    ],
    name="search_by_director",
)

# Хэндлер для удаления дорамы
delete_dorama_handler = ConversationHandler(
    entry_points=[CallbackQueryHandler(delete_dorama, pattern="^delete_dorama$")],
    states={
        DELETING_DORAMA: [
            MessageHandler(filters.TEXT & ~filters.COMMAND, handle_delete_dorama),
            CallbackQueryHandler(confirm_delete_dorama, pattern="^confirm_delete$"),
            CallbackQueryHandler(cancel, pattern="^cancel$"),  
        ],
    },
    fallbacks=[CallbackQueryHandler(cancel, pattern="^cancel$")],
    name="delete_dorama",
)
  
# Хэндлер добавления дорамы
add_dorama_handler = ConversationHandler(
    entry_points=[CallbackQueryHandler(add_dorama, pattern="^add_dorama$")],
    states={
        ADDING_TITLE_RU: [
            MessageHandler(filters.TEXT & ~filters.COMMAND, receive_title_ru),
            CallbackQueryHandler(cancel, pattern="^cancel$")  
        ],  
        ADDING_TITLE_EN: [
            MessageHandler(filters.TEXT & ~filters.COMMAND, receive_title_en),
            CallbackQueryHandler(cancel, pattern="^cancel$")  
        ],  
        ADDING_COUNTRY: [
            CallbackQueryHandler(receive_country_for_add, pattern="^select_country:"),
            CallbackQueryHandler(cancel, pattern="^cancel$")
        ], 
        ADDING_YEAR: [
            MessageHandler(filters.TEXT & ~filters.COMMAND, receive_year),
            CallbackQueryHandler(cancel, pattern="^cancel$")  
        ],  
        ADDING_DIRECTOR: [
            MessageHandler(filters.TEXT & ~filters.COMMAND, receive_director),
            CallbackQueryHandler(cancel, pattern="^cancel$")  
        ],  
        ADDING_LEAD_ACTRESS: [
            MessageHandler(filters.TEXT & ~filters.COMMAND, receive_lead_actress),
            CallbackQueryHandler(cancel, pattern="^cancel$")  
        ],  
        ADDING_LEAD_ACTOR: [
            MessageHandler(filters.TEXT & ~filters.COMMAND, receive_lead_actor),
            CallbackQueryHandler(cancel, pattern="^cancel$")  
        ],  
        ADDING_PERSONAL_RATING: [
            CallbackQueryHandler(receive_personal_rating, pattern="^rating:"),
            CallbackQueryHandler(cancel, pattern="^cancel$")  
        ],
        ADDING_COMMENT: [
            MessageHandler(filters.TEXT & ~filters.COMMAND, receive_comment),
            CallbackQueryHandler(cancel, pattern="^cancel$")  
        ],  
        ADDING_PLOT: [
            MessageHandler(filters.TEXT & ~filters.COMMAND, receive_plot),
            CallbackQueryHandler(cancel, pattern="^cancel$")  
        ],  
        ADDING_POSTER_URL: [
            MessageHandler(filters.TEXT & ~filters.COMMAND, receive_poster_url)
        ],
    },
    fallbacks=[CallbackQueryHandler(cancel, pattern="^cancel$")],
    name="add_dorama",
)

def stop_application():
    print("Получен сигнал для остановки приложения.")
    application.stop()
    print("Бот остановлен.")

# Обработка сигналов
signal.signal(signal.SIGINT, lambda sig, frame: stop_application())
signal.signal(signal.SIGTERM, lambda sig, frame: stop_application())

# ======== Функция для регистрации обработчиков ==========
def setup_handlers(application: Application):
    
    # Установим обработчики команд
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("restart", restart))
    application.add_handler(CommandHandler("menu", show_menu))
    application.add_handler(CommandHandler("search_by_title", start_search_by_title))
    application.add_handler(CommandHandler("users", get_users))
    application.add_handler(CommandHandler("get_user_actions", get_user_actions))
    application.add_handler(CallbackQueryHandler(show_menu, pattern="^show_menu$"))
    application.add_handler(CallbackQueryHandler(handle_back_to_menu, pattern="^return_to_main_menu$"))

    # Установим обработчики для добавления, удаления и получения информации о дорамах
    application.add_handler(get_dorama_handler)  # Хэндлер для получения информации о дораме по ID
    application.add_handler(add_dorama_handler)  # Хэндлер для добавления дорамы
    application.add_handler(delete_dorama_handler)  # Хэндлер для удаления дорамы

    # Установим обработчики для поиска
    application.add_handler(search_country_handler)  # Хэндлер для поиска по стране
    application.add_handler(search_title_handler)  # Хэндлер для поиска по названию
    application.add_handler(search_actor_handler)  # Хэндлер для поиска по актеру
    application.add_handler(search_actress_handler)  # Хэндлер для поиска по актрисе
    application.add_handler(search_director_handler)  # Хэндлер для поиска по режиссеру

    # Установим обработчики для списков дорам и фильтров
    application.add_handler(CallbackQueryHandler(list_doramas_menu, pattern="^list_doramas$"))
    application.add_handler(CallbackQueryHandler(handle_language_choice, pattern="^language_"))
    application.add_handler(CallbackQueryHandler(choose_language, pattern="^language_"))
    application.add_handler(CallbackQueryHandler(list_doramas_menu, pattern="^list_doramas_menu$"))
    application.add_handler(CallbackQueryHandler(choose_language, pattern="list_by_letter"))
    application.add_handler(CallbackQueryHandler(list_doramas_by_letter, pattern="^list_doramas_by_letter"))
    application.add_handler(CallbackQueryHandler(show_doramas_by_letter, pattern="^filter_by_letter_"))
    application.add_handler(CallbackQueryHandler(list_doramas_by_rating, pattern="^list_doramas_by_rating"))
    application.add_handler(CallbackQueryHandler(handle_letter_doramas_pagination, pattern="^letter_doramas_page_"))
    application.add_handler(CallbackQueryHandler(list_years, pattern="^list_years:[0-9]+$"))
    application.add_handler(CallbackQueryHandler(list_years, pattern="^list_years$"))
    application.add_handler(CallbackQueryHandler(list_doramas_by_year, pattern="^list_doramas_year_[0-9]+_[0-9]+$"))
    application.add_handler(CallbackQueryHandler(list_doramas_by_year, pattern="^list_doramas_year_[0-9]+$"))

    # Установим обработчики для выбора актеров, актрис и режиссеров
    application.add_handler(CallbackQueryHandler(handle_choose_actor, pattern="^choose_actor:.$"))
    application.add_handler(CallbackQueryHandler(handle_choose_actress, pattern="^choose_actress:.$"))
    application.add_handler(CallbackQueryHandler(handle_choose_director, pattern="^choose_director:.*$"))

    # Установим обработчики для поиска актеров, актрис и режиссеров
    application.add_handler(CallbackQueryHandler(search_by_actor, pattern="^search_by_actor$"))
    application.add_handler(CallbackQueryHandler(search_by_actress, pattern="^search_by_actress$"))
    application.add_handler(CallbackQueryHandler(search_by_director, pattern="^search_by_director$"))

    # Установим обработчики для пагинации
    application.add_handler(CallbackQueryHandler(handle_pagination, pattern="^(country|title|actor|actress|director):\d+$"))
    application.add_handler(CallbackQueryHandler(handle_show_dorama, pattern="^show_dorama:"))
    
    #Определяем callback_handler
    application.add_handler(CallbackQueryHandler(handle_callback_query))
    application.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), handle_text_message))
    application.add_handler(MessageHandler(filters.COMMAND, unknown))  
    application.add_error_handler(error_handler)


# --- Главная функция ---
# Основная функция запуска бота
async def main():
    application = Application.builder().token(TOKEN).build()

    # Инициализация базы данных
    try:
        await init_db()
        await init_user_db()
    except Exception as e:
        logger.error(f"Ошибка при инициализации БД: {e}", exc_info=True)
        return  # Прерываем запуск бота, если не удалось инициализировать БД
    
    setup_handlers(application)

    # Запуск бота
    try:
        logger.info("Бот успешно запущен.")
        await application.run_polling()
    except RuntimeError as e:
        if "Cannot close a running event loop" in str(e):
            pass
    

# --- Запуск программы ---
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    nest_asyncio.apply()
    asyncio.run(main())
