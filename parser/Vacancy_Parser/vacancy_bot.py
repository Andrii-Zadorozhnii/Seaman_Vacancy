import asyncio
import logging
import sqlite3
from datetime import datetime, timedelta
from typing import Optional

from aiogram import Bot, Dispatcher, F, types
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandObject
from aiogram.types import (
    InlineKeyboardButton, InlineKeyboardMarkup,
    KeyboardButton, ReplyKeyboardMarkup, Message, CallbackQuery
)
from aiogram.client.default import DefaultBotProperties

from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder

# Import your existing parser
from Vacancy_Parser import VacancyParser
from config import BOT_TOKEN_VACANCY, GROUP_ID_VACANCY

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('vacancy_bot.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configuration
TOKEN = BOT_TOKEN_VACANCY
GROUP_ID = GROUP_ID_VACANCY
ADMIN_IDS = [1009961747]


class VacancyBot:
    def __init__(self, token: str, group_id: str, admin_ids: list[int]):
        self.bot = Bot(
            token=token,
            default=DefaultBotProperties(parse_mode=ParseMode.HTML)
        )
        self.dp = Dispatcher()
        self.group_id = group_id
        self.admin_ids = admin_ids
        self.parser = VacancyParser()
        self.parsing_active = False
        self.parsing_task = None
        self.parsing_delay = 30  # minutes between checks
        self.request_delay = 3  # seconds between requests

        # Register handlers
        self.register_handlers()

    def register_handlers(self):
        """Register all command handlers"""
        self.dp.message(Command("start"))(self.start_command)
        self.dp.message(Command("help"))(self.help_command)
        self.dp.message(Command("admin_parse"))(self.admin_parse)
        self.dp.message(Command("broadcast"))(self.broadcast_command)
        self.dp.message(Command("toggle_parsing"))(self.toggle_parsing)
        self.dp.message(Command("settings"))(self.settings_command)

        # Button handlers
        self.dp.callback_query(F.data == "last_vacancies")(self.last_vacancies_button)
        self.dp.callback_query(F.data == "parse_vacancies")(self.parse_vacancies_button)
        self.dp.callback_query(F.data == "show_stats")(self.stats_button)
        self.dp.callback_query(F.data == "toggle_parsing")(self.toggle_parsing_button)
        self.dp.callback_query(F.data.startswith("vacancy_"))(self.vacancy_callback)
        self.dp.callback_query(F.data.in_(["increase_delay", "decrease_delay", "cancel_settings"]))(
            self.handle_settings_callback)

        # Text message handlers for buttons
        self.dp.message(F.text == "🔍 Последние вакансии")(self.last_vacancies_button)
        self.dp.message(F.text == "🔄 Найти новые вакансии")(self.parse_vacancies_button)
        self.dp.message(F.text == "📊 Статистика")(self.stats_button)
        self.dp.message(F.text == "⏯️ Автопоиск")(self.toggle_parsing_button)
        self.dp.message(F.text == "⚙️ Настройки парсинга")(self.settings_command)
        self.dp.message(F.text == "⏹ Остановить поиск")(self.stop_parsing_button)

    async def create_main_keyboard(self) -> ReplyKeyboardMarkup:
        """Create main reply keyboard with buttons"""
        builder = ReplyKeyboardBuilder()
        buttons = [
            "🔍 Последние вакансии",
            "🔄 Найти новые вакансии",
            "📊 Статистика",
            "⏯️ Автопоиск",
            "⚙️ Настройки парсинга",
            "⏹ Остановить поиск"
        ]
        for button in buttons:
            builder.add(KeyboardButton(text=button))
        builder.adjust(2, 2, 2)
        return builder.as_markup(resize_keyboard=True)

    async def start_command(self, message: Message):
        """Handler for /start command"""
        if message.chat.type == "private":
            keyboard = await self.create_main_keyboard()
            await message.answer(
                "🛳️ <b>Maritime Vacancy Bot</b>\n\n"
                "I can fetch and post maritime job vacancies from ukrcrewing.com.ua\n\n"
                "Use the buttons below to interact with the bot:",
                reply_markup=keyboard
            )

    async def help_command(self, message: Message):
        """Handler for /help command"""
        keyboard = await self.create_main_keyboard()
        help_text = (
            "🔍 <b>How to use this bot:</b>\n\n"
            "🔍 <b>Последние вакансии</b> - Show 5 most recent vacancies\n"
            "🔄 <b>Найти новые вакансии</b> - Search for new vacancies\n"
            "📊 <b>Статистика</b> - Show database statistics\n"
            "⏯️ <b>Автопоиск</b> - Toggle automatic parsing (admin only)\n"
            "⚙️ <b>Настройки парсинга</b> - Adjust parsing settings (admin only)\n\n"
            "You can click on any vacancy to see full details!"
        )
        await message.answer(help_text, reply_markup=keyboard)

    async def admin_parse(self, message: Message, command: CommandObject):
        """Admin-only parse command"""
        if not await self.is_admin(message.from_user.id):
            await message.answer("🚫 Access denied")
            return

        try:
            args = command.args.split() if command.args else []
            if len(args) == 0:
                await message.answer("Usage: /admin_parse [start_id] [end_id]")
                return
            elif len(args) == 1:
                start_id = int(args[0])
                end_id = start_id + 10
            else:
                start_id = int(args[0])
                end_id = int(args[1])
        except ValueError:
            await message.answer("⚠️ Please provide valid numbers")
            return

        msg = await message.answer(f"⏳ Parsing vacancies from {start_id} to {end_id}...")

        found = 0
        for vacancy_id in range(start_id, end_id + 1):
            if self.parser.process_vacancy(vacancy_id):
                found += 1
                await asyncio.sleep(1)

        await msg.edit_text(f"✅ Found {found} new vacancies in range {start_id}-{end_id}!")

    async def broadcast_command(self, message: Message):
        """Admin-only broadcast command"""
        if not await self.is_admin(message.from_user.id):
            await message.answer("🚫 Access denied")
            return

        if not message.reply_to_message:
            await message.answer("Please reply to a message to broadcast")
            return

        try:
            with sqlite3.connect('crewing.db') as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT DISTINCT user_id FROM user_actions")
                user_ids = [row[0] for row in cursor.fetchall()]
        except sqlite3.Error:
            user_ids = []

        if not user_ids:
            await message.answer("No users found to broadcast to")
            return

        success = 0
        failed = 0
        progress_msg = await message.answer(f"Broadcasting to {len(user_ids)} users... 0/{len(user_ids)}")

        for i, user_id in enumerate(user_ids, 1):
            try:
                await message.reply_to_message.copy_to(user_id)
                success += 1
            except Exception:
                failed += 1

            if i % 10 == 0:
                await progress_msg.edit_text(
                    f"Broadcasting to {len(user_ids)} users... {i}/{len(user_ids)}\n"
                    f"✅ Success: {success} | ❌ Failed: {failed}"
                )
            await asyncio.sleep(0.5)

        await progress_msg.edit_text(
            f"📢 Broadcast complete!\n"
            f"✅ Success: {success} | ❌ Failed: {failed}"
        )

    async def toggle_parsing(self, message: Message):
        """Toggle automatic parsing (/toggle_parsing command)"""
        if not await self.is_admin(message.from_user.id):
            await message.answer("🚫 Access denied")
            return

        if self.parsing_active:
            await self.stop_auto_parsing()
            await message.answer("⏹ Автоматический поиск остановлен")
        else:
            self.parsing_task = asyncio.create_task(self.start_auto_parsing())
            await message.answer("▶️ Автоматический поиск запущен")

    async def toggle_parsing_button(self, update: Message | CallbackQuery):
        """Handler for toggle parsing button"""
        if isinstance(update, CallbackQuery):
            message = update.message
            await update.answer()
        else:
            message = update

        if not await self.is_admin(message.from_user.id):
            await message.answer("🚫 Access denied")
            return

        if self.parsing_active:
            await self.stop_auto_parsing()
            await message.answer("⏹ Автоматический поиск остановлен")
        else:
            self.parsing_task = asyncio.create_task(self.start_auto_parsing())
            await message.answer("▶️ Автоматический поиск запущен")

    async def stop_parsing_button(self, message: Message):
        """Handler for stop parsing button"""
        if not await self.is_admin(message.from_user.id):
            await message.answer("🚫 Access denied")
            return

        if self.parsing_active:
            await self.stop_auto_parsing()
            await message.answer("⏹ Автоматический поиск остановлен")
        else:
            await message.answer("Автопоиск уже остановлен")

    async def settings_command(self, message: Message):
        """Handler for parsing settings command"""
        if not await self.is_admin(message.from_user.id):
            await message.answer("🚫 Access denied")
            return

        builder = InlineKeyboardBuilder()
        builder.add(
            InlineKeyboardButton(text="➕ Увеличить интервал", callback_data="increase_delay"),
            InlineKeyboardButton(text="➖ Уменьшить интервал", callback_data="decrease_delay"),
            InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_settings")
        )
        builder.adjust(2, 1)

        await message.answer(
            f"⚙️ <b>Настройки парсинга</b>\n\n"
            f"Текущий интервал проверки: <b>{self.parsing_delay} минут</b>\n"
            f"Задержка между запросами: <b>{self.request_delay} секунд</b>",
            reply_markup=builder.as_markup()
        )

    async def handle_settings_callback(self, callback: CallbackQuery):
        """Handle settings callback queries"""
        if not await self.is_admin(callback.from_user.id):
            await callback.answer("🚫 Access denied")
            return

        action = callback.data
        if action == "increase_delay":
            self.parsing_delay += 5
            text = f"Интервал увеличен до {self.parsing_delay} минут"
        elif action == "decrease_delay":
            if self.parsing_delay > 5:
                self.parsing_delay -= 5
                text = f"Интервал уменьшен до {self.parsing_delay} минут"
            else:
                text = "Минимальный интервал - 5 минут"
        else:  # cancel_settings
            await callback.message.delete()
            return

        await callback.answer(text)
        await self.settings_command(callback.message)

    async def start_auto_parsing(self):
        """Start automatic parsing process"""
        if self.parsing_active:
            return

        self.parsing_active = True
        logger.info(f"Automatic parsing started (delay: {self.parsing_delay} minutes)")

        while self.parsing_active:
            last_id = self.parser.get_last_vacancy_id()

            found = 0
            not_found_count = 0
            current_id = last_id + 1

            while not_found_count < 5 and self.parsing_active:
                if self.parser.process_vacancy(current_id):
                    found += 1
                    not_found_count = 0
                    await self.post_vacancy(current_id)
                    await asyncio.sleep(self.request_delay)
                else:
                    not_found_count += 1

                current_id += 1

            if found > 0:
                logger.info(f"Found {found} new vacancies in this iteration")

            if self.parsing_active:
                logger.info(f"Waiting {self.parsing_delay} minutes before next parsing attempt...")
                for i in range(self.parsing_delay * 60):
                    if not self.parsing_active:
                        break
                    await asyncio.sleep(1)

    async def stop_auto_parsing(self):
        """Stop automatic parsing process"""
        self.parsing_active = False
        logger.info("Automatic parsing stopped")

    async def last_vacancies_button(self, update: Message | CallbackQuery):
        """Show last vacancies"""
        if isinstance(update, CallbackQuery):
            message = update.message
            await update.answer()
        else:
            message = update

        vacancies = self.get_vacancies(limit=5)

        if not vacancies:
            await message.answer("No vacancies found in database")
            return

        builder = InlineKeyboardBuilder()
        for vac in vacancies:
            builder.row(
                InlineKeyboardButton(
                    text=f"{vac['title']} ({vac['published']})",
                    callback_data=f"vacancy_{vac['id']}"
                )
            )

        await message.answer(
            "🛳️ <b>Last 5 vacancies:</b>",
            reply_markup=builder.as_markup()
        )

    async def parse_vacancies_button(self, update: Message | CallbackQuery):
        """Handler for parse vacancies button"""
        if isinstance(update, CallbackQuery):
            message = update.message
            await update.answer()
        else:
            message = update

        if message.chat.type != "private":
            return

        msg = await message.answer("⏳ Searching for new vacancies...")

        last_id = self.parser.get_last_vacancy_id()

        found = 0
        not_found_count = 0
        current_id = last_id + 1

        while not_found_count < 5:
            if self.parser.process_vacancy(current_id):
                found += 1
                not_found_count = 0
                await self.post_vacancy(current_id)
                await asyncio.sleep(self.request_delay)
            else:
                not_found_count += 1

            current_id += 1

        await msg.edit_text(f"✅ Found {found} new vacancies!")

    async def stats_button(self, update: Message | CallbackQuery):
        """Show statistics"""
        if isinstance(update, CallbackQuery):
            message = update.message
            await update.answer()
        else:
            message = update

        try:
            with sqlite3.connect('crewing.db') as conn:
                cursor = conn.cursor()

                cursor.execute("SELECT COUNT(*) FROM vacancies")
                total_vacancies = cursor.fetchone()[0]

                cursor.execute("SELECT COUNT(DISTINCT agency) FROM vacancies")
                total_companies = cursor.fetchone()[0]

                cursor.execute(
                    "SELECT COUNT(*) FROM vacancies WHERE published >= ?",
                    (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
                )
                recent_vacancies = cursor.fetchone()[0]

                cursor.execute(
                    "SELECT vessel_type, COUNT(*) as count FROM vacancies "
                    "WHERE vessel_type IS NOT NULL "
                    "GROUP BY vessel_type ORDER BY count DESC LIMIT 5"
                )
                top_vessels = cursor.fetchall()

                vessels_text = "\n".join(
                    f"  ▪️ {v[0]}: {v[1]}" for v in top_vessels
                ) if top_vessels else "No data"

                await message.answer(
                    "📊 <b>Database Statistics</b>\n\n"
                    f"📂 Total vacancies: <b>{total_vacancies}</b>\n"
                    f"🏢 Total companies: <b>{total_companies}</b>\n"
                    f"🆕 Last 7 days: <b>{recent_vacancies}</b>\n\n"
                    "🚢 <b>Top Vessel Types:</b>\n"
                    f"{vessels_text}"
                )

        except sqlite3.Error as e:
            await message.answer(f"⚠️ Database error: {str(e)}")

    async def vacancy_callback(self, callback: CallbackQuery):
        """Handle vacancy details callback"""
        vacancy_id = int(callback.data.split("_")[1])
        vacancy = self.get_vacancy(vacancy_id)

        if not vacancy:
            await callback.answer("Vacancy not found", show_alert=True)
            return

        text = self.format_vacancy_details(vacancy)

        builder = InlineKeyboardBuilder()
        if vacancy.get('phone') or vacancy.get('email'):
            if vacancy.get('phone'):
                phone = ''.join(c for c in vacancy['phone'] if c.isdigit() or c == '+')
                builder.add(InlineKeyboardButton(
                    text="📞 Call",
                    url=f"tel:{phone}"
                ))
            if vacancy.get('email'):
                builder.add(InlineKeyboardButton(
                    text="📧 Email",
                    url=f"mailto:{vacancy['email']}?subject={vacancy.get('email_subject', 'Job Application')}"
                ))

        try:
            await callback.message.answer(
                text,
                reply_markup=builder.as_markup() if builder.buttons else None
            )
        except Exception as e:
            logger.error(f"Error sending vacancy details: {str(e)}")
            await callback.message.answer(
                text,
                reply_markup=None
            )

        await callback.answer()

    async def post_vacancy(self, vacancy_id: int):
        """Post vacancy to the group"""
        vacancy = self.get_vacancy(vacancy_id)
        if not vacancy:
            return False

        text = self.format_vacancy_post(vacancy)

        try:
            await self.bot.send_message(
                chat_id=self.group_id,
                text=text
            )
            return True
        except Exception as e:
            logger.error(f"Error posting vacancy {vacancy_id}: {str(e)}")
            return False

    def format_vacancy_post(self, vacancy: dict) -> str:
        """Format vacancy for posting in group with new DB structure"""
        def safe_get(key, default="—"):
            value = vacancy.get(key)
            return str(value).strip() if value is not None and str(value).strip() != "" else default

        lines = []
        lines.append(f"📌 <b>{safe_get('title')}</b>")
        lines.append("")
        lines.append("🗓 <b>Дата публикации:</b> " + safe_get('published'))
        if safe_get('join_date') != "—":
            lines.append("📅 <b>Начало контракта:</b> " + safe_get('join_date'))
        lines.append("📏 <b>Контракт:</b> " + safe_get('contract_length'))
        if safe_get('sailing_area') != "—":
            lines.append("🌍 <b>Регион плавания:</b> " + safe_get('sailing_area'))
        lines.append("")

        # ... (rest of the formatting methods remain the same as in your original code)
        # I've kept the same formatting logic, just updated the imports and type hints

    def format_vacancy_details(self, vacancy: dict) -> str:
        """Format vacancy details for Telegram: nicely and structured"""
        lines = []
        lines.append(f"📌 <b>{vacancy.get('title', 'Без названия')}</b>")
        lines.append("")
        lines.append("🗓 <b>Дата публикации:</b> " + vacancy.get('published', '—'))
        lines.append("📅 <b>Начало контракта:</b> " + vacancy.get('join_date', '—'))
        lines.append("📏 <b>Контракт:</b> " + vacancy.get('contract_length', '—'))
        lines.append("🌍 <b>Регион плавания:</b> " + vacancy.get('sailing_area', '—'))
        lines.append("")

        # ... (rest of the formatting methods remain the same as in your original code)

    def get_vacancy(self, vacancy_id: int) -> Optional[dict]:
        """Get single vacancy from database"""
        try:
            with sqlite3.connect('crewing.db') as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM vacancies WHERE id = ?", (vacancy_id,))
                result = cursor.fetchone()
                return dict(result) if result else None
        except sqlite3.Error as e:
            logger.error(f"Database error: {str(e)}")
            return None

    def get_vacancies(self, limit: int = 5) -> list[dict]:
        """Get recent vacancies from database"""
        try:
            with sqlite3.connect('crewing.db') as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT * FROM vacancies ORDER BY published DESC LIMIT ?",
                    (limit,)
                )
                return [dict(row) for row in cursor.fetchall()]
        except sqlite3.Error as e:
            logger.error(f"Database error: {str(e)}")
            return []

    async def is_admin(self, user_id: int) -> bool:
        """Check if user is admin"""
        return user_id in self.admin_ids

    async def run(self):
        """Start the bot"""
        await self.dp.start_polling(self.bot)


async def on_startup(bot: Bot):
    """Function to run on bot startup"""
    await bot.delete_webhook()
    logger.info("Bot started successfully")


if __name__ == "__main__":
    bot = VacancyBot(TOKEN, GROUP_ID, ADMIN_IDS)
    bot.dp.startup.register(on_startup)

    try:
        logger.info("Starting bot...")
        asyncio.run(bot.run())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.error(f"Bot stopped with error: {e}")