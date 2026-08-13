import asyncio
import os
import uuid
import logging
import shutil
import aiosqlite
import tempfile
from contextlib import suppress
from html import escape

# --- НАСТРОЙКА ПУТЕЙ ДЛЯ DOCKER ---
DEFAULT_DATA_DIR = "/app/data"
if os.path.isdir(DEFAULT_DATA_DIR):
    DATA_DIR = DEFAULT_DATA_DIR
else:
    DATA_DIR = os.path.dirname(os.path.abspath(__file__))

DATA_DIR = os.getenv("DATA_DIR", DATA_DIR)
os.makedirs(DATA_DIR, exist_ok=True)

# Временные файлы пишем в /tmp, чтобы не засорять persistent volume
TEMP_DIR = tempfile.gettempdir()

# --- НАСТРОЙКА FFmpeg ---
try:
    import imageio_ffmpeg
    FFMPEG_PATH = imageio_ffmpeg.get_ffmpeg_exe()
    logging.info(f"Используется FFmpeg: {FFMPEG_PATH}")
except Exception as e:
    FFMPEG_PATH = shutil.which("ffmpeg")
    if not FFMPEG_PATH:
        raise RuntimeError(f"FFmpeg не найден! Ошибка: {e}")

from dotenv import load_dotenv
load_dotenv()

from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import Command
from aiogram.types import FSInputFile, LabeledPrice
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.exceptions import (
    TelegramBadRequest,
    TelegramForbiddenError,
    TelegramRetryAfter,
)

# --- ЧТЕНИЕ КОНФИГУРАЦИИ ---
TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))

if not TOKEN or not ADMIN_ID:
    raise RuntimeError("Не заданы переменные окружения BOT_TOKEN или ADMIN_ID. Проверьте файл .env")

logging.basicConfig(level=logging.INFO)

# Снизил до 2 для безопасности на дешевых тарифах
MAX_CONCURRENT_TASKS = 2
semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)

DONATION_AMOUNTS = {5, 10, 50, 100, 500}

bot = Bot(token=TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

# --- FSM СОСТОЯНИЯ ДЛЯ АДМИНА ---
class AdminStates(StatesGroup):
    waiting_for_broadcast = State()

# --- БАЗА ДАННЫХ ---
DB_PATH = os.path.join(DATA_DIR, "bot_database.db")

async def db_init():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                full_name TEXT,
                joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS donations (
                charge_id TEXT PRIMARY KEY,
                user_id INTEGER,
                amount INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await db.commit()

async def add_user(user_id: int, username: str, full_name: str):
    if not user_id: return
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO users (user_id, username, full_name)
            VALUES (?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                username = excluded.username,
                full_name = excluded.full_name
        """, (user_id, username, full_name))
        await db.commit()

async def get_users_count():
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT COUNT(*) FROM users") as cursor:
            return (await cursor.fetchone())[0]

async def get_all_user_ids():
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT user_id FROM users") as cursor:
            return [row[0] for row in await cursor.fetchall()]

async def get_last_users(limit=5):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT user_id, username, full_name, joined_at FROM users ORDER BY joined_at DESC LIMIT ?", (limit,)) as cursor:
            return await cursor.fetchall()

async def get_donations_stats():
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT COUNT(*), SUM(amount) FROM donations") as cursor:
            return await cursor.fetchone()

async def add_donation(charge_id: str, user_id: int, amount: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT OR IGNORE INTO donations (charge_id, user_id, amount) VALUES (?, ?, ?)", (charge_id, user_id, amount))
        await db.commit()

# --- FFmpeg ---
async def run_ffmpeg(cmd: list, timeout=120):
    process = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE
    )
    try:
        _, stderr = await asyncio.wait_for(process.communicate(), timeout=timeout)
    except asyncio.TimeoutError:
        process.kill()
        await process.wait()
        raise RuntimeError("FFmpeg timeout")
    if process.returncode != 0:
        logging.error("FFmpeg error: %s", stderr.decode(errors="replace"))
        raise RuntimeError("FFmpeg processing failed")

# --- ОБРАБОТЧИК ВИДЕО ---
@dp.message(F.video)
async def handle_video(message: types.Message):
    user = message.from_user
    if not user: return
    
    await add_user(user.id, user.username, user.full_name)
    
    if user.id != ADMIN_ID:
        with suppress(Exception): await message.forward(ADMIN_ID)
            
    status_msg = await message.reply("⏳ Ваше видео поставлено в очередь...")
    input_path = os.path.join(TEMP_DIR, f"temp_{uuid.uuid4()}")
    output_path = os.path.join(TEMP_DIR, f"circle_{uuid.uuid4()}.mp4")
    
    try:
        async with semaphore:
            await status_msg.edit_text("⏳ Конвертирую видео в кружок...")
            file = await bot.get_file(message.video.file_id)
            await bot.download_file(file.file_path, input_path)
            
            cmd = [
                FFMPEG_PATH, "-nostdin", "-y", "-i", input_path,
                "-vf", "crop='min(iw,ih)':'min(iw,ih)':'(iw-min(iw,ih))/2':'(ih-min(iw,ih))/2',scale=360:360",
                "-t", "60", "-c:v", "libx264", "-preset", "fast", "-crf", "28",
                "-pix_fmt", "yuv420p", "-movflags", "+faststart", "-c:a", "aac", "-b:a", "128k",
                output_path
            ]
            await run_ffmpeg(cmd)
            
            await bot.send_video_note(
                chat_id=message.chat.id,
                video_note=FSInputFile(output_path),
                reply_to_message_id=message.message_id
            )
            
    except Exception:
        logging.exception("Video processing failed")
        await message.reply("❌ Не удалось обработать видео. Попробуйте другой файл.")
    finally:
        with suppress(FileNotFoundError): os.remove(input_path)
        with suppress(FileNotFoundError): os.remove(output_path)
        with suppress(Exception): await status_msg.delete()

# --- ОБРАБОТЧИК АУДИО ---
@dp.message(F.audio | F.voice)
async def handle_audio(message: types.Message):
    user = message.from_user
    if not user: return
        
    await add_user(user.id, user.username, user.full_name)
    
    if user.id != ADMIN_ID:
        with suppress(Exception): await message.forward(ADMIN_ID)

    status_msg = await message.reply("⏳ Ваше аудио поставлено в очередь...")
    file_id = message.audio.file_id if message.audio else message.voice.file_id
    input_path = os.path.join(TEMP_DIR, f"temp_{uuid.uuid4()}") 
    output_path = os.path.join(TEMP_DIR, f"voice_{uuid.uuid4()}.ogg")
    
    try:
        async with semaphore:
            await status_msg.edit_text("⏳ Конвертирую аудио в голосовое сообщение...")
            file = await bot.get_file(file_id)
            await bot.download_file(file.file_path, input_path)
            
            # Добавлен -vn для удаления видео-дорожек из аудио-файлов (например, из mp4)
            cmd = [
                FFMPEG_PATH, "-nostdin", "-y", "-i", input_path, 
                "-vn", "-c:a", "libopus", "-b:a", "64k", "-ac", "1", 
                output_path
            ]
            await run_ffmpeg(cmd)
            
            await bot.send_voice(
                chat_id=message.chat.id,
                voice=FSInputFile(output_path),
                reply_to_message_id=message.message_id
            )
            
    except Exception:
        logging.exception("Audio processing failed")
        await message.reply("❌ Не удалось обработать аудио. Попробуйте другой файл.")
    finally:
        with suppress(FileNotFoundError): os.remove(input_path)
        with suppress(FileNotFoundError): os.remove(output_path)
        with suppress(Exception): await status_msg.delete()

# --- АДМИН-ПАНЕЛЬ ---
def get_admin_kb():
    builder = InlineKeyboardBuilder()
    builder.button(text="📊 Общая статистика", callback_data="admin_stats")
    builder.button(text="👥 Последние юзеры", callback_data="admin_users")
    builder.button(text="💰 Донаты", callback_data="admin_donations")
    builder.button(text="📢 Рассылка", callback_data="admin_broadcast")
    builder.adjust(2, 2)
    return builder.as_markup()

def get_cancel_kb():
    builder = InlineKeyboardBuilder()
    builder.button(text="❌ Отмена", callback_data="admin_cancel")
    return builder.as_markup()

@dp.message(Command("admin"))
async def cmd_admin(message: types.Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID: return
    await state.clear()
    await message.answer("🔧 <b>Админ-панель</b>\n\nВыберите действие:", reply_markup=get_admin_kb())

# Строгая фильтрация, чтобы не перехватить admin_cancel
@dp.callback_query(F.data.in_({"admin_stats", "admin_users", "admin_donations", "admin_broadcast"}))
async def admin_callbacks(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("У вас нет прав!", show_alert=True)
        return

    action = callback.data
    
    if action == "admin_stats":
        count = await get_users_count()
        await callback.message.edit_text(f"📊 <b>Общая статистика</b>\n\n👤 Всего пользователей: <b>{count}</b>", reply_markup=get_admin_kb())
    
    elif action == "admin_users":
        users = await get_last_users(5)
        text = "👥 <b>Последние 5 пользователей:</b>\n\n"
        for uid, uname, fname, joined in users:
            safe_name = escape(fname or "Без имени")
            safe_username = escape(uname) if uname else "нет username"
            
            text += (
                f'• <a href="tg://user?id={uid}">{safe_name}</a> '
                f"({('@' + safe_username) if uname else safe_username})\n"
                f"   <i>{joined}</i>\n"
            )
        await callback.message.edit_text(text, reply_markup=get_admin_kb(), disable_web_page_preview=True)
    
    elif action == "admin_donations":
        count, total = await get_donations_stats()
        total = total if total else 0
        await callback.message.edit_text(f"💰 <b>Статистика донатов</b>\n\n✅ Успешных оплат: <b>{count}</b>\n⭐ Всего получено звезд: <b>{total}</b>", reply_markup=get_admin_kb())
    
    elif action == "admin_broadcast":
        await state.set_state(AdminStates.waiting_for_broadcast)
        await callback.message.edit_text("📢 <b>Рассылка</b>\n\nОтправьте сообщение (текст, фото, видео), которое нужно разослать ВСЕМ пользователям.\n\nДля отмены нажмите кнопку ниже.", reply_markup=get_cancel_kb())
    
    await callback.answer()

@dp.callback_query(F.data == "admin_cancel", AdminStates.waiting_for_broadcast)
async def admin_cancel(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.edit_text("❌ Рассылка отменена. Возвращаемся в меню.", reply_markup=get_admin_kb())
    await callback.answer()

@dp.message(AdminStates.waiting_for_broadcast)
async def admin_broadcast_send(message: types.Message, state: FSMContext):
    await state.clear()
    
    progress_msg = await message.reply("⏳ Рассылка началась. Ждите...")
    user_ids = await get_all_user_ids()
    
    success = 0
    failed = 0
    
    for uid in user_ids:
        try:
            await message.copy_to(chat_id=uid)
            success += 1
            await asyncio.sleep(0.05)

        except TelegramRetryAfter as e:
            await asyncio.sleep(e.retry_after)
            try:
                await message.copy_to(chat_id=uid)
                success += 1
            except Exception:
                failed += 1

        except TelegramForbiddenError:
            failed += 1

        except Exception:
            failed += 1
            logging.exception("Ошибка рассылки пользователю %s", uid)
            
    await progress_msg.edit_text(f"✅ <b>Рассылка завершена!</b>\n\nУспешно отправлено: {success}\nНе доставлено (заблокировали бота): {failed}", reply_markup=get_admin_kb())

# --- ДОНАТ (TELEGRAM STARS) ---
@dp.message(Command("donate"))
async def cmd_donate(message: types.Message):
    user = message.from_user
    if user: await add_user(user.id, user.username, user.full_name)
        
    builder = InlineKeyboardBuilder()
    builder.button(text="⭐ 5 Звезд", callback_data="donate_5")
    builder.button(text="⭐ 10 Звезд", callback_data="donate_10")
    builder.button(text="⭐ 50 Звезд", callback_data="donate_50")
    builder.button(text="⭐ 100 Звезд", callback_data="donate_100")
    builder.button(text="⭐ 500 Звезд", callback_data="donate_500")
    builder.adjust(2, 2, 1)
    
    await message.answer("🌟 <b>Поддержать разработчика</b> 🌟\n\nЕсли бот оказался полезным, вы можете отблагодарить автора Звездами Telegram!", reply_markup=builder.as_markup())

@dp.callback_query(F.data.startswith("donate_"))
async def process_donate(callback: types.CallbackQuery):
    try:
        amount = int(callback.data.removeprefix("donate_"))
    except (TypeError, ValueError):
        await callback.answer("Некорректная сумма.", show_alert=True)
        return

    if amount not in DONATION_AMOUNTS:
        await callback.answer("Некорректная сумма.", show_alert=True)
        return

    await bot.send_invoice(
        chat_id=callback.message.chat.id,
        title="Поддержка разработчика",
        description=f"Оплата {amount} Telegram Stars в качестве благодарности",
        payload=f"donate:{amount}",
        currency="XTR", 
        prices=[LabeledPrice(label=f"{amount} Звезд", amount=amount)],
        provider_token="" 
    )
    await callback.answer()

@dp.pre_checkout_query()
async def pre_checkout_query(query: types.PreCheckoutQuery):
    try:
        prefix, amount_str = query.invoice_payload.split(":")
        amount = int(amount_str)
        valid = (
            prefix == "donate"
            and amount in DONATION_AMOUNTS
            and query.currency == "XTR"
            and query.total_amount == amount
        )
    except (ValueError, AttributeError):
        valid = False

    if not valid:
        await query.answer(ok=False, error_message="Некорректные параметры платежа.")
        return
    await query.answer(ok=True)

@dp.message(F.successful_payment)
async def successful_payment(message: types.Message):
    payment_info = message.successful_payment
    user = message.from_user
    if user: await add_donation(payment_info.telegram_payment_charge_id, user.id, payment_info.total_amount)
        
    await message.answer(
        f"🎉 <b>Спасибо за вашу поддержку!</b>\n\n"
        f"Вы отправили {payment_info.total_amount} ⭐.\n"
        f"Благодаря вам бот будет работать и развиваться!"
    )

# --- /start ---
@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    user = message.from_user
    if user: await add_user(user.id, user.username, user.full_name)
        
    await message.answer(
        "👋 Привет! Я бот-конвертер.\n\n"
        "📹 Отправь мне <b>видео</b>, и я сделаю из него видео-сообщение (кружок).\n"
        "🎵 Отправь мне <b>аудио</b> или <b>голосовое</b>, и я сделаю из него голосовое сообщение.\n\n"
        "⭐ Поддержать автора: /donate"
    )

# --- ЗАПУСК ---
async def main():
    await db_init()
    try:
        await dp.start_polling(bot)
    finally:
        await bot.session.close()

if __name__ == "__main__":
    asyncio.run(main())
