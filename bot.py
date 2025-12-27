import asyncio
import os
import re
import logging
import hashlib
import subprocess
from datetime import datetime, timedelta
from typing import Optional
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.tl.types import MessageMediaPhoto, MessageMediaDocument, MessageMediaPoll
from aiogram import Bot, Dispatcher, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, FSInputFile, InputMediaPhoto, InputMediaVideo
from aiogram.filters import CommandStart
from openai import AsyncOpenAI
from dotenv import load_dotenv

load_dotenv()

API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
ADMIN_ID = int(os.getenv("ADMIN_ID"))
TARGET_CHANNEL = os.getenv("TARGET_CHANNEL")
SESSION_STRING = os.getenv("SESSION_STRING")

CHANNEL_FOOTER = "\n\n@neurostep_media"

SOURCE_CHANNELS = [
    "media1337",
    "iPumpBrain",
    "TrendWatching24"
]

AD_KEYWORDS = [
    "реклама", "партнёр", "партнер", "промокод", "promo", 
    "скидка", "розыгрыш", "giveaway", "спонсор", "sponsor",
    "utm_", "?ref=", "bit.ly", "clck.ru"
]

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/tmp/bot.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

userbot = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
openai_client = AsyncOpenAI(api_key=OPENAI_API_KEY)

pending_posts = {}
recent_hashes = []
MAX_HASHES = 100
scheduled_posts = {}

REWRITE_PROMPT = """Перепиши новость в стиле:
- Первое предложение = главный факт
- 2-3 коротких абзаца
- Без эмодзи
- Без воды и восторгов  
- Нейтральный взрослый тон
- Ссылки оставляй как есть, без markdown разметки
- Если упоминается Meta/Instagram/WhatsApp — добавь сноску: * — продукт компании Meta, признана экстремистской и запрещена в РФ.

Новость:
{text}"""


def get_text_hash(text: str) -> str:
    clean = re.sub(r'[^\w\s]', '', text.lower())
    clean = ' '.join(clean.split()[:20])
    return hashlib.md5(clean.encode()).hexdigest()


def is_duplicate(text: str) -> bool:
    if not text:
        return False
    h = get_text_hash(text)
    if h in recent_hashes:
        return True
    recent_hashes.append(h)
    if len(recent_hashes) > MAX_HASHES:
        recent_hashes.pop(0)
    return False


def is_ad(text: str) -> bool:
    if not text:
        return False
    text_lower = text.lower()
    return any(kw in text_lower for kw in AD_KEYWORDS)


def markdown_to_html(text: str) -> str:
    text = re.sub(r'\[([^\]]+)\]\(([^)]+)\)', r'<a href="\2">\1</a>', text)
    text = re.sub(r'\*\*([^*]+)\*\*', r'<b>\1</b>', text)
    text = re.sub(r'\*([^*]+)\*', r'<i>\1</i>', text)
    return text


async def compress_video(input_path: str, max_size_mb: int = 45) -> str:
    file_size = os.path.getsize(input_path) / (1024 * 1024)
    if file_size <= max_size_mb:
        return input_path
    
    output_path = input_path.replace('.mp4', '_compressed.mp4')
    try:
        cmd = [
            'ffmpeg', '-i', input_path,
            '-vcodec', 'libx264', '-crf', '28',
            '-preset', 'fast',
            '-acodec', 'aac', '-b:a', '128k',
            '-y', output_path
        ]
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        await process.communicate()
        
        if os.path.exists(output_path):
            os.remove(input_path)
            logger.info(f"Video compressed: {file_size:.1f}MB -> {os.path.getsize(output_path)/(1024*1024):.1f}MB")
            return output_path
    except Exception as e:
        logger.error(f"Video compression failed: {e}")
    
    return input_path


async def rewrite_text(text: str) -> str:
    if not text or len(text) < 20:
        return text
    try:
        response = await openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "user", "content": REWRITE_PROMPT.format(text=text)}
            ],
            max_tokens=1000
        )
        result = response.choices[0].message.content.strip()
        return markdown_to_html(result)
    except Exception as e:
        logger.error(f"OpenAI error: {e}")
        return text


def create_keyboard(post_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Опубликовать", callback_data=f"publish:{post_id}"),
            InlineKeyboardButton(text="❌ Пропустить", callback_data=f"skip:{post_id}")
        ],
        [
            InlineKeyboardButton(text="⏰ Через час", callback_data=f"delay:{post_id}"),
            InlineKeyboardButton(text="✏️ Редактировать", callback_data=f"edit:{post_id}")
        ]
    ])


async def send_error_alert(error_msg: str):
    try:
        await bot.send_message(ADMIN_ID, f"🚨 Ошибка бота:\n\n{error_msg}")
    except:
        pass


async def publish_post(post: dict, post_id: str) -> bool:
    try:
        text_with_footer = post["text"] + CHANNEL_FOOTER if post["text"] else CHANNEL_FOOTER
        
        if post.get("media_group"):
            media_group = []
            for i, media in enumerate(post["media_group"]):
                file = FSInputFile(media["path"])
                caption = text_with_footer if i == 0 else None
                if media["type"] == "photo":
                    media_group.append(InputMediaPhoto(media=file, caption=caption, parse_mode="HTML"))
                elif media["type"] == "video":
                    media_group.append(InputMediaVideo(media=file, caption=caption, parse_mode="HTML"))
            await bot.send_media_group(TARGET_CHANNEL, media_group)
            for media in post["media_group"]:
                try:
                    os.remove(media["path"])
                except:
                    pass
        elif post.get("media_path"):
            file = FSInputFile(post["media_path"])
            if post.get("media_type") == "photo":
                await bot.send_photo(TARGET_CHANNEL, file, caption=text_with_footer, parse_mode="HTML")
            elif post.get("media_type") == "video":
                await bot.send_video(TARGET_CHANNEL, file, caption=text_with_footer, parse_mode="HTML")
            elif post.get("media_type") == "gif":
                await bot.send_animation(TARGET_CHANNEL, file, caption=text_with_footer, parse_mode="HTML")
            else:
                await bot.send_document(TARGET_CHANNEL, file, caption=text_with_footer, parse_mode="HTML")
            try:
                os.remove(post["media_path"])
            except:
                pass
        else:
            await bot.send_message(TARGET_CHANNEL, text_with_footer, parse_mode="HTML")
        
        logger.info(f"Published post {post_id}")
        return True
    except Exception as e:
        logger.error(f"Publish error: {e}")
        await send_error_alert(f"Ошибка публикации: {e}")
        return False


async def scheduled_publisher():
    while True:
        now = datetime.now()
        to_publish = []
        for post_id, (publish_time, _) in list(scheduled_posts.items()):
            if now >= publish_time:
                to_publish.append(post_id)
        
        for post_id in to_publish:
            _, post = scheduled_posts.pop(post_id)
            if await publish_post(post, post_id):
                try:
                    await bot.send_message(ADMIN_ID, f"⏰ Отложенный пост опубликован")
                except:
                    pass
        
        await asyncio.sleep(60)


@dp.message(CommandStart())
async def start_handler(message: types.Message):
    if message.from_user.id == ADMIN_ID:
        await message.answer("Бот запущен. Жду новости из каналов.")


@dp.callback_query(lambda c: c.data.startswith("publish:"))
async def publish_callback(callback: types.CallbackQuery):
    post_id = callback.data.split(":")[1]
    if post_id not in pending_posts:
        await callback.answer("Пост не найден")
        return
    
    post = pending_posts.pop(post_id)
    if await publish_post(post, post_id):
        await callback.message.edit_reply_markup(reply_markup=None)
        await callback.message.reply("✅ Опубликовано")
    else:
        await callback.answer("Ошибка публикации")


@dp.callback_query(lambda c: c.data.startswith("delay:"))
async def delay_callback(callback: types.CallbackQuery):
    post_id = callback.data.split(":")[1]
    if post_id not in pending_posts:
        await callback.answer("Пост не найден")
        return
    
    post = pending_posts.pop(post_id)
    publish_time = datetime.now() + timedelta(hours=1)
    scheduled_posts[post_id] = (publish_time, post)
    
    await callback.message.edit_reply_markup(reply_markup=None)
    await callback.message.reply(f"⏰ Запланировано на {publish_time.strftime('%H:%M')}")
    logger.info(f"Post {post_id} scheduled for {publish_time}")


@dp.callback_query(lambda c: c.data.startswith("skip:"))
async def skip_callback(callback: types.CallbackQuery):
    post_id = callback.data.split(":")[1]
    if post_id in pending_posts:
        post = pending_posts.pop(post_id)
        if post.get("media_path"):
            try:
                os.remove(post["media_path"])
            except:
                pass
        if post.get("media_group"):
            for media in post["media_group"]:
                try:
                    os.remove(media["path"])
                except:
                    pass
    await callback.message.edit_reply_markup(reply_markup=None)
    await callback.message.reply("❌ Пропущено")


@dp.callback_query(lambda c: c.data.startswith("edit:"))
async def edit_callback(callback: types.CallbackQuery):
    post_id = callback.data.split(":")[1]
    if post_id not in pending_posts:
        await callback.answer("Пост не найден")
        return
    
    await callback.message.reply(
        "Отправь отредактированный текст ответом на это сообщение:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Отмена", callback_data=f"cancel_edit:{post_id}")]
        ])
    )
    pending_posts[post_id]["awaiting_edit"] = True
    await callback.answer()


@dp.callback_query(lambda c: c.data.startswith("cancel_edit:"))
async def cancel_edit_callback(callback: types.CallbackQuery):
    post_id = callback.data.split(":")[1]
    if post_id in pending_posts:
        pending_posts[post_id]["awaiting_edit"] = False
    await callback.message.delete()
    await callback.answer("Редактирование отменено")


@dp.message(lambda m: m.reply_to_message and m.from_user.id == ADMIN_ID)
async def handle_edit_reply(message: types.Message):
    for post_id, post in pending_posts.items():
        if post.get("awaiting_edit"):
            post["text"] = message.text
            post["awaiting_edit"] = False
            await message.reply(
                f"Текст обновлён:\n\n{message.text}",
                reply_markup=create_keyboard(post_id)
            )
            return


async def handle_new_post(event):
    try:
        source = event.chat.username or event.chat.title or "unknown"
        text = event.message.text or event.message.message or ""
        has_media = event.message.media is not None
        
        logger.info(f"New message from @{source}: text={len(text)} chars, media={has_media}")
        
        if not text and not has_media:
            logger.info("Skipped: no text and no media")
            return
        
        if len(text) < 20 and not has_media:
            logger.info(f"Skipped: too short ({len(text)} chars)")
            return
        
        if is_ad(text):
            logger.info("Skipped: detected as ad")
            return
        
        if is_duplicate(text):
            logger.info("Skipped: duplicate content")
            return
        
        logger.info("Processing post...")
        rewritten = await rewrite_text(text) if text else ""
        post_id = str(event.message.id) + "_" + str(event.message.date.timestamp())
        
        post_data = {
            "text": rewritten,
            "original": text,
            "source": source,
            "media_path": None,
            "media_type": None,
            "media_group": None,
            "awaiting_edit": False
        }
        
        if event.message.media:
            try:
                if isinstance(event.message.media, MessageMediaPhoto):
                    path = await event.message.download_media(file=f"/tmp/{post_id}.jpg")
                    post_data["media_path"] = path
                    post_data["media_type"] = "photo"
                    
                elif isinstance(event.message.media, MessageMediaDocument):
                    mime = event.message.file.mime_type or ""
                    
                    if mime.startswith("video"):
                        path = await event.message.download_media(file=f"/tmp/{post_id}.mp4")
                        path = await compress_video(path)
                        post_data["media_path"] = path
                        post_data["media_type"] = "video"
                        
                    elif mime == "image/gif" or (event.message.file and event.message.file.name and event.message.file.name.endswith('.gif')):
                        path = await event.message.download_media(file=f"/tmp/{post_id}.gif")
                        post_data["media_path"] = path
                        post_data["media_type"] = "gif"
                        
                    elif mime.startswith("image"):
                        path = await event.message.download_media(file=f"/tmp/{post_id}.jpg")
                        post_data["media_path"] = path
                        post_data["media_type"] = "photo"
                        
            except Exception as e:
                logger.error(f"Media download error: {e}")
        
        if event.message.grouped_id:
            pass
        
        if not rewritten and not post_data["media_path"]:
            logger.info("Skipped: no content after processing")
            return
        
        pending_posts[post_id] = post_data
        
        try:
            source_label = f"📍 Источник: @{source}\n\n"
            caption = f"📥 Новый пост\n{source_label}{rewritten}" if rewritten else f"📥 Новый пост\n{source_label}(без текста)"
            
            if len(caption) > 1024:
                caption = caption[:1020] + "..."
            
            if post_data["media_path"]:
                file = FSInputFile(post_data["media_path"])
                if post_data["media_type"] == "photo":
                    await bot.send_photo(ADMIN_ID, file, caption=caption, reply_markup=create_keyboard(post_id), parse_mode="HTML")
                elif post_data["media_type"] == "video":
                    await bot.send_video(ADMIN_ID, file, caption=caption, reply_markup=create_keyboard(post_id), parse_mode="HTML")
                elif post_data["media_type"] == "gif":
                    await bot.send_animation(ADMIN_ID, file, caption=caption, reply_markup=create_keyboard(post_id), parse_mode="HTML")
            else:
                await bot.send_message(ADMIN_ID, caption, reply_markup=create_keyboard(post_id), parse_mode="HTML")
            
            logger.info(f"Sent to admin: {post_id}")
        except Exception as e:
            logger.error(f"Send to admin error: {e}")
            await send_error_alert(f"Ошибка отправки админу: {e}")
            
    except Exception as e:
        logger.error(f"handle_new_post error: {e}")
        await send_error_alert(f"Ошибка обработки поста: {e}")


async def main():
    try:
        await userbot.start()
        logger.info("Userbot started")
        
        for channel in SOURCE_CHANNELS:
            try:
                entity = await userbot.get_entity(channel)
                userbot.add_event_handler(
                    handle_new_post,
                    events.NewMessage(chats=entity)
                )
                logger.info(f"Listening to: {channel}")
            except Exception as e:
                logger.error(f"Error connecting to {channel}: {e}")
                await send_error_alert(f"Не удалось подключиться к каналу {channel}: {e}")
        
        asyncio.create_task(dp.start_polling(bot))
        asyncio.create_task(scheduled_publisher())
        logger.info("Bot started")
        
        await bot.send_message(ADMIN_ID, "🟢 Бот запущен")
        
        await userbot.run_until_disconnected()
        
    except Exception as e:
        logger.critical(f"Bot crashed: {e}")
        await send_error_alert(f"Бот упал: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())
