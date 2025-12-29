import asyncio
import os
import re
import logging
import hashlib
import json
from datetime import datetime, timedelta
from typing import Dict
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.tl.types import MessageMediaPhoto, MessageMediaDocument
from aiogram import Bot, Dispatcher, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, FSInputFile, InputMediaPhoto, InputMediaVideo
from aiogram.filters import CommandStart, Command
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
STATS_FILE = "/tmp/bot_stats.json"

SOURCE_CHANNELS = [
    "media1337",
    "iPumpBrain",
    "TrendWatching24",
    "costperlead",
    "trendsetter",
    "provod",
    "MirFacto",
    "biz_FM",
    "Business_father"
]

AD_KEYWORDS = [
    "реклама", "партнёр", "партнер", "промокод", "promo", 
    "розыгрыш", "giveaway", "спонсор", "sponsor",
    "utm_", "?ref=", "bit.ly", "clck.ru", "erid",
    "кэшбэк", "cashback", "при поддержке",
    "интеграция", "нативная"
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
edit_state = {}

media_groups: Dict[int, Dict] = {}
MEDIA_GROUP_TIMEOUT = 10

stats = {
    "received": 0,
    "published": 0,
    "skipped": 0,
    "filtered_ad": 0,
    "filtered_duplicate": 0,
    "delayed": 0,
    "errors": 0,
    "by_source": {},
    "start_time": None
}

REWRITE_PROMPT = """Перепиши новость коротко и цепляюще.

ПРАВИЛА:
- 1-3 предложения максимум
- Первое предложение = главный факт
- Без эмодзи, без воды
- Простой понятный язык

УДАЛИТЬ:
- Любые @упоминания каналов
- Любые ссылки на t.me/telegram
- Чужие подписи каналов в конце

ССЫЛКИ НА САЙТЫ:
- Если есть ссылка на внешний сайт (НЕ telegram) — оформи так: <a href="URL">тут</a> или <a href="URL">подробнее</a>
- Пример: Подробности <a href="https://example.com">тут</a>

Meta/Instagram/WhatsApp → сноска: * — Meta, запрещена в РФ

Текст:
{text}"""


def load_stats():
    global stats
    try:
        if os.path.exists(STATS_FILE):
            with open(STATS_FILE, 'r') as f:
                saved = json.load(f)
                stats.update(saved)
    except:
        pass
    if not stats["start_time"]:
        stats["start_time"] = datetime.now().isoformat()


def save_stats():
    try:
        with open(STATS_FILE, 'w') as f:
            json.dump(stats, f)
    except:
        pass


def inc_stat(key: str, source: str = None):
    stats[key] = stats.get(key, 0) + 1
    if source:
        if source not in stats["by_source"]:
            stats["by_source"][source] = {"received": 0, "published": 0, "filtered": 0}
        if key == "received":
            stats["by_source"][source]["received"] += 1
        elif key == "published":
            stats["by_source"][source]["published"] += 1
        elif key in ["filtered_ad", "filtered_duplicate", "skipped"]:
            stats["by_source"][source]["filtered"] += 1
    save_stats()


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


def clean_text(text: str) -> str:
    text = re.sub(r'<a[^>]*href=["\'][^"\']*t\.me[^"\']*["\'][^>]*>.*?</a>', '', text, flags=re.IGNORECASE)
    text = re.sub(r'<a[^>]*href=["\'][^"\']*telegram[^"\']*["\'][^>]*>.*?</a>', '', text, flags=re.IGNORECASE)
    text = re.sub(r'@[\w_]+', '', text)
    text = re.sub(r'https?://t\.me/[\w_/]+', '', text)
    text = re.sub(r't\.me/[\w_/]+', '', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    text = re.sub(r' {2,}', ' ', text)
    text = re.sub(r'\s*[—\-–]\s*$', '', text)
    return text.strip()


def markdown_to_html(text: str) -> str:
    text = re.sub(r'\[([^\]]+)\]\(([^)]+)\)', r'<a href="\2">\1</a>', text)
    text = re.sub(r'\*\*([^*]+)\*\*', r'<b>\1</b>', text)
    text = re.sub(r'\*([^*]+)\*', r'<i>\1</i>', text)
    return text


async def rewrite_text(text: str) -> str:
    if not text or len(text) < 20:
        return clean_text(text)
    try:
        response = await openai_client.chat.completions.create(
            model="gpt-4.1",
            messages=[
                {"role": "user", "content": REWRITE_PROMPT.format(text=text)}
            ],
            max_tokens=500,
            temperature=0.7
        )
        result = response.choices[0].message.content.strip()
        result = markdown_to_html(result)
        result = clean_text(result)
        return result
    except Exception as e:
        logger.error(f"OpenAI error: {e}")
        inc_stat("errors")
        return clean_text(text)


def create_keyboard(post_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Опубликовать", callback_data=f"pub:{post_id[:50]}"),
            InlineKeyboardButton(text="❌ Пропустить", callback_data=f"skip:{post_id[:50]}")
        ],
        [
            InlineKeyboardButton(text="⏰ Через час", callback_data=f"delay:{post_id[:50]}"),
            InlineKeyboardButton(text="✏️ Редактировать", callback_data=f"edit:{post_id[:50]}")
        ]
    ])


async def send_error_alert(error_msg: str):
    try:
        await bot.send_message(ADMIN_ID, f"🚨 Ошибка:\n{error_msg[:500]}")
    except:
        pass


async def send_preview_to_admin(post_data: dict, post_id: str):
    """Отправляет превью поста админу"""
    try:
        await bot.send_message(ADMIN_ID, f"📍 Источник: @{post_data['source']}")
        
        caption = post_data["text"] if post_data["text"] else "(без текста)"
        if len(caption) > 1024:
            caption = caption[:1020] + "..."
        
        if post_data.get("media_group") and len(post_data["media_group"]) > 1:
            media_group = []
            for i, media in enumerate(post_data["media_group"]):
                if not os.path.exists(media["path"]):
                    continue
                file = FSInputFile(media["path"])
                cap = caption if i == 0 else None
                if media["type"] == "photo":
                    media_group.append(InputMediaPhoto(media=file, caption=cap, parse_mode="HTML"))
                elif media["type"] == "video":
                    media_group.append(InputMediaVideo(media=file, caption=cap, parse_mode="HTML"))
            
            if media_group:
                await bot.send_media_group(ADMIN_ID, media_group)
                await bot.send_message(ADMIN_ID, "👆 Выбери действие:", reply_markup=create_keyboard(post_id))
        
        elif post_data.get("media_path"):
            file = FSInputFile(post_data["media_path"])
            if post_data["media_type"] == "photo":
                await bot.send_photo(ADMIN_ID, file, caption=caption, reply_markup=create_keyboard(post_id), parse_mode="HTML")
            elif post_data["media_type"] == "video":
                await bot.send_video(ADMIN_ID, file, caption=caption, reply_markup=create_keyboard(post_id), parse_mode="HTML")
            elif post_data["media_type"] == "gif":
                await bot.send_animation(ADMIN_ID, file, caption=caption, reply_markup=create_keyboard(post_id), parse_mode="HTML")
        else:
            await bot.send_message(ADMIN_ID, caption, reply_markup=create_keyboard(post_id), parse_mode="HTML")
        
        logger.info(f"Preview sent: {post_id}")
        return True
    except Exception as e:
        logger.error(f"Preview error: {e}")
        inc_stat("errors")
        return False


async def publish_post(post: dict, post_id: str) -> bool:
    try:
        text_with_footer = (post["text"] + CHANNEL_FOOTER) if post["text"] else CHANNEL_FOOTER
        
        if post.get("media_group") and len(post["media_group"]) > 0:
            media_group = []
            for i, media in enumerate(post["media_group"]):
                if not os.path.exists(media["path"]):
                    continue
                file = FSInputFile(media["path"])
                caption = text_with_footer if i == 0 else None
                if media["type"] == "photo":
                    media_group.append(InputMediaPhoto(media=file, caption=caption, parse_mode="HTML"))
                elif media["type"] == "video":
                    media_group.append(InputMediaVideo(media=file, caption=caption, parse_mode="HTML"))
            
            if media_group:
                await bot.send_media_group(TARGET_CHANNEL, media_group)
            
            for media in post["media_group"]:
                try:
                    os.remove(media["path"])
                except:
                    pass
                    
        elif post.get("media_path") and os.path.exists(post["media_path"]):
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
        
        inc_stat("published", post.get("source"))
        logger.info(f"Published: {post_id}")
        return True
    except Exception as e:
        logger.error(f"Publish error: {e}")
        inc_stat("errors")
        await send_error_alert(f"Публикация: {e}")
        return False


async def scheduled_publisher():
    while True:
        now = datetime.now()
        to_publish = [pid for pid, (pt, _) in list(scheduled_posts.items()) if now >= pt]
        
        for post_id in to_publish:
            _, post = scheduled_posts.pop(post_id)
            if await publish_post(post, post_id):
                try:
                    await bot.send_message(ADMIN_ID, "⏰ Отложенный пост опубликован")
                except:
                    pass
        
        await asyncio.sleep(30)


async def process_media_group(group_id: int):
    await asyncio.sleep(MEDIA_GROUP_TIMEOUT)
    
    if group_id not in media_groups:
        logger.warning(f"Media group {group_id} not found after timeout")
        return
    
    group_data = media_groups.pop(group_id)
    messages = sorted(group_data["messages"], key=lambda m: m.id)
    source = group_data["source"]
    
    logger.info(f"Processing media group {group_id}: {len(messages)} items from @{source}")
    inc_stat("received", source)
    
    text = ""
    for msg in messages:
        msg_text = msg.text or msg.message or ""
        if msg_text:
            text = msg_text
            break
    
    if is_ad(text):
        logger.info(f"Skipped group {group_id}: ad detected")
        inc_stat("filtered_ad", source)
        return
    
    if is_duplicate(text):
        logger.info(f"Skipped group {group_id}: duplicate")
        inc_stat("filtered_duplicate", source)
        return
    
    rewritten = await rewrite_text(text) if text else ""
    post_id = f"g{group_id}"
    
    media_list = []
    for i, msg in enumerate(messages):
        try:
            if isinstance(msg.media, MessageMediaPhoto):
                path = await msg.download_media(file=f"/tmp/{post_id}_{i}.jpg")
                if path:
                    media_list.append({"path": path, "type": "photo"})
                    logger.info(f"Downloaded photo {i+1}/{len(messages)}")
            elif isinstance(msg.media, MessageMediaDocument):
                mime = msg.file.mime_type or ""
                if mime.startswith("video"):
                    path = await msg.download_media(file=f"/tmp/{post_id}_{i}.mp4")
                    if path:
                        media_list.append({"path": path, "type": "video"})
                        logger.info(f"Downloaded video {i+1}/{len(messages)}")
                elif mime.startswith("image"):
                    path = await msg.download_media(file=f"/tmp/{post_id}_{i}.jpg")
                    if path:
                        media_list.append({"path": path, "type": "photo"})
                        logger.info(f"Downloaded image {i+1}/{len(messages)}")
        except Exception as e:
            logger.error(f"Media download error: {e}")
    
    if not media_list:
        logger.info(f"Skipped group {group_id}: no media downloaded")
        return
    
    post_data = {
        "text": rewritten,
        "original": text,
        "source": source,
        "media_path": None,
        "media_type": None,
        "media_group": media_list,
        "awaiting_edit": False
    }
    
    pending_posts[post_id] = post_data
    await send_preview_to_admin(post_data, post_id)


@dp.message(CommandStart())
async def start_handler(message: types.Message):
    if message.from_user.id == ADMIN_ID:
        await message.answer("Бот работает ✅\n\n/stats — статистика\n/reset_stats — сброс")


@dp.message(Command("stats"))
async def stats_handler(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        return
    
    uptime = ""
    if stats["start_time"]:
        start = datetime.fromisoformat(stats["start_time"])
        delta = datetime.now() - start
        hours = int(delta.total_seconds() // 3600)
        minutes = int((delta.total_seconds() % 3600) // 60)
        uptime = f"⏱ Аптайм: {hours}ч {minutes}м\n\n"
    
    source_stats = ""
    for src, data in stats.get("by_source", {}).items():
        source_stats += f"@{src}: {data['received']} → {data['published']}\n"
    
    text = f"""📊 Статистика

{uptime}📥 Получено: {stats.get('received', 0)}
✅ Опубликовано: {stats.get('published', 0)}
❌ Пропущено: {stats.get('skipped', 0)}
🚫 Реклама: {stats.get('filtered_ad', 0)}
🔄 Дубликаты: {stats.get('filtered_duplicate', 0)}
⏰ Отложено: {stats.get('delayed', 0)}
⚠️ Ошибок: {stats.get('errors', 0)}

📍 Источники:
{source_stats if source_stats else 'Нет данных'}
⏳ В очереди: {len(pending_posts)}
📅 Запланировано: {len(scheduled_posts)}"""
    
    await message.answer(text)


@dp.message(Command("reset_stats"))
async def reset_stats_handler(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        return
    
    global stats
    stats = {
        "received": 0,
        "published": 0,
        "skipped": 0,
        "filtered_ad": 0,
        "filtered_duplicate": 0,
        "delayed": 0,
        "errors": 0,
        "by_source": {},
        "start_time": datetime.now().isoformat()
    }
    save_stats()
    await message.answer("🔄 Статистика сброшена")


@dp.callback_query(lambda c: c.data.startswith("pub:"))
async def publish_callback(callback: types.CallbackQuery):
    post_id = callback.data.split(":", 1)[1]
    
    found_id = None
    for pid in pending_posts:
        if pid.startswith(post_id) or post_id.startswith(pid[:50]):
            found_id = pid
            break
    
    if not found_id:
        await callback.answer("Пост не найден")
        return
    
    post = pending_posts.pop(found_id)
    if await publish_post(post, found_id):
        await callback.message.edit_reply_markup(reply_markup=None)
        await callback.message.reply("✅ Опубликовано")
    else:
        pending_posts[found_id] = post
        await callback.answer("Ошибка публикации")


@dp.callback_query(lambda c: c.data.startswith("delay:"))
async def delay_callback(callback: types.CallbackQuery):
    post_id = callback.data.split(":", 1)[1]
    
    found_id = None
    for pid in pending_posts:
        if pid.startswith(post_id) or post_id.startswith(pid[:50]):
            found_id = pid
            break
    
    if not found_id:
        await callback.answer("Пост не найден")
        return
    
    post = pending_posts.pop(found_id)
    publish_time = datetime.now() + timedelta(hours=1)
    scheduled_posts[found_id] = (publish_time, post)
    
    inc_stat("delayed", post.get("source"))
    await callback.message.edit_reply_markup(reply_markup=None)
    await callback.message.reply(f"⏰ В {publish_time.strftime('%H:%M')}")


@dp.callback_query(lambda c: c.data.startswith("skip:"))
async def skip_callback(callback: types.CallbackQuery):
    post_id = callback.data.split(":", 1)[1]
    
    found_id = None
    for pid in pending_posts:
        if pid.startswith(post_id) or post_id.startswith(pid[:50]):
            found_id = pid
            break
    
    if found_id and found_id in pending_posts:
        post = pending_posts.pop(found_id)
        inc_stat("skipped", post.get("source"))
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
    post_id = callback.data.split(":", 1)[1]
    
    found_id = None
    for pid in pending_posts:
        if pid.startswith(post_id) or post_id.startswith(pid[:50]):
            found_id = pid
            break
    
    if not found_id:
        await callback.answer("Пост не найден")
        return
    
    edit_state[callback.from_user.id] = found_id
    await callback.message.reply("✏️ Отправь новый текст:")
    await callback.answer()


@dp.message(lambda m: m.from_user.id == ADMIN_ID and m.from_user.id in edit_state)
async def handle_edit_text(message: types.Message):
    if message.from_user.id not in edit_state:
        return
    
    post_id = edit_state.pop(message.from_user.id)
    
    if post_id not in pending_posts:
        await message.reply("Пост уже не актуален")
        return
    
    pending_posts[post_id]["text"] = message.text
    await message.reply(
        f"✅ Текст обновлён:\n\n{message.text}",
        reply_markup=create_keyboard(post_id)
    )


async def handle_new_post(event):
    logger.info(f"EVENT RECEIVED: chat_id={event.chat_id}")
    try:
        source = event.chat.username or event.chat.title or "unknown"
        text = event.message.text or event.message.message or ""
        has_media = event.message.media is not None
        grouped_id = event.message.grouped_id
        
        logger.info(f"NEW: @{source} | {len(text)} chars | media={has_media} | group={grouped_id}")
        
        if grouped_id:
            if grouped_id not in media_groups:
                media_groups[grouped_id] = {"messages": [], "source": source}
                asyncio.create_task(process_media_group(grouped_id))
            media_groups[grouped_id]["messages"].append(event.message)
            logger.info(f"Group {grouped_id}: added msg, total={len(media_groups[grouped_id]['messages'])}")
            return
        
        inc_stat("received", source)
        
        if not text and not has_media:
            logger.info(f"SKIP: no text and no media")
            return
        
        if len(text) < 20 and not has_media:
            logger.info(f"SKIP: text too short ({len(text)} chars)")
            return
        
        if is_ad(text):
            logger.info(f"SKIP: ad detected")
            inc_stat("filtered_ad", source)
            return
        
        if is_duplicate(text):
            logger.info(f"SKIP: duplicate")
            inc_stat("filtered_duplicate", source)
            return
        
        rewritten = await rewrite_text(text) if text else ""
        post_id = f"{event.message.id}_{int(event.message.date.timestamp())}"
        
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
                    logger.info(f"Downloaded photo: {path}")
                elif isinstance(event.message.media, MessageMediaDocument):
                    mime = event.message.file.mime_type or ""
                    if mime.startswith("video"):
                        path = await event.message.download_media(file=f"/tmp/{post_id}.mp4")
                        post_data["media_path"] = path
                        post_data["media_type"] = "video"
                        logger.info(f"Downloaded video: {path}")
                    elif "gif" in mime or (event.message.file.name or "").endswith('.gif'):
                        path = await event.message.download_media(file=f"/tmp/{post_id}.gif")
                        post_data["media_path"] = path
                        post_data["media_type"] = "gif"
                        logger.info(f"Downloaded gif: {path}")
                    elif mime.startswith("image"):
                        path = await event.message.download_media(file=f"/tmp/{post_id}.jpg")
                        post_data["media_path"] = path
                        post_data["media_type"] = "photo"
                        logger.info(f"Downloaded image: {path}")
                    else:
                        logger.info(f"SKIP: unsupported media type: {mime}")
            except Exception as e:
                logger.error(f"Media download error: {e}")
                inc_stat("errors")
        
        if not rewritten and not post_data["media_path"]:
            logger.info(f"SKIP: no content after processing")
            return
        
        pending_posts[post_id] = post_data
        await send_preview_to_admin(post_data, post_id)
            
    except Exception as e:
        logger.error(f"Handler error: {e}")
        inc_stat("errors")


async def main():
    try:
        load_stats()
        stats["start_time"] = datetime.now().isoformat()
        
        await userbot.start()
        logger.info("Userbot started")
        
        for channel in SOURCE_CHANNELS:
            try:
                entity = await userbot.get_entity(channel)
                userbot.add_event_handler(handle_new_post, events.NewMessage(chats=entity))
                logger.info(f"Listening: @{channel} (id={entity.id})")
            except Exception as e:
                logger.error(f"Channel error @{channel}: {e}")
        
        @userbot.on(events.NewMessage)
        async def debug_all_messages(event):
            logger.info(f"DEBUG ALL: chat_id={event.chat_id}, text={len(event.message.text or '')} chars")
        
        asyncio.create_task(dp.start_polling(bot))
        asyncio.create_task(scheduled_publisher())
        logger.info("Bot started")
        
        await bot.send_message(ADMIN_ID, "🟢 Бот запущен\n/stats — статистика")
        await userbot.run_until_disconnected()
        
    except Exception as e:
        logger.critical(f"Crashed: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())
