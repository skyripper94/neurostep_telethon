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

CHANNEL_FOOTER = '\n\n<a href="https://t.me/nsmedia23">NS Media</a>'
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
    "Business_father",
    "bugnotfeature",
    "sale_caviar",
    "techno_media",
    "trends",
    "neuraldvig"
]

AD_KEYWORDS = [
    "реклама", "партнёр", "партнер", "промокод", "promo", 
    "розыгрыш", "giveaway", "спонсор", "sponsor",
    "utm_", "?ref=", "bit.ly", "clck.ru", "erid",
    "кэшбэк", "cashback", "при поддержке",
    "нативная"
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
registered_entities = []
resolved_channel_id = None

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

REWRITE_PROMPT = """Ты — опытный редактор с 20-летним стажем в издательстве. 
Твоя задача — переписать предоставленный текст так, как это сделал бы живой человек-редактор: сделай его более плавным, естественным, 
увлекательным и профессиональным.

ПРАВИЛА:
- Сохрани смысл, тон и факты — не выдумывай
- Сократи на 10-20% без потери сути
- Короткие абзацы, живой язык, активный залог
- Варьируй длину предложений, убери повторы и клише
- Если есть цитаты (в кавычках или блоках), перефразируй и заверни точно также в цитату через <blockquote>текст</blockquote>

УДАЛИТЬ:
- Любые @упоминания каналов
- Любые ссылки на t.me/telegram
- Чужие подписи каналов в конце

ССЫЛКИ НА САЙТЫ:
- Если есть ссылка на внешний сайт (НЕ telegram) — оформи: заменить на <a href="URL">тут</a> или <a href="URL">по ссылке</a>, или <a href="URL">здесь</a>, 
или <a href="URL">на сайте</a> случайным образом. Интегрируй в текст.

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


async def get_target_channel():
    global resolved_channel_id
    if resolved_channel_id:
        return resolved_channel_id
    
    channel = TARGET_CHANNEL
    if channel.startswith("@"):
        channel = channel[1:]
    
    if channel.lstrip("-").isdigit():
        resolved_channel_id = int(channel)
        return resolved_channel_id
    
    try:
        entity = await userbot.get_entity(channel)
        resolved_channel_id = int(f"-100{entity.id}")
        logger.info(f"Resolved channel @{channel} to {resolved_channel_id}")
        return resolved_channel_id
    except Exception as e:
        logger.error(f"Failed to resolve channel: {e}")
    
    return TARGET_CHANNEL


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


async def send_preview_to_admin(post_data: dict, post_id: str):
    try:
        text_with_footer = (post_data["text"] + CHANNEL_FOOTER) if post_data["text"] else CHANNEL_FOOTER
        caption = text_with_footer if len(text_with_footer) <= 1024 else text_with_footer[:1020] + "..."
        
        await bot.send_message(ADMIN_ID, f"📍 @{post_data['source']}")
        
        if post_data.get("media_group") and len(post_data["media_group"]) >= 1:
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
                await bot.send_message(ADMIN_ID, "👆", reply_markup=create_keyboard(post_id))
        
        elif post_data.get("media_path") and os.path.exists(post_data["media_path"]):
            file = FSInputFile(post_data["media_path"])
            if post_data["media_type"] == "photo":
                await bot.send_photo(ADMIN_ID, file, caption=caption, reply_markup=create_keyboard(post_id), parse_mode="HTML")
            elif post_data["media_type"] == "video":
                await bot.send_video(ADMIN_ID, file, caption=caption, reply_markup=create_keyboard(post_id), parse_mode="HTML")
            elif post_data["media_type"] == "gif":
                await bot.send_animation(ADMIN_ID, file, caption=caption, reply_markup=create_keyboard(post_id), parse_mode="HTML")
        else:
            await bot.send_message(ADMIN_ID, caption, reply_markup=create_keyboard(post_id), parse_mode="HTML")
        
    except Exception as e:
        logger.error(f"Preview error: {e}")
        inc_stat("errors")


async def publish_post(post: dict, post_id: str) -> bool:
    try:
        channel_id = await get_target_channel()
        text_with_footer = (post["text"] + CHANNEL_FOOTER) if post["text"] else CHANNEL_FOOTER
        
        if post.get("media_group") and len(post["media_group"]) > 0:
            media_group = []
            for i, media in enumerate(post["media_group"]):
                if not os.path.exists(media["path"]):
                    logger.warning(f"File not found: {media['path']}")
                    continue
                file = FSInputFile(media["path"])
                caption = text_with_footer if i == 0 else None
                if media["type"] == "photo":
                    media_group.append(InputMediaPhoto(media=file, caption=caption, parse_mode="HTML"))
                elif media["type"] == "video":
                    media_group.append(InputMediaVideo(media=file, caption=caption, parse_mode="HTML"))
            
            if media_group:
                await bot.send_media_group(channel_id, media_group)
            else:
                logger.error("No valid media in group")
                return False
            
            for media in post["media_group"]:
                try:
                    os.remove(media["path"])
                except:
                    pass
                    
        elif post.get("media_path") and os.path.exists(post["media_path"]):
            file = FSInputFile(post["media_path"])
            try:
                if post.get("media_type") == "photo":
                    await bot.send_photo(channel_id, file, caption=text_with_footer, parse_mode="HTML")
                elif post.get("media_type") == "video":
                    await bot.send_video(channel_id, file, caption=text_with_footer, parse_mode="HTML")
                elif post.get("media_type") == "gif":
                    await bot.send_animation(channel_id, file, caption=text_with_footer, parse_mode="HTML")
            except Exception as e:
                if "can't parse" in str(e).lower():
                    logger.warning(f"HTML parse error, retrying without parse_mode: {e}")
                    file = FSInputFile(post["media_path"])
                    if post.get("media_type") == "photo":
                        await bot.send_photo(channel_id, file, caption=text_with_footer)
                    elif post.get("media_type") == "video":
                        await bot.send_video(channel_id, file, caption=text_with_footer)
                    elif post.get("media_type") == "gif":
                        await bot.send_animation(channel_id, file, caption=text_with_footer)
                else:
                    raise
            try:
                os.remove(post["media_path"])
            except:
                pass
        else:
            try:
                await bot.send_message(channel_id, text_with_footer, parse_mode="HTML")
            except Exception as e:
                if "can't parse" in str(e).lower():
                    logger.warning(f"HTML parse error, retrying without parse_mode: {e}")
                    await bot.send_message(channel_id, text_with_footer)
                else:
                    raise
        
        inc_stat("published", post.get("source"))
        logger.info(f"Published: {post_id}")
        return True
    except Exception as e:
        logger.error(f"Publish error: {e}")
        inc_stat("errors")
        return False


async def scheduled_publisher():
    while True:
        now = datetime.now()
        to_publish = [pid for pid, (pt, _) in list(scheduled_posts.items()) if now >= pt]
        for post_id in to_publish:
            _, post = scheduled_posts.pop(post_id)
            try:
                if await publish_post(post, post_id):
                    await bot.send_message(ADMIN_ID, "⏰ Отложенный пост опубликован")
                else:
                    await bot.send_message(ADMIN_ID, f"❌ Ошибка публикации отложенного поста")
            except Exception as e:
                logger.error(f"Scheduled publish error: {e}")
                await bot.send_message(ADMIN_ID, f"❌ Ошибка: {str(e)[:100]}")
        await asyncio.sleep(30)


async def process_media_group(group_id: int):
    await asyncio.sleep(MEDIA_GROUP_TIMEOUT)
    
    if group_id not in media_groups:
        return
    
    group_data = media_groups.pop(group_id)
    messages = sorted(group_data["messages"], key=lambda m: m.id)
    source = group_data["source"]
    
    logger.info(f"GROUP: @{source} | {len(messages)} items | id={group_id}")
    inc_stat("received", source)
    
    text = ""
    for msg in messages:
        if msg.text or msg.message:
            text = msg.text or msg.message
            break
    
    if is_ad(text):
        logger.info(f"  SKIP: ad")
        inc_stat("filtered_ad", source)
        return
    
    if is_duplicate(text):
        logger.info(f"  SKIP: duplicate")
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
            elif isinstance(msg.media, MessageMediaDocument):
                mime = msg.file.mime_type or ""
                if mime.startswith("video"):
                    path = await msg.download_media(file=f"/tmp/{post_id}_{i}.mp4")
                    if path:
                        media_list.append({"path": path, "type": "video"})
        except Exception as e:
            logger.error(f"  Download error: {e}")
    
    if not media_list:
        logger.info(f"  SKIP: no media")
        return
    
    post_data = {
        "text": rewritten,
        "original": text,
        "source": source,
        "media_path": None,
        "media_type": None,
        "media_group": media_list
    }
    
    pending_posts[post_id] = post_data
    await send_preview_to_admin(post_data, post_id)
    logger.info(f"  SENT to admin: {post_id} ({len(media_list)} media)")


async def handle_new_post(event):
    try:
        chat = await event.get_chat()
        source = getattr(chat, 'username', None) or getattr(chat, 'title', None) or "unknown"
        text = event.message.text or event.message.message or ""
        has_media = event.message.media is not None
        grouped_id = event.message.grouped_id
        msg_id = event.message.id
        
        logger.info(f"NEW: @{source} | msg={msg_id} | {len(text)} chars | media={has_media} | group={grouped_id}")
        
        if grouped_id:
            if grouped_id not in media_groups:
                media_groups[grouped_id] = {"messages": [], "source": source}
                asyncio.create_task(process_media_group(grouped_id))
            media_groups[grouped_id]["messages"].append(event.message)
            return
        
        inc_stat("received", source)
        
        if not text and not has_media:
            logger.info(f"  SKIP: empty")
            return
        
        if len(text) < 20 and not has_media:
            logger.info(f"  SKIP: too short")
            return
        
        if is_ad(text):
            logger.info(f"  SKIP: ad")
            inc_stat("filtered_ad", source)
            return
        
        if is_duplicate(text):
            logger.info(f"  SKIP: duplicate")
            inc_stat("filtered_duplicate", source)
            return
        
        rewritten = await rewrite_text(text) if text else ""
        post_id = f"{msg_id}_{int(event.message.date.timestamp())}"
        
        post_data = {
            "text": rewritten,
            "original": text,
            "source": source,
            "media_path": None,
            "media_type": None,
            "media_group": None
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
                        post_data["media_path"] = path
                        post_data["media_type"] = "video"
                    elif "gif" in mime:
                        path = await event.message.download_media(file=f"/tmp/{post_id}.gif")
                        post_data["media_path"] = path
                        post_data["media_type"] = "gif"
            except Exception as e:
                logger.error(f"  Media error: {e}")
        
        if not rewritten and not post_data["media_path"]:
            logger.info(f"  SKIP: no content")
            return
        
        pending_posts[post_id] = post_data
        await send_preview_to_admin(post_data, post_id)
        logger.info(f"  SENT to admin: {post_id}")
            
    except Exception as e:
        logger.error(f"HANDLER ERROR: {e}")


@dp.message(CommandStart())
async def start_handler(message: types.Message):
    if message.from_user.id == ADMIN_ID:
        await message.answer("✅ Бот работает\n\n/stats — статистика\n/channels — проверка каналов\n/fetch @channel — получить пост\n/test — тест кнопок\n/debug — диагностика\n/cleanup — очистка")


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
        uptime = f"⏱ {hours}ч {minutes}м\n\n"
    
    source_stats = ""
    for src, data in stats.get("by_source", {}).items():
        source_stats += f"@{src}: {data['received']} → {data['published']}\n"
    
    text = f"""📊 Статистика

{uptime}📥 {stats.get('received', 0)} | ✅ {stats.get('published', 0)} | ❌ {stats.get('skipped', 0)}
🚫 Реклама: {stats.get('filtered_ad', 0)} | 🔄 Дубли: {stats.get('filtered_duplicate', 0)}

{source_stats if source_stats else ''}
⏳ Очередь: {len(pending_posts)} | 📅 Отложено: {len(scheduled_posts)}"""
    
    await message.answer(text)


@dp.message(Command("reset_stats"))
async def reset_stats_handler(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        return
    global stats
    stats = {"received": 0, "published": 0, "skipped": 0, "filtered_ad": 0, 
             "filtered_duplicate": 0, "delayed": 0, "errors": 0, "by_source": {},
             "start_time": datetime.now().isoformat()}
    save_stats()
    await message.answer("🔄 Сброшено")


@dp.message(Command("cleanup"))
async def cleanup_handler(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        return
    
    deleted_files = 0
    deleted_bytes = 0
    
    for f in os.listdir("/tmp"):
        if f.endswith((".jpg", ".mp4", ".gif", ".png", ".webp")):
            path = f"/tmp/{f}"
            try:
                size = os.path.getsize(path)
                os.remove(path)
                deleted_files += 1
                deleted_bytes += size
            except:
                pass
    
    media_groups.clear()
    recent_hashes.clear()
    
    mb = deleted_bytes / 1024 / 1024
    await message.answer(f"🧹 Очищено:\n• {deleted_files} файлов ({mb:.1f}MB)\n• Кэш дубликатов сброшен")


@dp.message(Command("test"))
async def test_handler(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        return
    
    post_id = f"test_{int(datetime.now().timestamp())}"
    post_data = {
        "text": "Тестовый пост для проверки работы бота.\n\nВсе кнопки должны работать корректно.",
        "original": "Тестовый текст",
        "source": "test_channel",
        "media_path": None,
        "media_type": None,
        "media_group": None
    }
    
    pending_posts[post_id] = post_data
    await send_preview_to_admin(post_data, post_id)
    logger.info(f"Test post created: {post_id}")


@dp.message(Command("channels"))
async def channels_handler(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        return
    
    await message.answer("🔍 Проверяю каналы...")
    
    results = []
    for channel in SOURCE_CHANNELS:
        try:
            entity = await userbot.get_entity(channel)
            msgs = await userbot.get_messages(entity, limit=1)
            last_msg = msgs[0] if msgs else None
            if last_msg:
                delta = datetime.now(last_msg.date.tzinfo) - last_msg.date
                hours = int(delta.total_seconds() // 3600)
                results.append(f"✅ @{channel} — {hours}ч назад")
            else:
                results.append(f"⚠️ @{channel} — пусто")
        except Exception as e:
            results.append(f"❌ @{channel} — {str(e)[:30]}")
    
    await message.answer("\n".join(results))


@dp.message(Command("debug"))
async def debug_handler(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        return
    
    handlers = userbot.list_event_handlers()
    handler_info = f"Event handlers: {len(handlers)}\n"
    for callback, event in handlers:
        handler_info += f"• {callback.__name__}: {type(event).__name__}\n"
        if hasattr(event, 'chats') and event.chats:
            handler_info += f"  Chats: {len(event.chats)} IDs\n"
    
    entities_info = f"IDs: {registered_entities[:5]}{'...' if len(registered_entities) > 5 else ''}\n" if registered_entities else "None\n"
    
    me = await userbot.get_me()
    channel_id = await get_target_channel()
    
    text = f"""🔧 Debug info

👤 Userbot: @{me.username} (id={me.id})
📡 Connected: {userbot.is_connected()}
📢 Target: {channel_id}

{handler_info}
📋 Registered: {len(registered_entities)} channels
{entities_info}
📊 Pending: {len(pending_posts)}
📅 Scheduled: {len(scheduled_posts)}"""
    
    await message.answer(text)


@dp.message(Command("fetch"))
async def fetch_handler(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        return
    
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await message.answer("Использование: /fetch channel_name")
        return
    
    channel = args[1].replace("@", "")
    
    try:
        entity = await userbot.get_entity(channel)
        msgs = await userbot.get_messages(entity, limit=1)
        
        if not msgs:
            await message.answer("Канал пуст")
            return
        
        msg = msgs[0]
        text = msg.text or msg.message or ""
        has_media = msg.media is not None
        
        if not text and not has_media:
            await message.answer("Последний пост пустой")
            return
        
        rewritten = await rewrite_text(text) if text else ""
        post_id = f"fetch_{int(datetime.now().timestamp())}"
        
        post_data = {
            "text": rewritten,
            "original": text,
            "source": channel,
            "media_path": None,
            "media_type": None,
            "media_group": None
        }
        
        if isinstance(msg.media, MessageMediaPhoto):
            path = await msg.download_media(file=f"/tmp/{post_id}.jpg")
            post_data["media_path"] = path
            post_data["media_type"] = "photo"
        elif isinstance(msg.media, MessageMediaDocument):
            mime = msg.file.mime_type or ""
            if mime.startswith("video"):
                path = await msg.download_media(file=f"/tmp/{post_id}.mp4")
                post_data["media_path"] = path
                post_data["media_type"] = "video"
        
        pending_posts[post_id] = post_data
        await send_preview_to_admin(post_data, post_id)
        
    except Exception as e:
        await message.answer(f"Ошибка: {e}")


@dp.callback_query(lambda c: c.data.startswith("pub:"))
async def publish_callback(callback: types.CallbackQuery):
    post_id = callback.data.split(":", 1)[1]
    found_id = None
    for pid in pending_posts:
        if pid.startswith(post_id) or post_id.startswith(pid[:50]):
            found_id = pid
            break
    if not found_id:
        await callback.answer("Не найден")
        return
    post = pending_posts.pop(found_id)
    for uid, pid in list(edit_state.items()):
        if pid == found_id:
            del edit_state[uid]
    try:
        if await publish_post(post, found_id):
            try:
                await callback.message.edit_reply_markup(reply_markup=None)
            except:
                pass
            await callback.message.reply("✅")
        else:
            pending_posts[found_id] = post
            await callback.message.reply("❌ Ошибка публикации, смотри логи")
    except Exception as e:
        logger.error(f"Publish callback error: {e}")
        pending_posts[found_id] = post
        await callback.message.reply(f"❌ {str(e)[:100]}")
    await callback.answer()


@dp.callback_query(lambda c: c.data.startswith("delay:"))
async def delay_callback(callback: types.CallbackQuery):
    post_id = callback.data.split(":", 1)[1]
    found_id = None
    for pid in pending_posts:
        if pid.startswith(post_id) or post_id.startswith(pid[:50]):
            found_id = pid
            break
    if not found_id:
        await callback.answer("Не найден")
        return
    post = pending_posts.pop(found_id)
    publish_time = datetime.now() + timedelta(hours=1)
    scheduled_posts[found_id] = (publish_time, post)
    inc_stat("delayed", post.get("source"))
    for uid, pid in list(edit_state.items()):
        if pid == found_id:
            del edit_state[uid]
    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except:
        pass
    await callback.message.reply(f"⏰ {publish_time.strftime('%H:%M')}")
    await callback.answer()


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
        for uid, pid in list(edit_state.items()):
            if pid == found_id:
                del edit_state[uid]
    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except:
        pass
    await callback.message.reply("❌")
    await callback.answer()


@dp.callback_query(lambda c: c.data.startswith("edit:"))
async def edit_callback(callback: types.CallbackQuery):
    post_id = callback.data.split(":", 1)[1]
    found_id = None
    for pid in pending_posts:
        if pid.startswith(post_id) or post_id.startswith(pid[:50]):
            found_id = pid
            break
    if not found_id:
        await callback.answer("Не найден")
        return
    edit_state[callback.from_user.id] = found_id
    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except:
        pass
    await callback.message.reply("✏️ Новый текст:")
    await callback.answer()


@dp.message(lambda m: m.from_user.id in edit_state and m.text and not m.text.startswith("/"))
async def handle_edit_text(message: types.Message):
    post_id = edit_state.pop(message.from_user.id)
    if post_id not in pending_posts:
        await message.reply("Не актуален")
        return
    pending_posts[post_id]["text"] = message.text
    await message.reply("✅ Текст обновлён")
    await send_preview_to_admin(pending_posts[post_id], post_id)


async def cleanup_cache():
    while True:
        now = datetime.now()
        next_cleanup = now.replace(hour=4, minute=0, second=0, microsecond=0)
        if now >= next_cleanup:
            next_cleanup += timedelta(days=1)
        
        wait_seconds = (next_cleanup - now).total_seconds()
        logger.info(f"Next cleanup at {next_cleanup.strftime('%H:%M')}")
        await asyncio.sleep(wait_seconds)
        
        try:
            deleted_files = 0
            deleted_bytes = 0
            
            for f in os.listdir("/tmp"):
                if f.endswith((".jpg", ".mp4", ".gif", ".png", ".webp")):
                    path = f"/tmp/{f}"
                    try:
                        size = os.path.getsize(path)
                        os.remove(path)
                        deleted_files += 1
                        deleted_bytes += size
                    except:
                        pass
            
            old_pending = []
            for pid in list(pending_posts.keys()):
                try:
                    ts = int(pid.split("_")[-1]) if "_" in pid else 0
                    if ts and (now.timestamp() - ts) > 86400:
                        old_pending.append(pid)
                except:
                    pass
            
            for pid in old_pending:
                post = pending_posts.pop(pid, None)
                if post:
                    if post.get("media_path"):
                        try:
                            os.remove(post["media_path"])
                        except:
                            pass
                    if post.get("media_group"):
                        for m in post["media_group"]:
                            try:
                                os.remove(m["path"])
                            except:
                                pass
            
            old_scheduled = []
            for pid, (pt, _) in list(scheduled_posts.items()):
                if (now - pt).total_seconds() > 86400:
                    old_scheduled.append(pid)
            
            for pid in old_scheduled:
                scheduled_posts.pop(pid, None)
            
            media_groups.clear()
            recent_hashes.clear()
            
            mb = deleted_bytes / 1024 / 1024
            logger.info(f"Cleanup: {deleted_files} files ({mb:.1f}MB), {len(old_pending)} old pending, {len(old_scheduled)} old scheduled")
            
            if deleted_files > 0 or old_pending or old_scheduled:
                await bot.send_message(ADMIN_ID, f"🧹 Очистка:\n• {deleted_files} файлов ({mb:.1f}MB)\n• {len(old_pending)} старых постов\n• {len(old_scheduled)} просроченных")
        
        except Exception as e:
            logger.error(f"Cleanup error: {e}")


async def keepalive():
    while True:
        await asyncio.sleep(300)
        try:
            if userbot.is_connected():
                await userbot.get_me()
            else:
                logger.warning("KEEPALIVE: disconnected, reconnecting...")
                await userbot.connect()
        except Exception as e:
            logger.error(f"KEEPALIVE: {e}")


async def run_bot_polling():
    retry_delay = 5
    max_delay = 60
    
    while True:
        try:
            logger.info("Starting aiogram polling...")
            retry_delay = 5
            await dp.start_polling(bot)
        except Exception as e:
            logger.error(f"Polling error: {e}")
            logger.info(f"Retrying in {retry_delay}s...")
            await asyncio.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, max_delay)


async def main():
    load_stats()
    stats["start_time"] = datetime.now().isoformat()
    
    logger.info("="*50)
    logger.info("BOT STARTING")
    logger.info("="*50)
    
    await userbot.start()
    logger.info("Userbot client started")
    
    me = await userbot.get_me()
    logger.info(f"Logged in as: @{me.username} (id={me.id})")
    
    channel_id = await get_target_channel()
    logger.info(f"Target channel: {channel_id}")
    
    await userbot.catch_up()
    logger.info("catch_up() completed")
    
    registered = 0
    entities = []
    global registered_entities
    
    logger.info(f"Registering {len(SOURCE_CHANNELS)} channels...")
    
    for channel in SOURCE_CHANNELS:
        try:
            entity = await userbot.get_entity(channel)
            msgs = await userbot.get_messages(entity, limit=1)
            last_post = ""
            if msgs and msgs[0]:
                delta = datetime.now(msgs[0].date.tzinfo) - msgs[0].date
                hours = int(delta.total_seconds() // 3600)
                last_post = f" (last: {hours}h ago)"
            
            logger.info(f"  ✓ @{channel} id={entity.id}{last_post}")
            entities.append(entity.id)
            registered += 1
        except Exception as e:
            logger.error(f"  ✗ @{channel}: {e}")
    
    logger.info(f"Registered {registered}/{len(SOURCE_CHANNELS)} channels")
    
    if entities:
        registered_entities = entities
        userbot.add_event_handler(
            handle_new_post,
            events.NewMessage(chats=entities)
        )
        logger.info(f"Handler registered for {len(entities)} channel IDs")
    
    asyncio.create_task(run_bot_polling())
    asyncio.create_task(scheduled_publisher())
    asyncio.create_task(keepalive())
    asyncio.create_task(cleanup_cache())
    
    logger.info("="*50)
    logger.info("BOT READY")
    logger.info("="*50)
    
    try:
        await bot.send_message(ADMIN_ID, f"🟢 Бот запущен\nКаналов: {registered}/{len(SOURCE_CHANNELS)}\nTarget: {channel_id}")
    except:
        pass
    
    while True:
        try:
            await userbot.run_until_disconnected()
        except Exception as e:
            logger.error(f"Userbot disconnected: {e}")
            await asyncio.sleep(5)
            try:
                if not userbot.is_connected():
                    await userbot.connect()
                    logger.info("Userbot reconnected!")
            except Exception as re:
                logger.error(f"Reconnect failed: {re}")


if __name__ == "__main__":
    asyncio.run(main())
