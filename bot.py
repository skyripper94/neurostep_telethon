import asyncio
import os
import re
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.tl.types import MessageMediaPhoto, MessageMediaDocument
from aiogram import Bot, Dispatcher, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, FSInputFile
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

SOURCE_CHANNELS = [
    "media1337",
    "iPumpBrain",
    "TrendWatching24"
]

userbot = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
openai_client = AsyncOpenAI(api_key=OPENAI_API_KEY)

pending_posts = {}

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


def markdown_to_html(text: str) -> str:
    text = re.sub(r'\[([^\]]+)\]\(([^)]+)\)', r'<a href="\2">\1</a>', text)
    text = re.sub(r'\*\*([^*]+)\*\*', r'<b>\1</b>', text)
    text = re.sub(r'\*([^*]+)\*', r'<i>\1</i>', text)
    return text


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
        print(f"OpenAI error: {e}")
        return text


def create_keyboard(post_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Опубликовать", callback_data=f"publish:{post_id}"),
            InlineKeyboardButton(text="❌ Пропустить", callback_data=f"skip:{post_id}")
        ],
        [
            InlineKeyboardButton(text="✏️ Редактировать", callback_data=f"edit:{post_id}")
        ]
    ])


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
    
    post = pending_posts[post_id]
    try:
        if post.get("media_path"):
            file = FSInputFile(post["media_path"])
            if post.get("media_type") == "photo":
                await bot.send_photo(
                    TARGET_CHANNEL,
                    file,
                    caption=post["text"],
                    parse_mode="HTML"
                )
            elif post.get("media_type") == "video":
                await bot.send_video(
                    TARGET_CHANNEL,
                    file,
                    caption=post["text"],
                    parse_mode="HTML"
                )
            else:
                await bot.send_document(
                    TARGET_CHANNEL,
                    file,
                    caption=post["text"],
                    parse_mode="HTML"
                )
            os.remove(post["media_path"])
        else:
            await bot.send_message(TARGET_CHANNEL, post["text"], parse_mode="HTML")
        
        await callback.message.edit_reply_markup(reply_markup=None)
        await callback.message.reply("✅ Опубликовано")
        del pending_posts[post_id]
    except Exception as e:
        print(f"Publish error: {e}")
        await callback.answer(f"Ошибка: {str(e)[:100]}")


@dp.callback_query(lambda c: c.data.startswith("skip:"))
async def skip_callback(callback: types.CallbackQuery):
    post_id = callback.data.split(":")[1]
    if post_id in pending_posts:
        if pending_posts[post_id].get("media_path"):
            try:
                os.remove(pending_posts[post_id]["media_path"])
            except:
                pass
        del pending_posts[post_id]
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
    text = event.message.text or event.message.message or ""
    has_media = event.message.media is not None
    
    print(f"New message from {event.chat.username}: text={len(text)} chars, media={has_media}")
    
    if not text and not has_media:
        print("Skipped: no text and no media")
        return
    
    if len(text) < 20 and not has_media:
        print(f"Skipped: too short ({len(text)} chars) and no media")
        return
    
    print("Processing post...")
    rewritten = await rewrite_text(text) if text else ""
    post_id = str(event.message.id) + "_" + str(event.message.date.timestamp())
    
    post_data = {
        "text": rewritten,
        "original": text,
        "media_path": None,
        "media_type": None,
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
                    post_data["media_path"] = path
                    post_data["media_type"] = "video"
                elif mime.startswith("image"):
                    path = await event.message.download_media(file=f"/tmp/{post_id}.jpg")
                    post_data["media_path"] = path
                    post_data["media_type"] = "photo"
        except Exception as e:
            print(f"Media download error: {e}")
    
    if not rewritten and not post_data["media_path"]:
        print("Skipped: no content after processing")
        return
    
    pending_posts[post_id] = post_data
    
    try:
        caption = f"📥 Новый пост\n\n{rewritten}" if rewritten else "📥 Новый пост (без текста)"
        
        if post_data["media_path"]:
            file = FSInputFile(post_data["media_path"])
            if post_data["media_type"] == "photo":
                await bot.send_photo(
                    ADMIN_ID,
                    file,
                    caption=caption,
                    reply_markup=create_keyboard(post_id),
                    parse_mode="HTML"
                )
            elif post_data["media_type"] == "video":
                await bot.send_video(
                    ADMIN_ID,
                    file,
                    caption=caption,
                    reply_markup=create_keyboard(post_id),
                    parse_mode="HTML"
                )
        else:
            await bot.send_message(
                ADMIN_ID,
                caption,
                reply_markup=create_keyboard(post_id),
                parse_mode="HTML"
            )
        print(f"Sent to admin: {post_id}")
    except Exception as e:
        print(f"Send to admin error: {e}")


async def main():
    await userbot.start()
    print("Userbot started")
    
    for channel in SOURCE_CHANNELS:
        try:
            entity = await userbot.get_entity(channel)
            userbot.add_event_handler(
                handle_new_post,
                events.NewMessage(chats=entity)
            )
            print(f"Listening to: {channel}")
        except Exception as e:
            print(f"Error connecting to {channel}: {e}")
    
    asyncio.create_task(dp.start_polling(bot))
    print("Bot started")
    
    await userbot.run_until_disconnected()


if __name__ == "__main__":
    asyncio.run(main())
