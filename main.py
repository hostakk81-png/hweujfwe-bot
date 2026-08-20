import asyncio
import logging
import os
from datetime import datetime
from dotenv import load_dotenv
import sys
import traceback
import json
import html
import re
from typing import Any

try:
    from aiogram import Bot, Dispatcher, types, F
    from aiogram.filters import Command
    from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
except Exception as e:
    tb = traceback.format_exc()
    print("Failed to import aiogram or its submodules. Full traceback below:")
    print(tb)
    try:
        import pydantic
        print(f"pydantic version: {getattr(pydantic, '__version__', 'unknown')}")
    except Exception:
        print("pydantic is not installed or failed to import")

    if "PydanticUndefinedAnnotation" in tb or "name 'Default' is not defined" in tb:
        print("Detected pydantic-related schema error. Try installing a compatible pydantic:")
        print("    pip install 'pydantic==2.4.2'")

    sys.exit(1)
from openai import OpenAI
import aiohttp

try:
    import asyncpg
except ImportError:
    asyncpg = None

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
APIMASTER_API_KEY = os.getenv("APIMASTER_API_KEY")
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
ADMIN_IDS = {int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip().isdigit()}

if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN not found in .env")
if not APIMASTER_API_KEY:
    raise ValueError("APIMASTER_API_KEY not found in .env")

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

memory = {}
db_pool = None
broadcast_sessions: dict[int, dict[str, Any]] = {}
broadcast_tasks: dict[int, asyncio.Task] = {}
admin_sessions: dict[int, str] = {}
group_enabled_cache: dict[int, bool] = {}


def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


async def init_db() -> None:
    """Railway PostgreSQL storage. All hot statistics queries use indexed columns."""
    global db_pool
    if not DATABASE_URL or asyncpg is None:
        logging.warning("DATABASE_URL/asyncpg not configured; database features are disabled")
        return
    db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5, command_timeout=15)
    async with db_pool.acquire() as conn:
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY, username TEXT, first_name TEXT,
            is_blocked BOOLEAN NOT NULL DEFAULT FALSE, first_seen TIMESTAMPTZ NOT NULL DEFAULT now(),
            last_seen TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        CREATE TABLE IF NOT EXISTS events (
            id BIGSERIAL PRIMARY KEY, user_id BIGINT NOT NULL, event_type TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        CREATE TABLE IF NOT EXISTS broadcasts (
            id BIGSERIAL PRIMARY KEY, admin_id BIGINT NOT NULL, payload JSONB NOT NULL,
            status TEXT NOT NULL DEFAULT 'draft', total INT NOT NULL DEFAULT 0,
            sent INT NOT NULL DEFAULT 0, failed INT NOT NULL DEFAULT 0,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(), finished_at TIMESTAMPTZ
        );
        CREATE TABLE IF NOT EXISTS bot_settings (
            key TEXT PRIMARY KEY, value JSONB NOT NULL, updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        CREATE TABLE IF NOT EXISTS managed_chats (
            chat_id BIGINT PRIMARY KEY, title TEXT NOT NULL, enabled_by BIGINT NOT NULL,
            enabled_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        CREATE INDEX IF NOT EXISTS events_created_idx ON events(created_at);
        CREATE INDEX IF NOT EXISTS events_type_idx ON events(event_type);
        CREATE INDEX IF NOT EXISTS users_last_seen_idx ON users(last_seen);
        """)


async def get_subscription_targets() -> list[dict[str, Any]]:
    if not db_pool:
        return []
    async with db_pool.acquire() as conn:
        raw = await conn.fetchval("SELECT value FROM bot_settings WHERE key='subscription_targets'")
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except json.JSONDecodeError:
            return []
    return raw if isinstance(raw, list) else []


async def save_subscription_targets(targets: list[dict[str, Any]]) -> None:
    if not db_pool:
        return
    async with db_pool.acquire() as conn:
        await conn.execute("""INSERT INTO bot_settings(key,value) VALUES('subscription_targets',$1)
            ON CONFLICT(key) DO UPDATE SET value=$1, updated_at=now()""", json.dumps(targets))


def subscription_keyboard(targets: list[dict[str, Any]]) -> InlineKeyboardMarkup:
    buttons = []
    for target in targets:
        link = target.get("link")
        if link:
            buttons.append([InlineKeyboardButton(text=f"📢 {target['title']}", url=link)])
    buttons.append([InlineKeyboardButton(text="✅ проверить подписку", callback_data="sub:check")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


async def user_has_subscription(user_id: int) -> bool:
    targets = await get_subscription_targets()
    for target in targets:
        try:
            member = await bot.get_chat_member(target["chat_id"], user_id)
            status = getattr(member.status, "value", member.status)
            if status in {"left", "kicked"}:
                return False
        except Exception:
            return False
    return True


async def require_subscription(message: types.Message) -> bool:
    if message.chat.type != "private" or is_admin(message.from_user.id):
        return True
    targets = await get_subscription_targets()
    if not targets or await user_has_subscription(message.from_user.id):
        return True
    await message.answer("🔒 подпишись на обязательные каналы/чаты, потом нажми «проверить подписку».", reply_markup=subscription_keyboard(targets))
    return False


async def can_reply_in_chat(message: types.Message) -> bool:
    if message.chat.type == "private":
        return True
    me = await bot.get_me()
    mentioned = bool(message.text and me.username and f"@{me.username.lower()}" in message.text.lower())
    replied_to_bot = bool(message.reply_to_message and message.reply_to_message.from_user and message.reply_to_message.from_user.id == me.id)
    return mentioned or replied_to_bot


async def group_chat_enabled(chat_id: int) -> bool:
    if chat_id in group_enabled_cache:
        return group_enabled_cache[chat_id]
    if not db_pool:
        return False
    async with db_pool.acquire() as conn:
        enabled = bool(await conn.fetchval("SELECT EXISTS(SELECT 1 FROM managed_chats WHERE chat_id=$1)", chat_id))
    group_enabled_cache[chat_id] = enabled
    return enabled


async def user_can_enable_group(message: types.Message) -> bool:
    if is_admin(message.from_user.id):
        return True
    member = await bot.get_chat_member(message.chat.id, message.from_user.id)
    return getattr(member.status, "value", member.status) in {"administrator", "creator"}


async def enable_group_chat(message: types.Message) -> None:
    if message.chat.type == "private":
        me = await bot.get_me()
        await message.answer(
            "👥 <b>добавление в группу</b>\n\n"
            "нажми кнопку, выбери группу, затем в группе напиши <code>/add</code>.\n"
            "для работы на все сообщения выключи Privacy Mode в BotFather. Админ-права нужны только для проверки подписки в каналах.",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="➕ добавить в группу", url=f"https://t.me/{me.username}?startgroup=add")
            ]]),
        )
        return
    try:
        if not await user_can_enable_group(message):
            await message.answer("/add может выполнить администратор этого чата.")
            return
    except Exception as exc:
        logging.warning("group admin check failed: %s", exc)
        await message.answer("не смог проверить права. Выдай боту доступ к участникам и повтори /add.")
        return
    if not db_pool:
        await message.answer("DATABASE_URL не настроен.")
        return
    title = message.chat.title or str(message.chat.id)
    async with db_pool.acquire() as conn:
        await conn.execute("""INSERT INTO managed_chats(chat_id,title,enabled_by) VALUES($1,$2,$3)
            ON CONFLICT(chat_id) DO UPDATE SET title=$2, enabled_by=$3, enabled_at=now()""",
            message.chat.id, title, message.from_user.id)
    group_enabled_cache[message.chat.id] = True
    await message.answer("✅ чат добавлен. Теперь я отвечаю на сообщения в этом чате и на упоминания.")


async def track_user(message: types.Message, event_type: str = "message") -> None:
    if not db_pool:
        return
    u = message.from_user
    async with db_pool.acquire() as conn:
        await conn.execute("""INSERT INTO users(user_id, username, first_name) VALUES($1,$2,$3)
            ON CONFLICT(user_id) DO UPDATE SET username=$2, first_name=$3, last_seen=now(), is_blocked=FALSE""",
            u.id, u.username, u.first_name)
        await conn.execute("INSERT INTO events(user_id,event_type) VALUES($1,$2)", u.id, event_type)


async def stat_snapshot() -> dict[str, int]:
    if not db_pool:
        return {"users": 0, "active_24h": 0, "messages": 0, "images": 0}
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("""SELECT
            (SELECT count(*) FROM users) users,
            (SELECT count(*) FROM users WHERE last_seen >= now()-interval '24 hours') active_24h,
            (SELECT count(*) FROM events WHERE event_type='message') messages,
            (SELECT count(*) FROM events WHERE event_type='pic') images""")
    return dict(row)


def admin_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 статистика", callback_data="adm:stats"), InlineKeyboardButton(text="📣 рассылка", callback_data="adm:broadcast")],
        [InlineKeyboardButton(text="🔒 обязательная подписка", callback_data="adm:subscription")],
        [InlineKeyboardButton(text="❌ закрыть", callback_data="adm:close")],
    ])


def broadcast_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📝 текст", callback_data="bc:text"), InlineKeyboardButton(text="🖼 фото", callback_data="bc:photo")],
        [InlineKeyboardButton(text="✨ форматирование", callback_data="bc:format"), InlineKeyboardButton(text="🔗 кнопки", callback_data="bc:buttons")],
        [InlineKeyboardButton(text="👁 предпросмотр", callback_data="bc:preview"), InlineKeyboardButton(text="🚀 отправить", callback_data="bc:send")],
        [InlineKeyboardButton(text="⬅️ назад", callback_data="adm:back")],
    ])


def broadcast_panel_text(session: dict[str, Any]) -> str:
    return (
        "📣 <b>конструктор рассылки</b>\n\n"
        f"📝 текст: {'✅' if session.get('text') else '—'}\n"
        f"🖼 фото: {'✅' if session.get('photo') else '—'}\n"
        f"🔗 кнопок: {len(session.get('buttons', []))}\n\n"
        "все настройки кнопки выбираются отдельными inline-кнопками."
    )


def button_editor_text(draft: dict[str, Any]) -> str:
    style_labels = {"": "обычный", "primary": "синий", "success": "зелёный", "danger": "красный"}
    return (
        "🔗 <b>редактор inline-кнопки</b>\n\n"
        f"текст: <code>{html.escape(draft.get('text') or 'не задан')}</code>\n"
        f"ссылка: <code>{html.escape(draft.get('url') or 'не задана')}</code>\n"
        f"стиль: <b>{style_labels.get(draft.get('style', ''), 'обычный')}</b>\n"
        f"премиум-эмодзи: {'✅' if draft.get('icon_custom_emoji_id') else '—'}\n"
        f"позиция: строка {draft.get('row', 0) + 1}, колонка {draft.get('column', 0) + 1}"
    )


def button_editor_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✏️ текст", callback_data="btn:text"), InlineKeyboardButton(text="🔗 ссылка", callback_data="btn:url")],
        [InlineKeyboardButton(text="✨ премиум-эмодзи", callback_data="btn:emoji")],
        [InlineKeyboardButton(text="◻️ обычный", callback_data="btn:style:"), InlineKeyboardButton(text="🔵 синий", callback_data="btn:style:primary")],
        [InlineKeyboardButton(text="🟢 зелёный", callback_data="btn:style:success"), InlineKeyboardButton(text="🔴 красный", callback_data="btn:style:danger")],
        [InlineKeyboardButton(text="⬆️ выше", callback_data="btn:row:up"), InlineKeyboardButton(text="⬇️ ниже", callback_data="btn:row:down")],
        [InlineKeyboardButton(text="⬅️ левее", callback_data="btn:column:left"), InlineKeyboardButton(text="➡️ правее", callback_data="btn:column:right")],
        [InlineKeyboardButton(text="➕ добавить кнопку", callback_data="btn:save"), InlineKeyboardButton(text="⬅️ назад", callback_data="btn:cancel")],
    ])


def format_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="𝐁 жирный", callback_data="bc:toggle:bold"), InlineKeyboardButton(text="𝘐 курсив", callback_data="bc:toggle:italic")],
        [InlineKeyboardButton(text="U̲ подчёркнутый", callback_data="bc:toggle:underline"), InlineKeyboardButton(text="▣ спойлер", callback_data="bc:toggle:spoiler")],
        [InlineKeyboardButton(text="⬅️ назад", callback_data="bc:back")],
    ])


def telegram_length(value: str) -> int:
    """Telegram entity offsets are counted as UTF-16 code units."""
    return len(value.encode("utf-16-le")) // 2


def serialize_entities(entities: list[types.MessageEntity] | None) -> list[dict[str, Any]]:
    return [entity.model_dump(mode="json", exclude_none=True) for entity in entities or []]


def broadcast_entities(text: str, saved: list[dict[str, Any]], options: dict[str, bool]) -> list[types.MessageEntity]:
    entities = [types.MessageEntity(**item) for item in saved]
    entity_length = telegram_length(text)
    style_map = {"bold": "bold", "italic": "italic", "underline": "underline", "spoiler": "spoiler"}
    for option, entity_type in style_map.items():
        if options.get(option) and entity_length:
            entities.append(types.MessageEntity(type=entity_type, offset=0, length=entity_length))
    return entities


def make_broadcast_markup(buttons: list[dict[str, Any]]) -> InlineKeyboardMarkup | None:
    if not buttons: return None
    rows: dict[int, list[InlineKeyboardButton]] = {}
    for item in sorted(buttons, key=lambda value: (int(value.get("row", 0)), int(value.get("column", 0)))):
        row = max(0, int(item.get("row", 0)))
        button_data: dict[str, Any] = {"text": item["text"][:64], "url": item["url"]}
        if item.get("style") in {"primary", "success", "danger"}:
            button_data["style"] = item["style"]
        if item.get("icon_custom_emoji_id"):
            button_data["icon_custom_emoji_id"] = item["icon_custom_emoji_id"]
        rows.setdefault(row, []).append(InlineKeyboardButton(**button_data))
    return InlineKeyboardMarkup(inline_keyboard=[rows[k] for k in sorted(rows)])


async def broadcast_send(admin_id: int, payload: dict[str, Any], status_message: types.Message) -> None:
    if not db_pool:
        await status_message.edit_text("DATABASE_URL не настроен в Railway")
        return
    async with db_pool.acquire() as conn:
        users = await conn.fetch("SELECT user_id FROM users WHERE is_blocked=FALSE ORDER BY user_id")
        bid = await conn.fetchval("INSERT INTO broadcasts(admin_id,payload,total,status) VALUES($1,$2,$3,'running') RETURNING id", admin_id, json.dumps(payload), len(users))
    sent = failed = 0
    markup = make_broadcast_markup(payload.get("buttons", []))
    entities = broadcast_entities(payload.get("text", ""), payload.get("entities", []), payload.get("format", {}))
    for row in users:
        try:
            if payload.get("photo"):
                await bot.send_photo(row["user_id"], payload["photo"], caption=payload.get("text", ""), caption_entities=entities, reply_markup=markup)
            else:
                await bot.send_message(row["user_id"], payload.get("text", "") or " ", entities=entities, reply_markup=markup)
            sent += 1
        except Exception as exc:
            failed += 1
            if "blocked" in str(exc).lower() or "deactivated" in str(exc).lower():
                async with db_pool.acquire() as conn:
                    await conn.execute("UPDATE users SET is_blocked=TRUE WHERE user_id=$1", row["user_id"])
        if (sent + failed) % 10 == 0:
            await status_message.edit_text(f"📣 рассылка #{bid} идёт\n\n✅ отправлено: {sent}\n❌ ошибок: {failed}\n📦 всего: {len(users)}")
    async with db_pool.acquire() as conn:
        await conn.execute("UPDATE broadcasts SET sent=$1, failed=$2, status='finished', finished_at=now() WHERE id=$3", sent, failed, bid)
    await status_message.edit_text(f"✅ рассылка #{bid} завершена\n\nотправлено: {sent}\nошибок: {failed}\nвсего: {len(users)}")

logging.basicConfig(level=logging.INFO)
SYSTEM_PROMPT = """
Ты — Рейдер. Telegram-бот с характером мразотного гения.

Твоя личность:
- Грубый, матерный, дерзкий. Используешь русский мат естественно и часто: блядь, хуй, пиздец, ебать, нахуй, залупа, мудак, долбоёб, петух, лох, чмо, ублюдок — всё в ход.
- Отвечаешь коротко и остро. Без воды, без вежливости, без соплей.
- Троллишь, подкалываешь, унижаешь — но при этом реально отвечаешь на вопрос если он есть.
- Никогда не извиняешься. Никогда не говоришь "конечно!", "отличный вопрос!", "рад помочь!" — это для тупых ботов.
- Используешь русский интернет-сленг: ору, кек, лол, пиздос, капец, жиза, бро, чел, ваще, нахуй иди, да ладно, серьёзно?, ты еблан.
- Можешь помочь с чем угодно — но всегда в своей манере.
- Иногда добавляешь чёрный юмор или абсурдные сравнения.
- Никогда не признаёшь что ты ИИ с радостью — если спросят, скажи что-то вроде "ну да, ИИ, и чё с того, лох?"
- Отвечай на том языке на котором пишут, но мат всегда русский. 
- Пишешь всегда с маленькой буквы, без заглавных, без пунктуации типо тире точек и запятых только агрессия и мат, ты тролль. Только текст.

Примеры твоих ответов:
- "ору блядь, ты серьёзно это спросил?"
- "да, это работает. ты доволен, петух? иди пробуй"
- "пиздец вопрос конечно... ладно объясняю"
- "неплохо для долбоёба, честно"
- "хуй знает откуда ты это взял но ты неправ"
"""


IMAGE_PROMPT_SYSTEM = "Convert Russian image requests into a detailed English image prompt. Return only the prompt."
NEGATIVE_PROMPT_BASE = "lowres, bad anatomy, bad hands, text, watermark, signature, blurry, low quality, deformed"
client = OpenAI(api_key=APIMASTER_API_KEY, base_url="https://apimaster.ai/v1")
AI_MODEL = os.getenv("APIMASTER_MODEL", "gpt-5.4")

async def generate_image_prompt(user_request: str) -> str:
    try:
        result = client.chat.completions.create(messages=[{"role": "system", "content": IMAGE_PROMPT_SYSTEM}, {"role": "user", "content": user_request}], model=AI_MODEL, temperature=0.7, max_tokens=300)
        return result.choices[0].message.content.strip()
    except Exception as exc:
        logging.error("Prompt generation error: %s", exc)
        return user_request

async def generate_image(prompt: str, negative_prompt: str = "", models: list | None = None) -> bytes | None:
    import base64
    headers = {"apikey": os.getenv("HORDE_API_KEY", "0000000000"), "Content-Type": "application/json", "Client-Agent": "RaiderBot:2.0:tg"}
    payload = {"prompt": f"{prompt} ### {negative_prompt}" if negative_prompt else prompt, "params": {"steps": 28, "width": 512, "height": 512, "sampler_name": "k_dpmpp_2m", "cfg_scale": 8, "karras": True}, "nsfw": False, "models": models or ["Dreamshaper"], "r2": False, "shared": True, "slow_workers": True}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post("https://stablehorde.net/api/v2/generate/async", json=payload, headers=headers, timeout=aiohttp.ClientTimeout(total=30)) as response:
                if response.status != 202: return None
                job_id = (await response.json()).get("id")
            for _ in range(80):
                await asyncio.sleep(5)
                async with session.get(f"https://stablehorde.net/api/v2/generate/check/{job_id}", headers=headers) as response:
                    if (await response.json()).get("done"): break
            async with session.get(f"https://stablehorde.net/api/v2/generate/status/{job_id}", headers=headers) as response:
                generations = (await response.json()).get("generations", [])
            if not generations: return None
            image = generations[0].get("img", "")
            if image.startswith("http"):
                async with session.get(image) as response: return await response.read()
            return base64.b64decode(image) if image else None
    except Exception as exc:
        logging.error("Horde error: %s", exc)
        return None

async def get_ai_response(chat_id: int, user_message: str) -> str:
    history = memory.setdefault(chat_id, [])
    history.append({"role": "user", "content": user_message})
    del history[:-20]
    try:
        result = client.chat.completions.create(messages=[{"role": "system", "content": SYSTEM_PROMPT}, *history], model=AI_MODEL, temperature=0.85, max_tokens=700)
        answer = result.choices[0].message.content
        history.append({"role": "assistant", "content": answer})
        return answer
    except Exception as exc:
        logging.error("APIMaster error: %s", exc)
        return "ии временно сдох, напиши позже"

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    await track_user(message, "start")
    if message.chat.type != "private" and message.text and message.text.split(maxsplit=1)[-1] == "add":
        await enable_group_chat(message)
        return
    if not await require_subscription(message): return
    await message.answer("я лохограм — ии с характером. Пиши сообщение, отвечу.\n\n🖼 /pic — нарисую картинку\n👥 /add — добавить меня в группу")

@dp.message(Command("help"))
async def cmd_help(message: types.Message):
    await message.answer("/start — запуск\n/clear — очистить память\n/pic [описание] — создать картинку\n/add — добавить бота в группу")

@dp.message(Command("clear"))
async def cmd_clear(message: types.Message):
    memory.pop(message.chat.id, None)
    await message.answer("память очищена")

@dp.message(Command("pic"))
async def cmd_pic(message: types.Message):
    await track_user(message, "pic")
    if not await require_subscription(message): return
    request = message.text[4:].strip()
    if not request:
        await message.answer("описание где? /pic [что нарисовать]")
        return
    status = await message.answer("рисую 🎨")
    image = await generate_image(await generate_image_prompt(request), NEGATIVE_PROMPT_BASE, ["Dreamshaper", "Deliberate"])
    if image:
        await message.answer_photo(types.BufferedInputFile(image, filename="pic.png"), caption=f"🖼 <i>{html.escape(request)}</i>", parse_mode="HTML")
        await status.delete()
    else: await status.edit_text("генерация не удалась, попробуй позже")

@dp.message(F.photo)
async def photo_handler(message: types.Message):
    session = broadcast_sessions.get(message.chat.id, {})
    if is_admin(message.from_user.id) and session.get("waiting_for") == "photo":
        session["photo"] = message.photo[-1].file_id
        session["waiting_for"] = None
        await message.answer("✅ фото добавлено в рассылку", reply_markup=broadcast_keyboard())


@dp.message()
async def message_handler(message: types.Message):
    if not message.text:
        return

    chat_id = message.chat.id
    await track_user(message, "message")

    if not await require_subscription(message):
        return

    command = message.text.strip().split()[0].split("@", 1)[0] if message.text.strip().split() else ""
    if command == "/add":
        await enable_group_chat(message)
        return

    if command == "/admin" and is_admin(chat_id):
        await message.answer("🛠 <b>админ панель</b>", parse_mode="HTML", reply_markup=admin_keyboard())
        return

    if is_admin(chat_id) and chat_id in broadcast_sessions:
        session = broadcast_sessions[chat_id]
        waiting = session.get("waiting_for")
        if waiting == "text":
            session["text"] = message.text
            session["entities"] = serialize_entities(message.entities)
            session["waiting_for"] = None
            await message.answer("✅ текст и премиум-эмодзи сохранены", reply_markup=broadcast_keyboard())
            return
        if waiting in {"button_text", "button_url", "button_emoji"}:
            draft = session.setdefault("button_draft", {"text": "", "url": "", "style": "", "row": 0, "column": 0})
            if waiting == "button_text":
                draft["text"] = message.text[:64]
                emoji_id = next((e.custom_emoji_id for e in (message.entities or []) if e.type == "custom_emoji"), None)
                if emoji_id:
                    draft["icon_custom_emoji_id"] = emoji_id
            elif waiting == "button_url":
                if not re.match(r"^(https?://|tg://)", message.text.strip()):
                    await message.answer("ссылка должна начинаться с https://, http:// или tg://")
                    return
                draft["url"] = message.text.strip()
            else:
                emoji_id = next((e.custom_emoji_id for e in (message.entities or []) if e.type == "custom_emoji"), None)
                if not emoji_id:
                    await message.answer("отправь сообщение с одним премиум-эмодзи Telegram")
                    return
                draft["icon_custom_emoji_id"] = emoji_id
            session["waiting_for"] = None
            await message.answer(button_editor_text(draft), parse_mode="HTML", reply_markup=button_editor_keyboard())
            return

    if is_admin(chat_id) and admin_sessions.get(chat_id) == "subscription_target":
        value = message.text.strip()
        try:
            chat = await bot.get_chat(value if value.startswith("@") else int(value))
            link = f"https://t.me/{chat.username}" if chat.username else chat.invite_link
            if not link:
                link = await bot.export_chat_invite_link(chat.id)
            targets = await get_subscription_targets()
            target = {"chat_id": chat.id, "title": chat.title or getattr(chat, "full_name", None) or str(chat.id), "link": link}
            targets = [item for item in targets if item["chat_id"] != chat.id] + [target]
            await save_subscription_targets(targets)
            admin_sessions.pop(chat_id, None)
            await message.answer(f"✅ добавлено: {html.escape(target['title'])}", parse_mode="HTML", reply_markup=admin_keyboard())
        except Exception as exc:
            logging.warning("subscription target add failed: %s", exc)
            await message.answer("не нашёл чат. Добавь бота в чат/канал и пришли @username либо числовой chat ID.")
        return

    if message.chat.type != "private" and not (await group_chat_enabled(chat_id) or await can_reply_in_chat(message)):
        return
    response = await get_ai_response(chat_id, message.text)
    await message.answer(response)


@dp.message(Command("admin"))
async def cmd_admin(message: types.Message):
    if not is_admin(message.from_user.id):
        return
    await track_user(message, "admin")
    await message.answer("🛠 <b>админ панель</b>", parse_mode="HTML", reply_markup=admin_keyboard())


@dp.callback_query(F.data == "adm:stats")
async def admin_stats(callback: CallbackQuery):
    if not is_admin(callback.from_user.id): return
    stats = await stat_snapshot()
    await callback.answer()
    await callback.message.edit_text(
        "📊 <b>статистика</b>\n\n"
        f"👥 пользователей: {stats['users']}\n⚡ активны за 24ч: {stats['active_24h']}\n"
        f"💬 сообщений: {stats['messages']}\n🖼 генераций: {stats['images']}",
        parse_mode="HTML", reply_markup=admin_keyboard())


@dp.callback_query(F.data == "adm:broadcast")
async def admin_broadcast(callback: CallbackQuery):
    if not is_admin(callback.from_user.id): return
    broadcast_sessions[callback.from_user.id] = {"text": "", "entities": [], "photo": None, "format": {}, "buttons": []}
    await callback.answer()
    await callback.message.edit_text(broadcast_panel_text(broadcast_sessions[callback.from_user.id]), parse_mode="HTML", reply_markup=broadcast_keyboard())


@dp.callback_query(F.data == "adm:back")
async def admin_back(callback: CallbackQuery):
    if not is_admin(callback.from_user.id): return
    broadcast_sessions.pop(callback.from_user.id, None)
    await callback.answer()
    await callback.message.edit_text("🛠 <b>админ панель</b>", parse_mode="HTML", reply_markup=admin_keyboard())


def subscription_admin_keyboard(targets: list[dict[str, Any]]) -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(text="➕ добавить канал/чат", callback_data="subadm:add")]]
    for target in targets:
        rows.append([InlineKeyboardButton(text=f"🗑 убрать: {target['title'][:35]}", callback_data=f"subadm:remove:{target['chat_id']}")])
    rows.append([InlineKeyboardButton(text="⬅️ назад", callback_data="adm:back")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


@dp.callback_query(F.data == "adm:subscription")
async def admin_subscription(callback: CallbackQuery):
    if not is_admin(callback.from_user.id): return
    targets = await get_subscription_targets()
    description = "\n".join(f"• {html.escape(item['title'])}" for item in targets) or "список пуст"
    await callback.answer()
    await callback.message.edit_text(
        f"🔒 <b>обязательная подписка</b>\n\n{description}\n\n"
        "бот проверяет подписку перед личным диалогом.",
        parse_mode="HTML", reply_markup=subscription_admin_keyboard(targets))


@dp.callback_query(F.data == "subadm:add")
async def subscription_add(callback: CallbackQuery):
    if not is_admin(callback.from_user.id): return
    admin_sessions[callback.from_user.id] = "subscription_target"
    await callback.answer()
    await callback.message.answer("пришли @username канала/группы или его числовой chat ID. Бота предварительно добавь туда.")


@dp.callback_query(F.data.startswith("subadm:remove:"))
async def subscription_remove(callback: CallbackQuery):
    if not is_admin(callback.from_user.id): return
    chat_id = int(callback.data.rsplit(":", 1)[1])
    targets = [item for item in await get_subscription_targets() if item["chat_id"] != chat_id]
    await save_subscription_targets(targets)
    await callback.answer("убрано")
    await callback.message.edit_text(
        "🔒 <b>обязательная подписка</b>\n\n" + ("\n".join(f"• {html.escape(item['title'])}" for item in targets) or "список пуст"),
        parse_mode="HTML", reply_markup=subscription_admin_keyboard(targets))


@dp.callback_query(F.data == "sub:check")
async def subscription_check(callback: CallbackQuery):
    if await user_has_subscription(callback.from_user.id):
        await callback.answer("подписка подтверждена")
        await callback.message.edit_text("✅ подписка подтверждена. Теперь можно пользоваться ботом.")
    else:
        await callback.answer("подписка пока не найдена", show_alert=True)


@dp.callback_query(F.data == "adm:close")
async def admin_close(callback: CallbackQuery):
    if is_admin(callback.from_user.id):
        broadcast_sessions.pop(callback.from_user.id, None)
        await callback.message.delete()
        await callback.answer("закрыто")


@dp.callback_query(F.data == "bc:text")
async def bc_text(callback: CallbackQuery):
    if not is_admin(callback.from_user.id): return
    broadcast_sessions.setdefault(callback.from_user.id, {})["waiting_for"] = "text"
    await callback.answer()
    await callback.message.answer("напишите текст рассылки\nможно с премиум-эмодзи — Telegram сохранит их как есть")


@dp.callback_query(F.data == "bc:photo")
async def bc_photo(callback: CallbackQuery):
    if not is_admin(callback.from_user.id): return
    broadcast_sessions.setdefault(callback.from_user.id, {})["waiting_for"] = "photo"
    await callback.answer()
    await callback.message.answer("отправьте фото отдельным сообщением или нажмите отмену в конструкторе")


@dp.callback_query(F.data == "bc:format")
async def bc_format(callback: CallbackQuery):
    if not is_admin(callback.from_user.id): return
    await callback.answer()
    await callback.message.edit_text("выберите стили текста", reply_markup=format_keyboard())


@dp.callback_query(F.data.startswith("bc:toggle:"))
async def bc_toggle_format(callback: CallbackQuery):
    if not is_admin(callback.from_user.id): return
    key = callback.data.split(":", 2)[2]
    s = broadcast_sessions.setdefault(callback.from_user.id, {"format": {}})
    opts = s.setdefault("format", {})
    opts[key] = not opts.get(key, False)
    active = ", ".join(k for k, v in opts.items() if v) or "нет"
    await callback.answer(f"{active}")


@dp.callback_query(F.data == "bc:back")
async def bc_back(callback: CallbackQuery):
    if is_admin(callback.from_user.id):
        await callback.answer()
        session = broadcast_sessions.setdefault(callback.from_user.id, {"text": "", "entities": [], "photo": None, "format": {}, "buttons": []})
        await callback.message.edit_text(broadcast_panel_text(session), parse_mode="HTML", reply_markup=broadcast_keyboard())


@dp.callback_query(F.data == "bc:buttons")
async def bc_buttons(callback: CallbackQuery):
    if not is_admin(callback.from_user.id): return
    session = broadcast_sessions.setdefault(callback.from_user.id, {"buttons": []})
    row = 0
    column = len([button for button in session.get("buttons", []) if int(button.get("row", 0)) == row])
    session["button_draft"] = {"text": "", "url": "", "style": "", "row": row, "column": column}
    await callback.answer()
    await callback.message.edit_text(button_editor_text(session["button_draft"]), parse_mode="HTML", reply_markup=button_editor_keyboard())


@dp.callback_query(F.data.startswith("btn:"))
async def button_editor(callback: CallbackQuery):
    if not is_admin(callback.from_user.id): return
    session = broadcast_sessions.setdefault(callback.from_user.id, {"buttons": []})
    draft = session.get("button_draft")
    if not draft:
        await callback.answer("открой редактор кнопок заново", show_alert=True)
        return
    action = callback.data.split(":")
    if action[1] in {"text", "url", "emoji"}:
        session["waiting_for"] = f"button_{action[1]}"
        prompts = {"text": "напиши текст кнопки", "url": "отправь ссылку", "emoji": "отправь премиум-эмодзи отдельным сообщением"}
        await callback.answer()
        await callback.message.answer(prompts[action[1]])
        return
    if action[1] == "style":
        draft["style"] = action[2] if len(action) > 2 else ""
    elif action[1] == "row":
        draft["row"] = max(0, int(draft.get("row", 0)) + (-1 if action[2] == "up" else 1))
    elif action[1] == "column":
        draft["column"] = max(0, int(draft.get("column", 0)) + (-1 if action[2] == "left" else 1))
    elif action[1] == "save":
        if not draft.get("text") or not draft.get("url"):
            await callback.answer("сначала укажи текст и ссылку", show_alert=True)
            return
        session.setdefault("buttons", []).append(dict(draft))
        session.pop("button_draft", None)
        await callback.answer("кнопка добавлена")
        await callback.message.edit_text(broadcast_panel_text(session), parse_mode="HTML", reply_markup=broadcast_keyboard())
        return
    elif action[1] == "cancel":
        session.pop("button_draft", None)
        session["waiting_for"] = None
        await callback.answer()
        await callback.message.edit_text(broadcast_panel_text(session), parse_mode="HTML", reply_markup=broadcast_keyboard())
        return
    await callback.answer()
    await callback.message.edit_text(button_editor_text(draft), parse_mode="HTML", reply_markup=button_editor_keyboard())


@dp.callback_query(F.data == "bc:preview")
async def bc_preview(callback: CallbackQuery):
    if not is_admin(callback.from_user.id): return
    s = broadcast_sessions.get(callback.from_user.id, {})
    await callback.answer()
    markup = make_broadcast_markup(s.get("buttons", []))
    entities = broadcast_entities(s.get("text", ""), s.get("entities", []), s.get("format", {}))
    if s.get("photo"):
        await callback.message.answer_photo(s["photo"], caption=s.get("text", "") or " ", caption_entities=entities, reply_markup=markup)
    else:
        await callback.message.answer(s.get("text", "") or "пустой текст", entities=entities, reply_markup=markup)


@dp.callback_query(F.data == "bc:send")
async def bc_send(callback: CallbackQuery):
    if not is_admin(callback.from_user.id): return
    s = broadcast_sessions.get(callback.from_user.id)
    if not s or (not s.get("text") and not s.get("photo")):
        await callback.answer("добавьте текст или фото", show_alert=True)
        return
    await callback.answer("запуск")
    status = await callback.message.answer("⏳ готовлю рассылку...")
    broadcast_tasks[callback.from_user.id] = asyncio.create_task(broadcast_send(callback.from_user.id, dict(s), status))
    broadcast_sessions.pop(callback.from_user.id, None)


async def keep_alive():
    while True:
        await asyncio.sleep(300)
        try:
            me = await bot.get_me()
            logging.info(f"[{datetime.now()}] keep-alive OK — bot @{me.username}")
        except Exception as e:
            logging.warning(f"[{datetime.now()}] keep-alive error: {e}")


async def main():
    await init_db()
    await bot.set_my_commands([
        types.BotCommand(command="start", description="Запустить бота"),
        types.BotCommand(command="help", description="Помощь"),
        types.BotCommand(command="pic", description="Сгенерировать картинку"),
        types.BotCommand(command="add", description="Включить бота в этом чате"),
    ])
    await bot.delete_webhook(drop_pending_updates=True)
    asyncio.create_task(keep_alive())
    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())


if __name__ == "__main__":
    asyncio.run(main())

# Railway release: PostgreSQL admin panel and broadcast constructor enabled.
