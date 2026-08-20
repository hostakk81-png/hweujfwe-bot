import asyncio
import logging
import os
import io
import urllib.parse
from datetime import datetime
from dotenv import load_dotenv
import sys
import traceback
import json
import html
import re
from collections import Counter
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
from PIL import Image, ImageDraw, ImageFont

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
ava_sessions = {}
db_pool = None
broadcast_sessions: dict[int, dict[str, Any]] = {}
broadcast_tasks: dict[int, asyncio.Task] = {}
admin_sessions: dict[int, str] = {}


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
            (SELECT count(*) FROM events WHERE event_type IN ('pic','ava')) images""")
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


IMAGE_PROMPT_SYSTEM = """
You are an expert image generation prompt engineer specializing in cinematic anime profile avatars.
The user describes an image in Russian. Convert it into a detailed English prompt for Stable Diffusion.

CRITICAL RULES for avatar prompts:
- Always include: masterpiece, best quality, ultra-detailed, sharp focus, 8k
- Always make it a SQUARE PORTRAIT (centered composition, face/upper body)
- Add dramatic effects: glowing aura, particle effects, energy lightning, volumetric light rays, atmospheric haze
- Dark cinematic background with colored glow (NOT plain/flat/simple backgrounds)
- Character should look intense/cool, looking at viewer
- Add depth with: depth of field, bokeh background, foreground elements
- Style: professional digital art, concept art quality

Reply with ONLY the English prompt, no explanations.
"""

REFINE_PROMPT_SYSTEM = """
You are an expert at refining AI image generation prompts for cinematic anime avatars.
You will receive:
1. CURRENT PROMPT: the existing English image generation prompt
2. USER REQUEST: what the user wants to change or add (in Russian)

Your job: return an updated English prompt that keeps everything good from the original
and incorporates the user's requested changes naturally.
Always keep: masterpiece, best quality, dramatic effects, cinematic atmosphere.
Reply with ONLY the updated English prompt, nothing else.
"""

NEGATIVE_PROMPT_BASE = "lowres, bad anatomy, bad hands, text, watermark, signature, username, error, blurry, jpeg artifacts, cropped, worst quality, low quality, normal quality, ugly, deformed, mutation, extra limbs, missing limbs, flat background, plain background, simple background, solid background, white background, gradient background, boring background, washed out colors, overexposed, underexposed, bad composition, amateur"

NEGATIVE_PROMPT_ANIME = f"{NEGATIVE_PROMPT_BASE}, 3d render, photorealistic, cgi, realistic skin, poorly drawn face, bad face, fused body, extra fingers, missing fingers"

NEGATIVE_PROMPT_REAL = f"{NEGATIVE_PROMPT_BASE}, anime, cartoon, illustration, painting, drawing, 3d, cgi, deformed face"

AVA_STYLES = {
    "anime": {
        "label": "🎌 Аниме",
        "models": ["Dreamshaper", "Nova Anime XL", "Abyss OrangeMix"],
        "negative": NEGATIVE_PROMPT_ANIME,
        "prompt": "masterpiece, best quality, ultra-detailed, (cinematic anime portrait avatar:1.3), dramatic character, intense expression looking at viewer, dynamic pose, highly detailed anime face, detailed hair with highlights, glowing aura around body, particle effects floating, volumetric light rays, dark atmospheric background with colored bokeh lights, depth of field, sharp focus on face, professional digital art, concept art, 8k resolution, vibrant colors"
    },
    "realistic": {
        "label": "📸 Реализм",
        "models": ["Dreamshaper", "Deliberate"],
        "negative": NEGATIVE_PROMPT_REAL,
        "prompt": "masterpiece, best quality, ultra-detailed, (cinematic portrait avatar:1.3), professional photography, dramatic studio lighting, rim light, octane render quality, photorealistic face, detailed skin texture, intense expression looking at viewer, dark moody background with bokeh, volumetric fog, cinematic color grading, sharp focus, 8k resolution, award winning photography"
    },
    "cyberpunk": {
        "label": "🤖 Киберпанк",
        "models": ["Dreamshaper", "Abyss OrangeMix"],
        "negative": NEGATIVE_PROMPT_ANIME,
        "prompt": "masterpiece, best quality, ultra-detailed, (cinematic cyberpunk anime portrait avatar:1.3), neon blue and purple glowing effects, cybernetic implants glowing, futuristic dark city background with rain reflections, neon light particles, electric sparks, circuit pattern aura, dramatic neon rim lighting, intense expression looking at viewer, dark atmospheric fog, holographic elements, 8k, sharp focus"
    },
    "cartoon": {
        "label": "🎨 Мультяшный",
        "models": ["Dreamshaper", "Nova Anime XL"],
        "negative": NEGATIVE_PROMPT_ANIME,
        "prompt": "masterpiece, best quality, ultra-detailed, (cinematic cartoon portrait avatar:1.3), bold clean lineart, vibrant saturated colors, dramatic lighting with colored shadows, expressive face looking at viewer, dynamic stylized background with geometric shapes and light rays, professional cartoon illustration, thick outlines, cel-shaded, sharp and clean, modern animation style, 8k"
    },
    "fantasy": {
        "label": "🧝 Фэнтези",
        "models": ["Dreamshaper", "Nova Anime XL"],
        "negative": NEGATIVE_PROMPT_ANIME,
        "prompt": "masterpiece, best quality, ultra-detailed, (epic fantasy anime portrait avatar:1.3), dramatic magical aura with glowing runes, fantasy armor or mystical robes with intricate details, magical particles and sparkles floating, intense expression looking at viewer, dark mystical background with glowing magical circles, volumetric light from magic, ethereal atmosphere, deep colors, 8k, sharp focus"
    },
    "pixel": {
        "label": "👾 Пиксель-арт",
        "models": ["Deliberate", "Dreamshaper"],
        "negative": f"{NEGATIVE_PROMPT_BASE}, blurry, anti-aliased, smooth",
        "prompt": "masterpiece, best quality, (detailed pixel art portrait avatar:1.3), retro 16-bit RPG game style, crisp sharp pixels, detailed pixel character face, dynamic pixel art background with pixel effects and particles, vibrant pixel colors, clean pixel lineart, RPG character portrait, pixel art shading, professional pixel art, 8k equivalent detail"
    },
    "dark": {
        "label": "🖤 Тёмный",
        "models": ["Dreamshaper", "Abyss OrangeMix"],
        "negative": NEGATIVE_PROMPT_ANIME,
        "prompt": "masterpiece, best quality, ultra-detailed, (dark cinematic anime portrait avatar:1.3), dramatic dark atmosphere, deep shadows, glowing red or purple energy aura, shadow particles disintegrating, intense cold expression looking at viewer, dark background with subtle dark fog and distant glow, cinematic shadow lighting from below, contrast between darkness and glow, menacing atmosphere, 8k, sharp focus"
    },
    "graffiti": {
        "label": "🎭 Граффити",
        "models": ["Deliberate", "Dreamshaper"],
        "negative": NEGATIVE_PROMPT_BASE,
        "prompt": "masterpiece, best quality, ultra-detailed, (cinematic graffiti art portrait avatar:1.3), bold spray paint texture, vibrant saturated colors, dynamic urban background with graffiti wall and street elements, paint splatter effects, dramatic hip hop aesthetic, expressive character looking at viewer, bold black outlines, professional street art style, urban particles, dripping paint effects, 8k"
    },
    "oil_painting": {
        "label": "🖼 Масло",
        "models": ["Dreamshaper", "Deliberate"],
        "negative": NEGATIVE_PROMPT_REAL,
        "prompt": "masterpiece, best quality, ultra-detailed, (cinematic oil painting portrait avatar:1.3), rich deep brushstrokes visible, classical fine art style, dramatic chiaroscuro lighting, dark moody atmospheric background, renaissance composition, rich saturated colors, warm golden rim light, museum quality artwork, painted texture, intense character expression looking at viewer, 8k"
    },
    "chibi": {
        "label": "🌸 Чиби",
        "models": ["Dreamshaper", "Nova Anime XL"],
        "negative": NEGATIVE_PROMPT_ANIME,
        "prompt": "masterpiece, best quality, ultra-detailed, (cute chibi anime portrait avatar:1.3), super deformed style, oversized head small body, huge sparkly glowing eyes, soft glowing pastel background with stars and sparkles, adorable kawaii expression, pastel color palette, soft lighting, clean smooth lineart, magical particle effects, floating hearts and stars, 8k, sharp"
    },
    "vaporwave": {
        "label": "🌊 Вейпорвейв",
        "models": ["Dreamshaper", "Abyss OrangeMix"],
        "negative": NEGATIVE_PROMPT_ANIME,
        "prompt": "masterpiece, best quality, ultra-detailed, (cinematic vaporwave anime portrait avatar:1.3), retrowave aesthetic, neon pink purple and cyan palette, glitch effects on edges, synthwave grid background with sunset, neon glow outline around character, retro 80s japanese aesthetic, VHS scanlines overlay, neon particle effects, intense look at viewer, chromatic aberration, dramatic neon lighting, 8k"
    },
    "sketch": {
        "label": "✏️ Скетч",
        "models": ["Deliberate", "Dreamshaper"],
        "negative": f"{NEGATIVE_PROMPT_BASE}, color, colorful, painted",
        "prompt": "masterpiece, best quality, ultra-detailed, (cinematic pencil sketch portrait avatar:1.3), dramatic black and white, detailed crosshatching and linework, professional concept art sketch, intense expression looking at viewer, dynamic sketch lines suggesting motion, ink wash shading, strong contrast between black ink and white paper, loose energetic sketch strokes, artist's sketchbook quality, 8k equivalent"
    },
}

client = OpenAI(
    api_key=APIMASTER_API_KEY,
    base_url="https://apimaster.ai/v1",
)
AI_MODEL = os.getenv("APIMASTER_MODEL", "gpt-5.4")


def build_style_keyboard():
    buttons = []
    styles = list(AVA_STYLES.items())
    for i in range(0, len(styles), 2):
        row = [InlineKeyboardButton(
            text=styles[i][1]["label"],
            callback_data=f"ava_style:{styles[i][0]}"
        )]
        if i + 1 < len(styles):
            row.append(InlineKeyboardButton(
                text=styles[i + 1][1]["label"],
                callback_data=f"ava_style:{styles[i + 1][0]}"
            ))
        buttons.append(row)
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def build_after_ava_keyboard(style_key: str):
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✏️ Изменить", callback_data="ava_edit"),
            InlineKeyboardButton(text="🎲 Ещё раз", callback_data=f"ava_regen:{style_key}"),
        ],
        [
            InlineKeyboardButton(text="📝 Добавить ник/текст", callback_data="ava_nickname"),
            InlineKeyboardButton(text="🔄 Другой стиль", callback_data="ava_restart"),
        ],
    ])


async def generate_image_prompt(user_request: str, system: str = IMAGE_PROMPT_SYSTEM) -> str:
    try:
        response = client.chat.completions.create(
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user_request}
            ],
            model=AI_MODEL,
            temperature=0.7,
            max_tokens=300,
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        logging.error(f"Prompt generation error: {e}")
        return user_request


async def refine_prompt(current_prompt: str, user_request: str) -> str:
    try:
        response = client.chat.completions.create(
            messages=[
                {"role": "system", "content": REFINE_PROMPT_SYSTEM},
                {"role": "user", "content": f"CURRENT PROMPT: {current_prompt}\nUSER REQUEST: {user_request}"}
            ],
            model=AI_MODEL,
            temperature=0.7,
            max_tokens=300,
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        logging.error(f"Refine prompt error: {e}")
        return current_prompt


async def download_telegram_photo(file_id: str) -> str | None:
    """Download a Telegram photo and return it as base64 string (resized to 768x768)."""
    import base64 as b64mod
    try:
        file = await bot.get_file(file_id)
        file_url = f"https://api.telegram.org/file/bot{BOT_TOKEN}/{file.file_path}"
        async with aiohttp.ClientSession() as session:
            async with session.get(file_url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                data = await resp.read()
        img = Image.open(io.BytesIO(data)).convert("RGB")
        img = img.resize((768, 768), Image.LANCZOS)
        buf = io.BytesIO()
        img.save(buf, format="PNG")
        return b64mod.b64encode(buf.getvalue()).decode("utf-8")
    except Exception as e:
        logging.error(f"download_telegram_photo error: {e}")
        return None


async def generate_image(
    prompt: str,
    width: int = 512,
    height: int = 512,
    negative_prompt: str = "",
    models: list = None,
    source_image_b64: str = None,
    denoising_strength: float = 0.75
) -> bytes | None:
    import base64 as b64mod
    horde_key = os.environ.get("HORDE_API_KEY", "0000000000")
    api_headers = {"apikey": horde_key, "Content-Type": "application/json", "Client-Agent": "RaiderBot:2.0:tg"}
    if models is None:
        models = ["Dreamshaper"]
    full_prompt = prompt
    if negative_prompt:
        full_prompt = f"{prompt} ### {negative_prompt}"
    payload = {
        "prompt": full_prompt,
        "params": {
            "steps": 28,
            "width": width,
            "height": height,
            "sampler_name": "k_dpmpp_2m",
            "cfg_scale": 8,
            "karras": True,
            "hires_fix": False,
            "clip_skip": 2,
            "denoising_strength": denoising_strength if source_image_b64 else 1.0,
        },
        "nsfw": False,
        "models": models,
        "r2": False,
        "shared": True,
        "slow_workers": True,
    }
    if source_image_b64:
        payload["source_image"] = source_image_b64
        payload["source_processing"] = "img2img"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                "https://stablehorde.net/api/v2/generate/async",
                json=payload, headers=api_headers,
                timeout=aiohttp.ClientTimeout(total=30)
            ) as resp:
                if resp.status != 202:
                    logging.warning(f"Horde submit failed: {resp.status} — {await resp.text()}")
                    return None
                job = await resp.json()
                job_id = job.get("id")

            if not job_id:
                logging.error("Horde: no job_id in response")
                return None

            logging.info(f"Horde job submitted: {job_id}")

            for attempt in range(80):
                await asyncio.sleep(5)
                try:
                    async with session.get(
                        f"https://stablehorde.net/api/v2/generate/check/{job_id}",
                        headers=api_headers,
                        timeout=aiohttp.ClientTimeout(total=10)
                    ) as resp:
                        check = await resp.json()
                        is_done = check.get("done", False)
                        queue = check.get("queue_position", "?")
                        eta = check.get("wait_time", "?")
                        logging.info(f"Horde [{attempt}]: done={is_done} queue={queue} eta={eta}s")
                        if is_done:
                            break
                except Exception as e:
                    logging.warning(f"Horde check error: {e}")
                    continue

            async with session.get(
                f"https://stablehorde.net/api/v2/generate/status/{job_id}",
                headers=api_headers,
                timeout=aiohttp.ClientTimeout(total=20)
            ) as resp:
                result = await resp.json()

            gens = result.get("generations", [])
            if not gens:
                logging.warning(f"Horde: no generations. Full response: {result}")
                return None

            img_data = gens[0].get("img", "")
            if not img_data:
                logging.warning("Horde: img field is empty")
                return None

            if img_data.startswith("http"):
                async with session.get(img_data, timeout=aiohttp.ClientTimeout(total=30)) as img_resp:
                    data = await img_resp.read()
            else:
                data = b64mod.b64decode(img_data)

            logging.info(f"Horde OK, size={len(data)}")
            return data

    except Exception as e:
        logging.error(f"Horde error: {e}")
    return None


def add_text_overlay(image_bytes: bytes, nickname: str, tagline: str = "") -> bytes:
    img = Image.open(io.BytesIO(image_bytes)).convert("RGBA")
    w, h = img.size

    overlay = Image.new("RGBA", img.size, (0, 0, 0, 0))
    draw = ImageDraw.Draw(overlay)

    gradient_height = int(h * 0.30)
    for y in range(gradient_height):
        alpha = int(200 * (y / gradient_height))
        draw.rectangle(
            [(0, h - gradient_height + y), (w, h - gradient_height + y + 1)],
            fill=(0, 0, 0, alpha)
        )

    try:
        nick_font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", size=int(h * 0.07))
        tag_font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", size=int(h * 0.04))
    except Exception:
        nick_font = ImageFont.load_default()
        tag_font = ImageFont.load_default()

    nick_bbox = draw.textbbox((0, 0), nickname, font=nick_font)
    nick_w = nick_bbox[2] - nick_bbox[0]
    nick_x = (w - nick_w) // 2
    nick_y = h - int(h * 0.18)

    for dx, dy in [(-2, -2), (2, -2), (-2, 2), (2, 2)]:
        draw.text((nick_x + dx, nick_y + dy), nickname, font=nick_font, fill=(0, 0, 0, 220))
    draw.text((nick_x, nick_y), nickname, font=nick_font, fill=(255, 255, 255, 255))

    if tagline:
        tag_bbox = draw.textbbox((0, 0), tagline, font=tag_font)
        tag_w = tag_bbox[2] - tag_bbox[0]
        tag_x = (w - tag_w) // 2
        tag_y = nick_y + int(h * 0.09)
        draw.text((tag_x, tag_y), tagline, font=tag_font, fill=(200, 200, 200, 220))

    result = Image.alpha_composite(img, overlay).convert("RGB")
    buf = io.BytesIO()
    result.save(buf, format="PNG")
    return buf.getvalue()


async def get_ai_response(chat_id: int, user_message: str) -> str:
    if chat_id not in memory:
        memory[chat_id] = []

    memory[chat_id].append({"role": "user", "content": user_message})
    if len(memory[chat_id]) > 20:
        memory[chat_id] = memory[chat_id][-20:]

    try:
        chat_completion = client.chat.completions.create(
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                *memory[chat_id]
            ],
            model=AI_MODEL,
            temperature=0.85,
            max_tokens=700,
        )
        response = chat_completion.choices[0].message.content
        memory[chat_id].append({"role": "assistant", "content": response})
        return response
    except Exception as e:
        logging.error(f"APIMaster error: {e}")
        return "ИИ сдох. Напиши позже, лох."


@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    await track_user(message, "start")
    if not await require_subscription(message):
        return
    await message.answer(
        "о, припёрся. ну ладно.\n\n"
        "я <b>лохограм</b> — ии с характером, не то говно что ты ожидал.\n\n"
        "чё умею:\n"
        "🖼 /pic — нарисую что скажешь\n"
        "💬 просто пиши — поговорим, долбоёб\n\n"
        "ну давай, чё хочешь?",
        parse_mode="HTML"
    )


@dp.message(Command("help"))
async def cmd_help(message: types.Message):
    await message.answer(
        "ору, помощь ему нужна. ладно:\n\n"
        "/start — перезапуск если совсем тупой\n"
        "/clear — очистить память чата\n"
        "/pic [описание] — картинку нарисую\n"
        "/ava [описание] — аватарку сделаю\n\n"
        "после аватарки можешь:\n"
        "✏️ изменить — напишешь что добавить/убрать\n"
        "📝 добавить ник и подпись снизу\n"
        "🎲 перегенерить в том же стиле\n\n"
        "или просто пиши что хочешь"
    )


@dp.message(Command("clear"))
async def cmd_clear(message: types.Message):
    chat_id = message.chat.id
    if chat_id in memory:
        del memory[chat_id]
    if chat_id in ava_sessions:
        del ava_sessions[chat_id]
    await message.answer("Память чиста. Начинаем заново, мудила.")


@dp.message(Command("pic"))
async def cmd_pic(message: types.Message):
    await track_user(message, "pic")
    if not await require_subscription(message):
        return
    user_request = message.text[4:].strip()
    if not user_request:
        await message.answer("Описание где? /pic [что нарисовать]")
        return

    msg = await message.answer("Рисую... 🎨")
    english_prompt = await generate_image_prompt(user_request)
    logging.info(f"Pic prompt: {english_prompt}")
    image_data = await generate_image(
        english_prompt,
        negative_prompt=NEGATIVE_PROMPT_BASE,
        models=["Dreamshaper", "Deliberate"]
    )

    if image_data:
        try:
            await message.answer_photo(
                photo=types.BufferedInputFile(image_data, filename="pic.png"),
                caption=f"🖼 держи.\n\n<i>{user_request}</i>",
                parse_mode="HTML"
            )
            await msg.delete()
        except Exception as e:
            logging.error(f"Send photo error: {e}")
            await msg.edit_text("Не смог отправить картинку.")
    else:
        await msg.edit_text("Генерация не удалась. Попробуй позже или измени описание.")


@dp.message(Command("ava"))
async def cmd_ava(message: types.Message):
    await track_user(message, "ava")
    if not await require_subscription(message):
        return
    chat_id = message.chat.id
    user_desc = message.text[4:].strip()

    prev = ava_sessions.get(chat_id, {})
    ava_sessions[chat_id] = {
        "description": user_desc,
        "current_prompt": "",
        "style_key": "",
        "waiting_for": None,
        "last_image": None,
        "ref_image_b64": prev.get("ref_image_b64"),
        "ref_mode": prev.get("ref_mode"),
        "ref_denoising": prev.get("ref_denoising", 0.75),
    }

    text = "🎭 <b>Генерация аватарки</b>\n\nВыбери стиль:"
    if user_desc:
        text += f"\n\n<i>Твоё описание: {user_desc}</i>"
    if prev.get("ref_mode"):
        mode_labels = {"background": "🖼 фон", "style": "🎨 стиль", "atmosphere": "🌈 атмосфера"}
        text += f"\n\n<i>📎 Референс активен: {mode_labels.get(prev.get('ref_mode'), prev.get('ref_mode'))}</i>"

    await message.answer(text, reply_markup=build_style_keyboard(), parse_mode="HTML")


async def do_generate_avatar(chat_id: int, style_key: str, prompt: str, status_msg):
    style = AVA_STYLES[style_key]
    session = ava_sessions.get(chat_id, {})
    ref_b64 = session.get("ref_image_b64")
    ref_denoising = session.get("ref_denoising", 0.75)
    image_data = await generate_image(
        prompt,
        width=768, height=768,
        negative_prompt=style.get("negative", NEGATIVE_PROMPT_BASE),
        models=style.get("models", ["Dreamshaper"]),
        source_image_b64=ref_b64,
        denoising_strength=ref_denoising
    )

    if image_data:
        ava_sessions[chat_id]["current_prompt"] = prompt
        ava_sessions[chat_id]["style_key"] = style_key
        ava_sessions[chat_id]["last_image"] = image_data
        ava_sessions[chat_id]["waiting_for"] = None

        keyboard = build_after_ava_keyboard(style_key)
        try:
            await status_msg.answer_photo(
                photo=types.BufferedInputFile(image_data, filename="avatar.png"),
                caption=f"🎭 Стиль: <b>{style['label']}</b>\n\nДокручивай как хочешь 👇",
                reply_markup=keyboard,
                parse_mode="HTML"
            )
            await status_msg.delete()
        except Exception as e:
            logging.error(f"Send avatar error: {e}")
            await status_msg.edit_text("Не смог отправить. Попробуй /ava ещё раз.")
    else:
        await status_msg.edit_text(
            "Генерация не удалась 😤\n"
            "Попробуй ещё раз через /ava или смени описание."
        )


@dp.callback_query(F.data.startswith("ava_style:"))
async def ava_style_chosen(callback: CallbackQuery):
    style_key = callback.data.split(":")[1]
    chat_id = callback.from_user.id

    if style_key not in AVA_STYLES:
        await callback.answer("Неизвестный стиль.")
        return

    style = AVA_STYLES[style_key]
    session = ava_sessions.get(chat_id, {})
    user_desc = session.get("description", "")

    await callback.answer(f"{style['label']}")
    await callback.message.edit_text(
        f"⏳ Генерирую в стиле <b>{style['label']}</b>...\n\nМожет занять 1-3 минуты, не закрывай чат",
        parse_mode="HTML"
    )

    if user_desc:
        full_prompt = await generate_image_prompt(
            f"Cinematic avatar portrait. Character description: {user_desc}. Base style reference: {style['prompt']}"
        )
    else:
        full_prompt = style["prompt"]

    logging.info(f"Avatar [{style_key}]: {full_prompt}")

    if chat_id not in ava_sessions:
        ava_sessions[chat_id] = {}
    ava_sessions[chat_id].update({
        "description": user_desc,
        "style_key": style_key,
        "waiting_for": None,
    })

    await do_generate_avatar(chat_id, style_key, full_prompt, callback.message)


@dp.callback_query(F.data.startswith("ava_regen:"))
async def ava_regen(callback: CallbackQuery):
    style_key = callback.data.split(":")[1]
    chat_id = callback.from_user.id
    session = ava_sessions.get(chat_id, {})
    current_prompt = session.get("current_prompt", "")

    if not current_prompt:
        await callback.answer("Нет данных для регенерации, запусти /ava заново")
        return

    style = AVA_STYLES.get(style_key, AVA_STYLES["anime"])
    await callback.answer("Перегенерирую...")
    await callback.message.edit_caption(
        f"⏳ Перегенерирую в стиле <b>{style['label']}</b>...",
        parse_mode="HTML"
    )

    image_data = await generate_image(
        current_prompt,
        width=768, height=768,
        negative_prompt=style.get("negative", NEGATIVE_PROMPT_BASE),
        models=style.get("models", ["Dreamshaper"])
    )
    if image_data:
        ava_sessions[chat_id]["last_image"] = image_data
        keyboard = build_after_ava_keyboard(style_key)
        try:
            media = types.InputMediaPhoto(
                media=types.BufferedInputFile(image_data, filename="avatar.png"),
                caption=f"🎭 Стиль: <b>{style['label']}</b>\n\nДокручивай как хочешь 👇",
                parse_mode="HTML"
            )
            await callback.message.edit_media(media=media, reply_markup=keyboard)
        except Exception:
            await callback.message.answer_photo(
                photo=types.BufferedInputFile(image_data, filename="avatar.png"),
                caption=f"🎭 Стиль: <b>{style['label']}</b>\n\nДокручивай как хочешь 👇",
                reply_markup=keyboard,
                parse_mode="HTML"
            )
    else:
        await callback.message.edit_caption("Генерация не удалась. Попробуй ещё раз.")


@dp.callback_query(F.data == "ava_edit")
async def ava_edit(callback: CallbackQuery):
    chat_id = callback.from_user.id
    session = ava_sessions.get(chat_id)

    if not session or not session.get("current_prompt"):
        await callback.answer("Нет активной аватарки, запусти /ava")
        return

    ava_sessions[chat_id]["waiting_for"] = "refinement"
    await callback.answer()
    await callback.message.reply(
        "✏️ Напиши что изменить:\n\n"
        "Примеры:\n"
        "• <i>добавь виньетку</i>\n"
        "• <i>сделай неоновое освещение</i>\n"
        "• <i>смени фон на закат в городе</i>\n"
        "• <i>добавь шрамы на лице</i>\n"
        "• <i>сделай цвет волос синим</i>",
        parse_mode="HTML"
    )


@dp.callback_query(F.data == "ava_nickname")
async def ava_nickname_cb(callback: CallbackQuery):
    chat_id = callback.from_user.id
    session = ava_sessions.get(chat_id)

    if not session or not session.get("last_image"):
        await callback.answer("Нет активной аватарки, запусти /ava")
        return

    ava_sessions[chat_id]["waiting_for"] = "nickname"
    await callback.answer()
    await callback.message.reply(
        "📝 Напиши ник и подпись в таком формате:\n\n"
        "<b>Ник | Подпись снизу</b>\n\n"
        "Примеры:\n"
        "• <code>РЕЙДЕР | ИИ с характером</code>\n"
        "• <code>xXNightXx</code>\n"
        "• <code>Аня | просто аня</code>",
        parse_mode="HTML"
    )


@dp.callback_query(F.data == "ava_restart")
async def ava_restart(callback: CallbackQuery):
    chat_id = callback.from_user.id
    session = ava_sessions.get(chat_id, {})
    user_desc = session.get("description", "")

    if chat_id in ava_sessions:
        ava_sessions[chat_id]["waiting_for"] = None

    text = "🎭 <b>Выбери новый стиль:</b>"
    if user_desc:
        text += f"\n\n<i>Описание: {user_desc}</i>"

    await callback.answer()
    await callback.message.reply(text, reply_markup=build_style_keyboard(), parse_mode="HTML")


def build_ref_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🖼 Использовать как фон", callback_data="ref_mode:background"),
            InlineKeyboardButton(text="🎨 Скопировать стиль", callback_data="ref_mode:style"),
        ],
        [
            InlineKeyboardButton(text="🌈 Взять атмосферу/цвета", callback_data="ref_mode:atmosphere"),
            InlineKeyboardButton(text="❌ Не использовать", callback_data="ref_mode:cancel"),
        ],
    ])


@dp.message(F.photo)
async def photo_handler(message: types.Message):
    chat_id = message.chat.id
    caption = message.caption or ""

    if is_admin(chat_id) and broadcast_sessions.get(chat_id, {}).get("waiting_for") == "photo":
        broadcast_sessions[chat_id]["photo"] = message.photo[-1].file_id
        broadcast_sessions[chat_id]["waiting_for"] = None
        await message.answer("✅ фото добавлено в рассылку", reply_markup=broadcast_keyboard())
        return

    await message.answer(
        "📎 <b>Фото получено!</b>\n\n"
        "Что с ним делаем при генерации авы?\n\n"
        "• <b>Фон</b> — твоё фото станет основой, бот добавит персонажа сверху\n"
        "• <b>Стиль</b> — бот скопирует арт-стиль/рисовку с этого фото\n"
        "• <b>Атмосфера</b> — возьмёт цвета и настроение\n",
        reply_markup=build_ref_keyboard(),
        parse_mode="HTML"
    )

    file_id = message.photo[-1].file_id
    if chat_id not in ava_sessions:
        ava_sessions[chat_id] = {}
    ava_sessions[chat_id]["_pending_file_id"] = file_id
    if caption:
        ava_sessions[chat_id]["description"] = caption


@dp.callback_query(F.data.startswith("ref_mode:"))
async def ref_mode_chosen(callback: CallbackQuery):
    chat_id = callback.from_user.id
    mode = callback.data.split(":")[1]

    if mode == "cancel":
        if chat_id in ava_sessions:
            ava_sessions[chat_id]["ref_image_b64"] = None
            ava_sessions[chat_id]["ref_mode"] = None
            ava_sessions[chat_id].pop("_pending_file_id", None)
        await callback.answer("Референс убран")
        await callback.message.edit_text("❌ Фото не будет использоваться.\n\nЗапусти /ava для генерации.")
        return

    denoising_map = {"background": 0.55, "style": 0.82, "atmosphere": 0.70}
    denoising = denoising_map[mode]
    mode_labels = {"background": "🖼 фон", "style": "🎨 стиль", "atmosphere": "🌈 атмосфера"}

    await callback.answer("Загружаю фото...")
    await callback.message.edit_text("⏳ Загружаю и обрабатываю фото...")

    file_id = ava_sessions.get(chat_id, {}).get("_pending_file_id")
    if not file_id:
        await callback.message.edit_text("Не нашёл фото. Скинь его снова.")
        return

    b64 = await download_telegram_photo(file_id)
    if not b64:
        await callback.message.edit_text("Не смог загрузить фото. Попробуй ещё раз.")
        return

    if chat_id not in ava_sessions:
        ava_sessions[chat_id] = {}
    ava_sessions[chat_id]["ref_image_b64"] = b64
    ava_sessions[chat_id]["ref_mode"] = mode
    ava_sessions[chat_id]["ref_denoising"] = denoising
    ava_sessions[chat_id].pop("_pending_file_id", None)

    await callback.message.edit_text(
        f"✅ Готово! Режим: <b>{mode_labels[mode]}</b>\n\n"
        f"Теперь запусти /ava — фото будет применено.\n"
        f"Можешь описать что хочешь: <code>/ava аниме парень с мечом</code>",
        parse_mode="HTML"
    )


@dp.message()
async def message_handler(message: types.Message):
    if not message.text:
        return

    chat_id = message.chat.id
    await track_user(message, "message")

    if not await require_subscription(message):
        return

    if message.text.strip().split() and message.text.strip().split()[0] == "/admin" and is_admin(chat_id):
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

    if message.chat.type != "private" and not await can_reply_in_chat(message):
        return
    session = ava_sessions.get(chat_id)

    if session and session.get("waiting_for") == "refinement":
        ava_sessions[chat_id]["waiting_for"] = None
        current_prompt = session.get("current_prompt", "")
        style_key = session.get("style_key", "anime")
        style = AVA_STYLES.get(style_key, AVA_STYLES["anime"])

        msg = await message.answer(f"⏳ Применяю изменения в стиле <b>{style['label']}</b>...", parse_mode="HTML")

        new_prompt = await refine_prompt(current_prompt, message.text)
        logging.info(f"Refined prompt: {new_prompt}")

        await do_generate_avatar(chat_id, style_key, new_prompt, msg)
        return

    if session and session.get("waiting_for") == "nickname":
        ava_sessions[chat_id]["waiting_for"] = None
        last_image = session.get("last_image")

        if not last_image:
            await message.answer("Нет картинки для наложения текста. Запусти /ava заново.")
            return

        parts = message.text.split("|", 1)
        nickname = parts[0].strip()
        tagline = parts[1].strip() if len(parts) > 1 else ""

        try:
            result_image = add_text_overlay(last_image, nickname, tagline)
            style_key = session.get("style_key", "anime")
            keyboard = build_after_ava_keyboard(style_key)
            await message.answer_photo(
                photo=types.BufferedInputFile(result_image, filename="avatar_nick.png"),
                caption=f"🎭 Вот с ником: <b>{nickname}</b>{f' | {tagline}' if tagline else ''}\n\nСтавь на профиль 😎",
                reply_markup=keyboard,
                parse_mode="HTML"
            )
        except Exception as e:
            logging.error(f"Text overlay error: {e}")
            await message.answer("Не смог добавить текст. Попробуй ещё раз.")
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
    await bot.delete_webhook(drop_pending_updates=True)
    asyncio.create_task(keep_alive())
    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())


if __name__ == "__main__":
    asyncio.run(main())

# Railway release: PostgreSQL admin panel and broadcast constructor enabled.
