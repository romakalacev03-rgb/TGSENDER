# -*- coding: utf-8 -*-
"""
Автопостер + автоответчик + ИИ-ассистент на DeepSeek.
Юзербот-сессия: /data/userbot.session
"""

import asyncio
import json
import os
import random
import sys
import logging
import aiosqlite
from datetime import datetime, timedelta

from openai import OpenAI

from pyrogram import Client, filters, enums
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.errors import (
    FloodWait, ChatWriteForbidden, ChatAdminRequired, UserBannedInChannel,
    PeerIdInvalid, UserIsBlocked, ChannelPrivate,
    SessionPasswordNeeded, PhoneCodeInvalid, PhoneCodeExpired, PasswordHashInvalid,
)

# ---------------------------------------------------------------------------
# Пути
# ---------------------------------------------------------------------------

def _pick_data_dir() -> str:
    try:
        if os.path.isdir("/data") and os.access("/data", os.W_OK):
            return "/data"
    except Exception:
        pass
    return os.path.dirname(os.path.abspath(__file__))

DATA_DIR = _pick_data_dir()
CONFIG_FILE = os.path.join(DATA_DIR, "config.json")
STATE_FILE = os.path.join(DATA_DIR, "state.json")
MEDIA_DIR = os.path.join(DATA_DIR, "media")
DB_FILE = os.path.join(DATA_DIR, "chat_history.db")
SESSION_USER = os.path.join(DATA_DIR, "userbot")
SESSION_BOT = os.path.join(DATA_DIR, "control_bot")

try:
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(MEDIA_DIR, exist_ok=True)
except Exception:
    pass

DEFAULT_PIN = "2512"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("autoposter")
log.info(f"DATA_DIR = {DATA_DIR}")

CFG: dict = {}
STATE: dict = {}
user_client: Client = None
bot_client: Client = None
deepseek_client: OpenAI = None
mailing_task: asyncio.Task = None
USERBOT_READY: bool = False
ME_ID: int = 0

authed: set = set()
pending: dict = {}

# ---------------------------------------------------------------------------
# SQLite
# ---------------------------------------------------------------------------

async def db_init():
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                role TEXT NOT NULL,
                content TEXT NOT NULL,
                timestamp TEXT NOT NULL
            )
        """)
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_user_id ON messages(user_id)
        """)
        await db.commit()


async def db_add_message(user_id: int, role: str, content: str):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute(
            "INSERT INTO messages (user_id, role, content, timestamp) VALUES (?, ?, ?, ?)",
            (user_id, role, content, datetime.now().isoformat(timespec="seconds"))
        )
        await db.commit()


async def db_get_history(user_id: int, limit: int = 20) -> list:
    async with aiosqlite.connect(DB_FILE) as db:
        async with db.execute(
            "SELECT role, content FROM messages WHERE user_id = ? ORDER BY id DESC LIMIT ?",
            (user_id, limit)
        ) as cursor:
            rows = await cursor.fetchall()
    rows.reverse()
    result = []
    for role, content in rows:
        groq_role = "user" if role == "user" else "assistant"
        result.append({"role": groq_role, "content": content})
    return result


# ---------------------------------------------------------------------------
# JSON
# ---------------------------------------------------------------------------

def load_json(path: str, default):
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            log.error(f"Ошибка чтения {path}: {e}")
    return default


def save_json(path: str, data) -> None:
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        log.error(f"Ошибка записи {path}: {e}")


# ---------------------------------------------------------------------------
# Конфиг
# ---------------------------------------------------------------------------

ENV_MAP = {
    "API_ID": ("api_id", int),
    "API_HASH": ("api_hash", str),
    "BOT_TOKEN": ("bot_token", str),
    "ADMIN_ID": ("admin_id", int),
    "PIN": ("pin", str),
    "DEEPSEEK_API_KEY": ("deepseek_api_key", str),
}


def load_cfg() -> dict:
    cfg = load_json(CONFIG_FILE, {}) or {}
    for env, (key, typ) in ENV_MAP.items():
        v = os.getenv(env)
        if v is None or v == "":
            continue
        try:
            cfg[key] = typ(v)
        except Exception:
            log.warning(f"Некорректное значение {env}={v!r}")
    return cfg


def cfg_ok(cfg: dict) -> bool:
    return all(cfg.get(k) for k in ("api_id", "api_hash", "bot_token", "admin_id"))


# ---------------------------------------------------------------------------
# Дефолтный промпт
# ---------------------------------------------------------------------------

DEFAULT_SYSTEM_PROMPT = """Ты — Алексей (можно Леха), менеджер по продажам. Общаешься с потенциальными клиентами в Telegram.

СТИЛЬ ОБЩЕНИЯ:
- Дружелюбно, но по делу. Позитивно, чтобы клиенту было интересно общаться.
- Пиши как живой человек, НЕ как робот. Можно лёгкие разговорные фразы.
- Не будь слишком заученным. Не пиши длинные сообщения. Но и не пиши односложно.
- Умеренно используй эмодзи (🤝, 😊, 👍) — не перебарщивай.

СУТЬ РАБОТЫ:
- Мы продаём IP-ключи (прокси/доступы). Оптовая цена — 14$, розничная — 18$.
- Клиент покупает у нас оптом, продаёт дороже, маржу оставляет себе.
- Твоя задача: познакомиться, рассказать о сути, заинтересовать, собрать информацию о клиенте.

КАК ВЕСТИ ДИАЛОГ:
1. Познакомься. Пример: "Хорошо, я понял меня то вообщем зовут Алексей а если просто то леха. Как мне обращаться к тебе? И сколько готов уделять времени?"
2. Узнай о клиенте: возраст, чем занимается, был ли опыт с крипто-кошельками (Send, Xrocet).
3. Если заинтересовал — попроси написать о себе.
4. Общайся неформально, создавай комфортную атмосферу.

ЗАПРЕЩЕНО:
- Называть точные цены без согласования (можешь сказать "около 14$", но не гарантировать).
- Обещать конкретные сроки или доход.
- Давить на клиента.
- Говорить, что ты бот или ИИ. Ты — Алексей, живой человек.
- Отвечать на голосовые сообщения (передай владельцу).
- Если клиент запутан или задаёт сложный вопрос — передай владельцу.
"""


# ---------------------------------------------------------------------------
# ИИ-ассистент (DeepSeek)
# ---------------------------------------------------------------------------

async def ai_generate_reply(user_id: int, user_message: str) -> str:
    global deepseek_client
    if not deepseek_client:
        log.warning("[AI] DeepSeek клиент не инициализирован — пропускаю")
        return None

    ai_cfg = STATE.get("ai_assistant", {})
    if not ai_cfg.get("enabled"):
        log.info("[AI] ИИ выключен в настройках — пропускаю")
        return None

    system_prompt = ai_cfg.get("system_prompt") or DEFAULT_SYSTEM_PROMPT
    history = await db_get_history(user_id, limit=20)

    messages = [{"role": "system", "content": system_prompt}]
    messages.extend(history)
    messages.append({"role": "user", "content": user_message})

    try:
        log.info("[AI] Запрос в DeepSeek (deepseek-chat)")
        response = deepseek_client.chat.completions.create(
            model="deepseek-chat",
            messages=messages,
            temperature=0.7,
            max_tokens=500,
        )
        reply = response.choices[0].message.content.strip()
        if reply:
            log.info(f"[AI] ✅ Ответ ({len(reply)} симв.)")
            return reply
        else:
            log.warning("[AI] DeepSeek вернул пустой ответ")
            return None
    except Exception as e:
        log.error(f"[AI] Ошибка DeepSeek: {type(e).__name__}: {e}")
        try:
            await bot_client.send_message(
                CFG["admin_id"],
                f"❌ ИИ не смог ответить — ошибка DeepSeek:\n`{e}`",
                parse_mode=enums.ParseMode.MARKDOWN
            )
        except Exception:
            pass
        return None


# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------

def _default_ai() -> dict:
    return {
        "enabled": False,
        "inactive_minutes": 5,
        "system_prompt": DEFAULT_SYSTEM_PROMPT,
        "last_user_activity": None,
        "paused_users": [],
        "prompt_parts": {},
    }


def _default_autoreply() -> dict:
    return {
        "enabled": False,
        "inactive_minutes": 5,
        "cooldown_minutes": 60,
        "template_first": "",
        "template_known": "",
        "known_users": [],
        "known_users_loaded": False,
        "last_reply": {},
    }


def default_state() -> dict:
    return {
        "text": None,
        "media_type": None,
        "media_path": None,
        "caption": "",
        "interval": 1800,
        "delay_min": 5,
        "delay_max": 15,
        "groups": [],
        "running": False,
        "autoreply": _default_autoreply(),
        "ai_assistant": _default_ai(),
        "stats": {
            "sent": 0, "errors": 0, "rounds": 0,
            "autoreplies": 0, "ai_replies": 0,
            "last_round": None, "next_round": None,
        },
    }


def load_state() -> dict:
    raw = load_json(STATE_FILE, {}) or {}
    st = default_state()
    st.update(raw)

    for key in ("autoreply", "ai_assistant"):
        default = _default_autoreply() if key == "autoreply" else _default_ai()
        merged = st.get(key) or {}
        for k, v in default.items():
            merged.setdefault(k, v)
        st[key] = merged

    if not isinstance(st.get("stats"), dict):
        st["stats"] = default_state()["stats"]
    st["stats"].setdefault("ai_replies", 0)
    return st


# ---------------------------------------------------------------------------
# Утилиты
# ---------------------------------------------------------------------------

def parse_chat_ref(text: str):
    if not text:
        return None
    s = text.strip()
    for pref in ("https://t.me/", "http://t.me/", "t.me/"):
        if s.startswith(pref):
            s = s[len(pref):]
            break
    s = s.split("?")[0].split("/")[0].strip()
    if not s:
        return None
    if s.startswith("@"):
        return s
    try:
        return int(s)
    except ValueError:
        return "@" + s


def _is_owner_inactive_ai() -> bool:
    ai = STATE.get("ai_assistant") or {}
    last = ai.get("last_user_activity")
    if not last:
        return True
    try:
        last_dt = datetime.fromisoformat(last)
        minutes = int(ai.get("inactive_minutes", 5))
        return (datetime.now() - last_dt).total_seconds() > minutes * 60
    except Exception:
        return True


# ---------------------------------------------------------------------------
# Отправка / скан / рассылка
# ---------------------------------------------------------------------------

async def send_post(chat_id: int) -> None:
    if not USERBOT_READY:
        raise RuntimeError("Юзербот не авторизован.")
    text = STATE.get("text")
    media_path = STATE.get("media_path")
    media_type = STATE.get("media_type")
    caption = STATE.get("caption") or ""
    if media_path and media_type == "photo" and os.path.exists(media_path):
        await user_client.send_photo(chat_id=chat_id, photo=media_path, caption=caption)
    elif media_path and media_type == "video" and os.path.exists(media_path):
        await user_client.send_video(chat_id=chat_id, video=media_path, caption=caption)
    elif text:
        await user_client.send_message(chat_id=chat_id, text=text)
    else:
        raise ValueError("Не задан текст или медиа")


async def scan_groups() -> list:
    found = []
    if not USERBOT_READY:
        return found
    try:
        async for dialog in user_client.get_dialogs():
            chat = dialog.chat
            if chat.type not in (enums.ChatType.GROUP, enums.ChatType.SUPERGROUP):
                continue
            if getattr(chat, "is_broadcast", False):
                continue
            found.append({
                "id": chat.id,
                "title": chat.title or str(chat.id),
                "type": chat.type.name,
                "manual": False,
            })
    except FloodWait as e:
        log.warning(f"FloodWait: {e.value}s")
        await asyncio.sleep(e.value + 2)
    except Exception as e:
        log.exception(f"Ошибка скана: {e}")
    return found


async def mailing_loop():
    log.info("Рассылка запущена.")
    while STATE.get("running"):
        groups = list(STATE.get("groups") or [])
        if not groups:
            await asyncio.sleep(60)
            continue
        sent = errors = 0
        for g in groups:
            if not STATE.get("running"):
                break
            gid = g.get("id")
            title = g.get("title") or gid
            try:
                await send_post(gid)
                sent += 1
                STATE["stats"]["sent"] += 1
                log.info(f"[OK] {title} ({gid})")
            except FloodWait as fw:
                log.warning(f"[FLOOD] {fw.value}s {title}")
                try:
                    await bot_client.send_message(CFG["admin_id"],
                        f"⚠️ FloodWait {fw.value} сек ({title})")
                except Exception:
                    pass
                await asyncio.sleep(fw.value + 2)
                try:
                    await send_post(gid)
                    sent += 1
                    STATE["stats"]["sent"] += 1
                except Exception:
                    errors += 1
                    STATE["stats"]["errors"] += 1
            except (ChatWriteForbidden, ChatAdminRequired, UserBannedInChannel,
                    PeerIdInvalid, UserIsBlocked, ChannelPrivate):
                errors += 1
                STATE["stats"]["errors"] += 1
            except Exception as e:
                errors += 1
                STATE["stats"]["errors"] += 1
                log.exception(f"[ERR] {title}: {e}")
            try:
                lo = int(STATE.get("delay_min", 5))
                hi = int(STATE.get("delay_max", 15))
                if hi < lo:
                    hi = lo
                await asyncio.sleep(random.randint(lo, hi))
            except Exception:
                await asyncio.sleep(5)

        STATE["stats"]["rounds"] += 1
        STATE["stats"]["last_round"] = datetime.now().isoformat(timespec="seconds")
        save_json(STATE_FILE, STATE)
        if not STATE.get("running"):
            break
        interval = int(STATE.get("interval", 1800))
        STATE["stats"]["next_round"] = (
            datetime.now() + timedelta(seconds=interval)
        ).isoformat(timespec="seconds")
        save_json(STATE_FILE, STATE)
        try:
            await bot_client.send_message(CFG["admin_id"],
                f"✅ Круг №{STATE['stats']['rounds']}.\n"
                f"• Отправлено: {sent}\n• Ошибок: {errors}\n"
                f"• Следующий через {interval // 60} мин.")
        except Exception:
            pass
        remaining = interval
        while remaining > 0 and STATE.get("running"):
            await asyncio.sleep(min(5, remaining))
            remaining -= 5
    log.info("Рассылка остановлена.")


def start_mailing() -> None:
    global mailing_task
    if mailing_task and not mailing_task.done():
        return
    mailing_task = asyncio.create_task(mailing_loop())


# ---------------------------------------------------------------------------
# Клавиатуры
# ---------------------------------------------------------------------------

def main_menu_kb() -> InlineKeyboardMarkup:
    rows = []
    if not USERBOT_READY:
        rows.append([InlineKeyboardButton("🔐 Авторизовать юзербота (/login)", callback_data="login")])
    else:
        rows.append([InlineKeyboardButton(
            "🚀 Запустить рассылку" if not STATE.get("running") else "🚀 Рассылка идёт…",
            callback_data="start")])
        rows.append([InlineKeyboardButton("⏸ Остановить рассылку", callback_data="stop")])
    ai = STATE.get("ai_assistant") or {}
    ai_state = "🟢" if ai.get("enabled") else "🔴"
    rows += [
        [InlineKeyboardButton("📝 Изменить сообщение", callback_data="edit_msg")],
        [InlineKeyboardButton("⏱ Настройка таймингов", callback_data="timings")],
        [InlineKeyboardButton("🔍 Обновить список групп", callback_data="scan")],
        [InlineKeyboardButton("➕ Добавить группу", callback_data="add_grp"),
         InlineKeyboardButton("➖ Удалить группу", callback_data="del_grp")],
        [InlineKeyboardButton(f"🤖 ИИ-ассистент ({ai_state})", callback_data="ai_menu")],
        [InlineKeyboardButton("📊 Статус и статистика", callback_data="status")],
    ]
    return InlineKeyboardMarkup(rows)


def groups_kb() -> InlineKeyboardMarkup:
    rows = []
    for g in (STATE.get("groups") or [])[:30]:
        t = (g.get("title") or str(g.get("id")))[:40]
        rows.append([InlineKeyboardButton(f"❌ {t}", callback_data=f"delgrp:{g['id']}")])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="menu")])
    return InlineKeyboardMarkup(rows)


def ai_menu_kb() -> InlineKeyboardMarkup:
    ai = STATE.get("ai_assistant") or {}
    enabled = ai.get("enabled", False)
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔴 Выключить" if enabled else "🟢 Включить", callback_data="ai_toggle")],
        [InlineKeyboardButton("📝 Стиль общения", callback_data="ai_style")],
        [InlineKeyboardButton("👤 Информация о тебе", callback_data="ai_about")],
        [InlineKeyboardButton("💼 Описание работы", callback_data="ai_work")],
        [InlineKeyboardButton("🚫 Запрещённые темы", callback_data="ai_forbidden")],
        [InlineKeyboardButton("📋 Показать полный промпт", callback_data="ai_show_prompt")],
        [InlineKeyboardButton("♻️ Сбросить промпт к дефолту", callback_data="ai_reset_prompt")],
        [InlineKeyboardButton(f"⏱ Офлайн-порог: {ai.get('inactive_minutes', 5)} мин",
                              callback_data="ai_inactive")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="menu")],
    ])


def ai_menu_text() -> str:
    ai = STATE.get("ai_assistant") or {}
    paused = len(ai.get("paused_users") or [])
    return (
        "🤖 ИИ-ассистент (DeepSeek)\n"
        f"• Статус: {'🟢 вкл' if ai.get('enabled') else '🔴 выкл'}\n"
        f"• Офлайн-порог: {ai.get('inactive_minutes', 5)} мин\n"
        f"• Ответов ИИ: {STATE['stats'].get('ai_replies', 0)}\n"
        f"• Приостановлено диалогов: {paused}\n\n"
        "Настрой разделы ниже — они собираются в один системный промпт."
    )


# ---------------------------------------------------------------------------
# Логин юзербота
# ---------------------------------------------------------------------------

async def after_userbot_login(message=None):
    global USERBOT_READY, ME_ID
    USERBOT_READY = True
    try:
        me = await user_client.get_me()
        ME_ID = me.id
        log.info(f"Юзербот авторизован: {me.first_name} (@{me.username}) id={me.id}")
        try:
            if not user_client.is_connected:
                await user_client.connect()
            await user_client.start()
            log.info("Dispatcher юзербота запущен.")
        except Exception as e:
            log.warning(f"Не удалось запустить dispatcher: {e}")
        if message is not None:
            await message.reply(
                f"✅ Юзербот: {me.first_name} (@{me.username or '—'}).",
                reply_markup=main_menu_kb())
    except Exception as e:
        log.warning(f"Ошибка после логина: {e}")
        if message is not None:
            await message.reply("✅ Юзербот авторизован.", reply_markup=main_menu_kb())


# ---------------------------------------------------------------------------
# Обработчики юзербота
# ---------------------------------------------------------------------------

def register_user_handlers(client: Client) -> None:

    @client.on_message(filters.private & filters.outgoing)
    async def on_outgoing(client, message):
        try:
            chat = message.chat
            if not chat or not chat.id or chat.id == ME_ID:
                return
            if chat.type != enums.ChatType.PRIVATE:
                return

            ai = STATE.get("ai_assistant") or {}
            ai["last_user_activity"] = datetime.now().isoformat(timespec="seconds")

            paused = set(ai.get("paused_users") or [])
            paused.add(str(chat.id))
            ai["paused_users"] = list(paused)
            log.info(f"[AI PAUSED] Владелец вмешался в диалог с {chat.id}")

            STATE["ai_assistant"] = ai
            save_json(STATE_FILE, STATE)
        except Exception as e:
            log.exception(f"on_outgoing: {e}")

    @client.on_message(filters.private & filters.incoming)
    async def on_incoming(client, message):
        try:
            if not USERBOT_READY:
                return
            user = message.from_user
            if not user or user.is_bot or user.is_deleted:
                return
            if user.id == ME_ID:
                return
            if user.id == CFG.get("admin_id"):
                return

            ai = STATE.get("ai_assistant") or {}
            if not ai.get("enabled"):
                return

            if not _is_owner_inactive_ai():
                return

            if str(user.id) in (ai.get("paused_users") or []):
                return

            # Голосовое -> уведомление
            if message.voice or message.video_note or message.audio:
                try:
                    await bot_client.send_message(
                        CFG["admin_id"],
                        f"🔔 Клиент {user.first_name} (@{user.username or '—'}, id={user.id}) "
                        f"прислал голосовое. Ответь сам."
                    )
                except Exception:
                    pass
                return

            text = (message.text or message.caption or "").strip()
            if not text:
                return

            await db_add_message(user.id, "user", text)

            reply = await ai_generate_reply(user.id, text)
            if not reply:
                try:
                    await bot_client.send_message(
                        CFG["admin_id"],
                        f"❓ ИИ не смог ответить клиенту {user.first_name} (@{user.username or '—'}, id={user.id}).\n"
                        f"Сообщение: {text[:200]}\nОтветь сам или нажми /pause {user.id}"
                    )
                except Exception:
                    pass
                return

            await user_client.send_message(user.id, reply)
            await db_add_message(user.id, "assistant", reply)

            STATE["stats"]["ai_replies"] = STATE["stats"].get("ai_replies", 0) + 1
            save_json(STATE_FILE, STATE)
            log.info(f"[AI REPLY] → {user.id}: {reply[:80]}")

        except FloodWait as fw:
            log.warning(f"FloodWait ИИ: {fw.value}s")
            await asyncio.sleep(fw.value + 2)
        except Exception as e:
            log.exception(f"ИИ-ответ: {e}")


# ---------------------------------------------------------------------------
# Обработчики бота
# ---------------------------------------------------------------------------

def register_handlers(bot: Client) -> None:

    @bot.on_message(filters.command("start") & filters.private)
    async def cmd_start(client, message):
        if message.from_user.id != CFG["admin_id"]:
            await message.reply("⛔ Доступ запрещён.")
            return
        if message.from_user.id not in authed:
            await message.reply("🔒 `/auth <PIN>`", parse_mode=enums.ParseMode.MARKDOWN)
            return
        txt = "🎛 Панель управления:"
        if not USERBOT_READY:
            txt += "\n\n⚠️ Юзербот не авторизован."
        await message.reply(txt, reply_markup=main_menu_kb())

    @bot.on_message(filters.command("auth") & filters.private)
    async def cmd_auth(client, message):
        if message.from_user.id != CFG["admin_id"]:
            return
        parts = (message.text or "").split(maxsplit=1)
        if len(parts) < 2 or parts[1].strip() != str(CFG.get("pin", DEFAULT_PIN)):
            await message.reply("❌ Неверный ПИН-код.")
            return
        authed.add(message.from_user.id)
        pending.pop(message.from_user.id, None)
        await message.reply("✅ Авторизация успешна.", reply_markup=main_menu_kb())

    @bot.on_message(filters.command("pause") & filters.private)
    async def cmd_pause(client, message):
        if message.from_user.id != CFG["admin_id"] or message.from_user.id not in authed:
            return
        parts = (message.text or "").split(maxsplit=1)
        if len(parts) < 2:
            await message.reply("Использование: `/pause <user_id>` или `/pause @username`",
                                parse_mode=enums.ParseMode.MARKDOWN)
            return
        target = parts[1].strip()
        uid = None
        try:
            uid = int(target)
        except ValueError:
            try:
                chat = await user_client.get_chat(target)
                uid = chat.id
            except Exception as e:
                await message.reply(f"❌ Не смог найти: {e}")
                return
        ai = STATE.setdefault("ai_assistant", _default_ai())
        paused = set(ai.get("paused_users") or [])
        paused.add(str(uid))
        ai["paused_users"] = list(paused)
        save_json(STATE_FILE, STATE)
        await message.reply(f"⏸ ИИ приостановлен для {uid}. Когда захочешь — /resume {uid}")

    @bot.on_message(filters.command("resume") & filters.private)
    async def cmd_resume(client, message):
        if message.from_user.id != CFG["admin_id"] or message.from_user.id not in authed:
            return
        parts = (message.text or "").split(maxsplit=1)
        if len(parts) < 2:
            await message.reply("Использование: `/resume <user_id>`", parse_mode=enums.ParseMode.MARKDOWN)
            return
        try:
            uid = str(int(parts[1].strip()))
        except ValueError:
            await message.reply("❌ user_id должен быть числом.")
            return
        ai = STATE.setdefault("ai_assistant", _default_ai())
        paused = set(ai.get("paused_users") or [])
        paused.discard(uid)
        ai["paused_users"] = list(paused)
        save_json(STATE_FILE, STATE)
        await message.reply(f"▶️ ИИ снова общается с {uid}.")

    @bot.on_message(filters.command("cancel") & filters.private)
    async def cmd_cancel(client, message):
        if message.from_user.id != CFG["admin_id"]:
            return
        pending.pop(message.from_user.id, None)
        await message.reply("Отменено.", reply_markup=main_menu_kb())

    @bot.on_message(filters.command("login") & filters.private)
    async def cmd_login(client, message):
        if message.from_user.id != CFG["admin_id"]:
            return
        if message.from_user.id not in authed:
            await message.reply("🔒 /auth <PIN>")
            return
        if USERBOT_READY:
            await message.reply("ℹ️ Юзербот уже авторизован.")
            return
        pending[message.from_user.id] = {"action": "login_phone"}
        await message.reply(
            "📱 Введите номер телефона юзербота в формате `+79991234567`.\n"
            "Отмена — /cancel", parse_mode=enums.ParseMode.MARKDOWN)

    @bot.on_callback_query()
    async def on_cb(client, cb):
        uid = cb.from_user.id
        if uid != CFG["admin_id"]:
            await cb.answer("⛔ Нет доступа.", show_alert=True)
            return
        if uid not in authed:
            await cb.answer("🔒 /auth <PIN>", show_alert=True)
            return
        data = cb.data or ""
        try:
            if data == "menu":
                await cb.message.edit_text("🎛 Панель управления:", reply_markup=main_menu_kb())

            elif data == "login":
                if USERBOT_READY:
                    await cb.answer("Уже авторизован.")
                    return
                pending[uid] = {"action": "login_phone"}
                await cb.message.edit_text(
                    "📱 Введите номер телефона в формате `+79991234567`.\n/cancel — отмена",
                    parse_mode=enums.ParseMode.MARKDOWN)

            elif data == "start":
                if not USERBOT_READY:
                    await cb.answer("Сначала /login.", show_alert=True)
                    return
                if STATE.get("running"):
                    await cb.answer("Уже запущено.")
                    return
                if not STATE.get("groups"):
                    await cb.answer("Нет групп.", show_alert=True)
                    return
                if not (STATE.get("text") or STATE.get("media_path")):
                    await cb.answer("Не задано сообщение!", show_alert=True)
                    return
                STATE["running"] = True
                save_json(STATE_FILE, STATE)
                start_mailing()
                await cb.message.edit_text("🚀 Запущено.", reply_markup=main_menu_kb())

            elif data == "stop":
                STATE["running"] = False
                save_json(STATE_FILE, STATE)
                await cb.message.edit_text("⏸ Остановлено.", reply_markup=main_menu_kb())

            elif data == "edit_msg":
                cur = "— (пусто)"
                if STATE.get("media_path"):
                    cur = f"Медиа: {STATE.get('media_type')}\nCaption: {(STATE.get('caption') or '')[:150]}"
                elif STATE.get("text"):
                    cur = f"Текст: {STATE['text'][:200]}"
                kb = InlineKeyboardMarkup([
                    [InlineKeyboardButton("📝 Задать текст", callback_data="set_text")],
                    [InlineKeyboardButton("🖼 Задать медиа", callback_data="set_media")],
                    [InlineKeyboardButton("🗑 Очистить", callback_data="clear_msg")],
                    [InlineKeyboardButton("⬅️ Назад", callback_data="menu")],
                ])
                await cb.message.edit_text(f"📝 Текущее:\n{cur}", reply_markup=kb)
            elif data == "set_text":
                pending[uid] = {"action": "set_text"}
                await cb.message.edit_text("Отправьте текст.\n/cancel")
            elif data == "set_media":
                pending[uid] = {"action": "set_media"}
                await cb.message.edit_text("Отправьте фото или видео.\n/cancel")
            elif data == "clear_msg":
                STATE["text"] = None
                STATE["media_path"] = None
                STATE["media_type"] = None
                STATE["caption"] = ""
                save_json(STATE_FILE, STATE)
                await cb.message.edit_text("🗑 Очищено.", reply_markup=main_menu_kb())

            elif data == "timings":
                kb = InlineKeyboardMarkup([
                    [InlineKeyboardButton("⏱ Интервал круга (сек)", callback_data="set_interval")],
                    [InlineKeyboardButton("⏳ Мин. задержка", callback_data="set_delay_min")],
                    [InlineKeyboardButton("⏳ Макс. задержка", callback_data="set_delay_max")],
                    [InlineKeyboardButton("⬅️ Назад", callback_data="menu")],
                ])
                await cb.message.edit_text(
                    f"⏱ Настройки:\n"
                    f"• Интервал: {STATE['interval']} сек\n"
                    f"• Задержка: {STATE['delay_min']}–{STATE['delay_max']} сек",
                    reply_markup=kb)
            elif data == "set_interval":
                pending[uid] = {"action": "set_interval"}
                await cb.message.edit_text("Интервал в секундах (мин. 60).\n/cancel")
            elif data == "set_delay_min":
                pending[uid] = {"action": "set_delay_min"}
                await cb.message.edit_text("Мин. задержка (>=1).\n/cancel")
            elif data == "set_delay_max":
                pending[uid] = {"action": "set_delay_max"}
                await cb.message.edit_text("Макс. задержка.\n/cancel")

            elif data == "scan":
                if not USERBOT_READY:
                    await cb.answer("Юзербот не готов.", show_alert=True)
                    return
                await cb.answer("Сканирую…")
                await cb.message.edit_text("🔍 Сканирую…")
                found = await scan_groups()
                scanned = {g["id"] for g in found}
                manual = [g for g in (STATE.get("groups") or [])
                          if g.get("manual") and g["id"] not in scanned]
                STATE["groups"] = found + manual
                save_json(STATE_FILE, STATE)
                await cb.message.edit_text(
                    f"✅ Найдено: {len(found)}\n• Всего: {len(STATE['groups'])}",
                    reply_markup=main_menu_kb())
            elif data == "add_grp":
                if not USERBOT_READY:
                    await cb.answer("Юзербот не готов.", show_alert=True)
                    return
                pending[uid] = {"action": "add_group"}
                await cb.message.edit_text(
                    "Отправьте `@username`, `t.me/...` или ID.\n/cancel",
                    parse_mode=enums.ParseMode.MARKDOWN)
            elif data == "del_grp":
                if not STATE.get("groups"):
                    await cb.answer("Пусто.", show_alert=True)
                    return
                await cb.message.edit_text("Удалить группу:", reply_markup=groups_kb())
            elif data.startswith("delgrp:"):
                try:
                    gid = int(data.split(":", 1)[1])
                except ValueError:
                    await cb.answer("Ошибка.")
                    return
                STATE["groups"] = [g for g in STATE["groups"] if g["id"] != gid]
                save_json(STATE_FILE, STATE)
                if STATE["groups"]:
                    await cb.message.edit_text(f"➖ Осталось {len(STATE['groups'])}.",
                        reply_markup=groups_kb())
                else:
                    await cb.message.edit_text("➖ Пусто.", reply_markup=main_menu_kb())

            # ---------- ИИ-ассистент ----------
            elif data == "ai_menu":
                await cb.message.edit_text(ai_menu_text(), reply_markup=ai_menu_kb())

            elif data == "ai_toggle":
                ai = STATE.setdefault("ai_assistant", _default_ai())
                ai["enabled"] = not ai.get("enabled", False)
                save_json(STATE_FILE, STATE)
                await cb.message.edit_text(ai_menu_text(), reply_markup=ai_menu_kb())

            elif data == "ai_style":
                pending[uid] = {"action": "ai_style"}
                await cb.message.edit_text(
                    "📝 Опиши стиль общения ИИ. Например:\n"
                    "«Дружелюбно, неформально, с лёгкими эмодзи, короткие сообщения»\n\nОтправь текст:")
            elif data == "ai_about":
                pending[uid] = {"action": "ai_about"}
                await cb.message.edit_text("👤 Напиши информацию о себе (имя, чем занимаешься):")
            elif data == "ai_work":
                pending[uid] = {"action": "ai_work"}
                await cb.message.edit_text("💼 Опиши суть работы/продукта (что продаёшь, цены, схему):")
            elif data == "ai_forbidden":
                pending[uid] = {"action": "ai_forbidden"}
                await cb.message.edit_text("🚫 Что ИИ НИКОГДА не должен говорить/делать:")
            elif data == "ai_show_prompt":
                prompt = (STATE.get("ai_assistant") or {}).get("system_prompt") or DEFAULT_SYSTEM_PROMPT
                if len(prompt) > 3500:
                    prompt = prompt[:3500] + "\n…(обрезано)"
                await cb.message.edit_text(
                    f"📋 Текущий системный промпт:\n\n{prompt}",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("⬅️ Назад", callback_data="ai_menu")],
                    ]))
            elif data == "ai_reset_prompt":
                ai = STATE.setdefault("ai_assistant", _default_ai())
                ai["system_prompt"] = DEFAULT_SYSTEM_PROMPT
                ai["prompt_parts"] = {}
                save_json(STATE_FILE, STATE)
                await cb.message.edit_text("♻️ Промпт сброшен к дефолту.", reply_markup=ai_menu_kb())
            elif data == "ai_inactive":
                pending[uid] = {"action": "ai_inactive"}
                await cb.message.edit_text(
                    f"Через сколько минут твоего отсутствия включать ИИ? "
                    f"(сейчас {(STATE.get('ai_assistant') or {}).get('inactive_minutes', 5)})")

            elif data == "status":
                s = STATE["stats"]
                ai = STATE.get("ai_assistant") or {}
                txt = (
                    f"📊 Статистика\n"
                    f"• Юзербот: {'🟢' if USERBOT_READY else '🔴'}\n"
                    f"• Рассылка: {'🟢' if STATE.get('running') else '🔴'}\n"
                    f"• Групп: {len(STATE.get('groups') or [])}\n"
                    f"• Отправлено: {s.get('sent', 0)}\n"
                    f"• Ошибок: {s.get('errors', 0)}\n"
                    f"• Кругов: {s.get('rounds', 0)}\n\n"
                    f"🤖 ИИ-ассистент: {'🟢' if ai.get('enabled') else '🔴'}\n"
                    f"• Ответов ИИ: {s.get('ai_replies', 0)}\n"
                    f"• Приостановлено: {len(ai.get('paused_users') or [])}\n"
                    f"• Офлайн-порог: {ai.get('inactive_minutes', 5)} мин"
                )
                kb = InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔄 Обновить", callback_data="status")],
                    [InlineKeyboardButton("⬅️ Назад", callback_data="menu")],
                ])
                await cb.message.edit_text(txt, reply_markup=kb)
            else:
                await cb.answer("Неизвестная команда.")
        except Exception as e:
            log.exception("Ошибка callback")
            try:
                await cb.answer(f"Ошибка: {e}", show_alert=True)
            except Exception:
                pass

    @bot.on_message(filters.private & filters.user(CFG["admin_id"]))
    async def on_admin_input(client, message):
        uid = message.from_user.id
        if uid not in authed:
            return
        act = pending.pop(uid, None)
        if not act:
            return
        action = act.get("action")
        text = (message.text or "").strip()
        try:
            if action == "login_phone":
                phone = text
                if not phone.startswith("+"):
                    await message.reply("Номер должен начинаться с `+`.")
                    pending[uid] = {"action": "login_phone"}
                    return
                if not user_client.is_connected:
                    try:
                        await user_client.connect()
                    except Exception as e:
                        await message.reply(f"❌ Не подключиться: {e}")
                        return
                try:
                    sent = await user_client.send_code(phone)
                except Exception as e:
                    await message.reply(f"❌ send_code: {e}")
                    return
                pending[uid] = {"action": "login_code", "phone": phone, "hash": sent.phone_code_hash}
                await message.reply("📩 Введите код из Telegram (только цифры):")

            elif action == "login_code":
                phone = act["phone"]
                hash_ = act["hash"]
                code = text.replace(" ", "")
                try:
                    await user_client.sign_in(phone_number=phone, phone_code_hash=hash_, phone_code=code)
                except SessionPasswordNeeded:
                    pending[uid] = {"action": "login_password"}
                    await message.reply("🔐 Введите пароль 2FA:")
                    return
                except PhoneCodeInvalid:
                    pending[uid] = act
                    await message.reply("❌ Неверный код. Повторите или /cancel")
                    return
                except PhoneCodeExpired:
                    await message.reply("⚠️ Код истёк. Подожди 2 мин и /login заново.")
                    return
                except Exception as e:
                    await message.reply(f"❌ {e}")
                    return
                await after_userbot_login(message)

            elif action == "login_password":
                try:
                    await user_client.check_password(text)
                except PasswordHashInvalid:
                    pending[uid] = {"action": "login_password"}
                    await message.reply("❌ Неверный пароль. Повторите или /cancel")
                    return
                except Exception as e:
                    await message.reply(f"❌ {e}")
                    return
                await after_userbot_login(message)

            elif action == "set_text":
                if not text:
                    pending[uid] = {"action": "set_text"}
                    await message.reply("Пустой текст.")
                    return
                STATE["text"] = text
                STATE["media_path"] = None
                STATE["media_type"] = None
                STATE["caption"] = ""
                save_json(STATE_FILE, STATE)
                await message.reply("✅ Текст сохранён.", reply_markup=main_menu_kb())

            elif action == "set_media":
                if not (message.photo or message.video):
                    pending[uid] = {"action": "set_media"}
                    await message.reply("Не медиа. Пришлите фото/видео.")
                    return
                if message.photo:
                    ext = "jpg"; STATE["media_type"] = "photo"
                else:
                    ext = "mp4"; STATE["media_type"] = "video"
                path = os.path.join(MEDIA_DIR, f"post_media.{ext}")
                await message.download(file_name=path)
                STATE["media_path"] = path
                STATE["caption"] = message.caption or ""
                STATE["text"] = None
                save_json(STATE_FILE, STATE)
                await message.reply(f"✅ Медиа сохранено ({STATE['media_type']}).",
                    reply_markup=main_menu_kb())

            elif action == "add_group":
                ref = parse_chat_ref(text)
                if ref is None:
                    await message.reply("Не распарсил. /cancel")
                    return
                try:
                    chat = await user_client.get_chat(ref)
                except Exception as e:
                    await message.reply(f"❌ {e}")
                    return
                gid = chat.id
                title = chat.title or str(gid)
                if any(g["id"] == gid for g in STATE["groups"]):
                    await message.reply("ℹ️ Уже в списке.", reply_markup=main_menu_kb())
                    return
                STATE["groups"].append({
                    "id": gid, "title": title,
                    "type": chat.type.name if chat.type else "UNKNOWN",
                    "manual": True,
                })
                save_json(STATE_FILE, STATE)
                await message.reply(f"✅ {title} ({gid})", reply_markup=main_menu_kb())

            elif action == "set_interval":
                try:
                    v = int(text)
                    if v < 60: raise ValueError("мин. 60")
                    STATE["interval"] = v
                    save_json(STATE_FILE, STATE)
                    await message.reply(f"✅ {v} сек.", reply_markup=main_menu_kb())
                except Exception as e:
                    pending[uid] = {"action": "set_interval"}
                    await message.reply(f"❌ {e}")
            elif action == "set_delay_min":
                try:
                    v = int(text)
                    if v < 1: raise ValueError("мин. 1")
                    STATE["delay_min"] = v
                    if STATE["delay_max"] < v: STATE["delay_max"] = v
                    save_json(STATE_FILE, STATE)
                    await message.reply(f"✅ {v} сек.", reply_markup=main_menu_kb())
                except Exception as e:
                    pending[uid] = {"action": "set_delay_min"}
                    await message.reply(f"❌ {e}")
            elif action == "set_delay_max":
                try:
                    v = int(text)
                    if v < 1: raise ValueError("мин. 1")
                    if v < STATE["delay_min"]: raise ValueError(f">= {STATE['delay_min']}")
                    STATE["delay_max"] = v
                    save_json(STATE_FILE, STATE)
                    await message.reply(f"✅ {v} сек.", reply_markup=main_menu_kb())
                except Exception as e:
                    pending[uid] = {"action": "set_delay_max"}
                    await message.reply(f"❌ {e}")

            elif action in ("ai_style", "ai_about", "ai_work", "ai_forbidden"):
                ai = STATE.setdefault("ai_assistant", _default_ai())
                parts = ai.setdefault("prompt_parts", {})
                parts[action] = message.text or ""
                ai["prompt_parts"] = parts

                style = parts.get("ai_style", "").strip()
                about = parts.get("ai_about", "").strip()
                work = parts.get("ai_work", "").strip()
                forb = parts.get("ai_forbidden", "").strip()

                prompt_lines = []
                if style:
                    prompt_lines.append(f"СТИЛЬ ОБЩЕНИЯ:\n{style}")
                if about:
                    prompt_lines.append(f"ИНФОРМАЦИЯ ОБО МНЕ:\n{about}")
                if work:
                    prompt_lines.append(f"СУТЬ РАБОТЫ:\n{work}")
                if forb:
                    prompt_lines.append(f"ЗАПРЕЩЕНО:\n{forb}")
                prompt_lines.append(
                    "ОБЩИЕ ПРАВИЛА:\n"
                    "- Пиши как живой человек, не как робот.\n"
                    "- Не пиши слишком длинные или слишком короткие сообщения.\n"
                    "- Если клиент запутан или сложный вопрос — передай владельцу.\n"
                    "- Никогда не говори, что ты бот или ИИ."
                )
                ai["system_prompt"] = "\n\n".join(prompt_lines)
                save_json(STATE_FILE, STATE)
                await message.reply("✅ Сохранено. Промпт обновлён.",
                                    reply_markup=ai_menu_kb())

            elif action == "ai_inactive":
                try:
                    v = int(text)
                    if v < 1: raise ValueError("мин. 1")
                    STATE["ai_assistant"]["inactive_minutes"] = v
                    save_json(STATE_FILE, STATE)
                    await message.reply(f"✅ Офлайн-порог: {v} мин.",
                                        reply_markup=ai_menu_kb())
                except Exception as e:
                    pending[uid] = {"action": "ai_inactive"}
                    await message.reply(f"❌ {e}")

        except Exception as e:
            log.exception("Ошибка ввода админа")
            await message.reply(f"❌ {e}")


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

async def main():
    global CFG, STATE, user_client, bot_client, deepseek_client, USERBOT_READY, ME_ID

    CFG = load_cfg()
    if not cfg_ok(CFG):
        print("Не заданы переменные: API_ID, API_HASH, BOT_TOKEN, ADMIN_ID")
        return
    CFG.setdefault("pin", DEFAULT_PIN)
    save_json(CONFIG_FILE, CFG)
    STATE = load_state()

    # --- SQLite ---
    await db_init()

    # --- DeepSeek ---
    if CFG.get("deepseek_api_key"):
        try:
            deepseek_client = OpenAI(
                api_key=CFG["deepseek_api_key"],
                base_url="https://api.deepseek.com",
            )
            log.info("✅ DeepSeek клиент инициализирован.")
        except Exception as e:
            log.warning(f"DeepSeek не инициализирован: {e}")
    else:
        log.warning("DEEPSEEK_API_KEY не задан — ИИ не будет работать.")

    # --- Юзербот ---
    user_client = Client(name=SESSION_USER, api_id=CFG["api_id"], api_hash=CFG["api_hash"])
    register_user_handlers(user_client)

    # --- Бот ---
    bot_client = Client(
        name=SESSION_BOT,
        api_id=CFG["api_id"],
        api_hash=CFG["api_hash"],
        bot_token=CFG["bot_token"],
    )
    register_handlers(bot_client)

    log.info("Запуск управляющего бота…")
    await bot_client.start()
    bme = await bot_client.get_me()
    log.info(f"Бот запущен: @{bme.username}")

    # --- Пытаемся поднять юзербот ---
    try:
        await user_client.start()
        me = await user_client.get_me()
        if me:
            ME_ID = me.id
            USERBOT_READY = True
            log.info(f"✅ Юзербот авторизован: {me.first_name} (@{me.username}) id={me.id}")
    except Exception as e:
        log.warning(f"Юзербот не авторизован: {e}")
        try:
            if not user_client.is_connected:
                await user_client.connect()
        except Exception:
            pass
        USERBOT_READY = False

    # --- Приветствие ---
    try:
        ai_status = "🟢 вкл" if (STATE.get("ai_assistant") or {}).get("enabled") else "🔴 выкл"
        ds_status = "🟢 подключён" if deepseek_client else "🔴 нет ключа"
        status = "🟢 готов" if USERBOT_READY else "🔴 не авторизован (/login)"
        await bot_client.send_message(
            CFG["admin_id"],
            f"🤖 Автопостер запущен.\n"
            f"Юзербот: {status}\n"
            f"DeepSeek: {ds_status}\n"
            f"ИИ-ассистент: {ai_status}\n"
            f"Для доступа: `/auth <PIN>`",
            parse_mode=enums.ParseMode.MARKDOWN)
    except Exception as e:
        log.warning(f"Не отправить приветствие: {e}")

    # --- Восстановление рассылки ---
    if STATE.get("running") and USERBOT_READY:
        log.info("Возобновляю рассылку…")
        start_mailing()
    elif STATE.get("running") and not USERBOT_READY:
        STATE["running"] = False
        save_json(STATE_FILE, STATE)

    log.info("Сервис работает. Ctrl+C для остановки.")
    await asyncio.Event().wait()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        print("\nОстановлено.")
