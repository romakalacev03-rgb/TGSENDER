# -*- coding: utf-8 -*-
"""
Автопостер + автоответчик + ИИ-ассистент (Cloudflare Workers AI).
Сессии: /data/userbot.session, /data/control_bot.session
История диалогов: /data/chat_history.db (SQLite)

Приоритет ответа клиенту:
    1) ИИ (Cloudflare Workers AI) — если включён, владелец неактивен (или тест-режим), юзер не на паузе
    2) Автоответчик — если включён, владелец неактивен, cooldown ок
    3) Передача клиента владельцу — если ИИ не смог ответить
"""

import asyncio
import json
import os
import random
import logging
from datetime import datetime, timedelta

import aiohttp
import aiosqlite

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
http_session: aiohttp.ClientSession = None
mailing_task: asyncio.Task = None
peer_refresh_task: asyncio.Task = None
USERBOT_READY: bool = False
ME_ID: int = 0

authed: set = set()
pending: dict = {}
BOT_LAST_SENT: dict = {}

# ---------------------------------------------------------------------------
# Утилита очистки строк от пробелов/переносов
# ---------------------------------------------------------------------------

def clean_secret(s) -> str:
    """Удаляет ВСЕ пробелы, табы, переносы строк из строки.
    Нужно для токенов/ID, которые пользователь копирует из браузера."""
    if s is None:
        return ""
    return "".join(str(s).split())


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
        await db.execute("CREATE INDEX IF NOT EXISTS idx_user_id ON messages(user_id)")
        await db.commit()


async def db_add_message(user_id: int, role: str, content: str):
    try:
        async with aiosqlite.connect(DB_FILE) as db:
            await db.execute(
                "INSERT INTO messages (user_id, role, content, timestamp) VALUES (?, ?, ?, ?)",
                (user_id, role, content, datetime.now().isoformat(timespec="seconds"))
            )
            await db.commit()
    except Exception as e:
        log.error(f"DB add error: {e}")


async def db_get_history(user_id: int, limit: int = 30) -> list:
    try:
        async with aiosqlite.connect(DB_FILE) as db:
            async with db.execute(
                "SELECT role, content FROM messages WHERE user_id = ? ORDER BY id DESC LIMIT ?",
                (user_id, limit)
            ) as cursor:
                rows = await cursor.fetchall()
        rows.reverse()
        return [{"role": ("user" if r == "user" else "assistant"), "content": c} for r, c in rows]
    except Exception as e:
        log.error(f"DB history error: {e}")
        return []


async def db_cleanup(days: int = 30):
    try:
        cutoff = (datetime.now() - timedelta(days=days)).isoformat(timespec="seconds")
        async with aiosqlite.connect(DB_FILE) as db:
            await db.execute("DELETE FROM messages WHERE timestamp < ?", (cutoff,))
            await db.commit()
    except Exception as e:
        log.error(f"DB cleanup error: {e}")


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
    "CF_ACCOUNT_ID": ("cf_account_id", str),
    "CF_API_TOKEN": ("cf_api_token", str),
    "CF_MODEL": ("cf_model", str),
}


def load_cfg() -> dict:
    cfg = load_json(CONFIG_FILE, {}) or {}
    for env, (key, typ) in ENV_MAP.items():
        v = os.getenv(env)
        if v is None or v == "":
            continue
        try:
            if key in ("cf_account_id", "cf_api_token"):
                cfg[key] = clean_secret(v)
            else:
                cfg[key] = typ(v)
        except Exception:
            log.warning(f"Некорректное значение {env}={v!r}")
    return cfg


def cfg_ok(cfg: dict) -> bool:
    return all(cfg.get(k) for k in ("api_id", "api_hash", "bot_token", "admin_id"))


# ---------------------------------------------------------------------------
# Дефолтный промпт ИИ
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
1. Познакомься. Пример: "Меня зовут Алексей, а если просто — Лёха. Как мне обращаться к тебе?"
2. Узнай о клиенте: возраст, чем занимается, был ли опыт с крипто-кошельками.
3. Если заинтересовал — попроси написать о себе.
4. Общайся неформально, создавай комфортную атмосферу.
5. Мягко подводи к покупке.

ЗАПРЕЩЕНО:
- Называть точные цены без согласования (можешь сказать "около 14$").
- Обещать конкретные сроки или доход.
- Давить на клиента.
- Говорить, что ты бот или ИИ. Ты — Алексей, живой человек.
- Отвечать на голосовые сообщения (передай владельцу).
"""


# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------

def _default_ai() -> dict:
    return {
        "enabled": False,
        "test_mode": False,               # 🧪 тест: отвечать сразу, без ожидания неактивности
        "inactive_minutes": 5,
        "system_prompt": DEFAULT_SYSTEM_PROMPT,
        "prompt_parts": {},
        "paused_users": [],
        "cf_account_id": "",
        "cf_api_token": "",
        "cf_model": "@cf/meta/llama-3.3-70b-instruct-fp8-fast",
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
        "owner_last_activity": None,
        "autoreply": _default_autoreply(),
        "ai_assistant": _default_ai(),
        "stats": {
            "sent": 0, "errors": 0, "rounds": 0,
            "autoreplies": 0, "ai_replies": 0, "ai_fallbacks": 0,
            "last_round": None, "next_round": None,
        },
    }


def load_state() -> dict:
    raw = load_json(STATE_FILE, {}) or {}
    st = default_state()
    st.update(raw)
    for key, default_fn in (("autoreply", _default_autoreply), ("ai_assistant", _default_ai)):
        merged = st.get(key) or {}
        for k, v in default_fn().items():
            merged.setdefault(k, v)
        st[key] = merged

    # Чистим креды от возможного мусора (переносы, пробелы), если они там были сохранены
    ai = st.get("ai_assistant") or {}
    if ai.get("cf_account_id"):
        ai["cf_account_id"] = clean_secret(ai["cf_account_id"])
    if ai.get("cf_api_token"):
        ai["cf_api_token"] = clean_secret(ai["cf_api_token"])

    if not isinstance(st.get("stats"), dict):
        st["stats"] = default_state()["stats"]
    for k in ("autoreplies", "ai_replies", "ai_fallbacks"):
        st["stats"].setdefault(k, 0)
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


def _owner_inactive(minutes: int) -> bool:
    last = STATE.get("owner_last_activity")
    if not last:
        return True
    try:
        last_dt = datetime.fromisoformat(last)
        return (datetime.now() - last_dt).total_seconds() > minutes * 60
    except Exception:
        return True


def _owner_inactive_ai() -> bool:
    ai = STATE.get("ai_assistant") or {}
    return _owner_inactive(int(ai.get("inactive_minutes", 5)))


def _owner_inactive_ar() -> bool:
    ar = STATE.get("autoreply") or {}
    return _owner_inactive(int(ar.get("inactive_minutes", 5)))


def _mark_owner_activity():
    STATE["owner_last_activity"] = datetime.now().isoformat(timespec="seconds")
    save_json(STATE_FILE, STATE)


def _is_known_ar(user_id: int) -> bool:
    ar = STATE.get("autoreply") or {}
    return str(user_id) in (ar.get("known_users") or [])


def _mark_known_ar(user_id: int):
    ar = STATE.setdefault("autoreply", _default_autoreply())
    known = ar.setdefault("known_users", [])
    s = str(user_id)
    if s not in known:
        known.append(s)
        if len(known) > 5000:
            ar["known_users"] = known[-3000:]


def _cooldown_ok_ar(user_id: int) -> bool:
    ar = STATE.get("autoreply") or {}
    lr = ar.get("last_reply") or {}
    last = lr.get(str(user_id))
    if not last:
        return True
    try:
        last_dt = datetime.fromisoformat(last)
        cd = int(ar.get("cooldown_minutes", 60)) * 60
        return (datetime.now() - last_dt).total_seconds() >= cd
    except Exception:
        return True


def _set_cooldown_ar(user_id: int):
    ar = STATE.setdefault("autoreply", _default_autoreply())
    ar.setdefault("last_reply", {})[str(user_id)] = datetime.now().isoformat(timespec="seconds")
    save_json(STATE_FILE, STATE)


def _is_paused_ai(user_id: int) -> bool:
    ai = STATE.get("ai_assistant") or {}
    return str(user_id) in (ai.get("paused_users") or [])


def _pause_ai(user_id: int):
    ai = STATE.setdefault("ai_assistant", _default_ai())
    paused = set(ai.get("paused_users") or [])
    paused.add(str(user_id))
    ai["paused_users"] = list(paused)
    save_json(STATE_FILE, STATE)


def _unpause_ai(user_id: int):
    ai = STATE.setdefault("ai_assistant", _default_ai())
    paused = set(ai.get("paused_users") or [])
    paused.discard(str(user_id))
    ai["paused_users"] = list(paused)
    save_json(STATE_FILE, STATE)


def _register_bot_sent(user_id: int, text: str):
    BOT_LAST_SENT[user_id] = (text or "").strip()


def _was_sent_by_bot(user_id: int, text: str) -> bool:
    return BOT_LAST_SENT.get(user_id) == (text or "").strip()


async def _send_as_userbot(user_id: int, text: str, save_to_db: bool = True):
    _register_bot_sent(user_id, text)
    await user_client.send_message(user_id, text)
    if save_to_db:
        await db_add_message(user_id, "assistant", text)


# ---------------------------------------------------------------------------
# Cloudflare Workers AI
# ---------------------------------------------------------------------------

CF_MODELS = [
    "@cf/meta/llama-3.3-70b-instruct-fp8-fast",
    "@cf/meta/llama-3.1-8b-instruct-fast",
]


def _get_cf_creds():
    ai = STATE.get("ai_assistant") or {}
    account_id = clean_secret(
        ai.get("cf_account_id") or CFG.get("cf_account_id") or os.getenv("CF_ACCOUNT_ID") or ""
    )
    api_token = clean_secret(
        ai.get("cf_api_token") or CFG.get("cf_api_token") or os.getenv("CF_API_TOKEN") or ""
    )
    return account_id, api_token


async def ask_ai(user_id: int, user_message: str):
    """Возвращает (reply | None, reason | None). reason='api_error' если временный сбой."""
    ai_cfg = STATE.get("ai_assistant") or {}
    if not ai_cfg.get("enabled"):
        return None, "disabled"

    account_id, api_token = _get_cf_creds()
    if not account_id or not api_token:
        log.warning("[AI] Cloudflare: не заданы account_id или api_token")
        return None, "no_credentials"

    model = clean_secret(ai_cfg.get("cf_model") or CFG.get("cf_model") or CF_MODELS[0])
    if not model:
        model = CF_MODELS[0]

    system_prompt = ai_cfg.get("system_prompt") or DEFAULT_SYSTEM_PROMPT
    history = await db_get_history(user_id, limit=30)

    messages = [{"role": "system", "content": system_prompt}]
    messages.extend(history)
    if not history or history[-1].get("content") != user_message:
        messages.append({"role": "user", "content": user_message})

    headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json",
    }
    payload = {
        "messages": messages,
        "max_tokens": 1024,
        "temperature": 0.7,
    }

    models_to_try = [model] + [m for m in CF_MODELS if m != model]

    last_error = None
    for m in models_to_try:
        url = f"https://api.cloudflare.com/client/v4/accounts/{account_id}/ai/run/{m}"
        try:
            log.info(f"[AI] Запрос в Cloudflare ({m}) для {user_id}")
            async with http_session.post(
                url, json=payload, headers=headers,
                timeout=aiohttp.ClientTimeout(total=45),
            ) as resp:
                status = resp.status
                body = await resp.text()
                if status == 429:
                    log.warning(f"[AI] {m}: rate limit")
                    last_error = "rate_limit"
                    continue
                if status != 200:
                    log.error(f"[AI] {m} → HTTP {status}: {body[:300]}")
                    last_error = f"http_{status}"
                    if status in (401, 403):
                        return None, "auth_error"
                    continue
                try:
                    data = json.loads(body)
                except Exception:
                    log.error(f"[AI] Не распарсил JSON: {body[:300]}")
                    last_error = "bad_json"
                    continue
                if not data.get("success"):
                    errs = data.get("errors") or []
                    log.error(f"[AI] {m} success=false: {errs}")
                    last_error = "api_error"
                    continue
                reply = (data.get("result", {}).get("response") or "").strip()
                if reply and len(reply) > 2:
                    log.info(f"[AI] ✅ {m} ответ ({len(reply)} симв.)")
                    return reply, None
                log.warning(f"[AI] {m} вернул пустой ответ")
                last_error = "empty"
        except asyncio.TimeoutError:
            log.warning(f"[AI] {m}: timeout")
            last_error = "timeout"
            continue
        except Exception as e:
            log.exception(f"[AI] {m} exception: {e}")
            last_error = "exception"
            continue

    return None, (last_error or "unknown")


async def notify_ai_fallback(user_id: int, user_message: str, reason: str = ""):
    try:
        username = ""
        name = str(user_id)
        try:
            u = await user_client.get_users(user_id)
            if u:
                name = u.first_name or name
                username = f" (@{u.username})" if u.username else ""
        except Exception:
            pass
        await bot_client.send_message(
            CFG["admin_id"],
            f"⚠️ **ИИ не смог ответить клиенту**\n\n"
            f"👤 {name}{username}\n"
            f"🆔 `{user_id}`\n"
            f"💬 Сообщение: {user_message[:300]}\n"
            f"❓ Причина: {reason or 'неизвестно'}\n\n"
            f"ИИ приостановлен для этого клиента. Возобновить: `/resume {user_id}`",
            parse_mode=enums.ParseMode.MARKDOWN,
        )
    except Exception as e:
        log.error(f"Не удалось уведомить админа: {e}")


async def notify_ai_temp_error(user_id: int, user_message: str, reason: str):
    try:
        await bot_client.send_message(
            CFG["admin_id"],
            f"⚠️ Временный сбой ИИ ({reason}). Клиент `{user_id}` не получил ответ.\n"
            f"💬 {user_message[:200]}\n"
            f"(ИИ НЕ приостановлен для этого клиента.)",
            parse_mode=enums.ParseMode.MARKDOWN,
        )
    except Exception:
        pass


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
# Обновление кэша пиров
# ---------------------------------------------------------------------------

async def peer_refresh_loop():
    while True:
        await asyncio.sleep(1800)
        if not USERBOT_READY:
            continue
        try:
            count = 0
            async for _ in user_client.get_dialogs():
                count += 1
                if count >= 500:
                    break
            log.info(f"[PEERS] Кэш обновлён ({count} диалогов)")
        except FloodWait as e:
            log.warning(f"[PEERS] FloodWait {e.value}s")
            await asyncio.sleep(e.value + 5)
        except Exception as e:
            log.warning(f"[PEERS] Ошибка обновления кэша: {e}")


def start_peer_refresh():
    global peer_refresh_task
    if peer_refresh_task and not peer_refresh_task.done():
        return
    peer_refresh_task = asyncio.create_task(peer_refresh_loop())


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

    ar = STATE.get("autoreply") or {}
    ai = STATE.get("ai_assistant") or {}
    ar_state = "🟢" if ar.get("enabled") else "🔴"
    ai_state = "🟢" if ai.get("enabled") else "🔴"
    test_mark = " 🧪" if ai.get("test_mode") else ""

    rows += [
        [InlineKeyboardButton("📝 Изменить сообщение", callback_data="edit_msg")],
        [InlineKeyboardButton("⏱ Настройка таймингов", callback_data="timings")],
        [InlineKeyboardButton("🔍 Обновить список групп", callback_data="scan")],
        [InlineKeyboardButton("➕ Добавить группу", callback_data="add_grp"),
         InlineKeyboardButton("➖ Удалить группу", callback_data="del_grp")],
        [InlineKeyboardButton(f"🧠 ИИ-ассистент ({ai_state}){test_mark}", callback_data="ai_menu")],
        [InlineKeyboardButton(f"🤖 Автоответчик ({ar_state})", callback_data="ar_menu")],
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


# ---- ИИ ----

def ai_menu_kb() -> InlineKeyboardMarkup:
    ai = STATE.get("ai_assistant") or {}
    enabled = ai.get("enabled", False)
    test = ai.get("test_mode", False)
    has_cf = bool((ai.get("cf_account_id") or CFG.get("cf_account_id") or os.getenv("CF_ACCOUNT_ID")))
    has_tok = bool((ai.get("cf_api_token") or CFG.get("cf_api_token") or os.getenv("CF_API_TOKEN")))
    cred_state = "✅" if (has_cf and has_tok) else "❌"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔴 Выключить ИИ" if enabled else "🟢 Включить ИИ",
                              callback_data="ai_toggle")],
        [InlineKeyboardButton(
            f"🧪 Тест-режим: {'🟢 ВКЛ' if test else '🔴 выкл'}",
            callback_data="ai_test_toggle")],
        [InlineKeyboardButton("📝 Стиль общения", callback_data="ai_style")],
        [InlineKeyboardButton("👤 Информация обо мне", callback_data="ai_about")],
        [InlineKeyboardButton("💼 Описание работы", callback_data="ai_work")],
        [InlineKeyboardButton("🚫 Запрещённые темы", callback_data="ai_forbidden")],
        [InlineKeyboardButton("📋 Показать полный промпт", callback_data="ai_show_prompt")],
        [InlineKeyboardButton("♻️ Сбросить промпт к дефолту", callback_data="ai_reset_prompt")],
        [InlineKeyboardButton(f"⏱ Неактивность: {ai.get('inactive_minutes', 5)} мин",
                              callback_data="ai_inactive")],
        [InlineKeyboardButton(f"🔑 Cloudflare ключи {cred_state}", callback_data="ai_cf_menu")],
        [InlineKeyboardButton(f"🤖 Модель: {ai.get('cf_model', '')[:40]}", callback_data="ai_cf_model")],
        [InlineKeyboardButton(f"📋 Приостановленные ({len(ai.get('paused_users') or [])})",
                              callback_data="ai_paused")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="menu")],
    ])


def ai_cf_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔑 CF Account ID", callback_data="ai_cf_account")],
        [InlineKeyboardButton("🔑 CF API Token", callback_data="ai_cf_token")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="ai_menu")],
    ])


def ai_cf_menu_text() -> str:
    ai = STATE.get("ai_assistant") or {}
    cf_id = ai.get("cf_account_id") or CFG.get("cf_account_id") or os.getenv("CF_ACCOUNT_ID", "")
    cf_tok = ai.get("cf_api_token") or CFG.get("cf_api_token") or os.getenv("CF_API_TOKEN", "")
    return (
        "🔑 Cloudflare Workers AI — ключи\n\n"
        f"• Account ID: {'✅ ' + cf_id[:12] + '…' if cf_id else '❌ не задан'}\n"
        f"• API Token: {'✅ задан' if cf_tok else '❌ не задан'}\n\n"
        "Где взять:\n"
        "1. https://dash.cloudflare.com → AI → Workers AI\n"
        "2. 'Use REST API' → Create Token\n"
        "3. Скопируй Account ID и API Token\n"
        "4. Вставь сюда (бот сам вычистит переносы строк)."
    )


def ai_menu_text() -> str:
    ai = STATE.get("ai_assistant") or {}
    paused = len(ai.get("paused_users") or [])
    cf_id = ai.get("cf_account_id") or CFG.get("cf_account_id") or os.getenv("CF_ACCOUNT_ID", "")
    cf_tok = ai.get("cf_api_token") or CFG.get("cf_api_token") or os.getenv("CF_API_TOKEN", "")
    test = ai.get("test_mode", False)
    return (
        "🧠 ИИ-ассистент (Cloudflare Workers AI)\n"
        f"• Статус: {'🟢 вкл' if ai.get('enabled') else '🔴 выкл'}\n"
        f"• Тест-режим: {'🟢 ВКЛ (отвечает сразу)' if test else '🔴 выкл'}\n"
        f"• Неактивность: {ai.get('inactive_minutes', 5)} мин\n"
        f"• Модель: {ai.get('cf_model')}\n"
        f"• Ключи CF: {'✅' if (cf_id and cf_tok) else '❌'}\n"
        f"• Ответов ИИ: {STATE['stats'].get('ai_replies', 0)}\n"
        f"• Передач владельцу: {STATE['stats'].get('ai_fallbacks', 0)}\n"
        f"• Приостановлено диалогов: {paused}\n\n"
        "🧪 Тест-режим — ИИ отвечает сразу, даже если ты онлайн.\n"
        "Выключи его, когда закончишь тесты."
    )


def ai_paused_kb() -> InlineKeyboardMarkup:
    ai = STATE.get("ai_assistant") or {}
    rows = []
    for uid in (ai.get("paused_users") or [])[:25]:
        rows.append([InlineKeyboardButton(f"▶️ Возобновить {uid}", callback_data=f"ai_resume:{uid}")])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="ai_menu")])
    return InlineKeyboardMarkup(rows)


# ---- Автоответчик ----

def autoreply_menu_kb() -> InlineKeyboardMarkup:
    ar = STATE.get("autoreply") or {}
    enabled = ar.get("enabled", False)
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔴 Выключить" if enabled else "🟢 Включить",
                              callback_data="ar_toggle")],
        [InlineKeyboardButton("📝 Шаблон для новых", callback_data="ar_first")],
        [InlineKeyboardButton("📝 Шаблон для знакомых", callback_data="ar_known")],
        [InlineKeyboardButton(f"⏱ Неактивность: {ar.get('inactive_minutes', 5)} мин",
                              callback_data="ar_inactive")],
        [InlineKeyboardButton(f"⏳ Cooldown: {ar.get('cooldown_minutes', 60)} мин",
                              callback_data="ar_cooldown")],
        [InlineKeyboardButton("♻️ Сбросить список знакомых", callback_data="ar_reset_known")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="menu")],
    ])


def autoreply_menu_text() -> str:
    ar = STATE.get("autoreply") or {}
    tf = (ar.get("template_first") or "").strip()
    tk = (ar.get("template_known") or "").strip()
    return (
        "🤖 Автоответчик\n"
        f"• Статус: {'🟢 вкл' if ar.get('enabled') else '🔴 выкл'}\n"
        f"• Неактивность: {ar.get('inactive_minutes', 5)} мин\n"
        f"• Cooldown: {ar.get('cooldown_minutes', 60)} мин\n"
        f"• Знакомых: {len(ar.get('known_users') or [])}\n\n"
        f"📩 Шаблон для новых:\n{tf if tf else '— (не задан)'}\n\n"
        f"📩 Шаблон для знакомых:\n{tk if tk else '— (не задан)'}\n\n"
        "Срабатывает, если ИИ выключен или не смог ответить."
    )


# ---------------------------------------------------------------------------
# Заполнение known_users + логин
# ---------------------------------------------------------------------------

async def populate_known_users():
    global ME_ID
    if not USERBOT_READY:
        return
    ar = STATE.get("autoreply") or {}
    if ar.get("known_users_loaded"):
        return
    log.info("Загружаю известных юзеров…")
    known = set(ar.get("known_users") or [])
    try:
        async for dialog in user_client.get_dialogs():
            c = dialog.chat
            if c and c.type == enums.ChatType.PRIVATE and c.id > 0:
                known.add(str(c.id))
    except Exception as e:
        log.warning(f"Не смог получить диалоги: {e}")
    ar["known_users"] = list(known)
    ar["known_users_loaded"] = True
    STATE["autoreply"] = ar
    save_json(STATE_FILE, STATE)
    log.info(f"Известных юзеров: {len(known)}")


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
        except Exception as e:
            log.warning(f"Не удалось запустить dispatcher: {e}")
        asyncio.create_task(populate_known_users())
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
            text = (message.text or "").strip()
            if _was_sent_by_bot(chat.id, text):
                return
            _mark_owner_activity()
            _mark_known_ar(chat.id)
            # В тест-режиме не ставим на паузу — чтобы удобно было тестировать
            ai = STATE.get("ai_assistant") or {}
            if not ai.get("test_mode"):
                _pause_ai(chat.id)
                log.info(f"[OWNER ACTIVE] Владелец написал {chat.id} — ИИ на паузе")
            else:
                log.info(f"[OWNER ACTIVE] {chat.id} — тест-режим, пауза не ставится")
            if text:
                await db_add_message(chat.id, "assistant", text)
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
            ar = STATE.get("autoreply") or {}
            if not ai.get("enabled") and not ar.get("enabled"):
                return

            # Голосовые — уведомляем
            if message.voice or message.video_note or message.audio:
                try:
                    await bot_client.send_message(
                        CFG["admin_id"],
                        f"🔔 Клиент {user.first_name} (@{user.username or '—'}, "
                        f"id={user.id}) прислал голосовое. Ответь сам."
                    )
                except Exception:
                    pass
                return

            text = (message.text or message.caption or "").strip()
            if not text:
                return

            await db_add_message(user.id, "user", text)

            # ---- 1. ИИ ----
            test_mode = bool(ai.get("test_mode"))
            ai_should_run = (
                ai.get("enabled")
                and (test_mode or not _is_paused_ai(user.id))
                and (test_mode or _owner_inactive_ai())
            )
            if ai_should_run:
                reply, reason = await ask_ai(user.id, text)
                if reply:
                    await _send_as_userbot(user.id, reply)
                    STATE["stats"]["ai_replies"] = STATE["stats"].get("ai_replies", 0) + 1
                    save_json(STATE_FILE, STATE)
                    log.info(f"[AI REPLY] → {user.id}: {reply[:80]}")
                    return

                hard_fail = reason in ("no_credentials", "auth_error", "disabled", "empty")
                soft_fail = reason in ("timeout", "rate_limit", "exception", "bad_json")

                if hard_fail:
                    _pause_ai(user.id)
                    STATE["stats"]["ai_fallbacks"] = STATE["stats"].get("ai_fallbacks", 0) + 1
                    save_json(STATE_FILE, STATE)
                    await notify_ai_fallback(user.id, text, reason)
                elif soft_fail:
                    await notify_ai_temp_error(user.id, text, reason)
                else:
                    _pause_ai(user.id)
                    STATE["stats"]["ai_fallbacks"] = STATE["stats"].get("ai_fallbacks", 0) + 1
                    save_json(STATE_FILE, STATE)
                    await notify_ai_fallback(user.id, text, reason)

            # ---- 2. Автоответчик ----
            if (not test_mode) and ar.get("enabled") and _owner_inactive_ar() and _cooldown_ok_ar(user.id):
                known = _is_known_ar(user.id)
                template = (ar.get("template_known") if known else ar.get("template_first")) or ""
                template = template.strip()
                if template:
                    await _send_as_userbot(user.id, template)
                    _mark_known_ar(user.id)
                    _set_cooldown_ar(user.id)
                    STATE["stats"]["autoreplies"] = STATE["stats"].get("autoreplies", 0) + 1
                    save_json(STATE_FILE, STATE)
                    log.info(f"[AUTOREPLY] → {user.id} ({'known' if known else 'new'})")
                    return

        except FloodWait as fw:
            log.warning(f"FloodWait: {fw.value}s")
            await asyncio.sleep(fw.value + 2)
        except Exception as e:
            log.exception(f"on_incoming: {e}")


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

    @bot.on_message(filters.command("panel") & filters.private)
    async def cmd_panel(client, message):
        if message.from_user.id != CFG["admin_id"] or message.from_user.id not in authed:
            return
        await message.reply("🎛 Панель:", reply_markup=main_menu_kb())

    @bot.on_message(filters.command("cancel") & filters.private)
    async def cmd_cancel(client, message):
        if message.from_user.id != CFG["admin_id"]:
            return
        pending.pop(message.from_user.id, None)
        await message.reply("Отменено.", reply_markup=main_menu_kb())

    @bot.on_message(filters.command("pause") & filters.private)
    async def cmd_pause(client, message):
        if message.from_user.id != CFG["admin_id"] or message.from_user.id not in authed:
            return
        parts = (message.text or "").split(maxsplit=1)
        if len(parts) < 2:
            await message.reply("Использование: `/pause <user_id>`",
                                parse_mode=enums.ParseMode.MARKDOWN)
            return
        try:
            uid = int(parts[1].strip())
        except ValueError:
            await message.reply("❌ user_id должен быть числом.")
            return
        _pause_ai(uid)
        await message.reply(f"⏸ ИИ приостановлен для {uid}.")

    @bot.on_message(filters.command("resume") & filters.private)
    async def cmd_resume(client, message):
        if message.from_user.id != CFG["admin_id"] or message.from_user.id not in authed:
            return
        parts = (message.text or "").split(maxsplit=1)
        if len(parts) < 2:
            await message.reply("Использование: `/resume <user_id>`",
                                parse_mode=enums.ParseMode.MARKDOWN)
            return
        try:
            uid = int(parts[1].strip())
        except ValueError:
            await message.reply("❌ user_id должен быть числом.")
            return
        _unpause_ai(uid)
        await message.reply(f"▶️ ИИ снова общается с {uid}.")

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

            # ---------- ИИ ----------
            elif data == "ai_menu":
                await cb.message.edit_text(ai_menu_text(), reply_markup=ai_menu_kb())

            elif data == "ai_toggle":
                ai = STATE.setdefault("ai_assistant", _default_ai())
                ai["enabled"] = not ai.get("enabled", False)
                save_json(STATE_FILE, STATE)
                await cb.message.edit_text(ai_menu_text(), reply_markup=ai_menu_kb())

            elif data == "ai_test_toggle":
                ai = STATE.setdefault("ai_assistant", _default_ai())
                ai["test_mode"] = not ai.get("test_mode", False)
                save_json(STATE_FILE, STATE)
                st = "🟢 ВКЛ" if ai["test_mode"] else "🔴 выкл"
                await cb.answer(f"Тест-режим: {st}", show_alert=False)
                await cb.message.edit_text(ai_menu_text(), reply_markup=ai_menu_kb())

            elif data == "ai_style":
                pending[uid] = {"action": "ai_style"}
                await cb.message.edit_text(
                    "📝 Опиши стиль общения ИИ. Например:\n"
                    "«Дружелюбно, неформально, короткие сообщения, лёгкие эмодзи»\n/cancel")
            elif data == "ai_about":
                pending[uid] = {"action": "ai_about"}
                await cb.message.edit_text(
                    "👤 Напиши о себе (имя, чем занимаешься, как обращаться):\n/cancel")
            elif data == "ai_work":
                pending[uid] = {"action": "ai_work"}
                await cb.message.edit_text(
                    "💼 Опиши суть работы/продукта (что продаёшь, цены, схему):\n/cancel")
            elif data == "ai_forbidden":
                pending[uid] = {"action": "ai_forbidden"}
                await cb.message.edit_text(
                    "🚫 Что ИИ НИКОГДА не должен говорить/делать:\n/cancel")
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
                    f"(сейчас {(STATE.get('ai_assistant') or {}).get('inactive_minutes', 5)})\n/cancel")

            elif data == "ai_cf_menu":
                await cb.message.edit_text(ai_cf_menu_text(), reply_markup=ai_cf_menu_kb())

            elif data == "ai_cf_account":
                pending[uid] = {"action": "ai_cf_account"}
                await cb.message.edit_text(
                    "Отправь Cloudflare Account ID.\n"
                    "(Бот сам уберёт переносы строк, если они попадут при копировании.)\n/cancel")

            elif data == "ai_cf_token":
                pending[uid] = {"action": "ai_cf_token"}
                await cb.message.edit_text(
                    "Отправь Cloudflare API Token.\n"
                    "(Бот сам уберёт переносы строк, если они попадут при копировании.)\n/cancel")

            elif data == "ai_cf_model":
                models_text = "\n".join(f"`{m}`" for m in CF_MODELS)
                pending[uid] = {"action": "ai_cf_model"}
                await cb.message.edit_text(
                    f"Отправь ID модели Cloudflare Workers AI.\n\n"
                    f"Популярные:\n{models_text}\n\n/cancel",
                    parse_mode=enums.ParseMode.MARKDOWN)

            elif data == "ai_paused":
                ai = STATE.get("ai_assistant") or {}
                if not (ai.get("paused_users") or []):
                    await cb.answer("Список пуст.", show_alert=True)
                    return
                await cb.message.edit_text(
                    "📋 Приостановленные (ИИ не отвечает):",
                    reply_markup=ai_paused_kb())
            elif data.startswith("ai_resume:"):
                try:
                    target = int(data.split(":", 1)[1])
                except ValueError:
                    await cb.answer("Ошибка.")
                    return
                _unpause_ai(target)
                await cb.answer(f"▶️ {target} возобновлён.")
                await cb.message.edit_text(ai_menu_text(), reply_markup=ai_menu_kb())

            # ---------- Автоответчик ----------
            elif data == "ar_menu":
                await cb.message.edit_text(autoreply_menu_text(), reply_markup=autoreply_menu_kb())
            elif data == "ar_toggle":
                ar = STATE.setdefault("autoreply", _default_autoreply())
                ar["enabled"] = not ar.get("enabled", False)
                save_json(STATE_FILE, STATE)
                await cb.message.edit_text(autoreply_menu_text(), reply_markup=autoreply_menu_kb())
            elif data == "ar_first":
                pending[uid] = {"action": "ar_first"}
                await cb.message.edit_text("Отправь текст шаблона для НОВЫХ собеседников.\n/cancel")
            elif data == "ar_known":
                pending[uid] = {"action": "ar_known"}
                await cb.message.edit_text("Отправь текст шаблона для ЗНАКОМЫХ собеседников.\n/cancel")
            elif data == "ar_inactive":
                pending[uid] = {"action": "ar_inactive"}
                await cb.message.edit_text(
                    f"Через сколько минут отсутствия включать автоответ? "
                    f"(сейчас {STATE['autoreply'].get('inactive_minutes', 5)})\n/cancel")
            elif data == "ar_cooldown":
                pending[uid] = {"action": "ar_cooldown"}
                await cb.message.edit_text(
                    f"Cooldown на собеседника в минутах "
                    f"(сейчас {STATE['autoreply'].get('cooldown_minutes', 60)})\n/cancel")
            elif data == "ar_reset_known":
                ar = STATE.setdefault("autoreply", _default_autoreply())
                ar["known_users"] = []
                ar["known_users_loaded"] = False
                save_json(STATE_FILE, STATE)
                await cb.message.edit_text(
                    "♻️ Список знакомых сброшен.",
                    reply_markup=autoreply_menu_kb())

            # ---------- Статус ----------
            elif data == "status":
                s = STATE["stats"]
                ai = STATE.get("ai_assistant") or {}
                ar = STATE.get("autoreply") or {}
                cf_id = ai.get("cf_account_id") or CFG.get("cf_account_id") or os.getenv("CF_ACCOUNT_ID", "")
                cf_tok = ai.get("cf_api_token") or CFG.get("cf_api_token") or os.getenv("CF_API_TOKEN", "")
                txt = (
                    f"📊 Статистика\n"
                    f"• Юзербот: {'🟢' if USERBOT_READY else '🔴'}\n"
                    f"• Рассылка: {'🟢' if STATE.get('running') else '🔴'}\n"
                    f"• Групп: {len(STATE.get('groups') or [])}\n"
                    f"• Отправлено: {s.get('sent', 0)}\n"
                    f"• Ошибок: {s.get('errors', 0)}\n"
                    f"• Кругов: {s.get('rounds', 0)}\n\n"
                    f"🧠 ИИ (Cloudflare): {'🟢' if ai.get('enabled') else '🔴'}\n"
                    f"• Тест-режим: {'🟢' if ai.get('test_mode') else '🔴'}\n"
                    f"• Ключи CF: {'✅' if (cf_id and cf_tok) else '❌'}\n"
                    f"• Модель: {ai.get('cf_model')}\n"
                    f"• Ответов ИИ: {s.get('ai_replies', 0)}\n"
                    f"• Передач владельцу: {s.get('ai_fallbacks', 0)}\n"
                    f"• Приостановлено: {len(ai.get('paused_users') or [])}\n"
                    f"• Неактивность: {ai.get('inactive_minutes', 5)} мин\n\n"
                    f"🤖 Автоответчик: {'🟢' if ar.get('enabled') else '🔴'}\n"
                    f"• Автоответов: {s.get('autoreplies', 0)}\n"
                    f"• Неактивность: {ar.get('inactive_minutes', 5)} мин\n"
                    f"• Cooldown: {ar.get('cooldown_minutes', 60)} мин"
                )
                kb = InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔄 Обновить", callback_data="status")],
                    [InlineKeyboardButton("🗑 Очистить историю БД (30д)", callback_data="db_cleanup")],
                    [InlineKeyboardButton("⬅️ Назад", callback_data="menu")],
                ])
                await cb.message.edit_text(txt, reply_markup=kb)
            elif data == "db_cleanup":
                await db_cleanup(30)
                await cb.answer("✅ История за 30 дней очищена.", show_alert=True)
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
            # логин
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
                    await message.reply("⚠️ Код истёк. /login заново.")
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

            # сообщение рассылки
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

            # группы
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

            # тайминги
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

            # ИИ
            elif action in ("ai_style", "ai_about", "ai_work", "ai_forbidden"):
                ai = STATE.setdefault("ai_assistant", _default_ai())
                parts = ai.setdefault("prompt_parts", {})
                parts[action] = message.text or ""
                ai["prompt_parts"] = parts

                style = (parts.get("ai_style") or "").strip()
                about = (parts.get("ai_about") or "").strip()
                work = (parts.get("ai_work") or "").strip()
                forb = (parts.get("ai_forbidden") or "").strip()

                lines = []
                if style: lines.append(f"СТИЛЬ ОБЩЕНИЯ:\n{style}")
                if about: lines.append(f"ИНФОРМАЦИЯ ОБО МНЕ:\n{about}")
                if work:  lines.append(f"СУТЬ РАБОТЫ:\n{work}")
                if forb:  lines.append(f"ЗАПРЕЩЕНО:\n{forb}")
                lines.append(
                    "ОБЩИЕ ПРАВИЛА:\n"
                    "- Пиши как живой человек, не как робот.\n"
                    "- Не пиши слишком длинные или слишком короткие сообщения.\n"
                    "- Если клиент запутан или сложный вопрос — передай владельцу.\n"
                    "- Никогда не говори, что ты бот или ИИ."
                )
                ai["system_prompt"] = "\n\n".join(lines)
                save_json(STATE_FILE, STATE)
                await message.reply("✅ Сохранено. Промпт обновлён.",
                                    reply_markup=ai_menu_kb())

            elif action == "ai_inactive":
                try:
                    v = int(text)
                    if v < 1: raise ValueError("мин. 1")
                    STATE["ai_assistant"]["inactive_minutes"] = v
                    save_json(STATE_FILE, STATE)
                    await message.reply(f"✅ Неактивность ИИ: {v} мин.",
                                        reply_markup=ai_menu_kb())
                except Exception as e:
                    pending[uid] = {"action": "ai_inactive"}
                    await message.reply(f"❌ {e}")

            elif action == "ai_cf_account":
                cleaned = clean_secret(text)
                STATE["ai_assistant"]["cf_account_id"] = cleaned
                save_json(STATE_FILE, STATE)
                await message.reply(
                    f"✅ CF Account ID сохранён (длина {len(cleaned)}).",
                    reply_markup=ai_cf_menu_kb())

            elif action == "ai_cf_token":
                cleaned = clean_secret(text)
                STATE["ai_assistant"]["cf_api_token"] = cleaned
                save_json(STATE_FILE, STATE)
                await message.reply(
                    f"✅ CF API Token сохранён (длина {len(cleaned)}).",
                    reply_markup=ai_cf_menu_kb())

            elif action == "ai_cf_model":
                cleaned = clean_secret(text)
                STATE["ai_assistant"]["cf_model"] = cleaned or CF_MODELS[0]
                save_json(STATE_FILE, STATE)
                await message.reply(f"✅ Модель: {STATE['ai_assistant']['cf_model']}",
                                    reply_markup=ai_menu_kb())

            # автоответчик
            elif action == "ar_first":
                STATE.setdefault("autoreply", _default_autoreply())["template_first"] = message.text or ""
                save_json(STATE_FILE, STATE)
                await message.reply("✅ Шаблон для новых сохранён.",
                                    reply_markup=autoreply_menu_kb())
            elif action == "ar_known":
                STATE.setdefault("autoreply", _default_autoreply())["template_known"] = message.text or ""
                save_json(STATE_FILE, STATE)
                await message.reply("✅ Шаблон для знакомых сохранён.",
                                    reply_markup=autoreply_menu_kb())
            elif action == "ar_inactive":
                try:
                    v = int(text)
                    if v < 1: raise ValueError("мин. 1")
                    STATE["autoreply"]["inactive_minutes"] = v
                    save_json(STATE_FILE, STATE)
                    await message.reply(f"✅ Неактивность автоответчика: {v} мин.",
                                        reply_markup=autoreply_menu_kb())
                except Exception as e:
                    pending[uid] = {"action": "ar_inactive"}
                    await message.reply(f"❌ {e}")
            elif action == "ar_cooldown":
                try:
                    v = int(text)
                    if v < 0: raise ValueError("мин. 0")
                    STATE["autoreply"]["cooldown_minutes"] = v
                    save_json(STATE_FILE, STATE)
                    await message.reply(f"✅ Cooldown: {v} мин.",
                                        reply_markup=autoreply_menu_kb())
                except Exception as e:
                    pending[uid] = {"action": "ar_cooldown"}
                    await message.reply(f"❌ {e}")

        except Exception as e:
            log.exception("Ошибка ввода админа")
            await message.reply(f"❌ {e}")


# ---------------------------------------------------------------------------
# Глобальный обработчик исключений asyncio
# ---------------------------------------------------------------------------

def _install_asyncio_exception_handler(loop):
    def handler(loop, context):
        exc = context.get("exception")
        msg = str(context.get("message") or "")
        if isinstance(exc, ValueError) and "Peer id invalid" in str(exc):
            return
        if isinstance(exc, KeyError) and "ID not found" in str(exc):
            return
        if "Peer id invalid" in msg or "ID not found" in msg:
            return
        loop.default_exception_handler(context)
    loop.set_exception_handler(handler)


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

async def main():
    global CFG, STATE, user_client, bot_client, http_session, USERBOT_READY, ME_ID

    CFG = load_cfg()
    if not cfg_ok(CFG):
        print("Не заданы переменные: API_ID, API_HASH, BOT_TOKEN, ADMIN_ID")
        return
    CFG.setdefault("pin", DEFAULT_PIN)
    save_json(CONFIG_FILE, CFG)
    STATE = load_state()

    _install_asyncio_exception_handler(asyncio.get_running_loop())

    await db_init()

    http_session = aiohttp.ClientSession()

    user_client = Client(name=SESSION_USER, api_id=CFG["api_id"], api_hash=CFG["api_hash"])
    register_user_handlers(user_client)

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

    try:
        await user_client.start()
        me = await user_client.get_me()
        if me:
            ME_ID = me.id
            USERBOT_READY = True
            log.info(f"✅ Юзербот: {me.first_name} (@{me.username}) id={me.id}")
            asyncio.create_task(populate_known_users())
            start_peer_refresh()
    except Exception as e:
        log.warning(f"Юзербот не авторизован: {e}")
        try:
            if not user_client.is_connected:
                await user_client.connect()
        except Exception:
            pass
        USERBOT_READY = False

    try:
        ai = STATE.get("ai_assistant") or {}
        ar = STATE.get("autoreply") or {}
        cf_id, cf_tok = _get_cf_creds()
        cf_ok = "🟢" if (cf_id and cf_tok) else "🔴 (настрой в ИИ-меню)"
        await bot_client.send_message(
            CFG["admin_id"],
            f"🤖 Автопостер запущен.\n"
            f"• Юзербот: {'🟢 готов' if USERBOT_READY else '🔴 /login'}\n"
            f"• ИИ (Cloudflare): {'🟢 вкл' if ai.get('enabled') else '🔴 выкл'}\n"
            f"• Тест-режим: {'🧪 ВКЛ' if ai.get('test_mode') else '🔴 выкл'}\n"
            f"• CF ключи: {cf_ok}\n"
            f"• Автоответчик: {'🟢 вкл' if ar.get('enabled') else '🔴 выкл'}\n"
            f"Для доступа: `/auth <PIN>`",
            parse_mode=enums.ParseMode.MARKDOWN)
    except Exception as e:
        log.warning(f"Не отправить приветствие: {e}")

    if STATE.get("running") and USERBOT_READY:
        log.info("Возобновляю рассылку…")
        start_mailing()
    elif STATE.get("running") and not USERBOT_READY:
        STATE["running"] = False
        save_json(STATE_FILE, STATE)

    log.info("Сервис работает. Ctrl+C для остановки.")
    try:
        await asyncio.Event().wait()
    finally:
        if http_session:
            await http_session.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        print("\nОстановлено.")
