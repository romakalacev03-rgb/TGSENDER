# -*- coding: utf-8 -*-
"""
Автопостер с мульти-аккаунтами + массовая подписка + автоответчик.
+ Автоподписка на обязательные каналы
+ Автопрохождение капчи (inline "Я не бот" + обязательные подписки)
+ Поиск публичных ГРУПП (не каналов) по ключевым словам
+ Распределение групп в папки Telegram (dialog filters, по 100 групп)
+ Разбор ошибок отправки по типам
+ Проверка аккаунта через @SpamBot
+ Рандомные реакции в группах
+ Уведомления о личных сообщениях
"""

import asyncio
import json
import os
import random
import re
import logging
from datetime import datetime, timedelta
from collections import Counter

import aiohttp
import aiosqlite

from pyrogram import Client, filters, enums
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.errors import (
    FloodWait, SlowmodeWait,
    ChatWriteForbidden, ChatAdminRequired, UserBannedInChannel, UserNotParticipant,
    PeerIdInvalid, UserIsBlocked, ChannelPrivate, ChatForbidden,
    MessageTooLong, MediaCaptionTooLong,
    ChatSendPlainForbidden, ChatSendMediaForbidden,
    ChatSendPhotosForbidden, ChatSendVideosForbidden, ChatSendStickersForbidden,
    ChatSendGifsForbidden, ChatSendAudiosForbidden, ChatSendDocsForbidden,
    ChatSendPollForbidden, ChatSendInlineForbidden,
    SessionPasswordNeeded, PhoneCodeInvalid, PhoneCodeExpired, PasswordHashInvalid,
    UserAlreadyParticipant, UsernameInvalid, UsernameNotOccupied,
    InviteHashExpired, InviteHashInvalid, ChatIdInvalid,
)
from pyrogram.raw.functions.contacts import Search as RawSearch
from pyrogram.raw.functions.messages import GetDialogFilters, UpdateDialogFilter
from pyrogram.raw.types import Channel as RawChannel, Chat as RawChat, DialogFilter

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
SESSION_BOT = os.path.join(DATA_DIR, "control_bot")

try:
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(MEDIA_DIR, exist_ok=True)
except Exception:
    pass

DEFAULT_PIN = "2512"
SPAMBOT_USERNAME = "SpamBot"

REACTION_EMOJIS = ["👍", "❤", "🔥", "🤝", "😊", "💯", "⚡", "🎯", "👌", "🙏"]
GROUP_REACTION_EMOJIS = ["👍", "🔥", "💯", "🎯", "⚡", "🤝", "❤️", "😎"]

CAPTCHA_KEYWORDS = [
    "капч", "captcha", "проверк", "verify", "verification",
    "подтверд", "confirm", "нажмите", "нажми", "click",
    "подпишитесь", "subscribe", "робот", "robot", "не бот",
    "продолжить", "continue", "start", "старт", "чтобы писать",
    "чтобы отправлять", "доступ", "access",
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("autoposter")
log.info(f"DATA_DIR = {DATA_DIR}")

CFG: dict = {}
STATE: dict = {}
bot_client: Client = None
http_session: aiohttp.ClientSession = None

user_clients: dict = {}
mailing_tasks: dict = {}
subscribe_tasks: dict = {}
folder_tasks: dict = {}
ME_IDS: dict = {}

authed: set = set()
pending: dict = {}
MAIN_HANDLERS_REGISTERED = set()
CAPTCHA_HANDLERS_REGISTERED = set()
REACTION_HANDLERS_REGISTERED = set()

# acc_id -> {chat_id: iso_timestamp}
RECENT_JOINS: dict = {}
# uid -> {acc_id: [results]}
SEARCH_CACHE: dict = {}

DEVICE_PARAMS = {
    "app_version": "9.3.1",
    "device_model": "Samsung Galaxy S23 Ultra",
    "system_version": "Android 13",
    "lang_code": "ru",
}


def make_client(session_path: str) -> Client:
    return Client(
        name=session_path,
        api_id=CFG["api_id"],
        api_hash=CFG["api_hash"],
        app_version=DEVICE_PARAMS["app_version"],
        device_model=DEVICE_PARAMS["device_model"],
        system_version=DEVICE_PARAMS["system_version"],
        lang_code=DEVICE_PARAMS["lang_code"],
    )


def clean_secret(s) -> str:
    if s is None:
        return ""
    return "".join(str(s).split())


# ---------------------------------------------------------------------------
# Классификация ошибок
# ---------------------------------------------------------------------------

def classify_error(e: Exception) -> str:
    if isinstance(e, FloodWait):
        return f"FloodWait {e.value}с"
    if isinstance(e, SlowmodeWait):
        return f"Slowmode {e.value}с"
    if isinstance(e, ChatWriteForbidden):
        return "Запрещено писать в группе"
    if isinstance(e, ChatAdminRequired):
        return "Нужны права администратора"
    if isinstance(e, UserBannedInChannel):
        return "Забанен в группе"
    if isinstance(e, UserNotParticipant):
        return "Не участник группы"
    if isinstance(e, PeerIdInvalid):
        return "ID группы устарел"
    if isinstance(e, UserIsBlocked):
        return "Вы заблокированы в этой группе"
    if isinstance(e, ChannelPrivate):
        return "Приватная / уже вышел"
    if isinstance(e, ChatForbidden):
        return "Чат запрещён для записи"
    if isinstance(e, MessageTooLong):
        return "Текст слишком длинный"
    if isinstance(e, MediaCaptionTooLong):
        return "Подпись слишком длинная"
    if isinstance(e, ChatSendPlainForbidden):
        return "Запрещена отправка текста"
    if isinstance(e, ChatSendMediaForbidden):
        return "Запрещена отправка медиа"
    if isinstance(e, ChatSendPhotosForbidden):
        return "Запрещены фото"
    if isinstance(e, ChatSendVideosForbidden):
        return "Запрещены видео"
    if isinstance(e, ChatSendStickersForbidden):
        return "Запрещены стикеры"
    if isinstance(e, ChatSendGifsForbidden):
        return "Запрещены GIF"
    if isinstance(e, ChatSendAudiosForbidden):
        return "Запрещены аудио"
    if isinstance(e, ChatSendDocsForbidden):
        return "Запрещены документы"
    if isinstance(e, ChatSendPollForbidden):
        return "Запрещены опросы"
    if isinstance(e, ChatSendInlineForbidden):
        return "Запрещён inline"
    return f"{type(e).__name__}: {str(e)[:80]}"


# ---------------------------------------------------------------------------
# SQLite (только для истории ЛС — автоответчик)
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
                (user_id, role, content, datetime.now().isoformat(timespec="seconds")))
            await db.commit()
    except Exception as e:
        log.error(f"DB add error: {e}")


async def db_cleanup(days: int = 30):
    try:
        cutoff = (datetime.now() - timedelta(days=days)).isoformat(timespec="seconds")
        async with aiosqlite.connect(DB_FILE) as db:
            await db.execute("DELETE FROM messages WHERE timestamp < ?", (cutoff,))
            await db.commit()
    except Exception:
        pass


# ---------------------------------------------------------------------------
# JSON / config
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


ENV_MAP = {
    "API_ID": ("api_id", int),
    "API_HASH": ("api_hash", str),
    "BOT_TOKEN": ("bot_token", str),
    "ADMIN_ID": ("admin_id", int),
    "PIN": ("pin", str),
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
# State
# ---------------------------------------------------------------------------

def _default_autoreply() -> dict:
    return {
        "enabled": False, "inactive_minutes": 5, "cooldown_minutes": 60,
        "template_first": "", "template_known": "",
        "known_users": [], "known_users_loaded": False, "last_reply": {},
    }


def _new_account(name: str, session_name: str) -> dict:
    return {
        "id": f"acc_{int(datetime.now().timestamp() * 1000)}_{random.randint(100, 999)}",
        "name": name, "session_name": session_name,
        "user_id": None, "username": None,
        "text": "", "caption": "", "media_path": None, "media_type": None,
        "groups": [], "running": False,
        "interval": 1800, "delay_min": 5, "delay_max": 15,
        "stats": {"sent": 0, "errors": 0, "rounds": 0, "last_round": None, "next_round": None},
        "error_stats": {},          # {"Запрещено писать в группе": 12, ...}
        "failed_groups": {},        # {gid: {"title":..,"error":..,"count":N,"last":ts}}
        "subscribe_queue": [], "subscribe_status": "idle",
        "subscribe_delay_min": 40, "subscribe_delay_max": 120,
        "subscribe_stats": {"subscribed": 0, "skipped": 0, "errors": 0, "started_at": None},
        "auto_subscribe_enabled": True, "auto_subscribe_queue": [], "auto_subscribe_delay": 30,
        "last_spam_check": None, "spam_status": "unknown", "spam_message": "",
        "folder_index": 0, "folder_size": 100,
        "group_reactions_enabled": True, "group_reactions_chance": 5,
        "captcha_enabled": True,
        "auto_drop_dead": False,   # автоудаление битых групп
    }


def default_state() -> dict:
    return {
        "accounts": [], "main_account_id": "", "owner_last_activity": None,
        "autoreply": _default_autoreply(),
        "global_stats": {
            "autoreplies": 0, "auto_subscribes": 0,
            "group_reactions": 0, "folder_moves": 0, "captchas_passed": 0,
        },
        "groups": [], "known_channels": [],
    }


def load_state() -> dict:
    raw = load_json(STATE_FILE, {}) or {}
    st = default_state()
    if not raw.get("accounts"):
        has_old = any(k in raw for k in ("text", "groups", "interval", "delay_min"))
        if has_old:
            acc = _new_account("Основной", "userbot")
            acc["text"] = raw.get("text") or ""
            acc["caption"] = raw.get("caption") or ""
            acc["media_path"] = raw.get("media_path")
            acc["media_type"] = raw.get("media_type")
            acc["groups"] = raw.get("groups") or []
            acc["running"] = raw.get("running", False)
            acc["interval"] = raw.get("interval", 1800)
            acc["delay_min"] = raw.get("delay_min", 5)
            acc["delay_max"] = raw.get("delay_max", 15)
            st["accounts"] = [acc]
            st["main_account_id"] = acc["id"]
    for k, v in raw.items():
        if k in ("text", "caption", "media_path", "media_type", "groups",
                 "running", "interval", "delay_min", "delay_max", "stats",
                 "ai_assistant"):
            continue
        st[k] = v
    merged = st.get("autoreply") or {}
    for k, v in _default_autoreply().items():
        merged.setdefault(k, v)
    st["autoreply"] = merged
    if not isinstance(st.get("global_stats"), dict):
        st["global_stats"] = default_state()["global_stats"]
    for k in ("autoreplies", "auto_subscribes", "group_reactions",
              "folder_moves", "captchas_passed"):
        st["global_stats"].setdefault(k, 0)
    if not st.get("main_account_id") and st.get("accounts"):
        st["main_account_id"] = st["accounts"][0]["id"]
    for acc in st.get("accounts") or []:
        acc.setdefault("subscribe_queue", [])
        acc.setdefault("subscribe_status", "idle")
        acc.setdefault("subscribe_delay_min", 40)
        acc.setdefault("subscribe_delay_max", 120)
        acc.setdefault("subscribe_stats",
                       {"subscribed": 0, "skipped": 0, "errors": 0, "started_at": None})
        acc.setdefault("auto_subscribe_enabled", True)
        acc.setdefault("auto_subscribe_queue", [])
        acc.setdefault("auto_subscribe_delay", 30)
        acc.setdefault("last_spam_check", None)
        acc.setdefault("spam_status", "unknown")
        acc.setdefault("spam_message", "")
        acc.setdefault("folder_index", 0)
        acc.setdefault("folder_size", 100)
        acc.setdefault("group_reactions_enabled", True)
        acc.setdefault("group_reactions_chance", 5)
        acc.setdefault("captcha_enabled", True)
        acc.setdefault("error_stats", {})
        acc.setdefault("failed_groups", {})
        acc.setdefault("auto_drop_dead", False)
    if not isinstance(st.get("groups"), list):
        st["groups"] = []
    if not isinstance(st.get("known_channels"), list):
        st["known_channels"] = []
    return st


def get_account(acc_id: str) -> dict:
    for a in STATE.get("accounts") or []:
        if a.get("id") == acc_id:
            return a
    return {}


def get_main_account() -> dict:
    mid = STATE.get("main_account_id") or ""
    acc = get_account(mid)
    if acc:
        return acc
    accounts = STATE.get("accounts") or []
    return accounts[0] if accounts else {}


def add_account(name: str, session_name: str) -> dict:
    acc = _new_account(name, session_name)
    STATE.setdefault("accounts", []).append(acc)
    if not STATE.get("main_account_id"):
        STATE["main_account_id"] = acc["id"]
    save_json(STATE_FILE, STATE)
    return acc


def remove_account(acc_id: str) -> bool:
    accounts = STATE.get("accounts") or []
    new_list = [a for a in accounts if a.get("id") != acc_id]
    if len(new_list) == len(accounts):
        return False
    STATE["accounts"] = new_list
    if STATE.get("main_account_id") == acc_id:
        STATE["main_account_id"] = new_list[0]["id"] if new_list else ""
    save_json(STATE_FILE, STATE)
    return True


# ---------------------------------------------------------------------------
# Группы / рефы
# ---------------------------------------------------------------------------

def normalize_group_ref(g) -> str:
    if isinstance(g, dict):
        gid = g.get("id")
        return str(gid) if gid else ""
    return str(g)


def check_group_overlaps() -> dict:
    accounts = STATE.get("accounts") or []
    key_to_accs = {}
    for acc in accounts:
        for g in acc.get("groups") or []:
            key = normalize_group_ref(g)
            if not key:
                continue
            key_to_accs.setdefault(key, []).append(acc["id"])
    overlaps = {}
    for key, acc_ids in key_to_accs.items():
        if len(acc_ids) > 1:
            for aid in acc_ids:
                overlaps.setdefault(aid, []).append(key)
    return overlaps


def parse_groups_from_text(text: str) -> list:
    result = []
    for line in text.split("\n"):
        line = line.split("#")[0].strip()
        if not line:
            continue
        parts = [p.strip() for p in line.split(",") if p.strip()]
        for p in parts:
            ref = parse_chat_ref(p)
            if ref is not None:
                result.append(ref)
    seen = set()
    unique = []
    for r in result:
        key = str(r)
        if key not in seen:
            seen.add(key)
            unique.append(r)
    return unique


def parse_chat_ref(text: str):
    if not text:
        return None
    s = text.strip()
    for pref in ("https://t.me/", "http://t.me/", "t.me/"):
        if s.startswith(pref):
            s = s[len(pref):]
            break
    s = s.split("?")[0].strip()
    if s.startswith("+"):
        return "https://t.me/" + s
    s = s.split("/")[0].strip()
    if not s:
        return None
    if s.startswith("@"):
        return s
    try:
        return int(s)
    except ValueError:
        return "@" + s


# ---------------------------------------------------------------------------
# Автоответчик helpers
# ---------------------------------------------------------------------------

def _owner_inactive(minutes: int) -> bool:
    last = STATE.get("owner_last_activity")
    if not last:
        return True
    try:
        last_dt = datetime.fromisoformat(last)
        return (datetime.now() - last_dt).total_seconds() > minutes * 60
    except Exception:
        return True


def _owner_inactive_ar() -> bool:
    return _owner_inactive(int((STATE.get("autoreply") or {}).get("inactive_minutes", 5)))


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
    last = (ar.get("last_reply") or {}).get(str(user_id))
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


# ---------------------------------------------------------------------------
# Автокапча
# ---------------------------------------------------------------------------

def mark_recent_join(acc_id: str, chat_id: int):
    RECENT_JOINS.setdefault(acc_id, {})[chat_id] = datetime.now().isoformat(timespec="seconds")


def is_recent_join(acc_id: str, chat_id: int, minutes: int = 20) -> bool:
    ts = (RECENT_JOINS.get(acc_id) or {}).get(chat_id)
    if not ts:
        return False
    try:
        return (datetime.now() - datetime.fromisoformat(ts)).total_seconds() < minutes * 60
    except Exception:
        return False


async def _join_from_button(client: Client, url: str) -> bool:
    if not url:
        return False
    try:
        if "t.me/" not in url and not url.startswith("@"):
            return False
        try:
            await client.join_chat(url)
            return True
        except UserAlreadyParticipant:
            return True
        except FloodWait as fw:
            await asyncio.sleep(fw.value + 5)
            return True
        except Exception as e:
            log.debug(f"join_from_button: {e}")
            return False
    except Exception:
        return False


async def try_pass_captcha(client: Client, message, acc_id: str) -> bool:
    markup = message.reply_markup
    kb = getattr(markup, "inline_keyboard", None) if markup else None
    if not kb:
        return False

    clicked = False
    joined_any = False

    # 1) URL-кнопки (обязательные подписки)
    for row in kb:
        for btn in row:
            if getattr(btn, "url", None):
                ok = await _join_from_button(client, btn.url)
                if ok:
                    joined_any = True

    # 2) callback-кнопки ("Я не бот" и т.п.)
    for row in kb:
        for btn in row:
            cb_data = getattr(btn, "callback_data", None)
            if not cb_data:
                continue
            try:
                await client.request_callback_answer(
                    chat_id=message.chat.id,
                    message_id=message.id,
                    callback_data=cb_data,
                )
                clicked = True
                log.info(f"[{acc_id}] ✅ капча: нажата кнопка в {message.chat.id}")
                await asyncio.sleep(1)
            except FloodWait as fw:
                await asyncio.sleep(fw.value + 2)
            except Exception as e:
                log.debug(f"[{acc_id}] captcha cb error: {e}")

    # Если подписались, но не нажали — пробуем ещё раз через 4 сек (кнопки могут появиться)
    if joined_any and not clicked:
        try:
            await asyncio.sleep(4)
            msg2 = await client.get_messages(message.chat.id, message.id)
            if msg2 and msg2.reply_markup:
                kb2 = getattr(msg2.reply_markup, "inline_keyboard", None) or []
                for row in kb2:
                    for btn in row:
                        cd = getattr(btn, "callback_data", None)
                        if cd:
                            try:
                                await client.request_callback_answer(
                                    chat_id=msg2.chat.id,
                                    message_id=msg2.id,
                                    callback_data=cd,
                                )
                                clicked = True
                                await asyncio.sleep(1)
                            except Exception:
                                pass
        except Exception:
            pass

    if clicked or joined_any:
        STATE["global_stats"]["captchas_passed"] = \
            STATE["global_stats"].get("captchas_passed", 0) + 1
        save_json(STATE_FILE, STATE)

    return clicked or joined_any


def register_captcha_handler(c: Client, acc_id: str):
    if c.name in CAPTCHA_HANDLERS_REGISTERED:
        return
    CAPTCHA_HANDLERS_REGISTERED.add(c.name)

    @c.on_message(filters.incoming & (filters.group | filters.channel))
    async def on_group_msg(client, message):
        try:
            acc = get_account(acc_id)
            if not acc or not acc.get("captcha_enabled", True):
                return
            user = message.from_user
            if not user or not user.is_bot:
                return
            chat = message.chat
            if not chat:
                return
            chat_id = chat.id
            in_recent = is_recent_join(acc_id, chat_id)
            text_low = ((message.text or message.caption or "")).lower()
            has_kb = bool(message.reply_markup)
            matched = any(k in text_low for k in CAPTCHA_KEYWORDS)
            if not in_recent and not (has_kb and matched):
                return
            if not has_kb:
                return
            await try_pass_captcha(client, message, acc_id)
        except FloodWait as fw:
            await asyncio.sleep(fw.value + 2)
        except Exception as e:
            log.debug(f"captcha handler: {e}")


# ---------------------------------------------------------------------------
# Реакции в группах
# ---------------------------------------------------------------------------

def register_reaction_handler(c: Client, acc_id: str):
    if c.name in REACTION_HANDLERS_REGISTERED:
        return
    REACTION_HANDLERS_REGISTERED.add(c.name)

    @c.on_message(filters.incoming & (filters.group | filters.channel))
    async def on_group_msg_react(client, message):
        try:
            acc = get_account(acc_id)
            if not acc or not acc.get("group_reactions_enabled", True):
                return
            if not message.from_user or message.from_user.is_bot:
                return
            try:
                chance = int(acc.get("group_reactions_chance", 5))
            except Exception:
                chance = 5
            if random.randint(1, 100) > chance:
                return
            emoji = random.choice(GROUP_REACTION_EMOJIS)
            try:
                await client.send_reaction(
                    chat_id=message.chat.id, message_id=message.id, emoji=emoji)
                STATE["global_stats"]["group_reactions"] = \
                    STATE["global_stats"].get("group_reactions", 0) + 1
                save_json(STATE_FILE, STATE)
            except Exception:
                pass
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Поиск публичных ГРУПП (только группы, не каналы)
# ---------------------------------------------------------------------------

async def search_public_groups(client: Client, query: str, limit: int = 50) -> list:
    try:
        result = await client.invoke(RawSearch(q=query, limit=limit))
    except FloodWait as fw:
        await asyncio.sleep(fw.value + 2)
        return []
    except Exception as e:
        log.warning(f"search error: {e}")
        return []

    found = []
    seen = set()
    for chat in result.chats:
        try:
            if isinstance(chat, RawChannel):
                # только супергруппы (megagroup=True, broadcast=False)
                if getattr(chat, "broadcast", False):
                    continue
                if not getattr(chat, "megagroup", False):
                    continue
                username = getattr(chat, "username", None)
                if not username:
                    continue
                key = username.lower()
                if key in seen:
                    continue
                seen.add(key)
                found.append({
                    "ref": "@" + username,
                    "title": chat.title or username,
                    "username": username,
                    "participants": getattr(chat, "participants_count", 0) or 0,
                    "type": "supergroup",
                })
            elif isinstance(chat, RawChat):
                # классическая группа
                username = getattr(chat, "username", None)
                if not username:
                    continue
                key = username.lower()
                if key in seen:
                    continue
                seen.add(key)
                found.append({
                    "ref": "@" + username,
                    "title": chat.title or username,
                    "username": username,
                    "participants": getattr(chat, "participants_count", 0) or 0,
                    "type": "group",
                })
        except Exception:
            continue
    found.sort(key=lambda x: x["participants"], reverse=True)
    return found


# ---------------------------------------------------------------------------
# Папки Telegram (dialog filters)
# ---------------------------------------------------------------------------

async def distribute_to_folders(acc_id: str) -> tuple:
    """Распределяет группы аккаунта в папки Telegram по folder_size штук."""
    acc = get_account(acc_id)
    c = user_clients.get(acc_id)
    if not acc or not c:
        return 0, "Нет аккаунта/клиента"
    groups = acc.get("groups") or []
    if not groups:
        return 0, "Нет групп"

    folder_size = max(10, int(acc.get("folder_size", 100)))
    folder_prefix = f"AP·{acc.get('name', 'acc')[:14]}"

    # resolve peers
    input_peers = []
    resolve_failed = 0
    for g in groups:
        try:
            peer = await c.resolve_peer(g["id"])
            input_peers.append(peer)
        except Exception:
            resolve_failed += 1
        await asyncio.sleep(0.1)

    # существующие папки
    existing_titles = {}
    try:
        res = await c.invoke(GetDialogFilters())
        for f in res.filters:
            t = getattr(f, "title", None)
            fid = getattr(f, "id", None)
            if t and fid is not None:
                existing_titles[t] = fid
    except Exception as e:
        return 0, f"Не получил список папок: {e}"

    chunks = [input_peers[i:i + folder_size]
              for i in range(0, len(input_peers), folder_size)]

    created = 0
    errors = 0
    for i, chunk in enumerate(chunks, 1):
        title = f"{folder_prefix} #{i}"
        folder_id = existing_titles.get(title)
        if folder_id is None:
            # берём свободный id (2..255, но на практике до 10-20 папок)
            used = set(existing_titles.values())
            folder_id = 2
            while folder_id in used and folder_id < 255:
                folder_id += 1
        try:
            filt = DialogFilter(
                id=folder_id,
                title=title,
                pinned_peers=[],
                include_peers=chunk,
                exclude_peers=[],
            )
            await c.invoke(UpdateDialogFilter(id=folder_id, filter=filt))
            created += 1
        except Exception as e:
            log.warning(f"folder create '{title}': {e}")
            errors += 1
        await asyncio.sleep(1.2)

    acc["folder_index"] = len(chunks)
    save_json(STATE_FILE, STATE)

    msg = (f"📂 Папок: {created}/{len(chunks)} | "
           f"Групп: {len(input_peers)} | "
           f"Ошибок resolve: {resolve_failed} | "
           f"Ошибок создания: {errors}")
    return created, msg


def start_folder_sort(acc_id: str):
    t = folder_tasks.get(f"folders_{acc_id}")
    if t and not t.done():
        return
    folder_tasks[f"folders_{acc_id}"] = asyncio.create_task(_folder_sort_task(acc_id))


async def _folder_sort_task(acc_id: str):
    acc = get_account(acc_id)
    if not acc:
        return
    try:
        created, msg = await distribute_to_folders(acc_id)
        await bot_client.send_message(
            CFG["admin_id"],
            f"📂 [{acc.get('name')}] Распределение по папкам завершено.\n{msg}")
    except Exception as e:
        log.exception("folder_sort")
        try:
            await bot_client.send_message(
                CFG["admin_id"], f"❌ Ошибка распределения: {e}")
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Автоподписка на обязательные каналы
# ---------------------------------------------------------------------------

async def try_join_required_chats(client: Client, chat_id: int) -> bool:
    try:
        async for msg in client.get_chat_history(chat_id, limit=5):
            if not msg.reply_markup:
                continue
            for row in msg.reply_markup.inline_keyboard:
                for btn in row:
                    if not btn.url:
                        continue
                    url = btn.url
                    if "t.me/" in url and "joinchat" not in url and "+" not in url:
                        username = url.split("t.me/")[-1].split("/")[0].strip()
                        if username:
                            try:
                                await client.join_chat(username)
                                STATE["global_stats"]["auto_subscribes"] = \
                                    STATE["global_stats"].get("auto_subscribes", 0) + 1
                                save_json(STATE_FILE, STATE)
                                return True
                            except UserAlreadyParticipant:
                                pass
                            except FloodWait as fw:
                                await asyncio.sleep(fw.value + 5)
                            except Exception:
                                pass
        return False
    except Exception:
        return False


async def auto_subscribe_loop(acc_id: str):
    acc = get_account(acc_id)
    if not acc:
        return
    c = user_clients.get(acc_id)
    if not c:
        return
    while acc.get("auto_subscribe_enabled", True):
        queue = acc.get("auto_subscribe_queue") or []
        if not queue:
            await asyncio.sleep(30)
            continue
        ref = queue.pop(0)
        try:
            chat = await c.get_chat(ref)
            await try_join_required_chats(c, chat.id)
        except Exception:
            pass
        acc["auto_subscribe_queue"] = queue
        save_json(STATE_FILE, STATE)
        delay = int(acc.get("auto_subscribe_delay", 30))
        await asyncio.sleep(delay + random.randint(5, 15))


def start_auto_subscribe(acc_id: str):
    t = folder_tasks.get(f"autosub_{acc_id}")
    if t and not t.done():
        return
    folder_tasks[f"autosub_{acc_id}"] = asyncio.create_task(auto_subscribe_loop(acc_id))


# ---------------------------------------------------------------------------
# SpamBot
# ---------------------------------------------------------------------------

async def check_spambot(acc_id: str) -> tuple:
    c = user_clients.get(acc_id)
    if not c:
        return "error", "Клиент не подключён"
    try:
        await c.send_message(SPAMBOT_USERNAME, "/start")
        await asyncio.sleep(3)
        messages = []
        async for msg in c.get_chat_history(SPAMBOT_USERNAME, limit=3):
            if msg.text:
                messages.append(msg.text)
            if len(messages) >= 2:
                break
        if not messages:
            return "unknown", "Нет ответа от SpamBot"
        response = messages[0]
        low = response.lower()
        if "good news" in low or "no limits" in low:
            status, msg = "good", "Аккаунт в порядке, ограничений нет"
        elif "limited" in low or "restricted" in low:
            status, msg = "limited", response[:500]
        else:
            status, msg = "unknown", response[:500]
        acc = get_account(acc_id)
        if acc:
            acc["last_spam_check"] = datetime.now().isoformat(timespec="seconds")
            acc["spam_status"] = status
            acc["spam_message"] = msg
            save_json(STATE_FILE, STATE)
        return status, msg
    except FloodWait as fw:
        return "flood", f"FloodWait {fw.value}s"
    except Exception as e:
        return "error", str(e)[:200]


async def notify_spam_check(acc_id: str):
    acc = get_account(acc_id)
    if not acc:
        return
    status, msg = await check_spambot(acc_id)
    name = acc.get("name", "?")
    emoji = {"good": "✅", "limited": "🚫", "flood": "⏳",
             "unknown": "❓", "error": "❌"}.get(status, "❓")
    try:
        await bot_client.send_message(
            CFG["admin_id"],
            f"{emoji} **Проверка аккаунта** `{name}`\n\n"
            f"Статус: **{status}**\n\n{msg}",
            parse_mode=enums.ParseMode.MARKDOWN)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Рассылка
# ---------------------------------------------------------------------------

async def send_post_for_account(acc_id: str, chat_id: int) -> None:
    c = user_clients.get(acc_id)
    if not c:
        raise RuntimeError("Клиент не подключён.")
    acc = get_account(acc_id)
    text = acc.get("text")
    media_path = acc.get("media_path")
    media_type = acc.get("media_type")
    caption = acc.get("caption") or ""
    if media_path and media_type == "photo" and os.path.exists(media_path):
        await c.send_photo(chat_id=chat_id, photo=media_path, caption=caption)
    elif media_path and media_type == "video" and os.path.exists(media_path):
        await c.send_video(chat_id=chat_id, video=media_path, caption=caption)
    elif text:
        try:
            await c.send_message(chat_id=chat_id, text=text,
                                 parse_mode=enums.ParseMode.MARKDOWN)
        except Exception:
            await c.send_message(chat_id=chat_id, text=text)
    else:
        raise ValueError("Не задан текст или медиа")


def _bump_error(acc: dict, chat_id: int, title: str, err: str):
    acc["stats"]["errors"] = acc["stats"].get("errors", 0) + 1
    es = acc.setdefault("error_stats", {})
    es[err] = es.get(err, 0) + 1
    fg = acc.setdefault("failed_groups", {})
    key = str(chat_id)
    item = fg.get(key) or {"title": title or str(chat_id), "error": err, "count": 0}
    item["error"] = err
    item["count"] = item.get("count", 0) + 1
    item["last"] = datetime.now().isoformat(timespec="seconds")
    fg[key] = item


async def mailing_loop_for_account(acc_id: str):
    acc = get_account(acc_id)
    if not acc:
        return
    while acc.get("running"):
        groups = list(acc.get("groups") or [])
        if not groups:
            await asyncio.sleep(60)
            continue
        sent = errors = 0
        drop_dead = bool(acc.get("auto_drop_dead"))
        to_remove = []
        for g in groups:
            if not acc.get("running"):
                break
            gid = g.get("id")
            title = g.get("title") or str(gid)
            try:
                await send_post_for_account(acc_id, gid)
                sent += 1
                acc["stats"]["sent"] = acc["stats"].get("sent", 0) + 1
                # при успехе — уменьшаем счётчик ошибок
                fg = acc.get("failed_groups") or {}
                if str(gid) in fg:
                    fg.pop(str(gid), None)
            except FloodWait as fw:
                await asyncio.sleep(fw.value + 2)
                try:
                    await send_post_for_account(acc_id, gid)
                    sent += 1
                    acc["stats"]["sent"] = acc["stats"].get("sent", 0) + 1
                except Exception as e2:
                    errors += 1
                    _bump_error(acc, gid, title, classify_error(e2))
            except Exception as e:
                errors += 1
                err_text = classify_error(e)
                _bump_error(acc, gid, title, err_text)
                # авто-удаление мёртвых групп
                if drop_dead and err_text in (
                        "Запрещено писать в группе", "Забанен в группе",
                        "Не участник группы", "ID группы устарел",
                        "Приватная / уже вышел", "Чат запрещён для записи"):
                    to_remove.append(gid)
            # задержка
            try:
                lo = int(acc.get("delay_min", 5))
                hi = int(acc.get("delay_max", 15))
                if hi < lo:
                    hi = lo
                await asyncio.sleep(random.randint(lo, hi))
            except Exception:
                await asyncio.sleep(5)

        if to_remove:
            acc["groups"] = [g for g in (acc.get("groups") or [])
                             if g["id"] not in to_remove]
            log.info(f"[{acc.get('name')}] автоудалено битых групп: {len(to_remove)}")

        acc["stats"]["rounds"] = acc["stats"].get("rounds", 0) + 1
        acc["stats"]["last_round"] = datetime.now().isoformat(timespec="seconds")
        save_json(STATE_FILE, STATE)
        if not acc.get("running"):
            break
        interval = int(acc.get("interval", 1800))
        acc["stats"]["next_round"] = (
            datetime.now() + timedelta(seconds=interval)).isoformat(timespec="seconds")
        save_json(STATE_FILE, STATE)

        # отчёт по кругу с разбором ошибок
        try:
            err_brief = ""
            es = acc.get("error_stats") or {}
            if es:
                top = Counter(es).most_common(3)
                err_brief = "\n⚠️ Топ ошибок:\n" + "\n".join(
                    f"  • {k}: {v}" for k, v in top)
            await bot_client.send_message(
                CFG["admin_id"],
                f"✅ [{acc['name']}] Круг №{acc['stats']['rounds']}.\n"
                f"Отправлено: {sent} | Ошибок: {errors}{err_brief}")
        except Exception:
            pass

        remaining = interval
        while remaining > 0 and acc.get("running"):
            await asyncio.sleep(min(5, remaining))
            remaining -= 5


def start_mailing_for_account(acc_id: str):
    t = mailing_tasks.get(acc_id)
    if t and not t.done():
        return
    mailing_tasks[acc_id] = asyncio.create_task(mailing_loop_for_account(acc_id))


async def scan_groups_for_account(acc_id: str) -> list:
    c = user_clients.get(acc_id)
    if not c:
        return []
    found = []
    try:
        async for dialog in c.get_dialogs():
            chat = dialog.chat
            if chat.type not in (enums.ChatType.GROUP, enums.ChatType.SUPERGROUP):
                continue
            if getattr(chat, "is_broadcast", False):
                continue
            found.append({"id": chat.id, "title": chat.title or str(chat.id),
                          "type": chat.type.name, "manual": False})
    except FloodWait as e:
        await asyncio.sleep(e.value + 2)
    except Exception:
        pass
    return found


async def subscribe_one_group(client: Client, ref):
    try:
        chat = await client.join_chat(ref)
        return "ok", None, chat
    except UserAlreadyParticipant:
        try:
            chat = await client.get_chat(ref)
            return "already", None, chat
        except Exception:
            return "already", None, None
    except FloodWait as fw:
        return "flood", fw.value, None
    except (UsernameInvalid, UsernameNotOccupied, InviteHashExpired,
            InviteHashInvalid, ChatIdInvalid):
        return "error", "недоступна", None
    except ChatAdminRequired:
        return "error", "нужно одобрение", None
    except UserBannedInChannel:
        return "error", "забанен", None
    except Exception as e:
        return "error", str(e)[:100], None


async def subscribe_loop(acc_id: str):
    acc = get_account(acc_id)
    if not acc:
        return
    c = user_clients.get(acc_id)
    if not c:
        acc["subscribe_status"] = "error"
        save_json(STATE_FILE, STATE)
        return
    acc["subscribe_status"] = "running"
    save_json(STATE_FILE, STATE)
    while acc.get("subscribe_status") == "running":
        queue = acc.get("subscribe_queue") or []
        if not queue:
            acc["subscribe_status"] = "done"
            save_json(STATE_FILE, STATE)
            break
        ref = queue[0]
        try:
            status, extra, chat = await subscribe_one_group(c, ref)
        except Exception as e:
            status, extra, chat = "error", str(e)[:100], None
        stats = acc.setdefault("subscribe_stats", {
            "subscribed": 0, "skipped": 0, "errors": 0, "started_at": None})
        if status == "ok":
            stats["subscribed"] += 1
            if chat:
                gid = chat.id
                mark_recent_join(acc_id, gid)
                exists = any(g["id"] == gid for g in (acc.get("groups") or []))
                if not exists:
                    acc.setdefault("groups", []).append({
                        "id": gid, "title": chat.title or str(gid),
                        "type": chat.type.name if chat.type else "UNKNOWN",
                        "manual": True,
                    })
            queue.pop(0)
        elif status == "already":
            stats["skipped"] += 1
            if chat:
                gid = chat.id
                mark_recent_join(acc_id, gid)
                exists = any(g["id"] == gid for g in (acc.get("groups") or []))
                if not exists:
                    acc.setdefault("groups", []).append({
                        "id": gid, "title": chat.title or str(gid),
                        "type": chat.type.name if chat.type else "UNKNOWN",
                        "manual": True,
                    })
            queue.pop(0)
        elif status == "flood":
            wait_s = int(extra or 60) + random.randint(5, 20)
            for _ in range(wait_s):
                if acc.get("subscribe_status") != "running":
                    break
                await asyncio.sleep(1)
            continue
        else:
            stats["errors"] += 1
            queue.pop(0)
        acc["subscribe_queue"] = queue
        save_json(STATE_FILE, STATE)
        if acc.get("subscribe_status") == "running" and queue:
            lo = int(acc.get("subscribe_delay_min", 40))
            hi = int(acc.get("subscribe_delay_max", 120))
            if hi < lo:
                hi = lo
            delay = random.randint(lo, hi)
            for _ in range(delay):
                if acc.get("subscribe_status") != "running":
                    break
                await asyncio.sleep(1)
    try:
        stats = acc.get("subscribe_stats") or {}
        await bot_client.send_message(
            CFG["admin_id"],
            f"📥 **Подписка завершена** ({acc.get('name')})\n"
            f"• Подписался: {stats.get('subscribed', 0)}\n"
            f"• Уже был: {stats.get('skipped', 0)}\n"
            f"• Ошибок: {stats.get('errors', 0)}",
            parse_mode=enums.ParseMode.MARKDOWN)
    except Exception:
        pass


def start_subscribe(acc_id: str):
    t = subscribe_tasks.get(acc_id)
    if t and not t.done():
        return
    subscribe_tasks[acc_id] = asyncio.create_task(subscribe_loop(acc_id))


# ---------------------------------------------------------------------------
# Хелперы ЛС
# ---------------------------------------------------------------------------

def get_main_client() -> Client:
    acc = get_main_account()
    if not acc:
        return None
    return user_clients.get(acc["id"])


async def mark_chat_read(user_id: int) -> None:
    c = get_main_client()
    if not c:
        return
    try:
        await c.read_chat_history(user_id)
    except Exception:
        pass


async def try_send_reaction(user_id: int, message_id: int):
    c = get_main_client()
    if not c:
        return
    ar = STATE.get("autoreply") or {}
    if not ar.get("enabled"):
        return
    if random.randint(1, 100) > 20:
        return
    emoji = random.choice(REACTION_EMOJIS)
    try:
        await c.send_reaction(chat_id=user_id, message_id=message_id, emoji=emoji)
    except Exception:
        pass


async def simulate_typing(user_id: int, text: str) -> None:
    c = get_main_client()
    if not c or not text:
        return
    delay = min(10.0, max(1.5, (len(text) / 12.0) * random.uniform(0.7, 1.3)))
    loop = asyncio.get_event_loop()
    end_time = loop.time() + delay
    try:
        while loop.time() < end_time:
            await c.send_chat_action(user_id, enums.ChatAction.TYPING)
            remain = end_time - loop.time()
            await asyncio.sleep(min(4.0, max(0.2, remain)))
    except Exception:
        pass


async def _send_as_userbot(user_id: int, text: str):
    c = get_main_client()
    if not c:
        return
    await simulate_typing(user_id, text)
    try:
        await c.send_message(user_id, text, parse_mode=enums.ParseMode.MARKDOWN)
    except Exception:
        try:
            await c.send_message(user_id, text)
        except Exception:
            return
    await db_add_message(user_id, "assistant", text)


# ---------------------------------------------------------------------------
# Userbot handlers (основной аккаунт)
# ---------------------------------------------------------------------------

def register_main_handlers(c: Client):
    if c.name in MAIN_HANDLERS_REGISTERED:
        return
    MAIN_HANDLERS_REGISTERED.add(c.name)

    @c.on_message(filters.private & filters.outgoing)
    async def on_outgoing(client, message):
        try:
            chat = message.chat
            if not chat or not chat.id:
                return
            if chat.type != enums.ChatType.PRIVATE:
                return
            _mark_owner_activity()
            _mark_known_ar(chat.id)
        except Exception:
            pass

    @c.on_message(filters.private & filters.incoming)
    async def on_incoming(client, message):
        try:
            user = message.from_user
            if not user or user.is_bot or user.is_deleted:
                return
            main_id = ME_IDS.get(STATE.get("main_account_id"), 0)
            if user.id == main_id:
                return
            if user.id == CFG.get("admin_id"):
                return
            ar = STATE.get("autoreply") or {}
            if not ar.get("enabled"):
                return
            await mark_chat_read(user.id)
            text_raw = (message.text or message.caption or "").strip()

            try:
                username = f" (@{user.username})" if user.username else ""
                await bot_client.send_message(
                    CFG["admin_id"],
                    f"📩 **Новое ЛС**\n👤 {user.first_name}{username}\n"
                    f"🆔 `{user.id}`\n💬 {text_raw[:300] or '—'}",
                    parse_mode=enums.ParseMode.MARKDOWN)
            except Exception:
                pass

            if message.voice or message.video_note or message.audio:
                return
            text = (message.text or message.caption or "").strip()
            if not text:
                return
            await db_add_message(user.id, "user", text)
            asyncio.create_task(try_send_reaction(user.id, message.id))

            if ar.get("enabled") and _owner_inactive_ar() and _cooldown_ok_ar(user.id):
                known = _is_known_ar(user.id)
                template = (ar.get("template_known") if known
                            else ar.get("template_first")) or ""
                template = template.strip()
                if template:
                    await _send_as_userbot(user.id, template)
                    _mark_known_ar(user.id)
                    _set_cooldown_ar(user.id)
                    STATE["global_stats"]["autoreplies"] = \
                        STATE["global_stats"].get("autoreplies", 0) + 1
                    save_json(STATE_FILE, STATE)
        except FloodWait as fw:
            await asyncio.sleep(fw.value + 2)
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Клавиатуры
# ---------------------------------------------------------------------------

def main_menu_kb() -> InlineKeyboardMarkup:
    ar = STATE.get("autoreply") or {}
    accounts = STATE.get("accounts") or []
    running_count = sum(1 for a in accounts if a.get("running"))
    subs_count = sum(1 for a in accounts if a.get("subscribe_status") == "running")
    ar_state = "🟢" if ar.get("enabled") else "🔴"
    subs_mark = f" 📥{subs_count}" if subs_count else ""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"📢 Рассылка ({running_count}/{len(accounts)})",
                              callback_data="accounts_menu")],
        [InlineKeyboardButton(f"📥 Массовая подписка{subs_mark}",
                              callback_data="sub_main")],
        [InlineKeyboardButton("🔍 Поиск групп", callback_data="search_main")],
        [InlineKeyboardButton(f"🤖 Автоответчик ({ar_state})", callback_data="ar_menu")],
        [InlineKeyboardButton("📊 Статистика", callback_data="stats_menu")],
    ])


def accounts_menu_kb() -> InlineKeyboardMarkup:
    accounts = STATE.get("accounts") or []
    rows = []
    for acc in accounts:
        mark = "🟢" if acc.get("running") else "⚪️"
        sub = " 📥" if acc.get("subscribe_status") == "running" else ""
        main_mark = " ⭐" if acc.get("id") == STATE.get("main_account_id") else ""
        name = (acc.get("name") or "—")[:25]
        rows.append([InlineKeyboardButton(f"{mark} {name}{main_mark}{sub}",
                                          callback_data=f"acc_open:{acc['id']}")])
    rows.append([InlineKeyboardButton("➕ Добавить аккаунт", callback_data="acc_add")])
    if len(accounts) > 1:
        rows.append([InlineKeyboardButton("🔀 Пересечения групп",
                                          callback_data="acc_check_overlaps")])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="menu")])
    return InlineKeyboardMarkup(rows)


def accounts_menu_text() -> str:
    accounts = STATE.get("accounts") or []
    main_id = STATE.get("main_account_id") or ""
    lines = ["📢 Рассылка по аккаунтам\n"]
    if not accounts:
        lines.append("_Пока ни одного аккаунта._")
    else:
        for i, acc in enumerate(accounts, 1):
            mark = "🟢" if acc.get("running") else "⚪️"
            main_mark = " ⭐" if acc.get("id") == main_id else ""
            sub_mark = " 📥" if acc.get("subscribe_status") == "running" else ""
            s = acc.get("stats") or {}
            spam_mark = ""
            ss = acc.get("spam_status", "unknown")
            if ss == "limited":
                spam_mark = " 🚫"
            elif ss == "good":
                spam_mark = " ✅"
            lines.append(f"{mark} **{i}. {acc.get('name', '?')}**{main_mark}{sub_mark}{spam_mark}\n"
                         f"   Групп: {len(acc.get('groups') or [])} | "
                         f"Отправлено: {s.get('sent', 0)} | Ошибок: {s.get('errors', 0)}")
    lines.append("\n⭐ основной · 📥 подписка · 🚫 спам-бан · ✅ ок")
    return "\n".join(lines)


def account_kb(acc_id: str) -> InlineKeyboardMarkup:
    acc = get_account(acc_id)
    running = acc.get("running", False)
    is_main = acc.get("id") == STATE.get("main_account_id")
    sub_status = acc.get("subscribe_status", "idle")
    sub_queue_len = len(acc.get("subscribe_queue") or [])
    failed_count = len(acc.get("failed_groups") or {})
    rows = [
        [InlineKeyboardButton("⏸ Остановить рассылку" if running else "🚀 Запустить рассылку",
                              callback_data=f"acc_toggle:{acc_id}")],
        [InlineKeyboardButton("📝 Изменить текст", callback_data=f"acc_text:{acc_id}")],
        [InlineKeyboardButton("🖼 Медиа", callback_data=f"acc_media:{acc_id}")],
        [InlineKeyboardButton("⏱ Тайминги рассылки", callback_data=f"acc_timing:{acc_id}")],
        [InlineKeyboardButton("🔍 Сканировать группы", callback_data=f"acc_scan:{acc_id}")],
        [InlineKeyboardButton("➕ Добавить группу", callback_data=f"acc_addgrp:{acc_id}"),
         InlineKeyboardButton("🗑 Список групп", callback_data=f"acc_delgrp:{acc_id}")],
    ]
    if sub_status == "running":
        rows.append([InlineKeyboardButton(
            f"⏸ Остановить подписку ({sub_queue_len})",
            callback_data=f"acc_sub_stop:{acc_id}")])
    else:
        rows.append([InlineKeyboardButton(
            "📥 Массовая подписка" + (f" ({sub_queue_len})" if sub_queue_len else ""),
            callback_data=f"acc_sub_menu:{acc_id}")])
    rows.append([InlineKeyboardButton("🔍 Проверить спам-бан",
                                       callback_data=f"acc_check_spam:{acc_id}")])
    rows.append([InlineKeyboardButton("📂 Разложить по папкам",
                                       callback_data=f"acc_sort_folders:{acc_id}")])
    rows.append([InlineKeyboardButton(
        f"😊 Реакции в группах: {'🟢' if acc.get('group_reactions_enabled', True) else '🔴'}",
        callback_data=f"acc_toggle_group_react:{acc_id}")])
    rows.append([InlineKeyboardButton(
        f"🛡 Автокапча: {'🟢' if acc.get('captcha_enabled', True) else '🔴'}",
        callback_data=f"acc_toggle_captcha:{acc_id}")])
    rows.append([InlineKeyboardButton(
        f"🧹 Автоудаление битых: {'🟢' if acc.get('auto_drop_dead') else '🔴'}",
        callback_data=f"acc_toggle_drop:{acc_id}")])
    rows.append([InlineKeyboardButton(
        f"⚠️ Ошибки ({failed_count})",
        callback_data=f"acc_errors:{acc_id}")])
    if not is_main:
        rows.append([InlineKeyboardButton("⭐ Сделать основным",
                                          callback_data=f"acc_setmain:{acc_id}")])
    rows.append([InlineKeyboardButton("🗑 Удалить аккаунт",
                                       callback_data=f"acc_remove:{acc_id}")])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="accounts_menu")])
    return InlineKeyboardMarkup(rows)


def account_text(acc_id: str) -> str:
    acc = get_account(acc_id)
    if not acc:
        return "Аккаунт не найден."
    s = acc.get("stats") or {}
    txt = (acc.get("text") or "").strip()
    media = acc.get("media_type")
    if media:
        preview = f"[{media}] caption: {(acc.get('caption') or '')[:100]}"
    elif txt:
        preview = txt[:200]
    else:
        preview = "— (пусто)"
    is_main = "⭐ основной" if acc.get("id") == STATE.get("main_account_id") else ""
    sub_status = acc.get("subscribe_status", "idle")
    sub_stats = acc.get("subscribe_stats") or {}
    sub_queue = len(acc.get("subscribe_queue") or [])
    sub_status_text = {"idle": "⚪️ idle", "running": f"🟢 идёт ({sub_queue})",
                       "paused": "⏸ пауза", "done": "✅ готово"}.get(sub_status, sub_status)
    spam_status = acc.get("spam_status", "unknown")
    spam_emoji = {"good": "✅", "limited": "🚫", "unknown": "❓"}.get(spam_status, "❓")
    return (
        f"📢 **{acc.get('name', '?')}** {is_main}\n\n"
        f"• Статус: {'🟢 идёт' if acc.get('running') else '⚪️ стоп'}\n"
        f"• Сессия: `{acc.get('session_name', '?')}`\n"
        f"• Групп: {len(acc.get('groups') or [])}\n"
        f"• Интервал: {acc.get('interval', 1800)} сек\n"
        f"• Задержка: {acc.get('delay_min', 5)}–{acc.get('delay_max', 15)} сек\n\n"
        f"📊 Отправлено: {s.get('sent', 0)} | Ошибок: {s.get('errors', 0)} | "
        f"Кругов: {s.get('rounds', 0)}\n\n"
        f"🔍 **Спам-статус:** {spam_emoji} ({acc.get('spam_status', 'unknown')})\n"
        f"• Проверка: {acc.get('last_spam_check') or '—'}\n\n"
        f"📥 **Подписка:** {sub_status_text}\n"
        f"• Подписался: {sub_stats.get('subscribed', 0)} | "
        f"Был: {sub_stats.get('skipped', 0)} | ❌{sub_stats.get('errors', 0)}\n"
        f"• Задержка: {acc.get('subscribe_delay_min', 40)}–"
        f"{acc.get('subscribe_delay_max', 120)} сек\n\n"
        f"📁 Папок: {acc.get('folder_index', 0)} (по {acc.get('folder_size', 100)} групп)\n"
        f"😊 Реакции: {'🟢' if acc.get('group_reactions_enabled', True) else '🔴'} "
        f"({acc.get('group_reactions_chance', 5)}%)\n"
        f"🛡 Автокапча: {'🟢 вкл' if acc.get('captcha_enabled', True) else '🔴 выкл'}\n"
        f"🧹 Автоудаление битых: {'🟢' if acc.get('auto_drop_dead') else '🔴'}\n\n"
        f"📝 Текст:\n{preview}"
    )


def subscribe_menu_kb(acc_id: str) -> InlineKeyboardMarkup:
    acc = get_account(acc_id)
    running = acc.get("subscribe_status") == "running"
    queue_len = len(acc.get("subscribe_queue") or [])
    rows = [
        [InlineKeyboardButton("📋 Загрузить список групп",
                              callback_data=f"acc_sub_load:{acc_id}")],
        [InlineKeyboardButton("🔍 Найти по ключевому слову",
                              callback_data=f"acc_sub_search:{acc_id}")],
        [InlineKeyboardButton(
            f"⏱ Задержка: {acc.get('subscribe_delay_min', 40)}–"
            f"{acc.get('subscribe_delay_max', 120)}с",
            callback_data=f"acc_sub_delay:{acc_id}")],
    ]
    if running:
        rows.append([InlineKeyboardButton("⏸ Остановить",
                                           callback_data=f"acc_sub_stop:{acc_id}")])
    elif queue_len:
        rows.append([InlineKeyboardButton("▶️ Продолжить подписку",
                                           callback_data=f"acc_sub_start:{acc_id}")])
        rows.append([InlineKeyboardButton("🗑 Очистить очередь",
                                           callback_data=f"acc_sub_clear:{acc_id}")])
    else:
        rows.append([InlineKeyboardButton("— очередь пуста —", callback_data="noop")])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data=f"acc_open:{acc_id}")])
    return InlineKeyboardMarkup(rows)


def subscribe_menu_text(acc_id: str) -> str:
    acc = get_account(acc_id)
    queue = acc.get("subscribe_queue") or []
    stats = acc.get("subscribe_stats") or {}
    status = acc.get("subscribe_status", "idle")
    status_text = {"idle": "⚪️ idle", "running": "🟢 идёт",
                   "paused": "⏸ пауза", "done": "✅ готово"}.get(status, status)
    preview = ""
    if queue:
        preview = "\n\nСледующие 5:\n"
        for i, ref in enumerate(queue[:5], 1):
            preview += f"{i}. `{ref}`\n"
        if len(queue) > 5:
            preview += f"…и ещё {len(queue) - 5}"
    return (
        f"📥 **Массовая подписка**\n"
        f"Аккаунт: **{acc.get('name')}**\n\n"
        f"• Статус: {status_text}\n"
        f"• В очереди: {len(queue)}\n"
        f"• Задержка: {acc.get('subscribe_delay_min', 40)}–"
        f"{acc.get('subscribe_delay_max', 120)} сек\n\n"
        f"📊 Подписался: {stats.get('subscribed', 0)} | "
        f"Был: {stats.get('skipped', 0)} | ❌{stats.get('errors', 0)}\n\n"
        f"💡 Загрузи список или найди по ключевому слову, потом ▶️"
        + preview
    )


def autoreply_menu_kb() -> InlineKeyboardMarkup:
    ar = STATE.get("autoreply") or {}
    enabled = ar.get("enabled", False)
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔴 Выкл" if enabled else "🟢 Вкл",
                              callback_data="ar_toggle")],
        [InlineKeyboardButton("📝 Для новых", callback_data="ar_first")],
        [InlineKeyboardButton("📝 Для знакомых", callback_data="ar_known")],
        [InlineKeyboardButton(f"⏱ Неактив: {ar.get('inactive_minutes', 5)} мин",
                              callback_data="ar_inactive")],
        [InlineKeyboardButton(f"⏳ Cooldown: {ar.get('cooldown_minutes', 60)} мин",
                              callback_data="ar_cooldown")],
        [InlineKeyboardButton("♻️ Сбросить знакомых", callback_data="ar_reset_known")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="menu")],
    ])


def autoreply_menu_text() -> str:
    ar = STATE.get("autoreply") or {}
    tf = (ar.get("template_first") or "").strip()
    tk = (ar.get("template_known") or "").strip()
    return (f"🤖 Автоответчик\n"
            f"• Статус: {'🟢 вкл' if ar.get('enabled') else '🔴 выкл'}\n"
            f"• Неактивность: {ar.get('inactive_minutes', 5)} мин\n"
            f"• Cooldown: {ar.get('cooldown_minutes', 60)} мин\n"
            f"• Знакомых: {len(ar.get('known_users') or [])}\n\n"
            f"📩 Новым: {tf[:200] if tf else '—'}\n\n"
            f"📩 Знакомым: {tk[:200] if tk else '—'}")


def stats_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔄 Обновить", callback_data="stats_menu")],
        [InlineKeyboardButton("🗑 Очистить БД (30д)", callback_data="db_cleanup")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="menu")],
    ])


def stats_menu_text() -> str:
    gs = STATE.get("global_stats") or {}
    lines = ["📊 Статистика\n"]
    accounts = STATE.get("accounts") or []
    total_sent = total_err = total_subs = 0
    for acc in accounts:
        s = acc.get("stats") or {}
        ss = acc.get("subscribe_stats") or {}
        total_sent += s.get("sent", 0)
        total_err += s.get("errors", 0)
        total_subs += ss.get("subscribed", 0)
        sub_mark = " 📥" if acc.get("subscribe_status") == "running" else ""
        lines.append(f"📢 {acc.get('name', '?')}: 🟢{s.get('sent', 0)} | "
                     f"❌{s.get('errors', 0)} | Кругов {s.get('rounds', 0)}{sub_mark}")
    lines.append(f"\n**Рассылка:** 🟢 {total_sent} | ❌ {total_err}")
    lines.append(f"**Подписок:** {total_subs}")
    lines.append(f"**Автоподписок:** {gs.get('auto_subscribes', 0)}")
    lines.append(f"**Капч пройдено:** {gs.get('captchas_passed', 0)}")
    lines.append(f"**Реакций в группах:** {gs.get('group_reactions', 0)}")
    lines.append(f"**Автоответов:** {gs.get('autoreplies', 0)}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Ошибки: экраны
# ---------------------------------------------------------------------------

def acc_errors_kb(acc_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔄 Обновить", callback_data=f"acc_errors:{acc_id}")],
        [InlineKeyboardButton("🧹 Удалить битые группы",
                              callback_data=f"acc_drop_dead:{acc_id}")],
        [InlineKeyboardButton("♻️ Сбросить статистику ошибок",
                              callback_data=f"acc_err_reset:{acc_id}")],
        [InlineKeyboardButton("⬅️ Назад", callback_data=f"acc_open:{acc_id}")],
    ])


def acc_errors_text(acc_id: str) -> str:
    acc = get_account(acc_id)
    if not acc:
        return "Не найден."
    es = acc.get("error_stats") or {}
    fg = acc.get("failed_groups") or {}
    lines = [f"⚠️ **Ошибки: {acc.get('name', '?')}**\n"]

    if es:
        lines.append("📊 **По типам:**")
        for k, v in sorted(es.items(), key=lambda x: -x[1])[:20]:
            lines.append(f"  • {k}: {v}")
    else:
        lines.append("📊 Пока ошибок нет.")

    if fg:
        lines.append(f"\n🚫 **Проблемные группы** (топ-10 из {len(fg)}):")
        sorted_fg = sorted(fg.items(), key=lambda x: -x[1].get("count", 0))[:10]
        for gid, info in sorted_fg:
            lines.append(f"  • {info.get('title', gid)[:40]} — "
                         f"_{info.get('error', '?')}_ (x{info.get('count', 1)})")

    text = "\n".join(lines)
    if len(text) > 3800:
        text = text[:3800] + "\n…"
    return text


# ---------------------------------------------------------------------------
# Поиск — экраны
# ---------------------------------------------------------------------------

def search_accounts_kb() -> InlineKeyboardMarkup:
    accounts = STATE.get("accounts") or []
    rows = []
    for acc in accounts:
        name = (acc.get("name") or "—")[:30]
        rows.append([InlineKeyboardButton(f"🔎 {name}",
                                          callback_data=f"search_acc:{acc['id']}")])
    if not accounts:
        rows.append([InlineKeyboardButton("— нет аккаунтов —", callback_data="noop")])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="menu")])
    return InlineKeyboardMarkup(rows)


def search_results_kb(acc_id: str, count: int) -> InlineKeyboardMarkup:
    rows = []
    if count > 0:
        rows.append([InlineKeyboardButton(
            f"✅ Добавить все ({count}) в очередь",
            callback_data=f"search_add_all:{acc_id}")])
        rows.append([InlineKeyboardButton(
            "➕ Добавить топ-10",
            callback_data=f"search_add_top:{acc_id}")])
    rows.append([InlineKeyboardButton("🔄 Новый поиск",
                                       callback_data=f"search_acc:{acc_id}")])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="search_main")])
    return InlineKeyboardMarkup(rows)


def search_results_text(query: str, results: list) -> str:
    if not results:
        return f"🔍 Поиск «{query}»\n\n❌ Ничего не нашлось (только группы, без каналов)."
    lines = [f"🔍 Поиск «{query}»\n",
             f"Найдено групп: **{len(results)}**\n"]
    for i, r in enumerate(results[:20], 1):
        p = r["participants"]
        p_str = f"{p:,}".replace(",", " ") if p else "—"
        lines.append(f"{i}. **{r['title'][:50]}** (@{r['username']})\n"
                     f"   👥 {p_str}")
    if len(results) > 20:
        lines.append(f"\n…и ещё {len(results) - 20}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Регистрация хендлеров управляющего бота
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
        await message.reply("🎛 Панель:", reply_markup=main_menu_kb())

    @bot.on_message(filters.command("auth") & filters.private)
    async def cmd_auth(client, message):
        if message.from_user.id != CFG["admin_id"]:
            return
        parts = (message.text or "").split(maxsplit=1)
        if len(parts) < 2 or parts[1].strip() != str(CFG.get("pin", DEFAULT_PIN)):
            await message.reply("❌ Неверный ПИН.")
            return
        authed.add(message.from_user.id)
        pending.pop(message.from_user.id, None)
        await message.reply("✅ Авторизация.", reply_markup=main_menu_kb())

    @bot.on_message(filters.command("cancel") & filters.private)
    async def cmd_cancel(client, message):
        if message.from_user.id != CFG["admin_id"]:
            return
        pending.pop(message.from_user.id, None)
        await message.reply("Отменено.", reply_markup=main_menu_kb())

    @bot.on_message(filters.command("panel") & filters.private)
    async def cmd_panel(client, message):
        if message.from_user.id != CFG["admin_id"] or message.from_user.id not in authed:
            return
        await message.reply("🎛 Панель:", reply_markup=main_menu_kb())

    @bot.on_message(filters.command("spam") & filters.private)
    async def cmd_spam(client, message):
        if message.from_user.id != CFG["admin_id"] or message.from_user.id not in authed:
            return
        accounts = STATE.get("accounts") or []
        if not accounts:
            await message.reply("Нет аккаунтов.")
            return
        msg = await message.reply("🔍 Проверяю все аккаунты через @SpamBot…")
        for acc in accounts:
            await notify_spam_check(acc["id"])
            await asyncio.sleep(5)
        await msg.edit_text("✅ Проверка завершена.")

    @bot.on_message(filters.command("folders") & filters.private)
    async def cmd_folders(client, message):
        if message.from_user.id != CFG["admin_id"] or message.from_user.id not in authed:
            return
        accounts = STATE.get("accounts") or []
        for acc in accounts:
            if acc.get("groups"):
                start_folder_sort(acc["id"])
        await message.reply("📂 Запустил распределение по папкам Telegram.")

    @bot.on_callback_query()
    async def on_cb(client, cb):
        uid = cb.from_user.id
        if uid != CFG["admin_id"]:
            await cb.answer("⛔", show_alert=True)
            return
        if uid not in authed:
            await cb.answer("🔒 /auth <PIN>", show_alert=True)
            return
        data = cb.data or ""
        try:
            if data == "menu":
                await cb.message.edit_text("🎛 Панель:", reply_markup=main_menu_kb())
            elif data == "noop":
                await cb.answer("—")

            # -------- Поиск --------
            elif data == "search_main":
                accounts = STATE.get("accounts") or []
                if not accounts:
                    await cb.answer("Нет аккаунтов. Сначала добавь.", show_alert=True)
                    return
                await cb.message.edit_text(
                    "🔍 **Поиск групп**\n\n"
                    "Ищем только группы (не каналы), где можно писать.\n"
                    "Выбери аккаунт:",
                    reply_markup=search_accounts_kb())
            elif data.startswith("search_acc:"):
                acc_id = data.split(":", 1)[1]
                if not get_account(acc_id):
                    await cb.answer("Не найден.")
                    return
                pending[uid] = {"action": "search_query", "acc_id": acc_id}
                await cb.message.edit_text(
                    "🔍 Отправь ключевое слово.\n"
                    "Например: `общение`, `знакомства`, `крипта`, `работа`.\n\n/cancel",
                    parse_mode=enums.ParseMode.MARKDOWN)
            elif data.startswith("search_add_all:") or data.startswith("search_add_top:"):
                acc_id = data.split(":", 1)[1]
                acc = get_account(acc_id)
                if not acc:
                    return
                cache = SEARCH_CACHE.get(uid, {}).get(acc_id) or []
                if not cache:
                    await cb.answer("Поиск устарел, попробуй снова.", show_alert=True)
                    return
                take = cache if data.startswith("search_add_all:") else cache[:10]
                q = acc.setdefault("subscribe_queue", [])
                have = {str(x) for x in q}
                added = 0
                for r in take:
                    if r["ref"] not in have:
                        q.append(r["ref"])
                        have.add(r["ref"])
                        added += 1
                if acc.get("subscribe_status") != "running":
                    acc["subscribe_status"] = "idle"
                save_json(STATE_FILE, STATE)
                await cb.answer(f"✅ +{added}")
                await cb.message.edit_text(
                    f"✅ Добавлено в очередь: **{added}**\n"
                    f"Всего в очереди: **{len(q)}**\n\n"
                    f"Открой 📥 Массовая подписка → ▶️ Запустить.",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("📥 Открыть подписку",
                                              callback_data=f"acc_sub_menu:{acc_id}")],
                        [InlineKeyboardButton("⬅️ В меню", callback_data="menu")],
                    ]))

            # -------- Аккаунты --------
            elif data == "accounts_menu":
                await cb.message.edit_text(accounts_menu_text(), reply_markup=accounts_menu_kb())
            elif data == "acc_add":
                pending[uid] = {"action": "acc_add_phone"}
                await cb.message.edit_text(
                    "➕ **Добавление аккаунта**\n\n"
                    "Отправь номер телефона в формате `+79991234567`.\n\n/cancel",
                    parse_mode=enums.ParseMode.MARKDOWN)
            elif data == "acc_check_overlaps":
                overlaps = check_group_overlaps()
                if not overlaps:
                    await cb.answer("✅ Пересечений нет!", show_alert=True)
                    return
                lines = ["⚠️ Пересечения:\n"]
                accounts = STATE.get("accounts") or []
                id_to_name = {a["id"]: a.get("name", "?") for a in accounts}
                for acc_id, keys in overlaps.items():
                    name = id_to_name.get(acc_id, acc_id[:8])
                    lines.append(f"\n📢 **{name}** — {len(keys)}")
                    for k in keys[:5]:
                        lines.append(f"  • `{k}`")
                text = "\n".join(lines)
                if len(text) > 3500:
                    text = text[:3500] + "\n…"
                await cb.message.edit_text(text,
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("⬅️ Назад", callback_data="accounts_menu")]]))
            elif data.startswith("acc_open:"):
                acc_id = data.split(":", 1)[1]
                if not get_account(acc_id):
                    await cb.answer("Не найден.")
                    return
                await cb.message.edit_text(account_text(acc_id), reply_markup=account_kb(acc_id))
            elif data.startswith("acc_toggle:"):
                acc_id = data.split(":", 1)[1]
                acc = get_account(acc_id)
                if not acc:
                    return
                if not user_clients.get(acc_id):
                    await cb.answer("Клиент не подключён.", show_alert=True)
                    return
                if acc.get("running"):
                    acc["running"] = False
                    save_json(STATE_FILE, STATE)
                    await cb.answer("⏸")
                else:
                    if not acc.get("groups"):
                        await cb.answer("Нет групп!", show_alert=True)
                        return
                    if not (acc.get("text") or acc.get("media_path")):
                        await cb.answer("Не задан текст!", show_alert=True)
                        return
                    acc["running"] = True
                    save_json(STATE_FILE, STATE)
                    start_mailing_for_account(acc_id)
                    await cb.answer("🚀")
                await cb.message.edit_text(account_text(acc_id), reply_markup=account_kb(acc_id))
            elif data.startswith("acc_text:"):
                acc_id = data.split(":", 1)[1]
                pending[uid] = {"action": "acc_text", "acc_id": acc_id}
                await cb.message.edit_text(
                    "📝 Отправь текст для рассылки.\n\n"
                    "Markdown: **жирный**, __курсив__, ||спойлер||, [ссылка](url)\n/cancel",
                    parse_mode=enums.ParseMode.MARKDOWN)
            elif data.startswith("acc_media:"):
                acc_id = data.split(":", 1)[1]
                pending[uid] = {"action": "acc_media", "acc_id": acc_id}
                await cb.message.edit_text("🖼 Отправь фото или видео.\n/cancel")
            elif data.startswith("acc_timing:"):
                acc_id = data.split(":", 1)[1]
                acc = get_account(acc_id)
                if not acc:
                    return
                kb = InlineKeyboardMarkup([
                    [InlineKeyboardButton(f"⏱ Интервал: {acc.get('interval', 1800)}с",
                                          callback_data=f"acc_setint:{acc_id}")],
                    [InlineKeyboardButton(f"⏳ Мин: {acc.get('delay_min', 5)}с",
                                          callback_data=f"acc_setdmin:{acc_id}")],
                    [InlineKeyboardButton(f"⏳ Макс: {acc.get('delay_max', 15)}с",
                                          callback_data=f"acc_setdmax:{acc_id}")],
                    [InlineKeyboardButton(f"📁 Размер папки: {acc.get('folder_size', 100)}",
                                          callback_data=f"acc_setfoldersize:{acc_id}")],
                    [InlineKeyboardButton("⬅️ Назад", callback_data=f"acc_open:{acc_id}")],
                ])
                await cb.message.edit_text(
                    f"⏱ Тайминги **{acc.get('name')}**\n"
                    f"• Интервал: {acc.get('interval')} сек\n"
                    f"• Задержка: {acc.get('delay_min')}–{acc.get('delay_max')} сек\n"
                    f"• Папка: по {acc.get('folder_size', 100)} групп",
                    reply_markup=kb)
            elif data.startswith("acc_setint:"):
                acc_id = data.split(":", 1)[1]
                pending[uid] = {"action": "acc_setint", "acc_id": acc_id}
                await cb.message.edit_text("Интервал (>=60):\n/cancel")
            elif data.startswith("acc_setdmin:"):
                acc_id = data.split(":", 1)[1]
                pending[uid] = {"action": "acc_setdmin", "acc_id": acc_id}
                await cb.message.edit_text("Мин. задержка (>=1):\n/cancel")
            elif data.startswith("acc_setdmax:"):
                acc_id = data.split(":", 1)[1]
                pending[uid] = {"action": "acc_setdmax", "acc_id": acc_id}
                await cb.message.edit_text("Макс. задержка:\n/cancel")
            elif data.startswith("acc_setfoldersize:"):
                acc_id = data.split(":", 1)[1]
                pending[uid] = {"action": "acc_setfoldersize", "acc_id": acc_id}
                await cb.message.edit_text(
                    "📁 Размер папки (10-100):\n\n"
                    "Telegram лимит — 100 чатов на папку.\n/cancel")
            elif data.startswith("acc_scan:"):
                acc_id = data.split(":", 1)[1]
                c = user_clients.get(acc_id)
                if not c:
                    await cb.answer("Клиент не подключён.", show_alert=True)
                    return
                await cb.answer("Сканирую…")
                await cb.message.edit_text("🔍 Сканирую…")
                found = await scan_groups_for_account(acc_id)
                acc = get_account(acc_id)
                scanned = {g["id"] for g in found}
                manual = [g for g in (acc.get("groups") or [])
                          if g.get("manual") and g["id"] not in scanned]
                acc["groups"] = found + manual
                save_json(STATE_FILE, STATE)
                await cb.message.edit_text(
                    f"✅ Найдено: {len(found)}. Всего: {len(acc['groups'])}.",
                    reply_markup=account_kb(acc_id))
            elif data.startswith("acc_addgrp:"):
                acc_id = data.split(":", 1)[1]
                pending[uid] = {"action": "acc_addgrp", "acc_id": acc_id}
                await cb.message.edit_text("@username / t.me/... / ID:\n/cancel")
            elif data.startswith("acc_delgrp:"):
                acc_id = data.split(":", 1)[1]
                acc = get_account(acc_id)
                if not acc or not acc.get("groups"):
                    await cb.answer("Пусто.", show_alert=True)
                    return
                rows = []
                for g in (acc.get("groups") or [])[:30]:
                    t = (g.get("title") or str(g.get("id")))[:35]
                    rows.append([InlineKeyboardButton(f"❌ {t}",
                                                      callback_data=f"acc_delg:{acc_id}:{g['id']}")])
                rows.append([InlineKeyboardButton("⬅️ Назад", callback_data=f"acc_open:{acc_id}")])
                await cb.message.edit_text("Удалить группу:",
                                            reply_markup=InlineKeyboardMarkup(rows))
            elif data.startswith("acc_delg:"):
                parts = data.split(":", 2)
                if len(parts) < 3:
                    return
                acc_id, gid_s = parts[1], parts[2]
                try:
                    gid = int(gid_s)
                except ValueError:
                    return
                acc = get_account(acc_id)
                if acc:
                    acc["groups"] = [g for g in acc["groups"] if g["id"] != gid]
                    save_json(STATE_FILE, STATE)
                await cb.answer("Удалено.")
                await cb.message.edit_text(account_text(acc_id), reply_markup=account_kb(acc_id))
            elif data.startswith("acc_setmain:"):
                acc_id = data.split(":", 1)[1]
                if get_account(acc_id):
                    STATE["main_account_id"] = acc_id
                    save_json(STATE_FILE, STATE)
                    c = user_clients.get(acc_id)
                    if c:
                        register_main_handlers(c)
                    await cb.answer("⭐ Основной изменён.", show_alert=True)
                await cb.message.edit_text(account_text(acc_id), reply_markup=account_kb(acc_id))
            elif data.startswith("acc_check_spam:"):
                acc_id = data.split(":", 1)[1]
                await cb.answer("🔍 Проверяю…")
                await notify_spam_check(acc_id)
                await cb.message.edit_text(account_text(acc_id), reply_markup=account_kb(acc_id))
            elif data.startswith("acc_sort_folders:"):
                acc_id = data.split(":", 1)[1]
                await cb.answer("📂 Запускаю…")
                start_folder_sort(acc_id)
                await cb.message.edit_text("📂 Раскладываю по папкам…")
            elif data.startswith("acc_toggle_group_react:"):
                acc_id = data.split(":", 1)[1]
                acc = get_account(acc_id)
                if acc:
                    acc["group_reactions_enabled"] = not acc.get("group_reactions_enabled", True)
                    save_json(STATE_FILE, STATE)
                await cb.answer("✅")
                await cb.message.edit_text(account_text(acc_id), reply_markup=account_kb(acc_id))
            elif data.startswith("acc_toggle_captcha:"):
                acc_id = data.split(":", 1)[1]
                acc = get_account(acc_id)
                if acc:
                    acc["captcha_enabled"] = not acc.get("captcha_enabled", True)
                    save_json(STATE_FILE, STATE)
                await cb.answer("✅")
                await cb.message.edit_text(account_text(acc_id), reply_markup=account_kb(acc_id))
            elif data.startswith("acc_toggle_drop:"):
                acc_id = data.split(":", 1)[1]
                acc = get_account(acc_id)
                if acc:
                    acc["auto_drop_dead"] = not acc.get("auto_drop_dead", False)
                    save_json(STATE_FILE, STATE)
                await cb.answer("✅")
                await cb.message.edit_text(account_text(acc_id), reply_markup=account_kb(acc_id))

            # -------- Ошибки --------
            elif data.startswith("acc_errors:"):
                acc_id = data.split(":", 1)[1]
                await cb.message.edit_text(acc_errors_text(acc_id),
                                            reply_markup=acc_errors_kb(acc_id))
            elif data.startswith("acc_err_reset:"):
                acc_id = data.split(":", 1)[1]
                acc = get_account(acc_id)
                if acc:
                    acc["error_stats"] = {}
                    acc["failed_groups"] = {}
                    save_json(STATE_FILE, STATE)
                await cb.answer("♻️")
                await cb.message.edit_text(acc_errors_text(acc_id),
                                            reply_markup=acc_errors_kb(acc_id))
            elif data.startswith("acc_drop_dead:"):
                acc_id = data.split(":", 1)[1]
                acc = get_account(acc_id)
                if not acc:
                    return
                fg = acc.get("failed_groups") or {}
                dead_ids = set()
                for gid_str, info in fg.items():
                    if info.get("error") in (
                            "Запрещено писать в группе", "Забанен в группе",
                            "Не участник группы", "ID группы устарел",
                            "Приватная / уже вышел", "Чат запрещён для записи"):
                        try:
                            dead_ids.add(int(gid_str))
                        except Exception:
                            pass
                before = len(acc.get("groups") or [])
                acc["groups"] = [g for g in (acc.get("groups") or [])
                                 if g["id"] not in dead_ids]
                removed = before - len(acc["groups"])
                save_json(STATE_FILE, STATE)
                await cb.answer(f"🗑 Удалено: {removed}", show_alert=True)
                await cb.message.edit_text(acc_errors_text(acc_id),
                                            reply_markup=acc_errors_kb(acc_id))

            # -------- Удаление аккаунта --------
            elif data.startswith("acc_remove:"):
                acc_id = data.split(":", 1)[1]
                kb = InlineKeyboardMarkup([
                    [InlineKeyboardButton("✅ Да", callback_data=f"acc_remove_ok:{acc_id}")],
                    [InlineKeyboardButton("❌ Отмена", callback_data=f"acc_open:{acc_id}")],
                ])
                await cb.message.edit_text("🗑 Удалить аккаунт?", reply_markup=kb)
            elif data.startswith("acc_remove_ok:"):
                acc_id = data.split(":", 1)[1]
                acc = get_account(acc_id)
                if acc:
                    acc["running"] = False
                    acc["subscribe_status"] = "paused"
                    c = user_clients.get(acc_id)
                    if c:
                        try:
                            await c.stop()
                        except Exception:
                            pass
                        user_clients.pop(acc_id, None)
                    sname = acc.get("session_name")
                    if sname:
                        for ext in (".session", ".session-journal"):
                            p = os.path.join(DATA_DIR, sname + ext)
                            try:
                                if os.path.exists(p):
                                    os.remove(p)
                            except Exception:
                                pass
                    remove_account(acc_id)
                    await cb.answer("🗑")
                await cb.message.edit_text(accounts_menu_text(), reply_markup=accounts_menu_kb())

            # -------- Подписка --------
            elif data == "sub_main":
                await cb.message.edit_text(
                    "📥 **Массовая подписка**\n\nВыбери аккаунт:",
                    reply_markup=InlineKeyboardMarkup(
                        [[InlineKeyboardButton(
                            f"{(a.get('name') or '—')[:30]}",
                            callback_data=f"acc_sub_menu:{a['id']}")]
                         for a in (STATE.get("accounts") or [])] +
                        [[InlineKeyboardButton("⬅️ Назад", callback_data="menu")]]))
            elif data.startswith("acc_sub_menu:"):
                acc_id = data.split(":", 1)[1]
                if not get_account(acc_id):
                    await cb.answer("Не найден.")
                    return
                await cb.message.edit_text(subscribe_menu_text(acc_id),
                                            reply_markup=subscribe_menu_kb(acc_id))
            elif data.startswith("acc_sub_load:"):
                acc_id = data.split(":", 1)[1]
                pending[uid] = {"action": "acc_sub_load", "acc_id": acc_id}
                await cb.message.edit_text(
                    "📋 **Отправь список групп**\n\n"
                    "• По одной на строку: `@group1`, `https://t.me/group2`, `-1001234567890`\n"
                    "• Через запятую: `@g1, @g2`\n"
                    "• Или `.txt` файлом\n\n"
                    "Строки с `#` — комментарии.\n\n/cancel",
                    parse_mode=enums.ParseMode.MARKDOWN)
            elif data.startswith("acc_sub_search:"):
                acc_id = data.split(":", 1)[1]
                pending[uid] = {"action": "search_query", "acc_id": acc_id,
                                "return_to": "subscribe"}
                await cb.message.edit_text(
                    "🔍 Отправь ключевое слово для поиска групп.\n"
                    "Например: `общение`, `знакомства`, `крипта`.\n\n/cancel",
                    parse_mode=enums.ParseMode.MARKDOWN)
            elif data.startswith("acc_sub_delay:"):
                acc_id = data.split(":", 1)[1]
                pending[uid] = {"action": "acc_sub_delay", "acc_id": acc_id}
                await cb.message.edit_text(
                    "⏱ Отправь мин и макс через пробел или дефис.\n"
                    "Например: `40 120` или `40-120`\n\n/cancel",
                    parse_mode=enums.ParseMode.MARKDOWN)
            elif data.startswith("acc_sub_start:"):
                acc_id = data.split(":", 1)[1]
                acc = get_account(acc_id)
                if not acc:
                    return
                if not acc.get("subscribe_queue"):
                    await cb.answer("Очередь пуста.", show_alert=True)
                    return
                if not user_clients.get(acc_id):
                    await cb.answer("Клиент не подключён.", show_alert=True)
                    return
                acc["subscribe_status"] = "running"
                save_json(STATE_FILE, STATE)
                start_subscribe(acc_id)
                await cb.answer("▶️")
                await cb.message.edit_text(subscribe_menu_text(acc_id),
                                            reply_markup=subscribe_menu_kb(acc_id))
            elif data.startswith("acc_sub_stop:"):
                acc_id = data.split(":", 1)[1]
                acc = get_account(acc_id)
                if acc:
                    acc["subscribe_status"] = "paused"
                    save_json(STATE_FILE, STATE)
                await cb.answer("⏸")
                await cb.message.edit_text(subscribe_menu_text(acc_id),
                                            reply_markup=subscribe_menu_kb(acc_id))
            elif data.startswith("acc_sub_clear:"):
                acc_id = data.split(":", 1)[1]
                acc = get_account(acc_id)
                if acc:
                    acc["subscribe_queue"] = []
                    acc["subscribe_status"] = "idle"
                    save_json(STATE_FILE, STATE)
                await cb.answer("🗑")
                await cb.message.edit_text(subscribe_menu_text(acc_id),
                                            reply_markup=subscribe_menu_kb(acc_id))

            # -------- Автоответчик --------
            elif data == "ar_menu":
                await cb.message.edit_text(autoreply_menu_text(), reply_markup=autoreply_menu_kb())
            elif data == "ar_toggle":
                ar = STATE.setdefault("autoreply", _default_autoreply())
                ar["enabled"] = not ar.get("enabled", False)
                save_json(STATE_FILE, STATE)
                await cb.message.edit_text(autoreply_menu_text(), reply_markup=autoreply_menu_kb())
            elif data == "ar_first":
                pending[uid] = {"action": "ar_first"}
                await cb.message.edit_text("Текст НОВЫМ:\n/cancel")
            elif data == "ar_known":
                pending[uid] = {"action": "ar_known"}
                await cb.message.edit_text("Текст ЗНАКОМЫМ:\n/cancel")
            elif data == "ar_inactive":
                pending[uid] = {"action": "ar_inactive"}
                await cb.message.edit_text("Минут неактивности:\n/cancel")
            elif data == "ar_cooldown":
                pending[uid] = {"action": "ar_cooldown"}
                await cb.message.edit_text("Cooldown (мин):\n/cancel")
            elif data == "ar_reset_known":
                ar = STATE.setdefault("autoreply", _default_autoreply())
                ar["known_users"] = []
                ar["known_users_loaded"] = False
                save_json(STATE_FILE, STATE)
                await cb.message.edit_text("♻️ Сброшено.", reply_markup=autoreply_menu_kb())

            # -------- Статистика --------
            elif data == "stats_menu":
                await cb.message.edit_text(stats_menu_text(), reply_markup=stats_menu_kb())
            elif data == "db_cleanup":
                await db_cleanup(30)
                await cb.answer("✅")
            else:
                await cb.answer("Неизвестная команда.")
        except Exception as e:
            log.exception("Ошибка callback")
            try:
                await cb.answer(f"Ошибка: {e}", show_alert=True)
            except Exception:
                pass

    # ---------- Ввод от админа ----------
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
            if action == "acc_add_phone":
                phone = text
                if not phone.startswith("+") or len(phone) < 8:
                    pending[uid] = {"action": "acc_add_phone"}
                    await message.reply("❌ Неверный формат. Ещё раз или /cancel.")
                    return
                sname = f"userbot_{int(datetime.now().timestamp())}"
                session_path = os.path.join(DATA_DIR, sname)
                try:
                    new_c = make_client(session_path)
                    await new_c.connect()
                    try:
                        sent = await new_c.send_code(phone, force_sms=True)
                    except TypeError:
                        sent = await new_c.send_code(phone)
                except Exception as e:
                    await message.reply(f"❌ send_code: {e}")
                    return
                pending[uid] = {
                    "action": "acc_add_code",
                    "phone": phone,
                    "hash": sent.phone_code_hash,
                    "session_name": sname,
                    "client": new_c,
                }
                await message.reply("📩 Код отправлен. Введи его:")

            elif action == "acc_add_code":
                phone = act["phone"]
                hash_ = act["hash"]
                sname = act["session_name"]
                new_c: Client = act["client"]
                code = text.replace(" ", "")
                try:
                    await new_c.sign_in(phone_number=phone,
                                        phone_code_hash=hash_,
                                        phone_code=code)
                except SessionPasswordNeeded:
                    pending[uid] = {
                        "action": "acc_add_password", "phone": phone,
                        "session_name": sname, "client": new_c,
                    }
                    await message.reply("🔐 Пароль 2FA:")
                    return
                except PhoneCodeInvalid:
                    pending[uid] = act
                    await message.reply("❌ Неверный код. Ещё раз:")
                    return
                except PhoneCodeExpired:
                    await message.reply("⚠️ Код истёк. /cancel и заново.")
                    return
                except Exception as e:
                    await message.reply(f"❌ {e}")
                    return
                me = await new_c.get_me()
                name = f"{me.first_name}" + (f" @{me.username}" if me.username else "")
                acc = add_account(name, sname)
                acc["user_id"] = me.id
                acc["username"] = me.username
                save_json(STATE_FILE, STATE)
                user_clients[acc["id"]] = new_c
                ME_IDS[acc["id"]] = me.id
                register_captcha_handler(new_c, acc["id"])
                register_reaction_handler(new_c, acc["id"])
                await message.reply(f"✅ Аккаунт добавлен: **{name}**.",
                                    reply_markup=main_menu_kb())

            elif action == "acc_add_password":
                phone = act["phone"]
                sname = act["session_name"]
                new_c: Client = act["client"]
                try:
                    await new_c.check_password(text)
                except PasswordHashInvalid:
                    pending[uid] = act
                    await message.reply("❌ Неверный пароль. Ещё раз:")
                    return
                except Exception as e:
                    await message.reply(f"❌ {e}")
                    return
                me = await new_c.get_me()
                name = f"{me.first_name}" + (f" @{me.username}" if me.username else "")
                acc = add_account(name, sname)
                acc["user_id"] = me.id
                acc["username"] = me.username
                save_json(STATE_FILE, STATE)
                user_clients[acc["id"]] = new_c
                ME_IDS[acc["id"]] = me.id
                register_captcha_handler(new_c, acc["id"])
                register_reaction_handler(new_c, acc["id"])
                await message.reply(f"✅ Аккаунт добавлен: **{name}**.",
                                    reply_markup=main_menu_kb())

            elif action == "acc_sub_load":
                acc_id = act["acc_id"]
                acc = get_account(acc_id)
                if not acc:
                    await message.reply("❌ Не найден.")
                    return
                raw_text = ""
                if message.document:
                    try:
                        path = await message.download()
                        with open(path, "r", encoding="utf-8") as f:
                            raw_text = f.read()
                        try:
                            os.remove(path)
                        except Exception:
                            pass
                    except Exception as e:
                        await message.reply(f"❌ {e}")
                        return
                else:
                    raw_text = message.text or ""
                if not raw_text.strip():
                    pending[uid] = act
                    await message.reply("Пусто. Ещё раз.")
                    return
                groups = parse_groups_from_text(raw_text)
                if not groups:
                    await message.reply("❌ Не распарсил.")
                    return
                existing_q = acc.setdefault("subscribe_queue", [])
                existing_q_keys = {str(x) for x in existing_q}
                added = 0
                for ref in groups:
                    if str(ref) not in existing_q_keys:
                        existing_q.append(ref)
                        existing_q_keys.add(str(ref))
                        added += 1
                acc["subscribe_status"] = "idle"
                save_json(STATE_FILE, STATE)
                await message.reply(
                    f"✅ Загружено: **{added}** новых.\n"
                    f"Всего в очереди: **{len(acc['subscribe_queue'])}**\n\n"
                    f"Зайди в 📥 Подписка → ▶️ Запустить",
                    reply_markup=account_kb(acc_id))

            elif action == "acc_sub_delay":
                acc_id = act["acc_id"]
                acc = get_account(acc_id)
                if not acc:
                    return
                text_clean = text.replace("-", " ").replace(",", " ")
                parts = [p for p in text_clean.split() if p.strip()]
                if len(parts) < 2:
                    pending[uid] = act
                    await message.reply("❌ Нужно 2 числа. Пример: `40 120`",
                                        parse_mode=enums.ParseMode.MARKDOWN)
                    return
                try:
                    lo = int(parts[0]); hi = int(parts[1])
                    if lo < 5: raise ValueError("мин. 5")
                    if hi < lo: raise ValueError("макс >= мин")
                    if hi > 1800: raise ValueError("макс 1800")
                except Exception as e:
                    pending[uid] = act
                    await message.reply(f"❌ {e}")
                    return
                acc["subscribe_delay_min"] = lo
                acc["subscribe_delay_max"] = hi
                save_json(STATE_FILE, STATE)
                await message.reply(f"✅ Задержка: {lo}–{hi} сек.",
                                    reply_markup=account_kb(acc_id))

            elif action == "acc_text":
                acc_id = act["acc_id"]
                acc = get_account(acc_id)
                if not acc:
                    return
                if not text:
                    pending[uid] = act
                    await message.reply("Пусто.")
                    return
                acc["text"] = text
                acc["media_path"] = None
                acc["media_type"] = None
                acc["caption"] = ""
                save_json(STATE_FILE, STATE)
                await message.reply("✅ Текст сохранён.", reply_markup=account_kb(acc_id))

            elif action == "acc_media":
                acc_id = act["acc_id"]
                acc = get_account(acc_id)
                if not acc:
                    return
                if not (message.photo or message.video):
                    pending[uid] = act
                    await message.reply("Не медиа.")
                    return
                if message.photo:
                    ext = "jpg"; acc["media_type"] = "photo"
                else:
                    ext = "mp4"; acc["media_type"] = "video"
                path = os.path.join(MEDIA_DIR, f"media_{acc_id}.{ext}")
                await message.download(file_name=path)
                acc["media_path"] = path
                acc["caption"] = message.caption or ""
                acc["text"] = ""
                save_json(STATE_FILE, STATE)
                await message.reply("✅ Медиа сохранено.", reply_markup=account_kb(acc_id))

            elif action == "acc_setint":
                acc_id = act["acc_id"]
                acc = get_account(acc_id)
                try:
                    v = int(text)
                    if v < 60: raise ValueError("мин. 60")
                    acc["interval"] = v
                    save_json(STATE_FILE, STATE)
                    await message.reply(f"✅ {v} сек.", reply_markup=account_kb(acc_id))
                except Exception as e:
                    pending[uid] = act
                    await message.reply(f"❌ {e}")

            elif action == "acc_setdmin":
                acc_id = act["acc_id"]
                acc = get_account(acc_id)
                try:
                    v = int(text)
                    if v < 1: raise ValueError("мин. 1")
                    acc["delay_min"] = v
                    if acc["delay_max"] < v:
                        acc["delay_max"] = v
                    save_json(STATE_FILE, STATE)
                    await message.reply(f"✅ {v}", reply_markup=account_kb(acc_id))
                except Exception as e:
                    pending[uid] = act
                    await message.reply(f"❌ {e}")

            elif action == "acc_setdmax":
                acc_id = act["acc_id"]
                acc = get_account(acc_id)
                try:
                    v = int(text)
                    if v < acc["delay_min"]:
                        raise ValueError(f">= {acc['delay_min']}")
                    acc["delay_max"] = v
                    save_json(STATE_FILE, STATE)
                    await message.reply(f"✅ {v}", reply_markup=account_kb(acc_id))
                except Exception as e:
                    pending[uid] = act
                    await message.reply(f"❌ {e}")

            elif action == "acc_setfoldersize":
                acc_id = act["acc_id"]
                acc = get_account(acc_id)
                try:
                    v = int(text)
                    if v < 10 or v > 100:
                        raise ValueError("10-100")
                    acc["folder_size"] = v
                    save_json(STATE_FILE, STATE)
                    await message.reply(f"✅ {v}", reply_markup=account_kb(acc_id))
                except Exception as e:
                    pending[uid] = act
                    await message.reply(f"❌ {e}")

            elif action == "acc_addgrp":
                acc_id = act["acc_id"]
                acc = get_account(acc_id)
                c = user_clients.get(acc_id)
                if not acc or not c:
                    await message.reply("❌ Не подключён.")
                    return
                ref = parse_chat_ref(text)
                if ref is None:
                    await message.reply("Не распарсил.")
                    return
                try:
                    chat = await c.get_chat(ref)
                except Exception as e:
                    await message.reply(f"❌ {classify_error(e)}")
                    return
                gid = chat.id
                title = chat.title or str(gid)
                if any(g["id"] == gid for g in acc["groups"]):
                    await message.reply("Уже в списке.")
                    return
                acc["groups"].append({
                    "id": gid, "title": title,
                    "type": chat.type.name if chat.type else "UNKNOWN",
                    "manual": True})
                save_json(STATE_FILE, STATE)
                await message.reply(f"✅ {title}", reply_markup=account_kb(acc_id))

            # -------- Поиск --------
            elif action == "search_query":
                acc_id = act["acc_id"]
                return_to = act.get("return_to")
                if not text or len(text) < 2:
                    pending[uid] = act
                    await message.reply("Слишком короткий запрос.")
                    return
                c = user_clients.get(acc_id)
                if not c:
                    await message.reply("❌ Клиент не подключён.")
                    return
                wait = await message.reply(f"🔍 Ищу «{text}»…")
                results = await search_public_groups(c, text, limit=50)
                SEARCH_CACHE.setdefault(uid, {})[acc_id] = results
                await wait.edit_text(
                    search_results_text(text, results),
                    reply_markup=search_results_kb(acc_id, len(results)))

            # -------- Автоответчик --------
            elif action == "ar_first":
                STATE.setdefault("autoreply", _default_autoreply())["template_first"] = text
                save_json(STATE_FILE, STATE)
                await message.reply("✅", reply_markup=autoreply_menu_kb())
            elif action == "ar_known":
                STATE.setdefault("autoreply", _default_autoreply())["template_known"] = text
                save_json(STATE_FILE, STATE)
                await message.reply("✅", reply_markup=autoreply_menu_kb())
            elif action == "ar_inactive":
                try:
                    v = int(text)
                    if v < 1: raise ValueError("мин. 1")
                    STATE["autoreply"]["inactive_minutes"] = v
                    save_json(STATE_FILE, STATE)
                    await message.reply(f"✅ {v}", reply_markup=autoreply_menu_kb())
                except Exception as e:
                    pending[uid] = act
                    await message.reply(f"❌ {e}")
            elif action == "ar_cooldown":
                try:
                    v = int(text)
                    if v < 0: raise ValueError("мин. 0")
                    STATE["autoreply"]["cooldown_minutes"] = v
                    save_json(STATE_FILE, STATE)
                    await message.reply(f"✅ {v}", reply_markup=autoreply_menu_kb())
                except Exception as e:
                    pending[uid] = act
                    await message.reply(f"❌ {e}")

        except Exception as e:
            log.exception("on_admin_input")
            await message.reply(f"❌ {e}")


# ---------------------------------------------------------------------------
# Запуск
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


async def start_userbot_clients():
    accounts = STATE.get("accounts") or []
    for acc in accounts:
        acc_id = acc["id"]
        session_name = acc.get("session_name") or f"userbot_{acc_id}"
        session_path = os.path.join(DATA_DIR, session_name)
        try:
            c = make_client(session_path)
            user_clients[acc_id] = c
        except Exception as e:
            log.warning(f"Не смог создать клиент {acc_id}: {e}")


async def try_start_existing_clients():
    accounts = STATE.get("accounts") or []
    for acc in accounts:
        acc_id = acc["id"]
        c = user_clients.get(acc_id)
        if not c:
            continue
        try:
            await c.start()
            me = await c.get_me()
            if me:
                ME_IDS[acc_id] = me.id
                acc["user_id"] = me.id
                acc["username"] = me.username
                log.info(f"✅ [{acc.get('name')}] {me.first_name} id={me.id}")
                register_captcha_handler(c, acc_id)
                register_reaction_handler(c, acc_id)
                if acc_id == STATE.get("main_account_id"):
                    register_main_handlers(c)
                    asyncio.create_task(populate_known_users_for_main())
                if acc.get("running"):
                    start_mailing_for_account(acc_id)
                if acc.get("subscribe_status") == "running":
                    start_subscribe(acc_id)
                if acc.get("auto_subscribe_enabled", True):
                    start_auto_subscribe(acc_id)
        except Exception as e:
            log.warning(f"[{acc.get('name')}] не залогинен: {e}")
            try:
                if not c.is_connected:
                    await c.connect()
            except Exception:
                pass
    save_json(STATE_FILE, STATE)


async def populate_known_users_for_main():
    ar = STATE.get("autoreply") or {}
    if ar.get("known_users_loaded"):
        return
    c = get_main_client()
    if not c:
        return
    known = set(ar.get("known_users") or [])
    try:
        async for dialog in c.get_dialogs():
            ch = dialog.chat
            if ch and ch.type == enums.ChatType.PRIVATE and ch.id > 0:
                known.add(str(ch.id))
    except Exception:
        pass
    ar["known_users"] = list(known)
    ar["known_users_loaded"] = True
    STATE["autoreply"] = ar
    save_json(STATE_FILE, STATE)


async def main():
    global CFG, STATE, bot_client, http_session

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

    await start_userbot_clients()

    bot_client = Client(
        name=SESSION_BOT, api_id=CFG["api_id"], api_hash=CFG["api_hash"],
        bot_token=CFG["bot_token"])
    register_handlers(bot_client)

    log.info("Запуск управляющего бота…")
    await bot_client.start()
    bme = await bot_client.get_me()
    log.info(f"Бот запущен: @{bme.username}")

    await try_start_existing_clients()

    try:
        accounts = STATE.get("accounts") or []
        await bot_client.send_message(
            CFG["admin_id"],
            f"🤖 Бот запущен.\n"
            f"• Аккаунтов: {len(accounts)}\n"
            f"• В рассылке: {sum(1 for a in accounts if a.get('running'))}\n"
            f"• В подписке: "
            f"{sum(1 for a in accounts if a.get('subscribe_status') == 'running')}\n"
            f"Для доступа: `/auth <PIN>`",
            parse_mode=enums.ParseMode.MARKDOWN)
    except Exception as e:
        log.warning(f"Приветствие: {e}")

    log.info("Сервис работает.")
    try:
        await asyncio.Event().wait()
    finally:
        if http_session:
            await http_session.close()
        for c in user_clients.values():
            try:
                await c.stop()
            except Exception:
                pass


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        print("\nОстановлено.")
