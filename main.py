# -*- coding: utf-8 -*-
"""
AUTOPOSTER 3.0 — ПОЛНАЯ АВТОМАТИЗАЦИЯ
Сам ищет группы по ключевикам -> подписывается -> раскладывает по папкам -> рассылает.
Плюс: автоответчик, автокапча, реакции, spam-чек, разбор ошибок.
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
    MessageTooLong, MediaCaptionTooLong, ChatSendMediaForbidden,
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
autosearch_tasks: dict = {}
ME_IDS: dict = {}

authed: set = set()
pending: dict = {}
MAIN_HANDLERS_REGISTERED = set()
CAPTCHA_HANDLERS_REGISTERED = set()
REACTION_HANDLERS_REGISTERED = set()

RECENT_JOINS: dict = {}
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


# ---------------------------------------------------------------------------
# Классификация ошибок
# ---------------------------------------------------------------------------

def classify_error(e: Exception) -> str:
    if isinstance(e, FloodWait):        return f"FloodWait {e.value}с"
    if isinstance(e, SlowmodeWait):     return f"Slowmode {e.value}с"
    if isinstance(e, ChatWriteForbidden):    return "Запрещено писать в группе"
    if isinstance(e, ChatAdminRequired):     return "Нужны права администратора"
    if isinstance(e, UserBannedInChannel):   return "Забанен в группе"
    if isinstance(e, UserNotParticipant):    return "Не участник группы"
    if isinstance(e, PeerIdInvalid):         return "ID группы устарел"
    if isinstance(e, UserIsBlocked):         return "Вы заблокированы в этой группе"
    if isinstance(e, ChannelPrivate):        return "Приватная / уже вышел"
    if isinstance(e, ChatForbidden):         return "Чат запрещён для записи"
    if isinstance(e, MessageTooLong):        return "Текст слишком длинный"
    if isinstance(e, MediaCaptionTooLong):   return "Подпись слишком длинная"
    if isinstance(e, ChatSendMediaForbidden):return "Запрещена отправка медиа"
    return f"{type(e).__name__}: {str(e)[:80]}"


# ---------------------------------------------------------------------------
# SQLite
# ---------------------------------------------------------------------------

async def db_init():
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL, role TEXT NOT NULL,
                content TEXT NOT NULL, timestamp TEXT NOT NULL)
        """)
        await db.execute("CREATE INDEX IF NOT EXISTS idx_user_id ON messages(user_id)")
        await db.commit()


async def db_add_message(user_id: int, role: str, content: str):
    try:
        async with aiosqlite.connect(DB_FILE) as db:
            await db.execute(
                "INSERT INTO messages (user_id, role, content, timestamp) VALUES (?,?,?,?)",
                (user_id, role, content, datetime.now().isoformat(timespec="seconds")))
            await db.commit()
    except Exception:
        pass


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
            log.error(f"Чтение {path}: {e}")
    return default


def save_json(path: str, data):
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        log.error(f"Запись {path}: {e}")


ENV_MAP = {
    "API_ID": ("api_id", int), "API_HASH": ("api_hash", str),
    "BOT_TOKEN": ("bot_token", str), "ADMIN_ID": ("admin_id", int),
    "PIN": ("pin", str),
}


def load_cfg() -> dict:
    cfg = load_json(CONFIG_FILE, {}) or {}
    for env, (key, typ) in ENV_MAP.items():
        v = os.getenv(env)
        if not v:
            continue
        try:
            cfg[key] = typ(v)
        except Exception:
            pass
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
        "error_stats": {}, "failed_groups": {},
        "subscribe_queue": [], "subscribe_status": "idle",
        "subscribe_delay_min": 40, "subscribe_delay_max": 120,
        "subscribe_stats": {"subscribed": 0, "skipped": 0, "errors": 0, "started_at": None},
        "auto_subscribe_enabled": True, "auto_subscribe_queue": [], "auto_subscribe_delay": 30,
        "last_spam_check": None, "spam_status": "unknown", "spam_message": "",
        "folder_index": 0, "folder_size": 100,
        "group_reactions_enabled": True, "group_reactions_chance": 5,
        "captcha_enabled": True, "auto_drop_dead": False,

        # АВТО-РЕЖИМ
        "auto_mode": False,              # главный тумблер
        "auto_search_enabled": False,
        "auto_search_keywords": [],      # список ключевиков
        "auto_search_interval": 900,     # сек между проходами по всем ключевикам
        "auto_search_limit": 30,         # сколько брать с одного ключевика
        "auto_search_min_participants": 50,
        "auto_folders": True,            # авто-раскладка после подписки
        "auto_mail_start": False,        # авто-старт рассылки после раскладки
        "auto_last_search": None,
        "auto_last_result": "",
    }


def default_state() -> dict:
    return {
        "accounts": [], "main_account_id": "", "owner_last_activity": None,
        "autoreply": _default_autoreply(),
        "global_stats": {
            "autoreplies": 0, "auto_subscribes": 0, "group_reactions": 0,
            "folder_moves": 0, "captchas_passed": 0, "auto_search_found": 0,
        },
        "groups": [], "known_channels": [],
    }


def load_state() -> dict:
    raw = load_json(STATE_FILE, {}) or {}
    st = default_state()
    for k, v in raw.items():
        if k == "ai_assistant":
            continue
        st[k] = v
    merged = st.get("autoreply") or {}
    for k, v in _default_autoreply().items():
        merged.setdefault(k, v)
    st["autoreply"] = merged
    if not isinstance(st.get("global_stats"), dict):
        st["global_stats"] = default_state()["global_stats"]
    for k in ("autoreplies", "auto_subscribes", "group_reactions",
              "folder_moves", "captchas_passed", "auto_search_found"):
        st["global_stats"].setdefault(k, 0)
    if not st.get("main_account_id") and st.get("accounts"):
        st["main_account_id"] = st["accounts"][0]["id"]
    for acc in st.get("accounts") or []:
        for k, v in _new_account(acc.get("name", "?"), acc.get("session_name", "?")).items():
            acc.setdefault(k, v)
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
    accs = STATE.get("accounts") or []
    new = [a for a in accs if a.get("id") != acc_id]
    if len(new) == len(accs):
        return False
    STATE["accounts"] = new
    if STATE.get("main_account_id") == acc_id:
        STATE["main_account_id"] = new[0]["id"] if new else ""
    save_json(STATE_FILE, STATE)
    return True


# ---------------------------------------------------------------------------
# Парсинг рефов / утилиты групп
# ---------------------------------------------------------------------------

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


def parse_groups_from_text(text: str) -> list:
    result = []
    for line in (text or "").split("\n"):
        line = line.split("#")[0].strip()
        if not line:
            continue
        for p in [x.strip() for x in line.split(",") if x.strip()]:
            ref = parse_chat_ref(p)
            if ref is not None:
                result.append(ref)
    seen, uniq = set(), []
    for r in result:
        k = str(r)
        if k not in seen:
            seen.add(k)
            uniq.append(r)
    return uniq


def parse_keywords(text: str) -> list:
    """Ключевики: одна строка с запятыми, или многострочно."""
    result = []
    for line in (text or "").split("\n"):
        for p in line.split(","):
            w = p.strip()
            if w:
                result.append(w)
    seen, uniq = set(), []
    for w in result:
        if w.lower() not in seen:
            seen.add(w.lower())
            uniq.append(w)
    return uniq


def check_group_overlaps() -> dict:
    key_to_accs = {}
    for acc in STATE.get("accounts") or []:
        for g in acc.get("groups") or []:
            key = str(g.get("id") or g) if g else ""
            if key:
                key_to_accs.setdefault(key, []).append(acc["id"])
    overlaps = {}
    for key, acc_ids in key_to_accs.items():
        if len(acc_ids) > 1:
            for aid in acc_ids:
                overlaps.setdefault(aid, []).append(key)
    return overlaps


# ---------------------------------------------------------------------------
# Автоответчик
# ---------------------------------------------------------------------------

def _owner_inactive(minutes: int) -> bool:
    last = STATE.get("owner_last_activity")
    if not last:
        return True
    try:
        return (datetime.now() - datetime.fromisoformat(last)).total_seconds() > minutes * 60
    except Exception:
        return True


def _owner_inactive_ar() -> bool:
    return _owner_inactive(int((STATE.get("autoreply") or {}).get("inactive_minutes", 5)))


def _mark_owner_activity():
    STATE["owner_last_activity"] = datetime.now().isoformat(timespec="seconds")
    save_json(STATE_FILE, STATE)


def _is_known_ar(uid: int) -> bool:
    return str(uid) in ((STATE.get("autoreply") or {}).get("known_users") or [])


def _mark_known_ar(uid: int):
    ar = STATE.setdefault("autoreply", _default_autoreply())
    known = ar.setdefault("known_users", [])
    if str(uid) not in known:
        known.append(str(uid))
        if len(known) > 5000:
            ar["known_users"] = known[-3000:]


def _cooldown_ok_ar(uid: int) -> bool:
    ar = STATE.get("autoreply") or {}
    last = (ar.get("last_reply") or {}).get(str(uid))
    if not last:
        return True
    try:
        return ((datetime.now() - datetime.fromisoformat(last)).total_seconds()
                >= int(ar.get("cooldown_minutes", 60)) * 60)
    except Exception:
        return True


def _set_cooldown_ar(uid: int):
    ar = STATE.setdefault("autoreply", _default_autoreply())
    ar.setdefault("last_reply", {})[str(uid)] = datetime.now().isoformat(timespec="seconds")
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
    if not url or ("t.me/" not in url and not url.startswith("@")):
        return False
    try:
        await client.join_chat(url)
        return True
    except UserAlreadyParticipant:
        return True
    except FloodWait as fw:
        await asyncio.sleep(fw.value + 5)
        return True
    except Exception:
        return False


async def try_pass_captcha(client: Client, message, acc_id: str) -> bool:
    kb = getattr(message.reply_markup, "inline_keyboard", None)
    if not kb:
        return False
    clicked = joined_any = False
    for row in kb:
        for btn in row:
            if getattr(btn, "url", None):
                if await _join_from_button(client, btn.url):
                    joined_any = True
    for row in kb:
        for btn in row:
            cd = getattr(btn, "callback_data", None)
            if not cd:
                continue
            try:
                await client.request_callback_answer(
                    chat_id=message.chat.id, message_id=message.id, callback_data=cd)
                clicked = True
                log.info(f"[{acc_id}] ✅ капча нажата в {message.chat.id}")
                await asyncio.sleep(1)
            except FloodWait as fw:
                await asyncio.sleep(fw.value + 2)
            except Exception:
                pass
    if joined_any and not clicked:
        try:
            await asyncio.sleep(4)
            m2 = await client.get_messages(message.chat.id, message.id)
            kb2 = getattr(getattr(m2, "reply_markup", None), "inline_keyboard", None) or []
            for row in kb2:
                for btn in row:
                    cd = getattr(btn, "callback_data", None)
                    if cd:
                        try:
                            await client.request_callback_answer(
                                chat_id=m2.chat.id, message_id=m2.id, callback_data=cd)
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
            u = message.from_user
            if not u or not u.is_bot:
                return
            chat = message.chat
            if not chat:
                return
            recent = is_recent_join(acc_id, chat.id)
            tl = ((message.text or message.caption or "")).lower()
            has_kb = bool(message.reply_markup)
            matched = any(k in tl for k in CAPTCHA_KEYWORDS)
            if not recent and not (has_kb and matched):
                return
            if not has_kb:
                return
            await try_pass_captcha(client, message, acc_id)
        except FloodWait as fw:
            await asyncio.sleep(fw.value + 2)
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Реакции в группах
# ---------------------------------------------------------------------------

def register_reaction_handler(c: Client, acc_id: str):
    if c.name in REACTION_HANDLERS_REGISTERED:
        return
    REACTION_HANDLERS_REGISTERED.add(c.name)

    @c.on_message(filters.incoming & (filters.group | filters.channel))
    async def on_grp(client, message):
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
            try:
                await client.send_reaction(
                    chat_id=message.chat.id, message_id=message.id,
                    emoji=random.choice(GROUP_REACTION_EMOJIS))
                STATE["global_stats"]["group_reactions"] = \
                    STATE["global_stats"].get("group_reactions", 0) + 1
            except Exception:
                pass
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Поиск групп
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
    found, seen = [], set()
    for chat in result.chats:
        try:
            if isinstance(chat, RawChannel):
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
                    "ref": "@" + username, "title": chat.title or username,
                    "username": username,
                    "participants": getattr(chat, "participants_count", 0) or 0,
                })
            elif isinstance(chat, RawChat):
                username = getattr(chat, "username", None)
                if not username:
                    continue
                key = username.lower()
                if key in seen:
                    continue
                seen.add(key)
                found.append({
                    "ref": "@" + username, "title": chat.title or username,
                    "username": username,
                    "participants": getattr(chat, "participants_count", 0) or 0,
                })
        except Exception:
            continue
    found.sort(key=lambda x: x["participants"], reverse=True)
    return found


# ---------------------------------------------------------------------------
# АВТО-ПОИСК — сердце автоматизации
# ---------------------------------------------------------------------------

def _known_refs(acc: dict) -> set:
    """Все рефы, которые уже в группах или в очереди."""
    refs = set()
    for g in acc.get("groups") or []:
        try:
            refs.add(str(g.get("id")))
        except Exception:
            pass
    for r in acc.get("subscribe_queue") or []:
        refs.add(str(r))
    return refs


async def auto_search_loop(acc_id: str):
    """Постоянно ищет группы по всем ключевикам и кладёт новые в очередь."""
    acc = get_account(acc_id)
    c = user_clients.get(acc_id)
    if not acc or not c:
        return
    log.info(f"[{acc_id}] ▶️ авто-поиск запущен")

    while acc.get("auto_search_enabled") and acc.get("auto_mode"):
        try:
            keywords = acc.get("auto_search_keywords") or []
            if not keywords:
                await asyncio.sleep(60)
                continue

            limit = int(acc.get("auto_search_limit", 30))
            min_parts = int(acc.get("auto_search_min_participants", 50))
            total_new = 0

            for kw in keywords:
                if not (acc.get("auto_search_enabled") and acc.get("auto_mode")):
                    break
                results = await search_public_groups(c, kw, limit=limit)
                known = _known_refs(acc)
                queue = acc.setdefault("subscribe_queue", [])
                added_here = 0
                for r in results:
                    if r["ref"] in known:
                        continue
                    if r["participants"] < min_parts:
                        continue
                    queue.append(r["ref"])
                    known.add(r["ref"])
                    added_here += 1
                total_new += added_here
                log.info(f"[{acc_id}] 🔍 '{kw}': +{added_here} новых (из {len(results)})")
                STATE["global_stats"]["auto_search_found"] = \
                    STATE["global_stats"].get("auto_search_found", 0) + added_here
                save_json(STATE_FILE, STATE)
                await asyncio.sleep(random.randint(8, 20))

            acc["auto_last_search"] = datetime.now().isoformat(timespec="seconds")
            acc["auto_last_result"] = f"+{total_new} новых"
            save_json(STATE_FILE, STATE)

            # если что-то нашли и подписка не бежит — стартуем
            if total_new > 0 and acc.get("subscribe_status") != "running":
                acc["subscribe_status"] = "running"
                save_json(STATE_FILE, STATE)
                start_subscribe(acc_id)
                try:
                    await bot_client.send_message(
                        CFG["admin_id"],
                        f"🔎 [{acc.get('name')}] Авто-поиск: +{total_new} групп.\n"
                        f"▶️ Запущена подписка.")
                except Exception:
                    pass

            interval = int(acc.get("auto_search_interval", 900))
            # спим интервал, но проверяем флаги
            for _ in range(interval):
                if not (acc.get("auto_search_enabled") and acc.get("auto_mode")):
                    break
                await asyncio.sleep(1)

        except FloodWait as fw:
            await asyncio.sleep(fw.value + 30)
        except Exception as e:
            log.exception(f"auto_search_loop {acc_id}: {e}")
            await asyncio.sleep(60)

    log.info(f"[{acc_id}] ⏹ авто-поиск остановлен")


def start_auto_search(acc_id: str):
    t = autosearch_tasks.get(acc_id)
    if t and not t.done():
        return
    autosearch_tasks[acc_id] = asyncio.create_task(auto_search_loop(acc_id))


def stop_auto_search(acc_id: str):
    acc = get_account(acc_id)
    if acc:
        acc["auto_search_enabled"] = False
        save_json(STATE_FILE, STATE)


# ---------------------------------------------------------------------------
# Папки Telegram
# ---------------------------------------------------------------------------

async def distribute_to_folders(acc_id: str) -> tuple:
    acc = get_account(acc_id)
    c = user_clients.get(acc_id)
    if not acc or not c:
        return 0, "Нет аккаунта/клиента"
    groups = acc.get("groups") or []
    if not groups:
        return 0, "Нет групп"
    folder_size = max(10, int(acc.get("folder_size", 100)))
    prefix = f"AP·{acc.get('name', 'acc')[:14]}"

    peers = []
    resolve_failed = 0
    for g in groups:
        try:
            peers.append(await c.resolve_peer(g["id"]))
        except Exception:
            resolve_failed += 1
        await asyncio.sleep(0.1)

    existing = {}
    try:
        res = await c.invoke(GetDialogFilters())
        for f in res.filters:
            t = getattr(f, "title", None)
            fid = getattr(f, "id", None)
            if t and fid is not None:
                existing[t] = fid
    except Exception as e:
        return 0, f"Ошибка списка папок: {e}"

    chunks = [peers[i:i + folder_size] for i in range(0, len(peers), folder_size)]
    created = errors = 0
    for i, chunk in enumerate(chunks, 1):
        title = f"{prefix} #{i}"
        fid = existing.get(title)
        if fid is None:
            used = set(existing.values())
            fid = 2
            while fid in used and fid < 255:
                fid += 1
        try:
            filt = DialogFilter(id=fid, title=title, pinned_peers=[],
                                include_peers=chunk, exclude_peers=[])
            await c.invoke(UpdateDialogFilter(id=fid, filter=filt))
            created += 1
        except Exception as e:
            log.warning(f"folder '{title}': {e}")
            errors += 1
        await asyncio.sleep(1.2)

    acc["folder_index"] = len(chunks)
    save_json(STATE_FILE, STATE)
    return created, (f"📂 Папок: {created}/{len(chunks)} | Групп: {len(peers)} | "
                     f"Ошибок resolve: {resolve_failed} | Ошибок: {errors}")


def start_folder_sort(acc_id: str):
    t = folder_tasks.get(f"folders_{acc_id}")
    if t and not t.done():
        return
    folder_tasks[f"folders_{acc_id}"] = asyncio.create_task(_folder_task(acc_id))


async def _folder_task(acc_id: str):
    acc = get_account(acc_id)
    if not acc:
        return
    try:
        _, msg = await distribute_to_folders(acc_id)
        try:
            await bot_client.send_message(
                CFG["admin_id"], f"📂 [{acc.get('name')}] {msg}")
        except Exception:
            pass
        # авто-старт рассылки
        if acc.get("auto_mail_start") and not acc.get("running") \
                and acc.get("groups") and (acc.get("text") or acc.get("media_path")):
            acc["running"] = True
            save_json(STATE_FILE, STATE)
            start_mailing_for_account(acc_id)
            try:
                await bot_client.send_message(
                    CFG["admin_id"], f"🚀 [{acc.get('name')}] Авто-старт рассылки.")
            except Exception:
                pass
    except Exception as e:
        log.exception("folder_task")


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
                    if not btn.url or "t.me/" not in btn.url:
                        continue
                    if "joinchat" in btn.url or "+" in btn.url:
                        continue
                    uname = btn.url.split("t.me/")[-1].split("/")[0].strip()
                    if not uname:
                        continue
                    try:
                        await client.join_chat(uname)
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
    c = user_clients.get(acc_id)
    if not acc or not c:
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
        await asyncio.sleep(int(acc.get("auto_subscribe_delay", 30)) + random.randint(5, 15))


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
        msgs = []
        async for m in c.get_chat_history(SPAMBOT_USERNAME, limit=3):
            if m.text:
                msgs.append(m.text)
            if len(msgs) >= 2:
                break
        if not msgs:
            return "unknown", "Нет ответа"
        resp = msgs[0]
        low = resp.lower()
        if "good news" in low or "no limits" in low:
            status, msg = "good", "Ограничений нет"
        elif "limited" in low or "restricted" in low:
            status, msg = "limited", resp[:500]
        else:
            status, msg = "unknown", resp[:500]
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
    emoji = {"good": "✅", "limited": "🚫", "flood": "⏳",
             "unknown": "❓", "error": "❌"}.get(status, "❓")
    try:
        await bot_client.send_message(
            CFG["admin_id"],
            f"{emoji} **Проверка `{acc.get('name', '?')}`**\n"
            f"Статус: **{status}**\n\n{msg}",
            parse_mode=enums.ParseMode.MARKDOWN)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Рассылка
# ---------------------------------------------------------------------------

async def send_post_for_account(acc_id: str, chat_id: int):
    c = user_clients.get(acc_id)
    if not c:
        raise RuntimeError("Нет клиента")
    acc = get_account(acc_id)
    text = acc.get("text")
    mp = acc.get("media_path")
    mt = acc.get("media_type")
    cap = acc.get("caption") or ""
    if mp and mt == "photo" and os.path.exists(mp):
        await c.send_photo(chat_id=chat_id, photo=mp, caption=cap)
    elif mp and mt == "video" and os.path.exists(mp):
        await c.send_video(chat_id=chat_id, video=mp, caption=cap)
    elif text:
        try:
            await c.send_message(chat_id=chat_id, text=text,
                                 parse_mode=enums.ParseMode.MARKDOWN)
        except Exception:
            await c.send_message(chat_id=chat_id, text=text)
    else:
        raise ValueError("Нет текста/медиа")


def _bump_error(acc, gid, title, err):
    acc["stats"]["errors"] = acc["stats"].get("errors", 0) + 1
    es = acc.setdefault("error_stats", {})
    es[err] = es.get(err, 0) + 1
    fg = acc.setdefault("failed_groups", {})
    k = str(gid)
    it = fg.get(k) or {"title": title or k, "error": err, "count": 0}
    it["error"] = err
    it["count"] = it.get("count", 0) + 1
    it["last"] = datetime.now().isoformat(timespec="seconds")
    fg[k] = it


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
        to_rm = []
        for g in groups:
            if not acc.get("running"):
                break
            gid = g.get("id")
            title = g.get("title") or str(gid)
            try:
                await send_post_for_account(acc_id, gid)
                sent += 1
                acc["stats"]["sent"] = acc["stats"].get("sent", 0) + 1
                fg = acc.get("failed_groups") or {}
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
                err = classify_error(e)
                _bump_error(acc, gid, title, err)
                if drop_dead and err in (
                        "Запрещено писать в группе", "Забанен в группе",
                        "Не участник группы", "ID группы устарел",
                        "Приватная / уже вышел", "Чат запрещён для записи"):
                    to_rm.append(gid)
            try:
                lo = int(acc.get("delay_min", 5))
                hi = int(acc.get("delay_max", 15))
                if hi < lo:
                    hi = lo
                await asyncio.sleep(random.randint(lo, hi))
            except Exception:
                await asyncio.sleep(5)

        if to_rm:
            acc["groups"] = [g for g in (acc.get("groups") or []) if g["id"] not in to_rm]

        acc["stats"]["rounds"] = acc["stats"].get("rounds", 0) + 1
        acc["stats"]["last_round"] = datetime.now().isoformat(timespec="seconds")
        save_json(STATE_FILE, STATE)
        if not acc.get("running"):
            break
        interval = int(acc.get("interval", 1800))
        acc["stats"]["next_round"] = (
            datetime.now() + timedelta(seconds=interval)).isoformat(timespec="seconds")
        save_json(STATE_FILE, STATE)
        try:
            err_brief = ""
            es = acc.get("error_stats") or {}
            if es:
                err_brief = "\n⚠️ Топ ошибок:\n" + "\n".join(
                    f"  • {k}: {v}" for k, v in Counter(es).most_common(3))
            await bot_client.send_message(
                CFG["admin_id"],
                f"✅ [{acc['name']}] Круг №{acc['stats']['rounds']}. "
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
        async for d in c.get_dialogs():
            ch = d.chat
            if ch.type not in (enums.ChatType.GROUP, enums.ChatType.SUPERGROUP):
                continue
            if getattr(ch, "is_broadcast", False):
                continue
            found.append({"id": ch.id, "title": ch.title or str(ch.id),
                          "type": ch.type.name, "manual": False})
    except Exception:
        pass
    return found


async def subscribe_one_group(client: Client, ref):
    try:
        chat = await client.join_chat(ref)
        return "ok", None, chat
    except UserAlreadyParticipant:
        try:
            return "already", None, await client.get_chat(ref)
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
        stats = acc.setdefault("subscribe_stats",
                               {"subscribed": 0, "skipped": 0, "errors": 0})
        if status == "ok":
            stats["subscribed"] += 1
            if chat:
                mark_recent_join(acc_id, chat.id)
                if not any(g["id"] == chat.id for g in (acc.get("groups") or [])):
                    acc.setdefault("groups", []).append({
                        "id": chat.id, "title": chat.title or str(chat.id),
                        "type": chat.type.name if chat.type else "UNKNOWN",
                        "manual": True})
            queue.pop(0)
        elif status == "already":
            stats["skipped"] += 1
            if chat:
                mark_recent_join(acc_id, chat.id)
                if not any(g["id"] == chat.id for g in (acc.get("groups") or [])):
                    acc.setdefault("groups", []).append({
                        "id": chat.id, "title": chat.title or str(chat.id),
                        "type": chat.type.name if chat.type else "UNKNOWN",
                        "manual": True})
            queue.pop(0)
        elif status == "flood":
            wait = int(extra or 60) + random.randint(5, 20)
            for _ in range(wait):
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
            for _ in range(random.randint(lo, hi)):
                if acc.get("subscribe_status") != "running":
                    break
                await asyncio.sleep(1)

    # ---- по завершении подписки: авто-папки ----
    if acc.get("auto_folders") and acc.get("groups"):
        log.info(f"[{acc_id}] авто-раскладка по папкам")
        await _folder_task(acc_id)

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
# ЛС хелперы
# ---------------------------------------------------------------------------

def get_main_client() -> Client:
    acc = get_main_account()
    return user_clients.get(acc["id"]) if acc else None


async def mark_chat_read(uid: int):
    c = get_main_client()
    if not c:
        return
    try:
        await c.read_chat_history(uid)
    except Exception:
        pass


async def try_send_reaction(uid: int, mid: int):
    c = get_main_client()
    if not c:
        return
    if not (STATE.get("autoreply") or {}).get("enabled"):
        return
    if random.randint(1, 100) > 20:
        return
    try:
        await c.send_reaction(chat_id=uid, message_id=mid,
                              emoji=random.choice(REACTION_EMOJIS))
    except Exception:
        pass


async def simulate_typing(uid: int, text: str):
    c = get_main_client()
    if not c or not text:
        return
    delay = min(10.0, max(1.5, (len(text) / 12.0) * random.uniform(0.7, 1.3)))
    loop = asyncio.get_event_loop()
    end = loop.time() + delay
    try:
        while loop.time() < end:
            await c.send_chat_action(uid, enums.ChatAction.TYPING)
            r = end - loop.time()
            await asyncio.sleep(min(4.0, max(0.2, r)))
    except Exception:
        pass


async def _send_as_userbot(uid: int, text: str):
    c = get_main_client()
    if not c:
        return
    await simulate_typing(uid, text)
    try:
        await c.send_message(uid, text, parse_mode=enums.ParseMode.MARKDOWN)
    except Exception:
        try:
            await c.send_message(uid, text)
        except Exception:
            return
    await db_add_message(uid, "assistant", text)


# ---------------------------------------------------------------------------
# Userbot handlers
# ---------------------------------------------------------------------------

def register_main_handlers(c: Client):
    if c.name in MAIN_HANDLERS_REGISTERED:
        return
    MAIN_HANDLERS_REGISTERED.add(c.name)

    @c.on_message(filters.private & filters.outgoing)
    async def on_out(client, message):
        try:
            chat = message.chat
            if not chat or chat.type != enums.ChatType.PRIVATE:
                return
            _mark_owner_activity()
            _mark_known_ar(chat.id)
        except Exception:
            pass

    @c.on_message(filters.private & filters.incoming)
    async def on_in(client, message):
        try:
            u = message.from_user
            if not u or u.is_bot or u.is_deleted:
                return
            if u.id == ME_IDS.get(STATE.get("main_account_id"), 0):
                return
            if u.id == CFG.get("admin_id"):
                return
            ar = STATE.get("autoreply") or {}
            if not ar.get("enabled"):
                return
            await mark_chat_read(u.id)
            text = (message.text or message.caption or "").strip()
            try:
                uname = f" (@{u.username})" if u.username else ""
                await bot_client.send_message(
                    CFG["admin_id"],
                    f"📩 **Новое ЛС**\n👤 {u.first_name}{uname}\n🆔 `{u.id}`\n"
                    f"💬 {text[:300] or '—'}",
                    parse_mode=enums.ParseMode.MARKDOWN)
            except Exception:
                pass
            if message.voice or message.video_note or message.audio:
                return
            if not text:
                return
            await db_add_message(u.id, "user", text)
            asyncio.create_task(try_send_reaction(u.id, message.id))
            if _owner_inactive_ar() and _cooldown_ok_ar(u.id):
                known = _is_known_ar(u.id)
                tmpl = (ar.get("template_known") if known
                        else ar.get("template_first")) or ""
                if tmpl.strip():
                    await _send_as_userbot(u.id, tmpl.strip())
                    _mark_known_ar(u.id)
                    _set_cooldown_ar(u.id)
                    STATE["global_stats"]["autoreplies"] = \
                        STATE["global_stats"].get("autoreplies", 0) + 1
                    save_json(STATE_FILE, STATE)
        except FloodWait as fw:
            await asyncio.sleep(fw.value + 2)
        except Exception:
            pass


# ===========================================================================
# КЛАВИАТУРЫ
# ===========================================================================

def main_menu_kb() -> InlineKeyboardMarkup:
    ar = STATE.get("autoreply") or {}
    accs = STATE.get("accounts") or []
    running = sum(1 for a in accs if a.get("running"))
    subs = sum(1 for a in accs if a.get("subscribe_status") == "running")
    auto = sum(1 for a in accs if a.get("auto_mode"))
    ar_s = "🟢" if ar.get("enabled") else "🔴"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"🤖 АВТО-РЕЖИМ ({auto}/{len(accs)})",
                              callback_data="auto_main")],
        [InlineKeyboardButton(f"📢 Рассылка ({running}/{len(accs)})",
                              callback_data="accounts_menu")],
        [InlineKeyboardButton(f"📥 Подписка" + (f" 📥{subs}" if subs else ""),
                              callback_data="sub_main")],
        [InlineKeyboardButton("🔍 Поиск вручную", callback_data="search_main")],
        [InlineKeyboardButton(f"🤖 Автоответчик ({ar_s})", callback_data="ar_menu")],
        [InlineKeyboardButton("📊 Статистика", callback_data="stats_menu")],
    ])


# ---------- АВТО-РЕЖИМ ----------

def auto_main_kb() -> InlineKeyboardMarkup:
    accs = STATE.get("accounts") or []
    rows = []
    for a in accs:
        mark = "🟢" if a.get("auto_mode") else "⚪️"
        kw_n = len(a.get("auto_search_keywords") or [])
        name = (a.get("name") or "—")[:22]
        rows.append([InlineKeyboardButton(
            f"{mark} {name} · 🔑{kw_n}",
            callback_data=f"auto_acc:{a['id']}")])
    if not accs:
        rows.append([InlineKeyboardButton("— нет аккаунтов —", callback_data="noop")])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="menu")])
    return InlineKeyboardMarkup(rows)


def auto_main_text() -> str:
    lines = ["🤖 **АВТО-РЕЖИМ**\n",
             "Скрипт сам ищет группы по ключевикам, подписывается, "
             "раскладывает по папкам и запускает рассылку.\n"]
    accs = STATE.get("accounts") or []
    if not accs:
        lines.append("⚠️ Нет аккаунтов.")
    else:
        for a in accs:
            st = "🟢" if a.get("auto_mode") else "⚪️"
            kws = a.get("auto_search_keywords") or []
            lines.append(
                f"{st} **{a.get('name', '?')}**\n"
                f"  🔑 ключей: {len(kws)} | 📥 очередь: {len(a.get('subscribe_queue') or [])} "
                f"| 🗂 групп: {len(a.get('groups') or [])}\n"
                f"  🕒 последний поиск: {a.get('auto_last_search') or '—'}")
    return "\n".join(lines)


def auto_acc_kb(acc_id: str) -> InlineKeyboardMarkup:
    a = get_account(acc_id)
    on = a.get("auto_mode", False)
    rows = [
        [InlineKeyboardButton("🔴 ВЫКЛЮЧИТЬ АВТО" if on else "🟢 ВКЛЮЧИТЬ АВТО",
                              callback_data=f"auto_toggle:{acc_id}")],
        [InlineKeyboardButton(
            f"🔑 Ключевые слова ({len(a.get('auto_search_keywords') or [])})",
            callback_data=f"auto_kw_menu:{acc_id}")],
        [InlineKeyboardButton(
            f"⏱ Интервал поиска: {a.get('auto_search_interval', 900)}с",
            callback_data=f"auto_setint:{acc_id}")],
        [InlineKeyboardButton(
            f"📊 Мин. участников: {a.get('auto_search_min_participants', 50)}",
            callback_data=f"auto_setminp:{acc_id}")],
        [InlineKeyboardButton(
            f"📥 Лимит на ключ: {a.get('auto_search_limit', 30)}",
            callback_data=f"auto_setlimit:{acc_id}")],
        [InlineKeyboardButton(
            f"📂 Авто-папки: {'🟢' if a.get('auto_folders') else '🔴'}",
            callback_data=f"auto_toggle_folders:{acc_id}")],
        [InlineKeyboardButton(
            f"🚀 Авто-старт рассылки: {'🟢' if a.get('auto_mail_start') else '🔴'}",
            callback_data=f"auto_toggle_mail:{acc_id}")],
        [InlineKeyboardButton("🧪 Прогнать поиск сейчас",
                              callback_data=f"auto_run_now:{acc_id}")],
        [InlineKeyboardButton("📋 Показать найденные группы",
                              callback_data=f"auto_show_found:{acc_id}")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="auto_main")],
    ]
    return InlineKeyboardMarkup(rows)


def auto_acc_text(acc_id: str) -> str:
    a = get_account(acc_id)
    if not a:
        return "Не найден."
    kws = a.get("auto_search_keywords") or []
    return (
        f"🤖 **Авто-режим: {a.get('name', '?')}**\n\n"
        f"• Статус: {'🟢 ВКЛ' if a.get('auto_mode') else '🔴 выкл'}\n"
        f"• Ключей: **{len(kws)}**\n"
        f"• Интервал поиска: {a.get('auto_search_interval', 900)} сек\n"
        f"• Мин. участников: {a.get('auto_search_min_participants', 50)}\n"
        f"• Лимит на ключ: {a.get('auto_search_limit', 30)}\n"
        f"• Авто-папки: {'🟢' if a.get('auto_folders') else '🔴'}\n"
        f"• Авто-старт рассылки: {'🟢' if a.get('auto_mail_start') else '🔴'}\n\n"
        f"📊 Очередь подписки: {len(a.get('subscribe_queue') or [])}\n"
        f"🗂 Групп: {len(a.get('groups') or [])}\n"
        f"🕒 Последний поиск: {a.get('auto_last_search') or '—'}\n"
        f"📈 Результат: {a.get('auto_last_result') or '—'}"
    )


def auto_kw_kb(acc_id: str) -> InlineKeyboardMarkup:
    a = get_account(acc_id)
    kws = a.get("auto_search_keywords") or []
    rows = [
        [InlineKeyboardButton("➕ Добавить ключи", callback_data=f"auto_kw_add:{acc_id}")],
        [InlineKeyboardButton("✏️ Заменить все", callback_data=f"auto_kw_set:{acc_id}")],
    ]
    if kws:
        rows.append([InlineKeyboardButton("🗑 Очистить всё",
                                          callback_data=f"auto_kw_clear:{acc_id}")])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data=f"auto_acc:{acc_id}")])
    return InlineKeyboardMarkup(rows)


def auto_kw_text(acc_id: str) -> str:
    a = get_account(acc_id)
    kws = a.get("auto_search_keywords") or []
    text = f"🔑 **Ключевые слова** ({len(kws)})\n\n"
    if not kws:
        text += "_Пока нет. Добавь через ➕._"
    else:
        for i, kw in enumerate(kws[:60], 1):
            text += f"{i}. `{kw}`\n"
        if len(kws) > 60:
            text += f"…и ещё {len(kws) - 60}"
    return text


# ---------- Аккаунты ----------

def accounts_menu_kb() -> InlineKeyboardMarkup:
    rows = []
    for a in STATE.get("accounts") or []:
        mark = "🟢" if a.get("running") else "⚪️"
        sub = " 📥" if a.get("subscribe_status") == "running" else ""
        mm = " ⭐" if a.get("id") == STATE.get("main_account_id") else ""
        rows.append([InlineKeyboardButton(
            f"{mark} {(a.get('name') or '—')[:22]}{mm}{sub}",
            callback_data=f"acc_open:{a['id']}")])
    rows.append([InlineKeyboardButton("➕ Добавить аккаунт", callback_data="acc_add")])
    if len(STATE.get("accounts") or []) > 1:
        rows.append([InlineKeyboardButton("🔀 Пересечения",
                                          callback_data="acc_check_overlaps")])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="menu")])
    return InlineKeyboardMarkup(rows)


def accounts_menu_text() -> str:
    accs = STATE.get("accounts") or []
    mid = STATE.get("main_account_id") or ""
    lines = ["📢 **Аккаунты**\n"]
    for i, a in enumerate(accs, 1):
        mark = "🟢" if a.get("running") else "⚪️"
        mm = " ⭐" if a.get("id") == mid else ""
        s = a.get("stats") or {}
        lines.append(f"{mark} **{i}. {a.get('name', '?')}**{mm}\n"
                     f"   🗂 {len(a.get('groups') or [])} | 🟢{s.get('sent', 0)} "
                     f"| ❌{s.get('errors', 0)}")
    return "\n".join(lines) if accs else "Нет аккаунтов."


def account_kb(acc_id: str) -> InlineKeyboardMarkup:
    a = get_account(acc_id)
    running = a.get("running", False)
    is_main = a.get("id") == STATE.get("main_account_id")
    sub_st = a.get("subscribe_status", "idle")
    sub_q = len(a.get("subscribe_queue") or [])
    fail = len(a.get("failed_groups") or {})
    rows = [
        [InlineKeyboardButton("🤖 АВТО-РЕЖИМ", callback_data=f"auto_acc:{acc_id}")],
        [InlineKeyboardButton("⏸ Остановить рассылку" if running else "🚀 Запустить рассылку",
                              callback_data=f"acc_toggle:{acc_id}")],
        [InlineKeyboardButton("📝 Изменить текст", callback_data=f"acc_text:{acc_id}")],
        [InlineKeyboardButton("🖼 Медиа", callback_data=f"acc_media:{acc_id}")],
        [InlineKeyboardButton("⏱ Тайминги", callback_data=f"acc_timing:{acc_id}")],
        [InlineKeyboardButton("🔍 Сканировать группы", callback_data=f"acc_scan:{acc_id}")],
        [InlineKeyboardButton("➕ Группа", callback_data=f"acc_addgrp:{acc_id}"),
         InlineKeyboardButton("🗑 Список", callback_data=f"acc_delgrp:{acc_id}")],
    ]
    if sub_st == "running":
        rows.append([InlineKeyboardButton(
            f"⏸ Остановить подписку ({sub_q})",
            callback_data=f"acc_sub_stop:{acc_id}")])
    else:
        rows.append([InlineKeyboardButton(
            "📥 Подписка" + (f" ({sub_q})" if sub_q else ""),
            callback_data=f"acc_sub_menu:{acc_id}")])
    rows += [
        [InlineKeyboardButton("🔍 Спам-чек", callback_data=f"acc_check_spam:{acc_id}")],
        [InlineKeyboardButton("📂 Разложить по папкам",
                              callback_data=f"acc_sort_folders:{acc_id}")],
        [InlineKeyboardButton(
            f"😊 Реакции: {'🟢' if a.get('group_reactions_enabled', True) else '🔴'}",
            callback_data=f"acc_toggle_group_react:{acc_id}")],
        [InlineKeyboardButton(
            f"🛡 Автокапча: {'🟢' if a.get('captcha_enabled', True) else '🔴'}",
            callback_data=f"acc_toggle_captcha:{acc_id}")],
        [InlineKeyboardButton(
            f"🧹 Автоудаление битых: {'🟢' if a.get('auto_drop_dead') else '🔴'}",
            callback_data=f"acc_toggle_drop:{acc_id}")],
        [InlineKeyboardButton(f"⚠️ Ошибки ({fail})",
                              callback_data=f"acc_errors:{acc_id}")],
    ]
    if not is_main:
        rows.append([InlineKeyboardButton("⭐ Сделать основным",
                                          callback_data=f"acc_setmain:{acc_id}")])
    rows.append([InlineKeyboardButton("🗑 Удалить",
                                       callback_data=f"acc_remove:{acc_id}")])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="accounts_menu")])
    return InlineKeyboardMarkup(rows)


def account_text(acc_id: str) -> str:
    a = get_account(acc_id)
    if not a:
        return "Не найден."
    s = a.get("stats") or {}
    txt = (a.get("text") or "").strip()
    media = a.get("media_type")
    if media:
        preview = f"[{media}] {(a.get('caption') or '')[:100]}"
    elif txt:
        preview = txt[:200]
    else:
        preview = "— (пусто)"
    return (
        f"📢 **{a.get('name', '?')}** "
        f"{'⭐' if a.get('id') == STATE.get('main_account_id') else ''}\n\n"
        f"• 🤖 АВТО: {'🟢' if a.get('auto_mode') else '🔴'}\n"
        f"• Рассылка: {'🟢' if a.get('running') else '⚪️'}\n"
        f"• Групп: {len(a.get('groups') or [])}\n"
        f"• Интервал: {a.get('interval', 1800)}с | Задержка: "
        f"{a.get('delay_min', 5)}–{a.get('delay_max', 15)}с\n\n"
        f"📊 Отправлено: {s.get('sent', 0)} | ❌ {s.get('errors', 0)} | "
        f"Кругов: {s.get('rounds', 0)}\n"
        f"📥 Очередь подписки: {len(a.get('subscribe_queue') or [])}\n"
        f"📁 Папок: {a.get('folder_index', 0)} (по {a.get('folder_size', 100)})\n\n"
        f"📝 {preview}"
    )


# ---------- Подписка ----------

def sub_main_kb() -> InlineKeyboardMarkup:
    rows = []
    for a in STATE.get("accounts") or []:
        st = a.get("subscribe_status", "idle")
        icon = {"running": "🟢", "paused": "⏸", "done": "✅"}.get(st, "⚪️")
        q = len(a.get("subscribe_queue") or [])
        rows.append([InlineKeyboardButton(
            f"{icon} {(a.get('name') or '—')[:22]}" + (f" ({q})" if q else ""),
            callback_data=f"acc_sub_menu:{a['id']}")])
    if not rows:
        rows.append([InlineKeyboardButton("— нет аккаунтов —", callback_data="noop")])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="menu")])
    return InlineKeyboardMarkup(rows)


def sub_main_text() -> str:
    return "📥 **Массовая подписка**\n\nВыбери аккаунт:"


def subscribe_menu_kb(acc_id: str) -> InlineKeyboardMarkup:
    a = get_account(acc_id)
    running = a.get("subscribe_status") == "running"
    q = len(a.get("subscribe_queue") or [])
    rows = [
        [InlineKeyboardButton("📋 Загрузить список",
                              callback_data=f"acc_sub_load:{acc_id}")],
        [InlineKeyboardButton("🔍 Поиск вручную",
                              callback_data=f"acc_sub_search:{acc_id}")],
        [InlineKeyboardButton(
            f"⏱ Задержка: {a.get('subscribe_delay_min', 40)}–"
            f"{a.get('subscribe_delay_max', 120)}с",
            callback_data=f"acc_sub_delay:{acc_id}")],
    ]
    if running:
        rows.append([InlineKeyboardButton("⏸ Остановить",
                                           callback_data=f"acc_sub_stop:{acc_id}")])
    elif q:
        rows.append([InlineKeyboardButton("▶️ Запустить",
                                           callback_data=f"acc_sub_start:{acc_id}")])
        rows.append([InlineKeyboardButton("🗑 Очистить очередь",
                                           callback_data=f"acc_sub_clear:{acc_id}")])
    else:
        rows.append([InlineKeyboardButton("— очередь пуста —", callback_data="noop")])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data=f"acc_open:{acc_id}")])
    return InlineKeyboardMarkup(rows)


def subscribe_menu_text(acc_id: str) -> str:
    a = get_account(acc_id)
    q = a.get("subscribe_queue") or []
    st = a.get("subscribe_stats") or {}
    status = a.get("subscribe_status", "idle")
    stext = {"idle": "⚪️", "running": "🟢 идёт",
             "paused": "⏸", "done": "✅ готово"}.get(status, status)
    preview = ""
    if q:
        preview = "\n\nСледующие 5:\n"
        for i, r in enumerate(q[:5], 1):
            preview += f"{i}. `{r}`\n"
        if len(q) > 5:
            preview += f"…и ещё {len(q) - 5}"
    return (
        f"📥 **Подписка: {a.get('name')}**\n\n"
        f"• Статус: {stext}\n"
        f"• В очереди: {len(q)}\n"
        f"• Задержка: {a.get('subscribe_delay_min', 40)}–"
        f"{a.get('subscribe_delay_max', 120)}с\n"
        f"• ✅{st.get('subscribed', 0)} ⏭{st.get('skipped', 0)} ❌{st.get('errors', 0)}"
        + preview
    )


# ---------- Автоответчик ----------

def autoreply_menu_kb() -> InlineKeyboardMarkup:
    ar = STATE.get("autoreply") or {}
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔴 Выкл" if ar.get("enabled") else "🟢 Вкл",
                              callback_data="ar_toggle")],
        [InlineKeyboardButton("📝 Для новых", callback_data="ar_first")],
        [InlineKeyboardButton("📝 Для знакомых", callback_data="ar_known")],
        [InlineKeyboardButton(f"⏱ Неактив: {ar.get('inactive_minutes', 5)} мин",
                              callback_data="ar_inactive")],
        [InlineKeyboardButton(f"⏳ Cooldown: {ar.get('cooldown_minutes', 60)} мин",
                              callback_data="ar_cooldown")],
        [InlineKeyboardButton("♻️ Сброс знакомых", callback_data="ar_reset_known")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="menu")],
    ])


def autoreply_menu_text() -> str:
    ar = STATE.get("autoreply") or {}
    return (f"🤖 **Автоответчик**\n"
            f"• Статус: {'🟢 вкл' if ar.get('enabled') else '🔴 выкл'}\n"
            f"• Неактив: {ar.get('inactive_minutes', 5)} мин\n"
            f"• Cooldown: {ar.get('cooldown_minutes', 60)} мин\n"
            f"• Знакомых: {len(ar.get('known_users') or [])}\n\n"
            f"📩 Новым: {(ar.get('template_first') or '—')[:200]}\n\n"
            f"📩 Знакомым: {(ar.get('template_known') or '—')[:200]}")


# ---------- Статистика ----------

def stats_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔄 Обновить", callback_data="stats_menu")],
        [InlineKeyboardButton("🗑 Очистить БД (30д)", callback_data="db_cleanup")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="menu")],
    ])


def stats_menu_text() -> str:
    gs = STATE.get("global_stats") or {}
    lines = ["📊 **Статистика**\n"]
    ts = te = tsub = 0
    for a in STATE.get("accounts") or []:
        s = a.get("stats") or {}
        ss = a.get("subscribe_stats") or {}
        ts += s.get("sent", 0)
        te += s.get("errors", 0)
        tsub += ss.get("subscribed", 0)
        lines.append(f"📢 {a.get('name', '?')}: 🟢{s.get('sent', 0)} ❌{s.get('errors', 0)} "
                     f"Кругов {s.get('rounds', 0)}")
    lines += [
        f"\n**Рассылка:** 🟢{ts} ❌{te}",
        f"**Подписок:** {tsub}",
        f"**Авто-найдено групп:** {gs.get('auto_search_found', 0)}",
        f"**Автоподписок на каналы:** {gs.get('auto_subscribes', 0)}",
        f"**Капч пройдено:** {gs.get('captchas_passed', 0)}",
        f"**Реакций в группах:** {gs.get('group_reactions', 0)}",
        f"**Автоответов:** {gs.get('autoreplies', 0)}",
    ]
    return "\n".join(lines)


# ---------- Ошибки ----------

def acc_errors_kb(acc_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔄 Обновить", callback_data=f"acc_errors:{acc_id}")],
        [InlineKeyboardButton("🧹 Удалить битые",
                              callback_data=f"acc_drop_dead:{acc_id}")],
        [InlineKeyboardButton("♻️ Сбросить статистику",
                              callback_data=f"acc_err_reset:{acc_id}")],
        [InlineKeyboardButton("⬅️ Назад", callback_data=f"acc_open:{acc_id}")],
    ])


def acc_errors_text(acc_id: str) -> str:
    a = get_account(acc_id)
    if not a:
        return "Не найден."
    es = a.get("error_stats") or {}
    fg = a.get("failed_groups") or {}
    lines = [f"⚠️ **Ошибки: {a.get('name', '?')}**\n"]
    if es:
        lines.append("**По типам:**")
        for k, v in sorted(es.items(), key=lambda x: -x[1])[:20]:
            lines.append(f"  • {k}: {v}")
    else:
        lines.append("Пока ошибок нет.")
    if fg:
        lines.append(f"\n🚫 **Проблемные** (топ-10 из {len(fg)}):")
        for gid, info in sorted(fg.items(), key=lambda x: -x[1].get("count", 0))[:10]:
            lines.append(f"  • {info.get('title', gid)[:40]} — "
                         f"_{info.get('error', '?')}_ (x{info.get('count', 1)})")
    t = "\n".join(lines)
    return t[:3800] + "\n…" if len(t) > 3800 else t


# ---------- Поиск вручную ----------

def search_accounts_kb() -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(f"🔎 {(a.get('name') or '—')[:30]}",
                                  callback_data=f"search_acc:{a['id']}")]
            for a in STATE.get("accounts") or []]
    if not rows:
        rows.append([InlineKeyboardButton("— нет —", callback_data="noop")])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="menu")])
    return InlineKeyboardMarkup(rows)


def search_results_kb(acc_id: str, count: int) -> InlineKeyboardMarkup:
    rows = []
    if count > 0:
        rows.append([InlineKeyboardButton(
            f"✅ Добавить все ({count})",
            callback_data=f"search_add_all:{acc_id}")])
        rows.append([InlineKeyboardButton("➕ Топ-10",
                                           callback_data=f"search_add_top:{acc_id}")])
    rows.append([InlineKeyboardButton("🔄 Новый поиск",
                                       callback_data=f"search_acc:{acc_id}")])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="search_main")])
    return InlineKeyboardMarkup(rows)


def search_results_text(query: str, results: list) -> str:
    if not results:
        return f"🔍 «{query}»\n\n❌ Ничего не нашлось."
    lines = [f"🔍 «{query}»\nНайдено: **{len(results)}**\n"]
    for i, r in enumerate(results[:20], 1):
        p = f"{r['participants']:,}".replace(",", " ") if r["participants"] else "—"
        lines.append(f"{i}. **{r['title'][:50]}** (@{r['username']}) — 👥 {p}")
    if len(results) > 20:
        lines.append(f"…и ещё {len(results) - 20}")
    return "\n".join(lines)


# ===========================================================================
# ХЕНДЛЕРЫ УПРАВЛЯЮЩЕГО БОТА
# ===========================================================================

def register_handlers(bot: Client):

    @bot.on_message(filters.command("start") & filters.private)
    async def cmd_start(client, message):
        if message.from_user.id != CFG["admin_id"]:
            return await message.reply("⛔")
        if message.from_user.id not in authed:
            return await message.reply("🔒 `/auth <PIN>`",
                                       parse_mode=enums.ParseMode.MARKDOWN)
        await message.reply("🎛 Панель:", reply_markup=main_menu_kb())

    @bot.on_message(filters.command("auth") & filters.private)
    async def cmd_auth(client, message):
        if message.from_user.id != CFG["admin_id"]:
            return
        parts = (message.text or "").split(maxsplit=1)
        if len(parts) < 2 or parts[1].strip() != str(CFG.get("pin", DEFAULT_PIN)):
            return await message.reply("❌ Неверный ПИН.")
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
        if not (STATE.get("accounts") or []):
            return await message.reply("Нет аккаунтов.")
        msg = await message.reply("🔍 Проверяю…")
        for acc in STATE["accounts"]:
            await notify_spam_check(acc["id"])
            await asyncio.sleep(5)
        await msg.edit_text("✅ Готово.")

    @bot.on_message(filters.command("folders") & filters.private)
    async def cmd_folders(client, message):
        if message.from_user.id != CFG["admin_id"] or message.from_user.id not in authed:
            return
        for a in STATE.get("accounts") or []:
            if a.get("groups"):
                start_folder_sort(a["id"])
        await message.reply("📂 Запущено.")

    # ---------- CALLBACKS ----------
    @bot.on_callback_query()
    async def on_cb(client, cb):
        uid = cb.from_user.id
        if uid != CFG["admin_id"]:
            return await cb.answer("⛔", show_alert=True)
        if uid not in authed:
            return await cb.answer("🔒 /auth <PIN>", show_alert=True)
        d = cb.data or ""
        try:
            if d == "menu":
                await cb.message.edit_text("🎛 Панель:", reply_markup=main_menu_kb())
            elif d == "noop":
                await cb.answer("—")

            # ====== АВТО-РЕЖИМ ======
            elif d == "auto_main":
                await cb.message.edit_text(auto_main_text(), reply_markup=auto_main_kb())
            elif d.startswith("auto_acc:"):
                aid = d.split(":", 1)[1]
                if not get_account(aid):
                    return await cb.answer("Не найден.")
                await cb.message.edit_text(auto_acc_text(aid), reply_markup=auto_acc_kb(aid))

            elif d.startswith("auto_toggle:"):
                aid = d.split(":", 1)[1]
                a = get_account(aid)
                if not a:
                    return
                new_state = not a.get("auto_mode", False)
                if new_state:
                    kws = a.get("auto_search_keywords") or []
                    if not kws:
                        return await cb.answer(
                            "Сначала задай ключевые слова 🔑", show_alert=True)
                    if not user_clients.get(aid):
                        return await cb.answer("Клиент не подключён", show_alert=True)
                    a["auto_mode"] = True
                    a["auto_search_enabled"] = True
                    # если очередь пуста и поиск ещё не гоняли — сразу запускаем
                    save_json(STATE_FILE, STATE)
                    start_auto_search(aid)
                    start_auto_subscribe(aid)
                    await cb.answer("🟢 АВТО включён")
                else:
                    a["auto_mode"] = False
                    a["auto_search_enabled"] = False
                    save_json(STATE_FILE, STATE)
                    await cb.answer("🔴 АВТО выключен")
                await cb.message.edit_text(auto_acc_text(aid), reply_markup=auto_acc_kb(aid))

            elif d.startswith("auto_run_now:"):
                aid = d.split(":", 1)[1]
                a = get_account(aid)
                c = user_clients.get(aid)
                if not a or not c:
                    return await cb.answer("Нет клиента", show_alert=True)
                kws = a.get("auto_search_keywords") or []
                if not kws:
                    return await cb.answer("Нет ключевиков", show_alert=True)
                await cb.answer("🧪 Ищу…")
                await cb.message.edit_text("🧪 Прогон поиска…")
                total = 0
                for kw in kws[:10]:
                    results = await search_public_groups(
                        c, kw, limit=int(a.get("auto_search_limit", 30)))
                    known = _known_refs(a)
                    q = a.setdefault("subscribe_queue", [])
                    for r in results:
                        if r["ref"] in known:
                            continue
                        if r["participants"] < int(a.get("auto_search_min_participants", 50)):
                            continue
                        q.append(r["ref"])
                        known.add(r["ref"])
                        total += 1
                    await asyncio.sleep(3)
                a["auto_last_search"] = datetime.now().isoformat(timespec="seconds")
                a["auto_last_result"] = f"+{total}"
                save_json(STATE_FILE, STATE)
                await cb.message.edit_text(
                    f"🧪 Готово. Найдено новых: **{total}**\n"
                    f"Очередь: **{len(a.get('subscribe_queue') or [])}**\n\n"
                    f"▶️ Запускаю подписку…",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("⬅️ Назад", callback_data=f"auto_acc:{aid}")]]))
                if total > 0 and a.get("subscribe_status") != "running":
                    a["subscribe_status"] = "running"
                    save_json(STATE_FILE, STATE)
                    start_subscribe(aid)

            elif d.startswith("auto_show_found:"):
                aid = d.split(":", 1)[1]
                a = get_account(aid)
                if not a:
                    return
                q = a.get("subscribe_queue") or []
                if not q:
                    return await cb.answer("Очередь пуста", show_alert=True)
                txt = f"📋 **Очередь ({len(q)})**\n\n"
                for i, r in enumerate(q[:40], 1):
                    txt += f"{i}. `{r}`\n"
                if len(q) > 40:
                    txt += f"…и ещё {len(q) - 40}"
                await cb.message.edit_text(txt[:3800],
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("⬅️ Назад",
                                              callback_data=f"auto_acc:{aid}")]]))

            elif d.startswith("auto_kw_menu:"):
                aid = d.split(":", 1)[1]
                await cb.message.edit_text(auto_kw_text(aid), reply_markup=auto_kw_kb(aid))
            elif d.startswith("auto_kw_add:"):
                aid = d.split(":", 1)[1]
                pending[uid] = {"action": "auto_kw_add", "acc_id": aid}
                await cb.message.edit_text(
                    "🔑 Отправь ключевые слова (через запятую или с новой строки):\n"
                    "Например: `общение, знакомства, чат, флудилка, крипта`\n\n/cancel",
                    parse_mode=enums.ParseMode.MARKDOWN)
            elif d.startswith("auto_kw_set:"):
                aid = d.split(":", 1)[1]
                pending[uid] = {"action": "auto_kw_set", "acc_id": aid}
                await cb.message.edit_text(
                    "🔑 Отправь НОВЫЙ полный список ключевиков — он ЗАМЕНИТ старые.\n\n/cancel")
            elif d.startswith("auto_kw_clear:"):
                aid = d.split(":", 1)[1]
                a = get_account(aid)
                if a:
                    a["auto_search_keywords"] = []
                    save_json(STATE_FILE, STATE)
                await cb.answer("🗑")
                await cb.message.edit_text(auto_kw_text(aid), reply_markup=auto_kw_kb(aid))

            elif d.startswith("auto_setint:"):
                aid = d.split(":", 1)[1]
                pending[uid] = {"action": "auto_setint", "acc_id": aid}
                await cb.message.edit_text(
                    "⏱ Интервал поиска в секундах (мин. 120):\n"
                    "Например: 900 = 15 мин\n/cancel")
            elif d.startswith("auto_setminp:"):
                aid = d.split(":", 1)[1]
                pending[uid] = {"action": "auto_setminp", "acc_id": aid}
                await cb.message.edit_text(
                    "📊 Мин. участников в группе (0 — без фильтра):\n/cancel")
            elif d.startswith("auto_setlimit:"):
                aid = d.split(":", 1)[1]
                pending[uid] = {"action": "auto_setlimit", "acc_id": aid}
                await cb.message.edit_text(
                    "📥 Сколько результатов с одного ключевика (5–100):\n/cancel")
            elif d.startswith("auto_toggle_folders:"):
                aid = d.split(":", 1)[1]
                a = get_account(aid)
                if a:
                    a["auto_folders"] = not a.get("auto_folders", True)
                    save_json(STATE_FILE, STATE)
                await cb.answer("✅")
                await cb.message.edit_text(auto_acc_text(aid), reply_markup=auto_acc_kb(aid))
            elif d.startswith("auto_toggle_mail:"):
                aid = d.split(":", 1)[1]
                a = get_account(aid)
                if a:
                    a["auto_mail_start"] = not a.get("auto_mail_start", False)
                    save_json(STATE_FILE, STATE)
                await cb.answer("✅")
                await cb.message.edit_text(auto_acc_text(aid), reply_markup=auto_acc_kb(aid))

            # ====== Аккаунты ======
            elif d == "accounts_menu":
                await cb.message.edit_text(accounts_menu_text(), reply_markup=accounts_menu_kb())
            elif d == "acc_add":
                pending[uid] = {"action": "acc_add_phone"}
                await cb.message.edit_text(
                    "➕ Отправь номер телефона `+79991234567`.\n/cancel",
                    parse_mode=enums.ParseMode.MARKDOWN)
            elif d == "acc_check_overlaps":
                ov = check_group_overlaps()
                if not ov:
                    return await cb.answer("✅ Пересечений нет!", show_alert=True)
                await cb.answer(f"⚠️ {len(ov)} аккаунтов пересекаются", show_alert=True)
            elif d.startswith("acc_open:"):
                aid = d.split(":", 1)[1]
                if not get_account(aid):
                    return await cb.answer("Не найден.")
                await cb.message.edit_text(account_text(aid), reply_markup=account_kb(aid))
            elif d.startswith("acc_toggle:"):
                aid = d.split(":", 1)[1]
                a = get_account(aid)
                if not a:
                    return
                if not user_clients.get(aid):
                    return await cb.answer("Клиент не подключён", show_alert=True)
                if a.get("running"):
                    a["running"] = False
                    save_json(STATE_FILE, STATE)
                    await cb.answer("⏸")
                else:
                    if not a.get("groups"):
                        return await cb.answer("Нет групп!", show_alert=True)
                    if not (a.get("text") or a.get("media_path")):
                        return await cb.answer("Нет текста!", show_alert=True)
                    a["running"] = True
                    save_json(STATE_FILE, STATE)
                    start_mailing_for_account(aid)
                    await cb.answer("🚀")
                await cb.message.edit_text(account_text(aid), reply_markup=account_kb(aid))
            elif d.startswith("acc_text:"):
                aid = d.split(":", 1)[1]
                pending[uid] = {"action": "acc_text", "acc_id": aid}
                await cb.message.edit_text("📝 Отправь текст рассылки.\n/cancel")
            elif d.startswith("acc_media:"):
                aid = d.split(":", 1)[1]
                pending[uid] = {"action": "acc_media", "acc_id": aid}
                await cb.message.edit_text("🖼 Отправь фото или видео.\n/cancel")
            elif d.startswith("acc_timing:"):
                aid = d.split(":", 1)[1]
                a = get_account(aid)
                if not a:
                    return
                kb = InlineKeyboardMarkup([
                    [InlineKeyboardButton(f"⏱ Интервал: {a.get('interval', 1800)}с",
                                          callback_data=f"acc_setint:{aid}")],
                    [InlineKeyboardButton(f"⏳ Мин: {a.get('delay_min', 5)}с",
                                          callback_data=f"acc_setdmin:{aid}")],
                    [InlineKeyboardButton(f"⏳ Макс: {a.get('delay_max', 15)}с",
                                          callback_data=f"acc_setdmax:{aid}")],
                    [InlineKeyboardButton(f"📁 Папка: {a.get('folder_size', 100)}",
                                          callback_data=f"acc_setfoldersize:{aid}")],
                    [InlineKeyboardButton("⬅️ Назад", callback_data=f"acc_open:{aid}")]])
                await cb.message.edit_text(
                    f"⏱ Тайминги **{a.get('name')}**\n"
                    f"• Интервал: {a.get('interval')}с\n"
                    f"• Задержка: {a.get('delay_min')}–{a.get('delay_max')}с\n"
                    f"• Размер папки: {a.get('folder_size', 100)}",
                    reply_markup=kb)
            elif d.startswith("acc_setint:"):
                aid = d.split(":", 1)[1]
                pending[uid] = {"action": "acc_setint", "acc_id": aid}
                await cb.message.edit_text("Интервал (≥60):\n/cancel")
            elif d.startswith("acc_setdmin:"):
                aid = d.split(":", 1)[1]
                pending[uid] = {"action": "acc_setdmin", "acc_id": aid}
                await cb.message.edit_text("Мин. задержка:\n/cancel")
            elif d.startswith("acc_setdmax:"):
                aid = d.split(":", 1)[1]
                pending[uid] = {"action": "acc_setdmax", "acc_id": aid}
                await cb.message.edit_text("Макс. задержка:\n/cancel")
            elif d.startswith("acc_setfoldersize:"):
                aid = d.split(":", 1)[1]
                pending[uid] = {"action": "acc_setfoldersize", "acc_id": aid}
                await cb.message.edit_text("📁 Размер папки (10–100):\n/cancel")
            elif d.startswith("acc_scan:"):
                aid = d.split(":", 1)[1]
                if not user_clients.get(aid):
                    return await cb.answer("Нет клиента", show_alert=True)
                await cb.answer("Сканирую…")
                await cb.message.edit_text("🔍 Сканирую…")
                found = await scan_groups_for_account(aid)
                a = get_account(aid)
                scanned = {g["id"] for g in found}
                manual = [g for g in (a.get("groups") or [])
                          if g.get("manual") and g["id"] not in scanned]
                a["groups"] = found + manual
                save_json(STATE_FILE, STATE)
                await cb.message.edit_text(
                    f"✅ Найдено: {len(found)}. Всего: {len(a['groups'])}.",
                    reply_markup=account_kb(aid))
            elif d.startswith("acc_addgrp:"):
                aid = d.split(":", 1)[1]
                pending[uid] = {"action": "acc_addgrp", "acc_id": aid}
                await cb.message.edit_text("@username / t.me/... / ID:\n/cancel")
            elif d.startswith("acc_delgrp:"):
                aid = d.split(":", 1)[1]
                a = get_account(aid)
                if not a or not a.get("groups"):
                    return await cb.answer("Пусто", show_alert=True)
                rows = [[InlineKeyboardButton(
                    f"❌ {(g.get('title') or str(g.get('id')))[:35]}",
                    callback_data=f"acc_delg:{aid}:{g['id']}")]
                    for g in (a.get("groups") or [])[:30]]
                rows.append([InlineKeyboardButton("⬅️ Назад",
                                                   callback_data=f"acc_open:{aid}")])
                await cb.message.edit_text("Удалить группу:",
                                            reply_markup=InlineKeyboardMarkup(rows))
            elif d.startswith("acc_delg:"):
                parts = d.split(":", 2)
                if len(parts) < 3:
                    return
                aid, gids = parts[1], parts[2]
                try:
                    gid = int(gids)
                except ValueError:
                    return
                a = get_account(aid)
                if a:
                    a["groups"] = [g for g in a["groups"] if g["id"] != gid]
                    save_json(STATE_FILE, STATE)
                await cb.answer("Удалено")
                await cb.message.edit_text(account_text(aid), reply_markup=account_kb(aid))
            elif d.startswith("acc_setmain:"):
                aid = d.split(":", 1)[1]
                if get_account(aid):
                    STATE["main_account_id"] = aid
                    save_json(STATE_FILE, STATE)
                    c = user_clients.get(aid)
                    if c:
                        register_main_handlers(c)
                await cb.answer("⭐ Основной изменён", show_alert=True)
                await cb.message.edit_text(account_text(aid), reply_markup=account_kb(aid))
            elif d.startswith("acc_check_spam:"):
                aid = d.split(":", 1)[1]
                await cb.answer("🔍 Проверяю…")
                await notify_spam_check(aid)
                await cb.message.edit_text(account_text(aid), reply_markup=account_kb(aid))
            elif d.startswith("acc_sort_folders:"):
                aid = d.split(":", 1)[1]
                await cb.answer("📂 Запускаю…")
                start_folder_sort(aid)
                await cb.message.edit_text("📂 Раскладываю…")
            elif d.startswith("acc_toggle_group_react:"):
                aid = d.split(":", 1)[1]
                a = get_account(aid)
                if a:
                    a["group_reactions_enabled"] = not a.get("group_reactions_enabled", True)
                    save_json(STATE_FILE, STATE)
                await cb.answer("✅")
                await cb.message.edit_text(account_text(aid), reply_markup=account_kb(aid))
            elif d.startswith("acc_toggle_captcha:"):
                aid = d.split(":", 1)[1]
                a = get_account(aid)
                if a:
                    a["captcha_enabled"] = not a.get("captcha_enabled", True)
                    save_json(STATE_FILE, STATE)
                await cb.answer("✅")
                await cb.message.edit_text(account_text(aid), reply_markup=account_kb(aid))
            elif d.startswith("acc_toggle_drop:"):
                aid = d.split(":", 1)[1]
                a = get_account(aid)
                if a:
                    a["auto_drop_dead"] = not a.get("auto_drop_dead", False)
                    save_json(STATE_FILE, STATE)
                await cb.answer("✅")
                await cb.message.edit_text(account_text(aid), reply_markup=account_kb(aid))
            elif d.startswith("acc_errors:"):
                aid = d.split(":", 1)[1]
                await cb.message.edit_text(acc_errors_text(aid), reply_markup=acc_errors_kb(aid))
            elif d.startswith("acc_err_reset:"):
                aid = d.split(":", 1)[1]
                a = get_account(aid)
                if a:
                    a["error_stats"] = {}
                    a["failed_groups"] = {}
                    save_json(STATE_FILE, STATE)
                await cb.answer("♻️")
                await cb.message.edit_text(acc_errors_text(aid), reply_markup=acc_errors_kb(aid))
            elif d.startswith("acc_drop_dead:"):
                aid = d.split(":", 1)[1]
                a = get_account(aid)
                if not a:
                    return
                dead = set()
                for gid_s, info in (a.get("failed_groups") or {}).items():
                    if info.get("error") in (
                            "Запрещено писать в группе", "Забанен в группе",
                            "Не участник группы", "ID группы устарел",
                            "Приватная / уже вышел", "Чат запрещён для записи"):
                        try:
                            dead.add(int(gid_s))
                        except Exception:
                            pass
                before = len(a.get("groups") or [])
                a["groups"] = [g for g in (a.get("groups") or []) if g["id"] not in dead]
                save_json(STATE_FILE, STATE)
                await cb.answer(f"🗑 Удалено: {before - len(a['groups'])}", show_alert=True)
                await cb.message.edit_text(acc_errors_text(aid), reply_markup=acc_errors_kb(aid))
            elif d.startswith("acc_remove:"):
                aid = d.split(":", 1)[1]
                kb = InlineKeyboardMarkup([
                    [InlineKeyboardButton("✅ Да", callback_data=f"acc_remove_ok:{aid}")],
                    [InlineKeyboardButton("❌ Нет", callback_data=f"acc_open:{aid}")]])
                await cb.message.edit_text("🗑 Удалить аккаунт?", reply_markup=kb)
            elif d.startswith("acc_remove_ok:"):
                aid = d.split(":", 1)[1]
                a = get_account(aid)
                if a:
                    a["running"] = False
                    a["auto_mode"] = False
                    a["subscribe_status"] = "paused"
                    c = user_clients.get(aid)
                    if c:
                        try:
                            await c.stop()
                        except Exception:
                            pass
                        user_clients.pop(aid, None)
                    sn = a.get("session_name")
                    if sn:
                        for ext in (".session", ".session-journal"):
                            p = os.path.join(DATA_DIR, sn + ext)
                            try:
                                if os.path.exists(p):
                                    os.remove(p)
                            except Exception:
                                pass
                    remove_account(aid)
                    await cb.answer("🗑")
                await cb.message.edit_text(accounts_menu_text(), reply_markup=accounts_menu_kb())

            # ====== Подписка ======
            elif d == "sub_main":
                await cb.message.edit_text(sub_main_text(), reply_markup=sub_main_kb())
            elif d.startswith("acc_sub_menu:"):
                aid = d.split(":", 1)[1]
                if not get_account(aid):
                    return await cb.answer("Не найден")
                await cb.message.edit_text(subscribe_menu_text(aid),
                                            reply_markup=subscribe_menu_kb(aid))
            elif d.startswith("acc_sub_load:"):
                aid = d.split(":", 1)[1]
                pending[uid] = {"action": "acc_sub_load", "acc_id": aid}
                await cb.message.edit_text(
                    "📋 Отправь список групп (или .txt файл).\n"
                    "`@user`, `https://t.me/...`, `-1001234567890`\n/cancel",
                    parse_mode=enums.ParseMode.MARKDOWN)
            elif d.startswith("acc_sub_search:"):
                aid = d.split(":", 1)[1]
                pending[uid] = {"action": "search_query", "acc_id": aid}
                await cb.message.edit_text("🔍 Ключевое слово:\n/cancel")
            elif d.startswith("acc_sub_delay:"):
                aid = d.split(":", 1)[1]
                pending[uid] = {"action": "acc_sub_delay", "acc_id": aid}
                await cb.message.edit_text("⏱ Мин и макс задержки: `40 120`\n/cancel",
                                            parse_mode=enums.ParseMode.MARKDOWN)
            elif d.startswith("acc_sub_start:"):
                aid = d.split(":", 1)[1]
                a = get_account(aid)
                if not a:
                    return
                if not a.get("subscribe_queue"):
                    return await cb.answer("Очередь пуста", show_alert=True)
                if not user_clients.get(aid):
                    return await cb.answer("Нет клиента", show_alert=True)
                a["subscribe_status"] = "running"
                save_json(STATE_FILE, STATE)
                start_subscribe(aid)
                await cb.answer("▶️")
                await cb.message.edit_text(subscribe_menu_text(aid),
                                            reply_markup=subscribe_menu_kb(aid))
            elif d.startswith("acc_sub_stop:"):
                aid = d.split(":", 1)[1]
                a = get_account(aid)
                if a:
                    a["subscribe_status"] = "paused"
                    save_json(STATE_FILE, STATE)
                await cb.answer("⏸")
                await cb.message.edit_text(subscribe_menu_text(aid),
                                            reply_markup=subscribe_menu_kb(aid))
            elif d.startswith("acc_sub_clear:"):
                aid = d.split(":", 1)[1]
                a = get_account(aid)
                if a:
                    a["subscribe_queue"] = []
                    a["subscribe_status"] = "idle"
                    save_json(STATE_FILE, STATE)
                await cb.answer("🗑")
                await cb.message.edit_text(subscribe_menu_text(aid),
                                            reply_markup=subscribe_menu_kb(aid))

            # ====== Поиск вручную ======
            elif d == "search_main":
                if not (STATE.get("accounts") or []):
                    return await cb.answer("Нет аккаунтов", show_alert=True)
                await cb.message.edit_text(
                    "🔍 **Поиск групп**\nВыбери аккаунт:",
                    reply_markup=search_accounts_kb())
            elif d.startswith("search_acc:"):
                aid = d.split(":", 1)[1]
                if not get_account(aid):
                    return await cb.answer("Не найден")
                pending[uid] = {"action": "search_query", "acc_id": aid}
                await cb.message.edit_text("🔍 Ключевое слово:\n/cancel")
            elif d.startswith("search_add_all:") or d.startswith("search_add_top:"):
                aid = d.split(":", 1)[1]
                a = get_account(aid)
                if not a:
                    return
                cache = SEARCH_CACHE.get(uid, {}).get(aid) or []
                if not cache:
                    return await cb.answer("Поиск устарел", show_alert=True)
                take = cache if d.startswith("search_add_all:") else cache[:10]
                q = a.setdefault("subscribe_queue", [])
                have = {str(x) for x in q}
                added = 0
                for r in take:
                    if r["ref"] not in have:
                        q.append(r["ref"])
                        have.add(r["ref"])
                        added += 1
                save_json(STATE_FILE, STATE)
                await cb.answer(f"✅ +{added}")
                await cb.message.edit_text(
                    f"✅ Добавлено: **{added}**\nОчередь: **{len(q)}**\n\n"
                    f"Подписка → ▶️",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("📥 Подписка",
                                              callback_data=f"acc_sub_menu:{aid}")],
                        [InlineKeyboardButton("⬅️ Меню", callback_data="menu")]]))

            # ====== Автоответчик ======
            elif d == "ar_menu":
                await cb.message.edit_text(autoreply_menu_text(), reply_markup=autoreply_menu_kb())
            elif d == "ar_toggle":
                ar = STATE.setdefault("autoreply", _default_autoreply())
                ar["enabled"] = not ar.get("enabled", False)
                save_json(STATE_FILE, STATE)
                await cb.message.edit_text(autoreply_menu_text(), reply_markup=autoreply_menu_kb())
            elif d == "ar_first":
                pending[uid] = {"action": "ar_first"}
                await cb.message.edit_text("Текст НОВЫМ:\n/cancel")
            elif d == "ar_known":
                pending[uid] = {"action": "ar_known"}
                await cb.message.edit_text("Текст ЗНАКОМЫМ:\n/cancel")
            elif d == "ar_inactive":
                pending[uid] = {"action": "ar_inactive"}
                await cb.message.edit_text("Минут неактивности:\n/cancel")
            elif d == "ar_cooldown":
                pending[uid] = {"action": "ar_cooldown"}
                await cb.message.edit_text("Cooldown (мин):\n/cancel")
            elif d == "ar_reset_known":
                ar = STATE.setdefault("autoreply", _default_autoreply())
                ar["known_users"] = []
                ar["known_users_loaded"] = False
                save_json(STATE_FILE, STATE)
                await cb.message.edit_text("♻️ Сброшено.", reply_markup=autoreply_menu_kb())

            elif d == "stats_menu":
                await cb.message.edit_text(stats_menu_text(), reply_markup=stats_menu_kb())
            elif d == "db_cleanup":
                await db_cleanup(30)
                await cb.answer("✅")
            else:
                await cb.answer("Неизвестная команда")
        except Exception as e:
            log.exception("cb error")
            try:
                await cb.answer(f"Ошибка: {e}", show_alert=True)
            except Exception:
                pass

    # ---------- ТЕКСТОВЫЙ ВВОД ОТ АДМИНА ----------
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
                    return await message.reply("❌ Формат: `+79991234567`",
                                               parse_mode=enums.ParseMode.MARKDOWN)
                sname = f"userbot_{int(datetime.now().timestamp())}"
                try:
                    new_c = make_client(os.path.join(DATA_DIR, sname))
                    await new_c.connect()
                    try:
                        sent = await new_c.send_code(phone, force_sms=True)
                    except TypeError:
                        sent = await new_c.send_code(phone)
                except Exception as e:
                    return await message.reply(f"❌ send_code: {e}")
                pending[uid] = {"action": "acc_add_code", "phone": phone,
                                "hash": sent.phone_code_hash,
                                "session_name": sname, "client": new_c}
                await message.reply("📩 Код отправлен:")

            elif action == "acc_add_code":
                new_c: Client = act["client"]
                try:
                    await new_c.sign_in(phone_number=act["phone"],
                                        phone_code_hash=act["hash"],
                                        phone_code=text.replace(" ", ""))
                except SessionPasswordNeeded:
                    pending[uid] = {"action": "acc_add_password",
                                    "phone": act["phone"],
                                    "session_name": act["session_name"],
                                    "client": new_c}
                    return await message.reply("🔐 Пароль 2FA:")
                except PhoneCodeInvalid:
                    pending[uid] = act
                    return await message.reply("❌ Неверный код. Ещё раз:")
                except Exception as e:
                    return await message.reply(f"❌ {e}")
                await _finish_add_account(new_c, act["session_name"], message)

            elif action == "acc_add_password":
                new_c: Client = act["client"]
                try:
                    await new_c.check_password(text)
                except PasswordHashInvalid:
                    pending[uid] = act
                    return await message.reply("❌ Неверный пароль. Ещё раз:")
                except Exception as e:
                    return await message.reply(f"❌ {e}")
                await _finish_add_account(new_c, act["session_name"], message)

            # ====== АВТО: ключевики ======
            elif action == "auto_kw_add":
                aid = act["acc_id"]
                a = get_account(aid)
                if not a:
                    return
                new_kws = parse_keywords(text)
                if not new_kws:
                    pending[uid] = act
                    return await message.reply("Пусто. Ещё раз:")
                existing = a.get("auto_search_keywords") or []
                lower = {k.lower() for k in existing}
                added = 0
                for k in new_kws:
                    if k.lower() not in lower:
                        existing.append(k)
                        lower.add(k.lower())
                        added += 1
                a["auto_search_keywords"] = existing
                save_json(STATE_FILE, STATE)
                await message.reply(
                    f"✅ Добавлено ключей: **{added}**\n"
                    f"Всего: **{len(existing)}**",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("⬅️ К авто-режиму",
                                              callback_data=f"auto_acc:{aid}")]]))

            elif action == "auto_kw_set":
                aid = act["acc_id"]
                a = get_account(aid)
                if not a:
                    return
                kws = parse_keywords(text)
                a["auto_search_keywords"] = kws
                save_json(STATE_FILE, STATE)
                await message.reply(
                    f"✅ Ключей: **{len(kws)}**",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("⬅️ К авто-режиму",
                                              callback_data=f"auto_acc:{aid}")]]))

            elif action == "auto_setint":
                aid = act["acc_id"]
                a = get_account(aid)
                try:
                    v = int(text)
                    if v < 120:
                        raise ValueError("мин. 120")
                    a["auto_search_interval"] = v
                    save_json(STATE_FILE, STATE)
                    await message.reply(f"✅ {v} сек.",
                                        reply_markup=InlineKeyboardMarkup([
                                            [InlineKeyboardButton(
                                                "⬅️ Назад",
                                                callback_data=f"auto_acc:{aid}")]]))
                except Exception as e:
                    pending[uid] = act
                    await message.reply(f"❌ {e}")

            elif action == "auto_setminp":
                aid = act["acc_id"]
                a = get_account(aid)
                try:
                    v = int(text)
                    if v < 0:
                        raise ValueError("≥0")
                    a["auto_search_min_participants"] = v
                    save_json(STATE_FILE, STATE)
                    await message.reply(f"✅ {v}",
                                        reply_markup=InlineKeyboardMarkup([
                                            [InlineKeyboardButton(
                                                "⬅️ Назад",
                                                callback_data=f"auto_acc:{aid}")]]))
                except Exception as e:
                    pending[uid] = act
                    await message.reply(f"❌ {e}")

            elif action == "auto_setlimit":
                aid = act["acc_id"]
                a = get_account(aid)
                try:
                    v = int(text)
                    if v < 5 or v > 100:
                        raise ValueError("5–100")
                    a["auto_search_limit"] = v
                    save_json(STATE_FILE, STATE)
                    await message.reply(f"✅ {v}",
                                        reply_markup=InlineKeyboardMarkup([
                                            [InlineKeyboardButton(
                                                "⬅️ Назад",
                                                callback_data=f"auto_acc:{aid}")]]))
                except Exception as e:
                    pending[uid] = act
                    await message.reply(f"❌ {e}")

            # ====== Остальное ======
            elif action == "acc_sub_load":
                aid = act["acc_id"]
                a = get_account(aid)
                if not a:
                    return
                raw = ""
                if message.document:
                    try:
                        path = await message.download()
                        raw = open(path, "r", encoding="utf-8").read()
                        os.remove(path)
                    except Exception as e:
                        return await message.reply(f"❌ {e}")
                else:
                    raw = message.text or ""
                groups = parse_groups_from_text(raw)
                if not groups:
                    pending[uid] = act
                    return await message.reply("❌ Не распарсил.")
                q = a.setdefault("subscribe_queue", [])
                have = {str(x) for x in q}
                added = 0
                for r in groups:
                    if str(r) not in have:
                        q.append(r)
                        have.add(str(r))
                        added += 1
                a["subscribe_status"] = "idle"
                save_json(STATE_FILE, STATE)
                await message.reply(
                    f"✅ +{added}.\nОчередь: {len(q)}",
                    reply_markup=account_kb(aid))

            elif action == "acc_sub_delay":
                aid = act["acc_id"]
                a = get_account(aid)
                parts = [p for p in text.replace("-", " ").replace(",", " ").split() if p]
                if len(parts) < 2:
                    pending[uid] = act
                    return await message.reply("❌ 2 числа: `40 120`")
                try:
                    lo, hi = int(parts[0]), int(parts[1])
                    if lo < 5 or hi < lo or hi > 1800:
                        raise ValueError("5 ≤ мин ≤ макс ≤ 1800")
                except Exception as e:
                    pending[uid] = act
                    return await message.reply(f"❌ {e}")
                a["subscribe_delay_min"] = lo
                a["subscribe_delay_max"] = hi
                save_json(STATE_FILE, STATE)
                await message.reply(f"✅ {lo}–{hi}", reply_markup=account_kb(aid))

            elif action == "acc_text":
                aid = act["acc_id"]
                a = get_account(aid)
                a["text"] = text
                a["media_path"] = None
                a["media_type"] = None
                a["caption"] = ""
                save_json(STATE_FILE, STATE)
                await message.reply("✅ Сохранено.", reply_markup=account_kb(aid))

            elif action == "acc_media":
                aid = act["acc_id"]
                a = get_account(aid)
                if not (message.photo or message.video):
                    pending[uid] = act
                    return await message.reply("Не медиа.")
                ext = "jpg" if message.photo else "mp4"
                a["media_type"] = "photo" if message.photo else "video"
                path = os.path.join(MEDIA_DIR, f"media_{aid}.{ext}")
                await message.download(file_name=path)
                a["media_path"] = path
                a["caption"] = message.caption or ""
                a["text"] = ""
                save_json(STATE_FILE, STATE)
                await message.reply("✅ Сохранено.", reply_markup=account_kb(aid))

            elif action == "acc_setint":
                aid = act["acc_id"]
                a = get_account(aid)
                try:
                    v = int(text)
                    if v < 60:
                        raise ValueError("мин. 60")
                    a["interval"] = v
                    save_json(STATE_FILE, STATE)
                    await message.reply(f"✅ {v}", reply_markup=account_kb(aid))
                except Exception as e:
                    pending[uid] = act
                    await message.reply(f"❌ {e}")

            elif action == "acc_setdmin":
                aid = act["acc_id"]
                a = get_account(aid)
                try:
                    v = int(text)
                    if v < 1:
                        raise ValueError("мин. 1")
                    a["delay_min"] = v
                    if a["delay_max"] < v:
                        a["delay_max"] = v
                    save_json(STATE_FILE, STATE)
                    await message.reply(f"✅ {v}", reply_markup=account_kb(aid))
                except Exception as e:
                    pending[uid] = act
                    await message.reply(f"❌ {e}")

            elif action == "acc_setdmax":
                aid = act["acc_id"]
                a = get_account(aid)
                try:
                    v = int(text)
                    if v < a["delay_min"]:
                        raise ValueError(f">= {a['delay_min']}")
                    a["delay_max"] = v
                    save_json(STATE_FILE, STATE)
                    await message.reply(f"✅ {v}", reply_markup=account_kb(aid))
                except Exception as e:
                    pending[uid] = act
                    await message.reply(f"❌ {e}")

            elif action == "acc_setfoldersize":
                aid = act["acc_id"]
                a = get_account(aid)
                try:
                    v = int(text)
                    if v < 10 or v > 100:
                        raise ValueError("10–100")
                    a["folder_size"] = v
                    save_json(STATE_FILE, STATE)
                    await message.reply(f"✅ {v}", reply_markup=account_kb(aid))
                except Exception as e:
                    pending[uid] = act
                    await message.reply(f"❌ {e}")

            elif action == "acc_addgrp":
                aid = act["acc_id"]
                a = get_account(aid)
                c = user_clients.get(aid)
                if not a or not c:
                    return await message.reply("❌ Нет клиента.")
                ref = parse_chat_ref(text)
                if ref is None:
                    return await message.reply("Не распарсил.")
                try:
                    chat = await c.get_chat(ref)
                except Exception as e:
                    return await message.reply(f"❌ {classify_error(e)}")
                if any(g["id"] == chat.id for g in a["groups"]):
                    return await message.reply("Уже в списке.")
                a["groups"].append({
                    "id": chat.id, "title": chat.title or str(chat.id),
                    "type": chat.type.name if chat.type else "UNKNOWN",
                    "manual": True})
                save_json(STATE_FILE, STATE)
                await message.reply(f"✅ {chat.title}", reply_markup=account_kb(aid))

            elif action == "search_query":
                aid = act["acc_id"]
                if not text or len(text) < 2:
                    pending[uid] = act
                    return await message.reply("Слишком коротко.")
                c = user_clients.get(aid)
                if not c:
                    return await message.reply("❌ Нет клиента.")
                wait = await message.reply(f"🔍 Ищу «{text}»…")
                results = await search_public_groups(c, text, limit=50)
                SEARCH_CACHE.setdefault(uid, {})[aid] = results
                await wait.edit_text(
                    search_results_text(text, results),
                    reply_markup=search_results_kb(aid, len(results)))

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
                    if v < 1:
                        raise ValueError("мин. 1")
                    STATE["autoreply"]["inactive_minutes"] = v
                    save_json(STATE_FILE, STATE)
                    await message.reply(f"✅ {v}", reply_markup=autoreply_menu_kb())
                except Exception as e:
                    pending[uid] = act
                    await message.reply(f"❌ {e}")
            elif action == "ar_cooldown":
                try:
                    v = int(text)
                    if v < 0:
                        raise ValueError("≥0")
                    STATE["autoreply"]["cooldown_minutes"] = v
                    save_json(STATE_FILE, STATE)
                    await message.reply(f"✅ {v}", reply_markup=autoreply_menu_kb())
                except Exception as e:
                    pending[uid] = act
                    await message.reply(f"❌ {e}")
        except Exception as e:
            log.exception("admin input")
            await message.reply(f"❌ {e}")


async def _finish_add_account(new_c: Client, sname: str, message):
    me = await new_c.get_me()
    name = me.first_name + (f" @{me.username}" if me.username else "")
    acc = add_account(name, sname)
    acc["user_id"] = me.id
    acc["username"] = me.username
    save_json(STATE_FILE, STATE)
    user_clients[acc["id"]] = new_c
    ME_IDS[acc["id"]] = me.id
    register_captcha_handler(new_c, acc["id"])
    register_reaction_handler(new_c, acc["id"])
    if not STATE.get("main_account_id"):
        STATE["main_account_id"] = acc["id"]
        save_json(STATE_FILE, STATE)
    await message.reply(f"✅ **{name}** добавлен.",
                        reply_markup=main_menu_kb())


# ===========================================================================
# ЗАПУСК
# ===========================================================================

def _install_asyncio_handler(loop):
    def h(loop, ctx):
        exc = ctx.get("exception")
        msg = str(ctx.get("message") or "")
        if isinstance(exc, ValueError) and "Peer id invalid" in str(exc):
            return
        if "Peer id invalid" in msg or "ID not found" in msg:
            return
        loop.default_exception_handler(ctx)
    loop.set_exception_handler(h)


async def start_userbot_clients():
    for acc in STATE.get("accounts") or []:
        aid = acc["id"]
        sname = acc.get("session_name") or f"userbot_{aid}"
        try:
            user_clients[aid] = make_client(os.path.join(DATA_DIR, sname))
        except Exception as e:
            log.warning(f"client {aid}: {e}")


async def try_start_existing_clients():
    for acc in STATE.get("accounts") or []:
        aid = acc["id"]
        c = user_clients.get(aid)
        if not c:
            continue
        try:
            await c.start()
            me = await c.get_me()
            if me:
                ME_IDS[aid] = me.id
                acc["user_id"] = me.id
                acc["username"] = me.username
                log.info(f"✅ [{acc.get('name')}] {me.first_name} id={me.id}")
                register_captcha_handler(c, aid)
                register_reaction_handler(c, aid)
                if aid == STATE.get("main_account_id"):
                    register_main_handlers(c)
                    asyncio.create_task(populate_known_users())
                if acc.get("running"):
                    start_mailing_for_account(aid)
                if acc.get("subscribe_status") == "running":
                    start_subscribe(aid)
                if acc.get("auto_subscribe_enabled", True):
                    start_auto_subscribe(aid)
                # АВТО-ПОИСК если был включён
                if acc.get("auto_mode") and acc.get("auto_search_keywords"):
                    acc["auto_search_enabled"] = True
                    start_auto_search(aid)
                    log.info(f"[{aid}] авто-поиск восстановлен")
        except Exception as e:
            log.warning(f"[{acc.get('name')}] не залогинен: {e}")
            try:
                if not c.is_connected:
                    await c.connect()
            except Exception:
                pass
    save_json(STATE_FILE, STATE)


async def populate_known_users():
    ar = STATE.get("autoreply") or {}
    if ar.get("known_users_loaded"):
        return
    c = get_main_client()
    if not c:
        return
    known = set(ar.get("known_users") or [])
    try:
        async for d in c.get_dialogs():
            ch = d.chat
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

    _install_asyncio_handler(asyncio.get_running_loop())
    await db_init()
    http_session = aiohttp.ClientSession()

    await start_userbot_clients()

    bot_client = Client(name=SESSION_BOT, api_id=CFG["api_id"],
                        api_hash=CFG["api_hash"], bot_token=CFG["bot_token"])
    register_handlers(bot_client)

    log.info("Запуск бота…")
    await bot_client.start()
    bme = await bot_client.get_me()
    log.info(f"Бот: @{bme.username}")

    await try_start_existing_clients()

    try:
        accs = STATE.get("accounts") or []
        await bot_client.send_message(
            CFG["admin_id"],
            f"🤖 Бот запущен.\n"
            f"• Аккаунтов: {len(accs)}\n"
            f"• В авто-режиме: {sum(1 for a in accs if a.get('auto_mode'))}\n"
            f"• В рассылке: {sum(1 for a in accs if a.get('running'))}\n\n"
            f"`/auth <PIN>` для входа.",
            parse_mode=enums.ParseMode.MARKDOWN)
    except Exception:
        pass

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
