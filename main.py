# -*- coding: utf-8 -*-
"""
Мульти-аккаунт автопостер + рассылка + массовая подписка.
Премиум-эмодзи через MessageEntity (CUSTOM_EMOJI).
Плашки «Переслано» в группах НЕТ.
Скан/добавление — ТОЛЬКО группы.
Исключения, проблемные, уведомления о входящих ЛС, статистика отписей.
Прокси на каждый аккаунт (глобальный вкл/выкл).
8 вариантов текста с автоподменой каждые 15-20 сообщений.
Рандомизация таймингов + периодические длинные паузы.
"""

import asyncio
import json
import os
import random
import re
import logging
import base64
import time
from datetime import datetime, timedelta

import aiosqlite

from pyrogram import Client, filters, enums
from pyrogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton, MessageEntity,
)
from pyrogram.enums import MessageEntityType, ChatType
from pyrogram.errors import (
    FloodWait, ChatWriteForbidden, ChatAdminRequired, UserBannedInChannel,
    PeerIdInvalid, UserIsBlocked, ChannelPrivate,
    SessionPasswordNeeded, PhoneCodeInvalid, PhoneCodeExpired, PasswordHashInvalid,
    UserAlreadyParticipant, UsernameInvalid, UsernameNotOccupied,
    InviteHashExpired, InviteHashInvalid, ChatIdInvalid,
    AuthKeyUnregistered, AuthKeyDuplicated, UserDeactivated, SessionRevoked,
)


# ===========================================================================
#  СЕССИЯ ПО УМОЛЧАНИЮ (вставь свою новую из Colab)
# ===========================================================================
DEFAULT_SESSION_STRING = (
    "ВСТАВЬ_СЮДА_СВОЮ_НОВУЮ_СТРОКУ_ИЗ_COLAB"
)


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
DEFAULT_TYPING_SECONDS = 2
GROUPS_PER_PAGE = 8
PROBLEM_PER_PAGE = 8
EXCLUDED_PER_PAGE = 10
INCOMING_LOG_PER_PAGE = 10
TEXTS_PER_PAGE = 5

INCOMING_NOTIFY_COOLDOWN_SEC = 300
INCOMING_WARMUP_SEC = 3

# Рандомизация таймингов между отправками
SHORT_PAUSE_PROB = 0.85          # 85% — короткая пауза delay_min..delay_max
LONG_PAUSE_EVERY_MIN = 8         # каждые N отправок — длинная пауза
LONG_PAUSE_EVERY_MAX = 15
LONG_PAUSE_MIN = 45
LONG_PAUSE_MAX = 180

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

user_clients: dict = {}
mailing_tasks: dict = {}
subscribe_tasks: dict = {}
ME_IDS: dict = {}
client_locks: dict = {}

authed: set = set()
pending: dict = {}

_incoming_ready: dict = {}
_last_notify_by_user: dict = {}
_handled_message_ids: set = set()

DEVICE_PARAMS = {
    "app_version": "9.3.1",
    "device_model": "Samsung Galaxy S23 Ultra",
    "system_version": "Android 13",
    "lang_code": "ru",
}


# ===========================================================================
#  Утилиты
# ===========================================================================

def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def today_str() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def entities_to_json(entities) -> list:
    if not entities:
        return []
    out = []
    for e in entities:
        try:
            etype = e.type.value if hasattr(e.type, "value") else str(e.type)
            item = {"type": etype, "offset": int(e.offset), "length": int(e.length)}
            url = getattr(e, "url", None)
            if url:
                item["url"] = url
            lang = getattr(e, "language", None)
            if lang:
                item["language"] = lang
            ce = getattr(e, "custom_emoji_id", None)
            if ce:
                item["custom_emoji_id"] = str(ce)
            out.append(item)
        except Exception:
            continue
    return out


def entities_from_json(data) -> list:
    if not data:
        return None
    out = []
    for d in data:
        try:
            try:
                etype = MessageEntityType(d["type"])
            except Exception:
                etype = d["type"]
            kwargs = {"type": etype, "offset": int(d["offset"]), "length": int(d["length"])}
            if d.get("url"):
                kwargs["url"] = d["url"]
            if d.get("language"):
                kwargs["language"] = d["language"]
            if d.get("custom_emoji_id"):
                kwargs["custom_emoji_id"] = int(d["custom_emoji_id"])
            out.append(MessageEntity(**kwargs))
        except Exception:
            continue
    return out or None


def is_target_group(chat) -> bool:
    if chat is None:
        return False
    try:
        ctype = getattr(chat, "type", None)
        if ctype not in (ChatType.GROUP, ChatType.SUPERGROUP):
            return False
        if getattr(chat, "is_broadcast", False):
            return False
        if getattr(chat, "is_fake", False) or getattr(chat, "is_scam", False):
            return False
        return True
    except Exception:
        return False


def build_chat_link(chat) -> str:
    try:
        username = getattr(chat, "username", None)
        if username:
            return f"https://t.me/{username}"
        invite = getattr(chat, "invite_link", None)
        if invite:
            return invite
        cid = getattr(chat, "id", None)
        if cid:
            s = str(cid)
            if s.startswith("-100"):
                s = s[4:]
            return f"tg://openmessage?chat_id={s}"
    except Exception:
        pass
    return ""


def build_group_record(chat, manual: bool = False) -> dict:
    return {
        "id": chat.id,
        "title": chat.title or str(chat.id),
        "type": chat.type.name if chat.type else "UNKNOWN",
        "username": getattr(chat, "username", None),
        "link": build_chat_link(chat),
        "manual": manual,
    }


# ===========================================================================
#  ПРОКСИ
# ===========================================================================

# Записи в account["proxy"]:
#   {"enabled": bool, "type": "socks5"|"http"|"mtproxy",
#    "host": "...", "port": 1080, "username": "", "password": "", "secret": ""}

def parse_proxy_line(line: str) -> dict:
    """
    Поддерживает:
      socks5://user:pass@host:port
      socks5://host:port
      http://user:pass@host:port
      https://host:port
      mtproxy://secret@host:port
      host:port:user:pass      (для socks5)
      host:port
    Возвращает dict или бросит ValueError.
    """
    s = (line or "").strip()
    if not s:
        raise ValueError("пустая строка")

    # URL-формат
    m = re.match(r"^(socks5|http|https|mtproxy)://(.+)$", s, re.IGNORECASE)
    if m:
        scheme = m.group(1).lower()
        rest = m.group(2)
        secret = None
        if "@" in rest and scheme == "mtproxy":
            secret, rest = rest.split("@", 1)
        creds = None
        if "@" in rest and scheme != "mtproxy":
            creds, rest = rest.split("@", 1)
        if ":" not in rest:
            raise ValueError("нет порта")
        host, port_s = rest.rsplit(":", 1)
        try:
            port = int(port_s)
        except ValueError:
            raise ValueError("порт не число")
        if not (1 <= port <= 65535):
            raise ValueError("порт вне диапазона")

        if scheme == "mtproxy":
            return {
                "enabled": True,
                "type": "mtproxy",
                "host": host,
                "port": port,
                "username": "",
                "password": "",
                "secret": secret or "",
            }

        username = password = ""
        if creds:
            if ":" in creds:
                username, password = creds.split(":", 1)
            else:
                username = creds
        return {
            "enabled": True,
            "type": "http" if scheme in ("http", "https") else "socks5",
            "host": host,
            "port": port,
            "username": username,
            "password": password,
            "secret": "",
        }

    # host:port[:user:pass]
    parts = s.split(":")
    if len(parts) == 2:
        host, port_s = parts
        user = pwd = ""
    elif len(parts) == 4:
        host, port_s, user, pwd = parts
    else:
        raise ValueError("формат: host:port[:user:pass]")
    try:
        port = int(port_s)
    except ValueError:
        raise ValueError("порт не число")
    if not (1 <= port <= 65535):
        raise ValueError("порт вне диапазона")
    return {
        "enabled": True,
        "type": "socks5",
        "host": host,
        "port": port,
        "username": user,
        "password": pwd,
        "secret": "",
    }


def proxy_dict_for_pyrogram(proxy: dict):
    """Pyrogram 2.x понимает: socks5/http/mtproxy — словарь."""
    if not proxy or not proxy.get("enabled"):
        return None
    try:
        ptype = (proxy.get("type") or "socks5").lower()
        host = proxy["host"]
        port = int(proxy["port"])
        if ptype == "mtproxy":
            d = {"scheme": "mtproxy", "hostname": host, "port": port,
                 "secret": proxy.get("secret") or ""}
            return d
        d = {"scheme": ptype, "hostname": host, "port": port}
        if proxy.get("username"):
            d["username"] = proxy["username"]
        if proxy.get("password"):
            d["password"] = proxy["password"]
        return d
    except Exception:
        return None


def proxy_is_active_for_account(acc: dict) -> bool:
    if not CFG.get("proxy_enabled", False):
        return False
    p = (acc or {}).get("proxy") or {}
    return bool(p.get("enabled") and p.get("host") and p.get("port"))


# ===========================================================================
#  БАЗА / JSON
# ===========================================================================

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


# ===========================================================================
#  КОНФИГ
# ===========================================================================

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
    try:
        if cfg.get("admin_id") is not None:
            cfg["admin_id"] = int(cfg["admin_id"])
    except Exception:
        pass
    try:
        if cfg.get("api_id") is not None:
            cfg["api_id"] = int(cfg["api_id"])
    except Exception:
        pass
    cfg.setdefault("proxy_enabled", False)  # глобальный выключатель
    return cfg


def cfg_ok(cfg: dict) -> bool:
    return all(cfg.get(k) for k in ("api_id", "api_hash", "bot_token", "admin_id"))


def validate_session_string(s: str):
    if not s:
        return False, "пустая"
    if len(s) < 100:
        return False, f"слишком короткая ({len(s)})"
    try:
        padded = s + "=" * (-len(s) % 4)
        decoded = base64.urlsafe_b64decode(padded)
        if len(decoded) < 200:
            return False, f"декодированная длина мала ({len(decoded)})"
    except Exception as e:
        return False, f"не base64: {e}"
    return True, "ok"


def make_client(session_name_or_string: str, proxy: dict = None) -> Client:
    raw = session_name_or_string or ""
    s = re.sub(r"\s+", "", raw).strip('"').strip("'").strip("`")
    proxy_arg = proxy_dict_for_pyrogram(proxy) if proxy else None
    if len(s) > 100:
        return Client(
            name=":memory:",
            api_id=CFG["api_id"], api_hash=CFG["api_hash"],
            session_string=s,
            app_version=DEVICE_PARAMS["app_version"],
            device_model=DEVICE_PARAMS["device_model"],
            system_version=DEVICE_PARAMS["system_version"],
            lang_code=DEVICE_PARAMS["lang_code"],
            proxy=proxy_arg,
        )
    session_path = os.path.join(DATA_DIR, s)
    return Client(
        name=session_path,
        api_id=CFG["api_id"], api_hash=CFG["api_hash"],
        app_version=DEVICE_PARAMS["app_version"],
        device_model=DEVICE_PARAMS["device_model"],
        system_version=DEVICE_PARAMS["system_version"],
        lang_code=DEVICE_PARAMS["lang_code"],
        proxy=proxy_arg,
    )


# ===========================================================================
#  STATE
# ===========================================================================

def _new_account(name: str, session_name: str) -> dict:
    return {
        "id": f"acc_{int(datetime.now().timestamp() * 1000)}_{random.randint(100, 999)}",
        "name": name, "session_name": session_name, "session_string": "",
        "user_id": None, "username": None,

        # базовый (не используется, если есть пул текстов)
        "text": "", "text_entities": [],
        "caption": "", "caption_entities": [],
        "media_path": None, "media_type": None,

        # пул текстов: список {text, entities, media_path, media_type, caption, caption_entities}
        "text_pool": [],
        "pool_index": 0,
        "rotate_every": 17,     # каждые N сообщений меняем вариант

        "source_chat_id": None, "source_message_id": None,
        "groups": [], "excluded": [], "problem": [],
        "running": False,
        "interval": 1800, "delay_min": 5, "delay_max": 15,
        "typing_seconds": DEFAULT_TYPING_SECONDS,

        "stats": {"sent": 0, "errors": 0, "rounds": 0,
                  "last_round": None, "next_round": None},

        "subscribe_queue": [], "subscribe_status": "idle",
        "subscribe_delay_min": 40, "subscribe_delay_max": 120,
        "subscribe_stats": {"subscribed": 0, "skipped": 0, "errors": 0,
                            "started_at": None},

        # прокси для аккаунта (используется только если CFG["proxy_enabled"] == True)
        "proxy": {"enabled": False, "type": "socks5",
                  "host": "", "port": 0,
                  "username": "", "password": "", "secret": ""},
    }


def default_state() -> dict:
    return {
        "accounts": [],
        "main_account_id": "",
        "global_stats": {"total_sent": 0, "total_errors": 0, "total_subscribed": 0},
        "incoming_users": {},
        "incoming_daily": {},
        "incoming_log": [],
    }


def load_state() -> dict:
    raw = load_json(STATE_FILE, {}) or {}
    st = default_state()

    if isinstance(raw.get("accounts"), list):
        st["accounts"] = raw["accounts"]
    st["main_account_id"] = raw.get("main_account_id") or ""
    if isinstance(raw.get("global_stats"), dict):
        for k in ("total_sent", "total_errors", "total_subscribed"):
            st["global_stats"][k] = int(raw["global_stats"].get(k, 0))
    st["incoming_users"] = raw.get("incoming_users") or {}
    st["incoming_daily"] = raw.get("incoming_daily") or {}
    st["incoming_log"] = raw.get("incoming_log") or []

    if not st.get("main_account_id") and st.get("accounts"):
        st["main_account_id"] = st["accounts"][0]["id"]

    for acc in st.get("accounts") or []:
        acc.setdefault("session_string", "")
        acc.setdefault("text", "")
        acc.setdefault("text_entities", [])
        acc.setdefault("caption", "")
        acc.setdefault("caption_entities", [])
        acc.setdefault("media_path", None)
        acc.setdefault("media_type", None)
        acc.setdefault("text_pool", [])
        acc.setdefault("pool_index", 0)
        acc.setdefault("rotate_every", 17)
        acc.setdefault("subscribe_queue", [])
        acc.setdefault("subscribe_status", "idle")
        acc.setdefault("subscribe_delay_min", 40)
        acc.setdefault("subscribe_delay_max", 120)
        acc.setdefault("subscribe_stats",
                       {"subscribed": 0, "skipped": 0, "errors": 0, "started_at": None})
        acc.setdefault("stats", {"sent": 0, "errors": 0, "rounds": 0})
        acc.setdefault("source_chat_id", None)
        acc.setdefault("source_message_id", None)
        acc.setdefault("typing_seconds", DEFAULT_TYPING_SECONDS)
        acc.setdefault("excluded", [])
        acc.setdefault("problem", [])
        if not isinstance(acc.get("proxy"), dict):
            acc["proxy"] = {"enabled": False, "type": "socks5",
                            "host": "", "port": 0,
                            "username": "", "password": "", "secret": ""}
        else:
            for k, v in {"enabled": False, "type": "socks5",
                          "host": "", "port": 0,
                          "username": "", "password": "", "secret": ""}.items():
                acc["proxy"].setdefault(k, v)
        for g in acc.get("groups") or []:
            g.setdefault("link", "")
            g.setdefault("username", None)

    st["_bootstrapped"] = raw.get("_bootstrapped", False)
    return st


def bootstrap_default_session():
    global STATE
    if not DEFAULT_SESSION_STRING or DEFAULT_SESSION_STRING.startswith("ВСТАВЬ"):
        return
    if STATE.get("_bootstrapped"):
        return
    if STATE.get("accounts"):
        STATE["_bootstrapped"] = True
        save_json(STATE_FILE, STATE)
        return
    ok, reason = validate_session_string(DEFAULT_SESSION_STRING)
    if not ok:
        log.warning(f"[BOOTSTRAP] DEFAULT_SESSION_STRING невалидна: {reason}")
        return
    acc = _new_account("Основной", f"string_{int(datetime.now().timestamp())}")
    acc["session_string"] = DEFAULT_SESSION_STRING
    STATE.setdefault("accounts", []).append(acc)
    STATE["main_account_id"] = acc["id"]
    STATE["_bootstrapped"] = True
    save_json(STATE_FILE, STATE)
    log.info("[BOOTSTRAP] Добавлен аккаунт «Основной»")


def get_account(acc_id: str) -> dict:
    for a in STATE.get("accounts") or []:
        if a.get("id") == acc_id:
            return a
    return {}


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


def normalize_group_ref(g) -> str:
    if isinstance(g, dict):
        gid = g.get("id")
        return str(gid) if gid else ""
    return str(g)


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
    s = s.split("?")[0].split("/")[0].strip()
    if not s:
        return None
    if s.startswith("@"):
        return s
    try:
        return int(s)
    except ValueError:
        return "@" + s


def parse_index_list(text: str) -> list:
    if not text:
        return []
    out = set()
    chunks = re.split(r"[,;\s]+", text.strip())
    for ch in chunks:
        if not ch:
            continue
        if "-" in ch:
            try:
                a, b = ch.split("-", 1)
                a, b = int(a), int(b)
                if a > b:
                    a, b = b, a
                for i in range(a, b + 1):
                    out.add(i)
            except Exception:
                continue
        else:
            try:
                out.add(int(ch))
            except Exception:
                continue
    return sorted(i for i in out if i > 0)


# ---------------------------------------------------------------------------
#  Проблемные/исключённые
# ---------------------------------------------------------------------------

def _group_key(g: dict) -> str:
    return str(g.get("id") or "")


def add_to_excluded(acc: dict, g: dict):
    if not acc or not g:
        return
    key = _group_key(g)
    if not key:
        return
    ex = acc.setdefault("excluded", [])
    if not any(_group_key(x) == key for x in ex):
        ex.append({
            "id": g.get("id"),
            "title": g.get("title") or str(g.get("id")),
            "type": g.get("type"),
            "username": g.get("username"),
            "link": g.get("link") or "",
            "added_iso": now_iso(),
        })


def remove_from_excluded(acc: dict, gid) -> bool:
    if not acc:
        return False
    key = str(gid)
    before = len(acc.get("excluded") or [])
    acc["excluded"] = [x for x in (acc.get("excluded") or [])
                       if str(x.get("id")) != key]
    return len(acc["excluded"]) != before


def mark_problem(acc: dict, g: dict, error: str):
    if not acc or not g:
        return
    key = _group_key(g)
    if not key:
        return
    prob = acc.setdefault("problem", [])
    for p in prob:
        if str(p.get("id")) == key:
            p["count"] = int(p.get("count", 1)) + 1
            p["error"] = str(error)[:200]
            p["last_iso"] = now_iso()
            return
    prob.append({
        "id": g.get("id"),
        "title": g.get("title") or str(g.get("id")),
        "type": g.get("type"),
        "username": g.get("username"),
        "link": g.get("link") or "",
        "error": str(error)[:200],
        "count": 1,
        "first_iso": now_iso(),
        "last_iso": now_iso(),
    })


def clear_problem(acc: dict, gid) -> bool:
    if not acc:
        return False
    key = str(gid)
    before = len(acc.get("problem") or [])
    acc["problem"] = [x for x in (acc.get("problem") or [])
                      if str(x.get("id")) != key]
    return len(acc["problem"]) != before


# ---------------------------------------------------------------------------
#  Учёт входящих ЛС
# ---------------------------------------------------------------------------

def register_incoming(user_id: int, name: str, username: str, text: str) -> bool:
    users = STATE.setdefault("incoming_users", {})
    daily = STATE.setdefault("incoming_daily", {})
    log_list = STATE.setdefault("incoming_log", [])
    key = str(user_id)
    day = today_str()

    daily.setdefault(day, {"new": 0, "messages": 0})
    daily[day]["messages"] += 1

    is_new_today = False
    entry = users.get(key)
    if not entry:
        users[key] = {
            "user_id": user_id, "name": name or "", "username": username or "",
            "first_iso": now_iso(), "first_day": day,
            "first_text": (text or "")[:500],
            "last_iso": now_iso(), "last_text": (text or "")[:500],
            "days": [day],
        }
        daily[day]["new"] += 1
        is_new_today = True
    else:
        entry["last_iso"] = now_iso()
        entry["last_text"] = (text or "")[:500]
        if name and not entry.get("name"):
            entry["name"] = name
        if username and not entry.get("username"):
            entry["username"] = username
        days = entry.setdefault("days", [])
        if day not in days:
            days.append(day)
            daily[day]["new"] += 1
            is_new_today = True

    log_list.append({
        "user_id": user_id, "name": name or "", "username": username or "",
        "text": (text or "")[:500], "iso": now_iso(), "day": day,
        "is_new_today": is_new_today,
    })
    if len(log_list) > 300:
        STATE["incoming_log"] = log_list[-300:]

    save_json(STATE_FILE, STATE)
    return is_new_today


def stats_for_period(days: int) -> dict:
    daily = STATE.get("incoming_daily") or {}
    cutoff = datetime.now().date() - timedelta(days=days - 1)
    new_total = 0
    msg_total = 0
    for day, d in daily.items():
        try:
            dt = datetime.strptime(day, "%Y-%m-%d").date()
        except Exception:
            continue
        if dt >= cutoff:
            new_total += int(d.get("new", 0))
            msg_total += int(d.get("messages", 0))

    unique_users = set()
    cutoff_iso = (datetime.now() - timedelta(days=days)).isoformat(timespec="seconds")
    for uid, entry in (STATE.get("incoming_users") or {}).items():
        if entry.get("first_iso", "") >= cutoff_iso:
            unique_users.add(uid)
    return {"new": new_total, "messages": msg_total, "unique_users": len(unique_users)}


# ===========================================================================
#  Ротация текста
# ===========================================================================

def account_has_content(acc: dict) -> bool:
    return bool(acc.get("text") or acc.get("media_path") or acc.get("text_pool"))


def current_pool_item(acc: dict) -> dict:
    """Возвращает элемент пула для текущей отправки (учитывая rotate_every)."""
    pool = acc.get("text_pool") or []
    if not pool:
        # fallback на базовые поля
        return {
            "text": acc.get("text") or "",
            "text_entities": acc.get("text_entities") or [],
            "media_path": acc.get("media_path"),
            "media_type": acc.get("media_type"),
            "caption": acc.get("caption") or "",
            "caption_entities": acc.get("caption_entities") or [],
        }

    sent_total = int((acc.get("stats") or {}).get("sent", 0))
    rotate_every = int(acc.get("rotate_every", 17) or 17)
    if rotate_every < 1:
        rotate_every = 1
    idx = (sent_total // rotate_every) % len(pool)
    acc["pool_index"] = idx
    return pool[idx]


# ===========================================================================
#  Отправка в группы
# ===========================================================================

async def _send_media(c: Client, chat_id: int, media_path: str, media_type: str,
                      caption: str, caption_entities):
    kwargs = {"chat_id": chat_id}
    if caption:
        kwargs["caption"] = caption
        if caption_entities:
            kwargs["caption_entities"] = caption_entities

    if media_type == "photo":
        await c.send_photo(photo=media_path, **kwargs)
    elif media_type == "video":
        await c.send_video(video=media_path, **kwargs)
    elif media_type == "animation":
        await c.send_animation(animation=media_path, **kwargs)
    elif media_type == "document":
        await c.send_document(document=media_path, **kwargs)
    elif media_type == "audio":
        await c.send_audio(audio=media_path, **kwargs)
    elif media_type == "voice":
        await c.send_voice(voice=media_path, **kwargs)
    else:
        raise ValueError(f"unsupported media type: {media_type}")


async def _simulate_typing(c: Client, chat_id: int, base_sec: int) -> None:
    try:
        await c.send_chat_action(chat_id=chat_id, action=enums.ChatAction.TYPING)
    except Exception:
        pass
    try:
        extra = random.uniform(0.0, 2.0)
        total = max(0.0, float(base_sec) + extra)
        if total > 4.0:
            await asyncio.sleep(3.5)
            try:
                await c.send_chat_action(chat_id=chat_id, action=enums.ChatAction.TYPING)
            except Exception:
                pass
            await asyncio.sleep(total - 3.5)
        else:
            await asyncio.sleep(total)
    except Exception:
        pass


def _pick_pause_seconds(acc: dict, sent_count_in_round: int) -> float:
    """
    Рандомизация таймингов:
    - 85% короткие паузы delay_min..delay_max с рандомным множителем 1.0..1.6
    - 15% длинные паузы delay_max*2..delay_max*4
    - Каждые LONG_PAUSE_EVERY_MIN..MAX отправок — «человеческая» длинная пауза
    """
    lo = max(1, int(acc.get("delay_min", 5)))
    hi = max(lo, int(acc.get("delay_max", 15)))

    long_every = random.randint(LONG_PAUSE_EVERY_MIN, LONG_PAUSE_EVERY_MAX)
    if sent_count_in_round > 0 and sent_count_in_round % long_every == 0:
        return float(random.randint(LONG_PAUSE_MIN, LONG_PAUSE_MAX))

    r = random.random()
    if r < SHORT_PAUSE_PROB:
        base = random.randint(lo, hi)
        return float(base) * random.uniform(1.0, 1.6)
    if r < SHORT_PAUSE_PROB + 0.10:
        return float(random.randint(hi + 1, hi * 3 + 1))
    return float(random.randint(LONG_PAUSE_MIN, LONG_PAUSE_MAX))


async def send_post_for_account(acc_id: str, chat_id: int) -> None:
    c = user_clients.get(acc_id)
    if not c:
        raise RuntimeError("Клиент не подключён.")
    acc = get_account(acc_id)

    typing_sec = int(acc.get("typing_seconds", DEFAULT_TYPING_SECONDS) or 0)
    if typing_sec > 0:
        await _simulate_typing(c, chat_id, typing_sec)

    src_chat = acc.get("source_chat_id")
    src_msg = acc.get("source_message_id")
    if src_chat and src_msg:
        try:
            await c.copy_message(chat_id=chat_id,
                                  from_chat_id=int(src_chat),
                                  message_id=int(src_msg))
            return
        except Exception as e:
            log.warning(f"[{acc.get('name')}] copy_message упал, fallback: {e}")

    item = current_pool_item(acc)
    text = item.get("text") or ""
    text_entities = entities_from_json(item.get("text_entities"))
    media_path = item.get("media_path")
    media_type = item.get("media_type")
    caption = item.get("caption") or ""
    caption_entities = entities_from_json(item.get("caption_entities"))

    if media_path and media_type and os.path.exists(media_path):
        await _send_media(c, chat_id, media_path, media_type, caption, caption_entities)
        return

    if text:
        if text_entities:
            try:
                await c.send_message(chat_id=chat_id, text=text, entities=text_entities)
                return
            except Exception as e:
                log.warning(f"[{acc.get('name')}] send_message(entities) упал: {e}")
        try:
            await c.send_message(chat_id=chat_id, text=text,
                                 parse_mode=enums.ParseMode.MARKDOWN)
        except Exception:
            await c.send_message(chat_id=chat_id, text=text)
        return

    raise ValueError("Не задан ни источник, ни текст, ни медиа")


async def _store_from_message(acc: dict, message, acc_id: str) -> bool:
    """Сохраняет сообщение в 'пул' (или в базовые поля если пул пуст)."""
    msg_text = message.text or message.caption or ""
    msg_entities = message.entities or message.caption_entities or []
    entities_json = entities_to_json(msg_entities)

    media_kind = None
    ext = "bin"
    if message.photo:
        media_kind, ext = "photo", "jpg"
    elif message.video:
        media_kind, ext = "video", "mp4"
    elif message.animation:
        media_kind, ext = "animation", "mp4"
    elif message.document:
        name = (message.document.file_name or "file.bin")
        media_kind, ext = "document", (name.rsplit(".", 1)[-1] or "bin")
    elif message.audio:
        name = (message.audio.file_name or "audio.mp3")
        media_kind, ext = "audio", (name.rsplit(".", 1)[-1] or "mp3")
    elif message.voice:
        media_kind, ext = "voice", "ogg"

    if media_kind:
        path = os.path.join(MEDIA_DIR, f"media_{acc_id}_{int(time.time()*1000)}.{ext}")
        try:
            await message.download(file_name=path)
            return {
                "text": "",
                "text_entities": [],
                "media_path": path,
                "media_type": media_kind,
                "caption": msg_text,
                "caption_entities": entities_json,
            }
        except Exception as e:
            log.warning(f"[{acc.get('name')}] download media упал: {e}")

    return {
        "text": msg_text,
        "text_entities": entities_json,
        "media_path": None,
        "media_type": None,
        "caption": "",
        "caption_entities": [],
    }


async def _store_pool_item(acc: dict, item: dict):
    acc["text_pool"].append(item)
    if len(acc["text_pool"]) > 8:
        # Ограничим максимум 8 вариантов
        acc["text_pool"] = acc["text_pool"][-8:]
        acc["pool_index"] = 0
    save_json(STATE_FILE, STATE)


async def mailing_loop_for_account(acc_id: str):
    acc = get_account(acc_id)
    if not acc:
        return
    log.info(f"[{acc.get('name')}] рассылка запущена")
    while acc.get("running"):
        groups = list(acc.get("groups") or [])
        if not groups:
            await asyncio.sleep(60)
            continue
        sent = errors = 0
        sent_in_round = 0
        for g in groups:
            if not acc.get("running"):
                break
            gid = g.get("id")
            try:
                await send_post_for_account(acc_id, gid)
                sent += 1
                sent_in_round += 1
                acc["stats"]["sent"] = acc["stats"].get("sent", 0) + 1
            except FloodWait as fw:
                await asyncio.sleep(fw.value + 2)
                try:
                    await send_post_for_account(acc_id, gid)
                    sent += 1
                    sent_in_round += 1
                    acc["stats"]["sent"] = acc["stats"].get("sent", 0) + 1
                except Exception as e2:
                    errors += 1
                    acc["stats"]["errors"] = acc["stats"].get("errors", 0) + 1
                    mark_problem(acc, g, str(e2))
            except Exception as e:
                log.warning(f"[{acc.get('name')}] send error: {e}")
                errors += 1
                acc["stats"]["errors"] = acc["stats"].get("errors", 0) + 1
                mark_problem(acc, g, str(e))
            save_json(STATE_FILE, STATE)

            # Рандомизированная пауза
            pause = _pick_pause_seconds(acc, sent_in_round)
            try:
                await asyncio.sleep(pause)
            except Exception:
                await asyncio.sleep(5)

        acc["stats"]["rounds"] = acc["stats"].get("rounds", 0) + 1
        acc["stats"]["last_round"] = now_iso()
        STATE["global_stats"]["total_sent"] = STATE["global_stats"].get("total_sent", 0) + sent
        STATE["global_stats"]["total_errors"] = STATE["global_stats"].get("total_errors", 0) + errors
        save_json(STATE_FILE, STATE)

        if not acc.get("running"):
            break
        interval = int(acc.get("interval", 1800))
        acc["stats"]["next_round"] = (
            datetime.now() + timedelta(seconds=interval)).isoformat(timespec="seconds")
        save_json(STATE_FILE, STATE)
        try:
            await bot_client.send_message(
                CFG["admin_id"],
                f"✅ [{acc.get('name')}] Круг №{acc['stats']['rounds']}. "
                f"Отправлено {sent}, ошибок {errors}.")
        except Exception:
            pass
        remaining = interval
        while remaining > 0 and acc.get("running"):
            await asyncio.sleep(min(5, remaining))
            remaining -= 5
    log.info(f"[{acc.get('name')}] рассылка остановлена")


def start_mailing_for_account(acc_id: str):
    t = mailing_tasks.get(acc_id)
    if t and not t.done():
        return
    mailing_tasks[acc_id] = asyncio.create_task(mailing_loop_for_account(acc_id))


async def scan_groups_for_account(acc_id: str) -> tuple:
    c = user_clients.get(acc_id)
    if not c:
        return [], 0
    acc = get_account(acc_id)
    excluded_keys = {str(x.get("id")) for x in (acc.get("excluded") or [])}
    found = []
    skipped = 0
    excluded_hit = 0
    try:
        async for dialog in c.get_dialogs():
            chat = dialog.chat
            if not is_target_group(chat):
                skipped += 1
                continue
            if str(chat.id) in excluded_keys:
                excluded_hit += 1
                continue
            found.append(build_group_record(chat, manual=False))
    except FloodWait as e:
        await asyncio.sleep(e.value + 2)
    except Exception as e:
        log.warning(f"scan_groups: {e}")
    log.info(f"[{acc.get('name')}] scan: found={len(found)} "
             f"skipped={skipped} excluded={excluded_hit}")
    return found, skipped


# ---------------------------------------------------------------------------
# Массовая подписка
# ---------------------------------------------------------------------------

async def subscribe_one_group(client: Client, ref):
    try:
        chat = await client.join_chat(ref)
        if not is_target_group(chat):
            return "error", "не группа", None
        return "ok", None, chat
    except UserAlreadyParticipant:
        try:
            chat = await client.get_chat(ref)
            if not is_target_group(chat):
                return "error", "не группа", None
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
        stats = acc.setdefault("subscribe_stats",
                               {"subscribed": 0, "skipped": 0, "errors": 0, "started_at": None})
        if status in ("ok", "already"):
            stats["subscribed" if status == "ok" else "skipped"] += 1
            if chat:
                gid = chat.id
                exists = any(g["id"] == gid for g in (acc.get("groups") or []))
                ex = any(str(x.get("id")) == str(gid) for x in (acc.get("excluded") or []))
                if not exists and not ex:
                    acc.setdefault("groups", []).append(build_group_record(chat, manual=True))
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
    if not t or t.done():
        subscribe_tasks[acc_id] = asyncio.create_task(subscribe_loop(acc_id))


# ===========================================================================
#  Слушатель входящих ЛС с антиспамом
# ===========================================================================

def attach_incoming_listener(client: Client, acc_id: str):
    _incoming_ready[acc_id] = False

    @client.on_message(filters.private & ~filters.me, group=100)
    async def _on_incoming(userbot: Client, message):
        try:
            if not _incoming_ready.get(acc_id):
                return
            if message.outgoing:
                return
            if not message.from_user:
                return
            if message.chat is None or message.chat.type != ChatType.PRIVATE:
                return

            fu = message.from_user
            if getattr(fu, "is_bot", False):
                return
            if int(fu.id) == int(CFG.get("admin_id", 0)):
                return
            my_id = ME_IDS.get(acc_id)
            if my_id is not None and int(fu.id) == int(my_id):
                return

            key = (acc_id, int(message.id))
            if key in _handled_message_ids:
                return
            _handled_message_ids.add(key)
            if len(_handled_message_ids) > 5000:
                _handled_message_ids.clear()
                _handled_message_ids.add(key)

            now_ts = time.time()
            uid_key = str(fu.id)
            last = _last_notify_by_user.get(uid_key, 0)
            if now_ts - last < INCOMING_NOTIFY_COOLDOWN_SEC:
                text = message.text or message.caption or "<медиа>"
                name = (fu.first_name or "") + (f" {fu.last_name}" if fu.last_name else "")
                register_incoming(int(fu.id), name.strip(), fu.username or "", text)
                return
            _last_notify_by_user[uid_key] = now_ts

            text = message.text or message.caption or "<медиа>"
            name = (fu.first_name or "") + (f" {fu.last_name}" if fu.last_name else "")
            username = fu.username or ""
            is_new_today = register_incoming(int(fu.id), name.strip(), username, text)

            acc = get_account(acc_id) or {}
            acc_name = acc.get("name") or acc_id

            if username:
                profile = f"https://t.me/{username}"
            else:
                profile = f"tg://user?id={fu.id}"

            header = "🆕 **Новое ЛС!**" if is_new_today else "💬 Новое сообщение"
            try:
                await bot_client.send_message(
                    CFG["admin_id"],
                    f"{header}\n"
                    f"👤 {name or '—'}"
                    f"{' (@' + username + ')' if username else ''}\n"
                    f"🆔 `{fu.id}`\n"
                    f"📥 Аккаунт: **{acc_name}**\n"
                    f"🔗 [Открыть профиль]({profile})\n\n"
                    f"✉️ {(text or '')[:500]}",
                    parse_mode=enums.ParseMode.MARKDOWN,
                    disable_web_page_preview=True,
                )
            except Exception as e:
                log.warning(f"incoming notify: {e}")
        except Exception:
            log.exception("incoming listener")


async def _mark_incoming_ready(acc_id: str, delay: int = INCOMING_WARMUP_SEC):
    await asyncio.sleep(delay)
    _incoming_ready[acc_id] = True
    log.info(f"[{acc_id}] incoming listener ready (warmup={delay}s)")


# ===========================================================================
#  Логин
# ===========================================================================

async def start_userbot_clients():
    accounts = STATE.get("accounts") or []
    for acc in accounts:
        acc_id = acc["id"]
        proxy = acc.get("proxy") if CFG.get("proxy_enabled") else None
        session_str = acc.get("session_string")
        if session_str:
            ok, reason = validate_session_string(session_str)
            if ok:
                try:
                    c = make_client(session_str, proxy=proxy)
                    user_clients[acc_id] = c
                    client_locks[acc_id] = asyncio.Lock()
                    continue
                except Exception as e:
                    log.warning(f"session_string {acc_id}: {e}")
            else:
                log.warning(f"[{acc.get('name')}] session_string невалидна: {reason}")
                acc["session_string"] = ""
                save_json(STATE_FILE, STATE)
        session_name = acc.get("session_name") or f"userbot_{acc_id}"
        try:
            c = make_client(session_name, proxy=proxy)
            user_clients[acc_id] = c
            client_locks[acc_id] = asyncio.Lock()
        except Exception as e:
            log.warning(f"client {acc_id}: {e}")


async def try_start_existing_clients():
    accounts = STATE.get("accounts") or []
    for acc in accounts:
        acc_id = acc["id"]
        c = user_clients.get(acc_id)
        if not c:
            continue
        try:
            await asyncio.wait_for(c.start(), timeout=30)
            me = await c.get_me()
            if me:
                ME_IDS[acc_id] = me.id
                acc["user_id"] = me.id
                acc["username"] = me.username
                log.info(f"✅ [{acc.get('name')}] {me.first_name} id={me.id}")

                attach_incoming_listener(c, acc_id)
                asyncio.create_task(_mark_incoming_ready(acc_id))

                if acc.get("running"):
                    start_mailing_for_account(acc_id)
                if acc.get("subscribe_status") == "running":
                    start_subscribe(acc_id)
        except asyncio.TimeoutError:
            log.warning(f"[{acc.get('name')}] таймаут — сессия удалена")
            acc["session_string"] = ""
            save_json(STATE_FILE, STATE)
            try:
                c.disconnect_handler = None
                if getattr(c, "network_task", None):
                    c.network_task.cancel()
                await c.stop()
            except Exception:
                pass
            user_clients.pop(acc_id, None)
        except (AuthKeyUnregistered, AuthKeyDuplicated, UserDeactivated, SessionRevoked) as e:
            log.warning(f"[{acc.get('name')}] сессия недействительна: {e}")
            acc["session_string"] = ""
            save_json(STATE_FILE, STATE)
            try:
                await c.stop()
            except Exception:
                pass
            user_clients.pop(acc_id, None)
        except Exception as e:
            log.warning(f"[{acc.get('name')}] не залогинен: {e}")
            try:
                if not c.is_connected:
                    await c.connect()
            except Exception:
                pass
    save_json(STATE_FILE, STATE)


# ===========================================================================
#  Клавиатуры
# ===========================================================================

def main_menu_kb() -> InlineKeyboardMarkup:
    accounts = STATE.get("accounts") or []
    running_count = sum(1 for a in accounts if a.get("running"))
    subs_count = sum(1 for a in accounts if a.get("subscribe_status") == "running")
    subs_mark = f" 📥{subs_count}" if subs_count else ""
    px_mark = "🟢" if CFG.get("proxy_enabled") else "⚪️"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"📢 Рассылка ({running_count}/{len(accounts)})",
                              callback_data="accounts_menu")],
        [InlineKeyboardButton(f"📥 Массовая подписка{subs_mark}",
                              callback_data="sub_main")],
        [InlineKeyboardButton("📈 Отписи", callback_data="incoming_menu")],
        [InlineKeyboardButton(f"{px_mark} Прокси: "
                              f"{'вкл' if CFG.get('proxy_enabled') else 'выкл'}",
                              callback_data="proxy_global_menu")],
        [InlineKeyboardButton("📊 Статистика", callback_data="stats_menu")],
    ])


def accounts_menu_kb() -> InlineKeyboardMarkup:
    accounts = STATE.get("accounts") or []
    rows = []
    for acc in accounts:
        mark = "🟢" if acc.get("running") else "⚪️"
        sub = " 📥" if acc.get("subscribe_status") == "running" else ""
        main_mark = " ⭐" if acc.get("id") == STATE.get("main_account_id") else ""
        px = "🧦" if (CFG.get("proxy_enabled") and proxy_is_active_for_account(acc)) else ""
        name = (acc.get("name") or "—")[:25]
        rows.append([InlineKeyboardButton(f"{mark} {name}{main_mark}{sub}{px}",
                                          callback_data=f"acc_open:{acc['id']}")])
    rows.append([InlineKeyboardButton("➕ Добавить аккаунт", callback_data="acc_add")])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="menu")])
    return InlineKeyboardMarkup(rows)


def accounts_menu_text() -> str:
    accounts = STATE.get("accounts") or []
    main_id = STATE.get("main_account_id") or ""
    lines = ["📢 Рассылка по аккаунтам\n"]
    if CFG.get("proxy_enabled"):
        lines.append("🧦 Прокси: **глобально включён**\n")
    else:
        lines.append("🧦 Прокси: глобально **выключен**\n")
    if not accounts:
        lines.append("_Нет аккаунтов._")
    else:
        for i, acc in enumerate(accounts, 1):
            mark = "🟢" if acc.get("running") else "⚪️"
            main_mark = " ⭐" if acc.get("id") == main_id else ""
            sub_mark = " 📥" if acc.get("subscribe_status") == "running" else ""
            s = acc.get("stats") or {}
            src = "✅" if (acc.get("source_chat_id") and acc.get("source_message_id")) else "❌"
            has_txt = "🅿️" if acc.get("text_entities") else ""
            pool_n = len(acc.get("text_pool") or [])
            pool_s = f"🧩{pool_n}" if pool_n else ""
            px = ""
            if CFG.get("proxy_enabled") and proxy_is_active_for_account(acc):
                p = acc.get("proxy") or {}
                px = f" 🧦 {p.get('type')}://{p.get('host')}:{p.get('port')}"
            lines.append(
                f"{mark} **{i}. {acc.get('name', '?')}**{main_mark}{sub_mark}{pool_s}\n"
                f"   Групп: {len(acc.get('groups') or [])} | "
                f"Искл: {len(acc.get('excluded') or [])} | "
                f"Пробл: {len(acc.get('problem') or [])}\n"
                f"   Source: {src}{has_txt} | "
                f"Sent: {s.get('sent', 0)} | ❌{s.get('errors', 0)}{px}")
    return "\n".join(lines)


def account_kb(acc_id: str) -> InlineKeyboardMarkup:
    acc = get_account(acc_id)
    running = acc.get("running", False)
    is_main = acc.get("id") == STATE.get("main_account_id")
    sub_status = acc.get("subscribe_status", "idle")
    sub_queue_len = len(acc.get("subscribe_queue") or [])
    excl_len = len(acc.get("excluded") or [])
    prob_len = len(acc.get("problem") or [])
    pool_len = len(acc.get("text_pool") or [])
    proxy_on = CFG.get("proxy_enabled")
    proxy_set = proxy_is_active_for_account(acc)

    rows = [
        [InlineKeyboardButton("⏸ Стоп рассылку" if running else "🚀 Запустить рассылку",
                              callback_data=f"acc_toggle:{acc_id}")],
        [InlineKeyboardButton("📎 Источник (премиум)",
                              callback_data=f"acc_source:{acc_id}")],
        [InlineKeyboardButton(f"🧩 Пул текстов ({pool_len})",
                              callback_data=f"acc_pool:{acc_id}:0")],
        [InlineKeyboardButton(f"🔄 Ротация: каждые {acc.get('rotate_every', 17)}",
                              callback_data=f"acc_rotate:{acc_id}")],
        [InlineKeyboardButton("📝 Базовый текст", callback_data=f"acc_text:{acc_id}"),
         InlineKeyboardButton("🖼 Медиа", callback_data=f"acc_media:{acc_id}")],
        [InlineKeyboardButton("⏱ Тайминги", callback_data=f"acc_timing:{acc_id}")],
        [InlineKeyboardButton(
            f"🧦 Прокси: {'✅ ' + str(acc.get('proxy', {}).get('host')) if proxy_on and proxy_set else ('—' if proxy_on else 'глобально off')}",
            callback_data=f"acc_proxy:{acc_id}")],
        [InlineKeyboardButton("🔍 Скан групп", callback_data=f"acc_scan:{acc_id}")],
        [InlineKeyboardButton("🗂 Список групп",
                              callback_data=f"acc_grp_list:{acc_id}:0"),
         InlineKeyboardButton(f"🚫 Исключённые ({excl_len})",
                              callback_data=f"acc_grp_excl:{acc_id}:0")],
        [InlineKeyboardButton(f"⚠️ Проблемные ({prob_len})",
                              callback_data=f"acc_grp_prob:{acc_id}:0")],
        [InlineKeyboardButton("➕ Группу", callback_data=f"acc_addgrp:{acc_id}")],
    ]
    if sub_status == "running":
        rows.append([InlineKeyboardButton(f"⏸ Стоп подписки ({sub_queue_len})",
                                          callback_data=f"acc_sub_stop:{acc_id}")])
    else:
        rows.append([InlineKeyboardButton(
            f"📥 Подписка" + (f" ({sub_queue_len})" if sub_queue_len else ""),
            callback_data=f"acc_sub_menu:{acc_id}")])
    if not is_main:
        rows.append([InlineKeyboardButton("⭐ Основной",
                                          callback_data=f"acc_setmain:{acc_id}")])
    rows.append([InlineKeyboardButton("🗑 Удалить аккаунт",
                                       callback_data=f"acc_remove:{acc_id}")])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="accounts_menu")])
    return InlineKeyboardMarkup(rows)


def account_text(acc_id: str) -> str:
    acc = get_account(acc_id)
    if not acc:
        return "Не найден."
    s = acc.get("stats") or {}
    pool = acc.get("text_pool") or []
    txt = (acc.get("text") or "").strip()
    has_ent = "🅿️" if acc.get("text_entities") else ""
    media = acc.get("media_type")
    src_chat = acc.get("source_chat_id")
    src_msg = acc.get("source_message_id")
    if media:
        preview = f"[{media}] {(acc.get('caption') or '')[:80]}"
    elif txt:
        preview = txt[:150]
    else:
        preview = "—"
    is_main = "⭐" if acc.get("id") == STATE.get("main_account_id") else ""
    sub_status = acc.get("subscribe_status", "idle")
    sub_stats = acc.get("subscribe_stats") or {}
    has_string = "✅" if acc.get("session_string") else "❌"
    if src_chat and src_msg:
        src_line = f"✅ chat={src_chat} msg={src_msg}"
    else:
        src_line = "❌ не задан"

    proxy_line = "глобально выключен"
    if CFG.get("proxy_enabled"):
        if proxy_is_active_for_account(acc):
            p = acc.get("proxy") or {}
            proxy_line = f"✅ {p.get('type')}://{p.get('host')}:{p.get('port')}"
        else:
            proxy_line = "❌ не задан (без прокси)"
    return (
        f"📢 **{acc.get('name', '?')}** {is_main}\n\n"
        f"• Статус: {'🟢' if acc.get('running') else '⚪️'}\n"
        f"• Session: {has_string}\n"
        f"• Групп: {len(acc.get('groups') or [])} | "
        f"🚫 {len(acc.get('excluded') or [])} | "
        f"⚠️ {len(acc.get('problem') or [])}\n"
        f"• Пул текстов: {len(pool)} вариантов, ротация каждые {acc.get('rotate_every', 17)} отправок\n"
        f"• Интервал: {acc.get('interval', 1800)}с\n"
        f"• Задержка: {acc.get('delay_min', 5)}–{acc.get('delay_max', 15)}с (+рандом)\n"
        f"• Печатает: {acc.get('typing_seconds', DEFAULT_TYPING_SECONDS)}с\n"
        f"🧦 Прокси: {proxy_line}\n"
        f"📎 Источник (copy): {src_line}\n"
        f"📊 Sent: {s.get('sent', 0)} | Err: {s.get('errors', 0)} | Кругов: {s.get('rounds', 0)}\n"
        f"📥 Подписка: {sub_status} | ✅{sub_stats.get('subscribed', 0)}\n\n"
        f"📝 Базовый текст {has_ent}:\n{preview}"
    )


# ---------------- Пул текстов ----------------

def pool_kb(acc_id: str, page: int = 0) -> InlineKeyboardMarkup:
    acc = get_account(acc_id)
    pool = acc.get("text_pool") or []
    total = len(pool)
    per_page = TEXTS_PER_PAGE
    pages = max(1, (total + per_page - 1) // per_page)
    page = max(0, min(page, pages - 1))
    start = page * per_page
    end = start + per_page
    page_items = pool[start:end]

    rows = []
    for i, item in enumerate(page_items, 1):
        idx = start + i
        t = (item.get("text") or "")[:30]
        kind = "🖼" if item.get("media_type") else "📝"
        rows.append([InlineKeyboardButton(
            f"❌ {idx}. {kind} {t or '<медиа>'}",
            callback_data=f"acc_pool_del:{acc_id}:{start+i-1}:{page}")])

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅️", callback_data=f"acc_pool:{acc_id}:{page-1}"))
    nav.append(InlineKeyboardButton(f"{page+1}/{pages}", callback_data="noop"))
    if page < pages - 1:
        nav.append(InlineKeyboardButton("➡️", callback_data=f"acc_pool:{acc_id}:{page+1}"))
    if nav:
        rows.append(nav)

    rows.append([InlineKeyboardButton("➕ Добавить текст в пул",
                                       callback_data=f"acc_pool_add:{acc_id}")])
    if total:
        rows.append([InlineKeyboardButton("💣 Очистить пул",
                                           callback_data=f"acc_pool_clear:{acc_id}")])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data=f"acc_open:{acc_id}")])
    return InlineKeyboardMarkup(rows)


def pool_text(acc_id: str, page: int = 0) -> str:
    acc = get_account(acc_id)
    pool = acc.get("text_pool") or []
    total = len(pool)
    per_page = TEXTS_PER_PAGE
    pages = max(1, (total + per_page - 1) // per_page)
    page = max(0, min(page, pages - 1))
    start = page * per_page
    end = start + per_page
    page_items = pool[start:end]

    lines = [f"🧩 **Пул текстов** ({acc.get('name')}) — всего {total}\n",
             f"_Ротация: каждые {acc.get('rotate_every', 17)} отправок. "
             f"Страница {page+1}/{pages}._\n"]
    if not total:
        lines.append("_Пусто. Добавь до 8 вариантов — так текст не будет повторяться подряд._")
    else:
        for i, item in enumerate(page_items, 1):
            idx = start + i
            kind = "🖼" if item.get("media_type") else "📝"
            t = (item.get("text") or item.get("caption") or "")[:160]
            ent_n = len(item.get("text_entities") or item.get("caption_entities") or [])
            lines.append(f"**{idx}.** {kind} (entities: {ent_n})\n{t or '<медиа>'}\n")
    return "\n".join(lines)


def pool_clear_confirm_kb(acc_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Да, очистить", callback_data=f"acc_pool_clear_ok:{acc_id}")],
        [InlineKeyboardButton("❌ Отмена", callback_data=f"acc_pool:{acc_id}:0")],
    ])


# ---------------- Прокси ----------------

def proxy_global_kb() -> InlineKeyboardMarkup:
    on = bool(CFG.get("proxy_enabled"))
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(
            ("🟢 Включено" if on else "⚪️ Выключено") + " — нажми переключить",
            callback_data="proxy_global_toggle")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="menu")],
    ])


def proxy_global_text() -> str:
    on = bool(CFG.get("proxy_enabled"))
    accounts = STATE.get("accounts") or []
    with_proxy = sum(1 for a in accounts if proxy_is_active_for_account(a))
    return (
        "🧦 **Прокси — глобальный переключатель**\n\n"
        f"Сейчас: **{'ВКЛ' if on else 'ВЫКЛ'}**\n"
        f"Аккаунтов с заданным прокси: {with_proxy}/{len(accounts)}\n\n"
        "_Когда глобально выключено — прокси не применяется ни к одному аккаунту, "
        "даже если он задан в карточке._\n"
        "_Когда включено — каждый аккаунт подключается через свой прокси "
        "(если он задан, иначе напрямую)._"
    )


def account_proxy_text(acc_id: str) -> str:
    acc = get_account(acc_id)
    if not acc:
        return "Не найден."
    p = acc.get("proxy") or {}
    g_on = bool(CFG.get("proxy_enabled"))
    has = proxy_is_active_for_account(acc)
    if has:
        line = (f"✅ `{p.get('type')}://{p.get('host')}:{p.get('port')}`"
                + (f" (user: {p.get('username')})" if p.get('username') else ""))
    else:
        line = "❌ не задан (напрямую)"
    return (
        f"🧦 **Прокси аккаунта** {acc.get('name')}\n\n"
        f"Глобально: {'🟢 ВКЛ' if g_on else '⚪️ ВЫКЛ'}\n"
        f"Для этого аккаунта: {line}\n\n"
        "_Форматы:_\n"
        "`socks5://user:pass@host:port`\n"
        "`http://host:port`\n"
        "`mtproxy://secret@host:port`\n"
        "`host:port` (socks5 без пароля)"
    )


def account_proxy_kb(acc_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✏️ Задать прокси", callback_data=f"acc_proxy_set:{acc_id}")],
        [InlineKeyboardButton("🗑 Очистить", callback_data=f"acc_proxy_clear:{acc_id}")],
        [InlineKeyboardButton("⬅️ Назад", callback_data=f"acc_open:{acc_id}")],
    ])


# ---------------- Список групп / проблемные / исключённые ----------------

def _fmt_group_line(idx: int, g: dict) -> str:
    title = (g.get("title") or str(g.get("id")))[:40]
    gtype = (g.get("type") or "?").lower()
    link = g.get("link") or ""
    username = g.get("username") or ""
    link_part = f" — [ссылка]({link})" if link else ""
    uname_part = f" (@{username})" if username else ""
    return f"`{idx:>3}`. [{gtype}] **{title}**{uname_part} · `{g.get('id')}`{link_part}"


def groups_list_kb(acc_id: str, page: int = 0) -> InlineKeyboardMarkup:
    acc = get_account(acc_id)
    groups = acc.get("groups") or []
    total = len(groups)
    per_page = GROUPS_PER_PAGE
    pages = max(1, (total + per_page - 1) // per_page)
    page = max(0, min(page, pages - 1))
    start = page * per_page
    end = start + per_page
    page_groups = groups[start:end]

    rows = []
    for i, g in enumerate(page_groups, 1):
        idx = start + i
        title = (g.get("title") or str(g.get("id")))[:32]
        gtype = (g.get("type") or "").upper()
        icon = "👥" if gtype in ("GROUP", "SUPERGROUP") else "❓"
        rows.append([InlineKeyboardButton(
            f"{icon} {idx}. {title}",
            callback_data=f"acc_grp_del_one:{acc_id}:{g['id']}:{page}")])

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅️", callback_data=f"acc_grp_list:{acc_id}:{page-1}"))
    nav.append(InlineKeyboardButton(f"{page+1}/{pages}", callback_data="noop"))
    if page < pages - 1:
        nav.append(InlineKeyboardButton("➡️", callback_data=f"acc_grp_list:{acc_id}:{page+1}"))
    if nav:
        rows.append(nav)

    bulk = []
    if total:
        bulk.append(InlineKeyboardButton("✂️ Удалить по номерам",
                                         callback_data=f"acc_grp_del_idx:{acc_id}"))
        bulk.append(InlineKeyboardButton("💣 Очистить всё",
                                         callback_data=f"acc_grp_clear:{acc_id}"))
    if bulk:
        rows.append(bulk)

    rows.append([InlineKeyboardButton("➕ Добавить", callback_data=f"acc_addgrp:{acc_id}"),
                 InlineKeyboardButton("🔍 Скан", callback_data=f"acc_scan:{acc_id}")])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data=f"acc_open:{acc_id}")])
    return InlineKeyboardMarkup(rows)


def groups_list_text(acc_id: str, page: int = 0) -> str:
    acc = get_account(acc_id)
    groups = acc.get("groups") or []
    total = len(groups)
    per_page = GROUPS_PER_PAGE
    pages = max(1, (total + per_page - 1) // per_page)
    page = max(0, min(page, pages - 1))
    start = page * per_page
    end = start + per_page
    page_groups = groups[start:end]

    lines = [f"🗂 **Список групп** ({acc.get('name')}) — всего {total}\n",
             f"_Страница {page+1}/{pages}. Тап — удалить (в исключения)._"]
    if not total:
        lines.append("_Список пуст._")
    else:
        for i, g in enumerate(page_groups, 1):
            lines.append(_fmt_group_line(start + i, g))
    return "\n".join(lines)


def groups_clear_confirm_kb(acc_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Да, в исключения",
                              callback_data=f"acc_grp_clear_ok:{acc_id}")],
        [InlineKeyboardButton("❌ Отмена",
                              callback_data=f"acc_grp_list:{acc_id}:0")],
    ])


def excluded_kb(acc_id: str, page: int = 0) -> InlineKeyboardMarkup:
    acc = get_account(acc_id)
    items = acc.get("excluded") or []
    total = len(items)
    per_page = EXCLUDED_PER_PAGE
    pages = max(1, (total + per_page - 1) // per_page)
    page = max(0, min(page, pages - 1))
    start = page * per_page
    end = start + per_page
    page_items = items[start:end]

    rows = []
    for i, g in enumerate(page_items, 1):
        idx = start + i
        title = (g.get("title") or str(g.get("id")))[:32]
        rows.append([InlineKeyboardButton(
            f"♻️ {idx}. {title}",
            callback_data=f"acc_excl_restore:{acc_id}:{g['id']}:{page}")])

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅️", callback_data=f"acc_grp_excl:{acc_id}:{page-1}"))
    nav.append(InlineKeyboardButton(f"{page+1}/{pages}", callback_data="noop"))
    if page < pages - 1:
        nav.append(InlineKeyboardButton("➡️", callback_data=f"acc_grp_excl:{acc_id}:{page+1}"))
    if nav:
        rows.append(nav)

    if total:
        rows.append([InlineKeyboardButton("💣 Очистить исключения",
                                          callback_data=f"acc_excl_clear:{acc_id}")])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data=f"acc_open:{acc_id}")])
    return InlineKeyboardMarkup(rows)


def excluded_text(acc_id: str, page: int = 0) -> str:
    acc = get_account(acc_id)
    items = acc.get("excluded") or []
    total = len(items)
    per_page = EXCLUDED_PER_PAGE
    pages = max(1, (total + per_page - 1) // per_page)
    page = max(0, min(page, pages - 1))
    start = page * per_page
    end = start + per_page
    page_items = items[start:end]

    lines = [f"🚫 **Исключённые группы** ({acc.get('name')}) — всего {total}\n",
             "_Тап — вернуть в список._\n"]
    if not total:
        lines.append("_Пусто._")
    else:
        for i, g in enumerate(page_items, 1):
            lines.append(_fmt_group_line(start + i, g))
    return "\n".join(lines)


def problem_kb(acc_id: str, page: int = 0) -> InlineKeyboardMarkup:
    acc = get_account(acc_id)
    items = acc.get("problem") or []
    total = len(items)
    per_page = PROBLEM_PER_PAGE
    pages = max(1, (total + per_page - 1) // per_page)
    page = max(0, min(page, pages - 1))
    start = page * per_page
    end = start + per_page
    page_items = items[start:end]

    rows = []
    for i, g in enumerate(page_items, 1):
        idx = start + i
        title = (g.get("title") or str(g.get("id")))[:26]
        cnt = g.get("count", 1)
        rows.append([InlineKeyboardButton(
            f"✅ {idx}. {title} ({cnt}×)",
            callback_data=f"acc_prob_clear:{acc_id}:{g['id']}:{page}")])

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅️", callback_data=f"acc_grp_prob:{acc_id}:{page-1}"))
    nav.append(InlineKeyboardButton(f"{page+1}/{pages}", callback_data="noop"))
    if page < pages - 1:
        nav.append(InlineKeyboardButton("➡️", callback_data=f"acc_grp_prob:{acc_id}:{page+1}"))
    if nav:
        rows.append(nav)

    if total:
        rows.append([InlineKeyboardButton(
            "📤 Отправить в исключения",
            callback_data=f"acc_prob_to_excl:{acc_id}")])
        rows.append([InlineKeyboardButton(
            "💣 Очистить список проблем",
            callback_data=f"acc_prob_clear_all:{acc_id}")])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data=f"acc_open:{acc_id}")])
    return InlineKeyboardMarkup(rows)


def problem_text(acc_id: str, page: int = 0) -> str:
    acc = get_account(acc_id)
    items = acc.get("problem") or []
    total = len(items)
    per_page = PROBLEM_PER_PAGE
    pages = max(1, (total + per_page - 1) // per_page)
    page = max(0, min(page, pages - 1))
    start = page * per_page
    end = start + per_page
    page_items = items[start:end]

    lines = [f"⚠️ **Проблемные группы** ({acc.get('name')}) — всего {total}\n"]
    if not total:
        lines.append("_Пусто. Отлично!_")
    else:
        for i, g in enumerate(page_items, 1):
            idx = start + i
            err = (g.get("error") or "")[:80]
            cnt = g.get("count", 1)
            title = (g.get("title") or str(g.get("id")))[:40]
            link = g.get("link") or ""
            link_part = f" — [ссылка]({link})" if link else ""
            lines.append(
                f"`{idx:>3}`. ⚠️ **{title}** ({cnt}×)\n"
                f"      id: `{g.get('id')}`{link_part}\n"
                f"      _ошибка:_ {err}")
    return "\n".join(lines)


# ---------------- Отписи ----------------

def incoming_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📅 За сегодня", callback_data="incoming_day")],
        [InlineKeyboardButton("📆 За 7 дней", callback_data="incoming_week")],
        [InlineKeyboardButton("🗓 За 30 дней", callback_data="incoming_month")],
        [InlineKeyboardButton("🆕 Последние новые", callback_data="incoming_last:0")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="menu")],
    ])


def incoming_menu_text() -> str:
    day = stats_for_period(1)
    week = stats_for_period(7)
    month = stats_for_period(30)
    total_users = len(STATE.get("incoming_users") or {})
    return (
        "📈 **Отписи (входящие ЛС)**\n\n"
        f"• Сегодня: новых **{day['new']}**, сообщений {day['messages']}\n"
        f"• За 7 дней: новых **{week['new']}**, сообщений {week['messages']}\n"
        f"• За 30 дней: новых **{month['new']}**, сообщений {month['messages']}\n\n"
        f"👥 Всего уникальных: **{total_users}**"
    )


def incoming_last_kb(page: int = 0) -> InlineKeyboardMarkup:
    log_list = list(reversed(STATE.get("incoming_log") or []))
    total = len(log_list)
    per_page = INCOMING_LOG_PER_PAGE
    pages = max(1, (total + per_page - 1) // per_page)
    page = max(0, min(page, pages - 1))
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅️", callback_data=f"incoming_last:{page-1}"))
    nav.append(InlineKeyboardButton(f"{page+1}/{pages}", callback_data="noop"))
    if page < pages - 1:
        nav.append(InlineKeyboardButton("➡️", callback_data=f"incoming_last:{page+1}"))
    rows = []
    if nav:
        rows.append(nav)
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="incoming_menu")])
    return InlineKeyboardMarkup(rows)


def incoming_last_text(page: int = 0) -> str:
    log_list = list(reversed(STATE.get("incoming_log") or []))
    total = len(log_list)
    per_page = INCOMING_LOG_PER_PAGE
    pages = max(1, (total + per_page - 1) // per_page)
    page = max(0, min(page, pages - 1))
    start = page * per_page
    end = start + per_page
    page_items = log_list[start:end]

    lines = [f"🆕 **Последние входящие** — {total}\n",
             f"_Страница {page+1}/{pages}_\n"]
    if not page_items:
        lines.append("_Пока пусто._")
    else:
        for item in page_items:
            uname = item.get("username") or ""
            profile = f"https://t.me/{uname}" if uname else f"tg://user?id={item.get('user_id')}"
            mark = "🆕" if item.get("is_new_today") else "💬"
            lines.append(
                f"{mark} **{item.get('name') or '—'}**"
                f"{' (@' + uname + ')' if uname else ''} "
                f"· [профиль]({profile}) · `{item.get('user_id')}`\n"
                f"   _{item.get('iso')}_\n"
                f"   ✉️ {(item.get('text') or '')[:200]}")
    return "\n".join(lines)


# ---------------- Подписка ----------------

def subscribe_menu_kb(acc_id: str) -> InlineKeyboardMarkup:
    acc = get_account(acc_id)
    running = acc.get("subscribe_status") == "running"
    queue_len = len(acc.get("subscribe_queue") or [])
    rows = [
        [InlineKeyboardButton("📋 Загрузить список групп",
                              callback_data=f"acc_sub_load:{acc_id}")],
        [InlineKeyboardButton(f"⏱ Задержка: {acc.get('subscribe_delay_min', 40)}–"
                              f"{acc.get('subscribe_delay_max', 120)}с",
                              callback_data=f"acc_sub_delay:{acc_id}")],
    ]
    if running:
        rows.append([InlineKeyboardButton("⏸ Остановить",
                                           callback_data=f"acc_sub_stop:{acc_id}")])
    elif queue_len:
        rows.append([InlineKeyboardButton("▶️ Запустить",
                                           callback_data=f"acc_sub_start:{acc_id}")])
        rows.append([InlineKeyboardButton("🗑 Очистить очередь",
                                           callback_data=f"acc_sub_clear:{acc_id}")])
    else:
        rows.append([InlineKeyboardButton("— пусто —", callback_data="noop")])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data=f"acc_open:{acc_id}")])
    return InlineKeyboardMarkup(rows)


def subscribe_menu_text(acc_id: str) -> str:
    acc = get_account(acc_id)
    queue = acc.get("subscribe_queue") or []
    stats = acc.get("subscribe_stats") or {}
    status = acc.get("subscribe_status", "idle")
    preview = ""
    if queue:
        preview = "\n\nСледующие 5:\n"
        for i, ref in enumerate(queue[:5], 1):
            preview += f"{i}. `{ref}`\n"
        if len(queue) > 5:
            preview += f"…и ещё {len(queue) - 5}"
    return (
        f"📥 **Массовая подписка**\nАккаунт: **{acc.get('name')}**\n\n"
        f"• Статус: {status}\n"
        f"• В очереди: {len(queue)}\n"
        f"• Задержка: {acc.get('subscribe_delay_min', 40)}–"
        f"{acc.get('subscribe_delay_max', 120)}с\n\n"
        f"📊 Подписался: {stats.get('subscribed', 0)} | "
        f"Был: {stats.get('skipped', 0)} | ❌{stats.get('errors', 0)}"
        + preview
    )


def sub_main_kb() -> InlineKeyboardMarkup:
    accounts = STATE.get("accounts") or []
    rows = []
    for acc in accounts:
        sub_status = acc.get("subscribe_status", "idle")
        status_icon = {"running": "🟢", "paused": "⏸", "done": "✅"}.get(sub_status, "⚪️")
        queue_len = len(acc.get("subscribe_queue") or [])
        queue_mark = f" ({queue_len})" if queue_len else ""
        name = (acc.get("name") or "—")[:25]
        rows.append([InlineKeyboardButton(f"{status_icon} {name}{queue_mark}",
                                          callback_data=f"acc_sub_menu:{acc['id']}")])
    if not accounts:
        rows.append([InlineKeyboardButton("— нет аккаунтов —", callback_data="noop")])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="menu")])
    return InlineKeyboardMarkup(rows)


def sub_main_text() -> str:
    accounts = STATE.get("accounts") or []
    lines = ["📥 **Массовая подписка**\n"]
    if not accounts:
        lines.append("⚠️ Сначала добавь аккаунт.")
    else:
        for i, acc in enumerate(accounts, 1):
            s = acc.get("subscribe_stats") or {}
            q = len(acc.get("subscribe_queue") or [])
            lines.append(f"{i}. **{acc.get('name', '?')}** — Q:{q} | "
                         f"✅{s.get('subscribed', 0)} ⏭{s.get('skipped', 0)} ❌{s.get('errors', 0)}")
    return "\n".join(lines)


def stats_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔄 Обновить", callback_data="stats_menu")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="menu")],
    ])


def stats_menu_text() -> str:
    lines = ["📊 Статистика\n"]
    accounts = STATE.get("accounts") or []
    total_sent = 0
    total_err = 0
    for acc in accounts:
        s = acc.get("stats") or {}
        total_sent += s.get("sent", 0)
        total_err += s.get("errors", 0)
        lines.append(f"📢 {acc.get('name', '?')}: 🟢{s.get('sent', 0)} | ❌{s.get('errors', 0)}")
    lines.append(f"\nИтого: 🟢{total_sent} | ❌{total_err}")
    return "\n".join(lines)


# ===========================================================================
#  PENDING
# ===========================================================================

async def _handle_pending(client, message, uid, act):
    action = act.get("action")
    raw_text = message.text or ""
    text = raw_text.strip()

    if action == "acc_source_forward":
        acc_id = act.get("acc_id")
        acc = get_account(acc_id)
        if not acc:
            pending.pop(uid, None)
            return
        try:
            item = await _store_from_message(acc, message, acc_id)
            acc["source_chat_id"] = None
            acc["source_message_id"] = None
            if not item:
                pending.pop(uid, None)
                await message.reply("❌ Не удалось сохранить.")
                return
            await _store_pool_item(acc, item)
            pending.pop(uid, None)
            try:
                await message.react(emoji="👍")
            except Exception:
                pass
            await message.reply(
                f"✅ Добавлено в пул. Всего вариантов: {len(acc.get('text_pool') or [])}.",
                reply_markup=account_kb(acc_id))
        except Exception as e:
            log.exception("acc_source_forward")
            pending.pop(uid, None)
            await message.reply(f"❌ {e}")
        return

    if action == "acc_pool_add":
        acc_id = act.get("acc_id")
        acc = get_account(acc_id)
        if not acc:
            pending.pop(uid, None)
            return
        try:
            item = await _store_from_message(acc, message, acc_id)
            if not item:
                pending.pop(uid, None)
                await message.reply("❌ Пустое сообщение.")
                return
            await _store_pool_item(acc, item)
            pending.pop(uid, None)
            try:
                await message.react(emoji="👍")
            except Exception:
                pass
            await message.reply(
                f"✅ В пуле теперь {len(acc.get('text_pool') or [])} вариантов.",
                reply_markup=pool_kb(acc_id, 0))
        except Exception as e:
            log.exception("acc_pool_add")
            pending.pop(uid, None)
            await message.reply(f"❌ {e}")
        return

    if action == "acc_rotate_set":
        acc_id = act.get("acc_id")
        acc = get_account(acc_id)
        try:
            v = int(text)
            if v < 5: raise ValueError("мин. 5")
            if v > 100: raise ValueError("макс 100")
            acc["rotate_every"] = v
            save_json(STATE_FILE, STATE)
            await message.reply(
                f"✅ Ротация: каждые {v} отправок.",
                reply_markup=account_kb(acc_id))
        except Exception as e:
            await message.reply(f"❌ {e}")
        return

    if action == "acc_proxy_set":
        acc_id = act.get("acc_id")
        acc = get_account(acc_id)
        if not acc:
            pending.pop(uid, None)
            return
        try:
            p = parse_proxy_line(text)
        except Exception as e:
            await message.reply(
                f"❌ Не понял: {e}\n"
                f"Пример: `socks5://user:pass@1.2.3.4:1080`",
                parse_mode=enums.ParseMode.MARKDOWN)
            return
        acc["proxy"] = p
        save_json(STATE_FILE, STATE)
        pending.pop(uid, None)
        await message.reply(
            "✅ Прокси сохранён.\n"
            "⚠️ Чтобы применился — **перезапусти аккаунт**: /panel → "
            "Аккаунты → аккаунт → Удалить, затем добавь сессию снова. "
            "Или перезапусти сервис в Amvera.",
            reply_markup=account_kb(acc_id))
        return

    if action == "add_session_string":
        pending.pop(uid, None)
        cleaned = re.sub(r"\s+", "", text).strip('"').strip("'").strip("`")
        if not cleaned:
            await message.reply("❌ Пусто.")
            return
        ok, reason = validate_session_string(cleaned)
        if not ok:
            await message.reply(
                f"❌ Не похоже на session_string: **{reason}**\nДлина: {len(cleaned)}",
                parse_mode=enums.ParseMode.MARKDOWN)
            return
        await message.reply("⏳ Подключаюсь…")
        try:
            new_c = make_client(cleaned)
            await asyncio.wait_for(new_c.start(), timeout=30)
            me = await new_c.get_me()
            if not me:
                await new_c.stop()
                await message.reply("❌ Нет данных.")
                return
            name = f"{me.first_name}" + (f" @{me.username}" if me.username else "")
            sname = f"string_session_{int(datetime.now().timestamp())}"
            acc = add_account(name, sname)
            acc["user_id"] = me.id
            acc["username"] = me.username
            acc["session_string"] = cleaned
            save_json(STATE_FILE, STATE)
            user_clients[acc["id"]] = new_c
            ME_IDS[acc["id"]] = me.id
            attach_incoming_listener(new_c, acc["id"])
            asyncio.create_task(_mark_incoming_ready(acc["id"]))
            await message.reply(f"✅ **{name}** (id={me.id}).",
                                reply_markup=main_menu_kb())
        except (AuthKeyUnregistered, AuthKeyDuplicated, UserDeactivated, SessionRevoked) as e:
            await message.reply(f"❌ Сессия недействительна: `{e}`",
                                parse_mode=enums.ParseMode.MARKDOWN)
        except Exception as e:
            await message.reply(f"❌ `{e}`", parse_mode=enums.ParseMode.MARKDOWN)
        return

    if action == "acc_source_manual":
        pending.pop(uid, None)
        acc_id = act["acc_id"]
        acc = get_account(acc_id)
        if not acc:
            return
        parts = [p.strip() for p in text.split("\n") if p.strip()]
        if len(parts) < 2:
            await message.reply("❌ Нужно 2 строки: chat_id и message_id.")
            return
        try:
            chat_id = int(parts[0]); msg_id = int(parts[1])
        except ValueError:
            await message.reply("❌ Только числа.")
            return
        acc["source_chat_id"] = chat_id
        acc["source_message_id"] = msg_id
        save_json(STATE_FILE, STATE)
        try:
            await message.react(emoji="👍")
        except Exception:
            pass
        return

    if action == "acc_add_phone":
        if not text.startswith("+") or len(text) < 8:
            await message.reply("❌ Формат: +79991234567")
            return
        pending.pop(uid, None)
        sname = f"userbot_{int(datetime.now().timestamp())}"
        session_path = os.path.join(DATA_DIR, sname)
        try:
            new_c = make_client(session_path)
            await new_c.connect()
            try:
                sent = await new_c.send_code(text, force_sms=True)
            except TypeError:
                sent = await new_c.send_code(text)
        except Exception as e:
            await message.reply(f"❌ send_code: {e}")
            return
        pending[uid] = {"action": "acc_add_code", "phone": text,
                        "hash": sent.phone_code_hash, "session_name": sname,
                        "client": new_c}
        await message.reply("📩 Код отправлен:")
        return

    if action == "acc_add_code":
        pending.pop(uid, None)
        phone = act["phone"]; hash_ = act["hash"]
        sname = act["session_name"]; new_c: Client = act["client"]
        code = text.replace(" ", "")
        try:
            await new_c.sign_in(phone_number=phone,
                                phone_code_hash=hash_, phone_code=code)
        except SessionPasswordNeeded:
            pending[uid] = {"action": "acc_add_password", "phone": phone,
                            "session_name": sname, "client": new_c}
            await message.reply("🔐 Пароль 2FA:")
            return
        except PhoneCodeInvalid:
            pending[uid] = act
            await message.reply("❌ Неверный код. Ещё раз:")
            return
        except PhoneCodeExpired:
            await message.reply("⚠️ Код истёк.")
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
        attach_incoming_listener(new_c, acc["id"])
        asyncio.create_task(_mark_incoming_ready(acc["id"]))
        await message.reply(f"✅ **{name}**", reply_markup=main_menu_kb())
        return

    if action == "acc_add_password":
        pending.pop(uid, None)
        phone = act["phone"]; sname = act["session_name"]
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
        attach_incoming_listener(new_c, acc["id"])
        asyncio.create_task(_mark_incoming_ready(acc["id"]))
        await message.reply(f"✅ **{name}**", reply_markup=main_menu_kb())
        return

    if action == "acc_sub_load":
        pending.pop(uid, None)
        acc_id = act["acc_id"]
        acc = get_account(acc_id)
        if not acc:
            return
        raw_file = ""
        if message.document:
            try:
                path = await message.download()
                with open(path, "r", encoding="utf-8") as f:
                    raw_file = f.read()
                try:
                    os.remove(path)
                except Exception:
                    pass
            except Exception as e:
                await message.reply(f"❌ {e}")
                return
        else:
            raw_file = message.text or ""
        if not raw_file.strip():
            await message.reply("Пусто.")
            return
        groups = parse_groups_from_text(raw_file)
        if not groups:
            await message.reply("❌ Не распарсил ни одной группы.")
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
        try:
            await message.react(emoji="👍")
        except Exception:
            pass
        await message.reply(
            f"✅ Добавлено в очередь: {added}. Всего: {len(existing_q)}.",
            reply_markup=subscribe_menu_kb(acc_id))
           
return

    if action == "acc_sub_delay":
        pending.pop(uid, None)
        acc_id = act["acc_id"]
        acc = get_account(acc_id)
        if not acc:
            return
        text_clean = text.replace("-", " ").replace(",", " ")
        parts = [p for p in text_clean.split() if p.strip()]
        if len(parts) < 2:
            await message.reply("❌ Нужно 2 числа (мин и макс).")
            return
        try:
            lo = int(parts[0]); hi = int(parts[1])
            if lo < 5: raise ValueError("мин. 5")
            if hi < lo: raise ValueError("макс >= мин")
            if hi > 1800: raise ValueError("макс 1800")
        except Exception as e:
            await message.reply(f"❌ {e}")
            return
        acc["subscribe_delay_min"] = lo
        acc["subscribe_delay_max"] = hi
        save_json(STATE_FILE, STATE)
        try:
            await message.react(emoji="👍")
        except Exception:
            pass
        await message.reply(f"✅ Задержка: {lo}–{hi}с.",
                            reply_markup=subscribe_menu_kb(acc_id))
        return

    if action == "acc_text":
        pending.pop(uid, None)
        acc_id = act["acc_id"]
        acc = get_account(acc_id)
        if not acc:
            return
        if not raw_text:
            await message.reply("❌ Пустой текст.")
            return
        acc["text"] = raw_text
        acc["text_entities"] = entities_to_json(message.entities or [])
        acc["media_path"] = None
        acc["media_type"] = None
        acc["caption"] = ""
        acc["caption_entities"] = []
        acc["source_chat_id"] = None
        acc["source_message_id"] = None
        save_json(STATE_FILE, STATE)
        try:
            await message.react(emoji="👍")
        except Exception:
            pass
        await message.reply(
            f"✅ Базовый текст сохранён (entities: {len(acc['text_entities'])}).",
            reply_markup=account_kb(acc_id))
        return

    if action == "acc_media":
        pending.pop(uid, None)
        acc_id = act["acc_id"]
        acc = get_account(acc_id)
        if not acc:
            return
        try:
            item = await _store_from_message(acc, message, acc_id)
        except Exception as e:
            log.exception("acc_media")
            await message.reply(f"❌ {e}")
            return
        if not item:
            await message.reply("❌ Не удалось сохранить медиа.")
            return
        acc["media_path"] = item.get("media_path")
        acc["media_type"] = item.get("media_type")
        acc["caption"] = item.get("caption") or ""
        acc["caption_entities"] = item.get("caption_entities") or []
        acc["text"] = ""
        acc["text_entities"] = []
        acc["source_chat_id"] = None
        acc["source_message_id"] = None
        save_json(STATE_FILE, STATE)
        try:
            await message.react(emoji="👍")
        except Exception:
            pass
        await message.reply(
            f"✅ Базовое медиа сохранено: {acc.get('media_type')}.",
            reply_markup=account_kb(acc_id))
        return

    if action == "acc_setint":
        pending.pop(uid, None)
        acc = get_account(act["acc_id"])
        try:
            v = int(text)
            if v < 60: raise ValueError("мин. 60")
            acc["interval"] = v
            save_json(STATE_FILE, STATE)
            await message.reply(f"✅ Интервал: {v}с.", reply_markup=account_kb(acc["id"]))
        except Exception as e:
            await message.reply(f"❌ {e}")
        return

    if action == "acc_setdmin":
        pending.pop(uid, None)
        acc = get_account(act["acc_id"])
        try:
            v = int(text)
            if v < 1: raise ValueError("мин. 1")
            acc["delay_min"] = v
            if acc["delay_max"] < v:
                acc["delay_max"] = v
            save_json(STATE_FILE, STATE)
            await message.reply(f"✅ Мин: {v}с.", reply_markup=account_kb(acc["id"]))
        except Exception as e:
            await message.reply(f"❌ {e}")
        return

    if action == "acc_setdmax":
        pending.pop(uid, None)
        acc = get_account(act["acc_id"])
        try:
            v = int(text)
            if v < acc["delay_min"]:
                raise ValueError(f">= {acc['delay_min']}")
            acc["delay_max"] = v
            save_json(STATE_FILE, STATE)
            await message.reply(f"✅ Макс: {v}с.", reply_markup=account_kb(acc["id"]))
        except Exception as e:
            await message.reply(f"❌ {e}")
        return

    if action == "acc_settyping":
        pending.pop(uid, None)
        acc = get_account(act["acc_id"])
        try:
            v = int(text)
            if v < 1 or v > 60: raise ValueError("1–60")
            acc["typing_seconds"] = v
            save_json(STATE_FILE, STATE)
            await message.reply(
                f"✅ «Печатает…»: {v}с (+0–2с рандома).",
                reply_markup=account_kb(acc["id"]))
        except Exception as e:
            await message.reply(f"❌ {e}")
        return

    if action == "acc_addgrp":
        pending.pop(uid, None)
        acc_id = act["acc_id"]
        acc = get_account(acc_id)
        c = user_clients.get(acc_id)
        if not acc or not c:
            await message.reply("❌ Аккаунт не подключён.")
            return
        ref = parse_chat_ref(text)
        if ref is None:
            await message.reply("❌ Неверная ссылка.")
            return
        try:
            chat = await c.get_chat(ref)
        except Exception as e:
            await message.reply(f"❌ {e}")
            return
        if not is_target_group(chat):
            await message.reply(
                f"❌ Это не группа (тип: `{getattr(chat.type, 'name', chat.type)}`).",
                parse_mode=enums.ParseMode.MARKDOWN)
            return
        gid = chat.id
        if any(g["id"] == gid for g in acc["groups"]):
            await message.reply("⚠️ Уже в списке.")
            return
        remove_from_excluded(acc, gid)
        acc["groups"].append(build_group_record(chat, manual=True))
        save_json(STATE_FILE, STATE)
        await message.reply(f"✅ Добавлено: {chat.title}",
                            reply_markup=account_kb(acc_id))
        return

    if action == "acc_grp_del_idx":
        pending.pop(uid, None)
        acc_id = act["acc_id"]
        acc = get_account(acc_id)
        if not acc:
            return
        idx_list = parse_index_list(text)
        if not idx_list:
            await message.reply("❌ Не распарсил. Пример: `1,3,5-7`",
                                parse_mode=enums.ParseMode.MARKDOWN)
            return
        groups = acc.get("groups") or []
        to_remove = set()
        for i in idx_list:
            if 1 <= i <= len(groups):
                to_remove.add(i - 1)
        removed_records = [groups[i] for i in to_remove]
        new_groups = [g for i, g in enumerate(groups) if i not in to_remove]
        acc["groups"] = new_groups
        for g in removed_records:
            add_to_excluded(acc, g)
        save_json(STATE_FILE, STATE)
        try:
            await message.react(emoji="👍")
        except Exception:
            pass
        await message.reply(
            f"✅ Удалено: {len(removed_records)} "
            f"(→ в исключения). Осталось: {len(new_groups)}.",
            reply_markup=groups_list_kb(acc_id, 0))
        return

    pending.pop(uid, None)
    await message.reply("⚠️ Действие устарело, начни заново через /panel.")


# ===========================================================================
#  РЕГИСТРАЦИЯ ХЕНДЛЕРОВ БОТА
# ===========================================================================

def register_handlers(bot: Client) -> None:
    admin_id = int(CFG["admin_id"])
    log.info(f"Регистрирую хендлеры для admin_id={admin_id}")

    @bot.on_message(filters.private & filters.user(admin_id))
    async def on_admin_message(client, message):
        try:
            uid = message.from_user.id
            text = (message.text or message.caption or "").strip()
            cmd = text.split(maxsplit=1)[0].lower() if text.startswith("/") else ""
            log.info(f"[ADMIN] uid={uid} cmd={cmd!r} pending="
                     f"{pending.get(uid, {}).get('action')} authed={uid in authed}")

            if cmd == "/start":
                if uid not in authed:
                    await message.reply("🔒 Для доступа отправь `/auth <PIN>`",
                                        parse_mode=enums.ParseMode.MARKDOWN)
                    return
                await message.reply("🎛 Панель:", reply_markup=main_menu_kb())
                return

            if cmd == "/auth":
                arg = text[len("/auth"):].strip()
                if arg != str(CFG.get("pin", DEFAULT_PIN)):
                    await message.reply("❌ Неверный PIN.")
                    return
                authed.add(uid)
                pending.pop(uid, None)
                await message.reply("✅ Авторизован.", reply_markup=main_menu_kb())
                return

            if cmd == "/cancel":
                pending.pop(uid, None)
                await message.reply("Отменено.", reply_markup=main_menu_kb())
                return

            if cmd == "/panel":
                if uid not in authed:
                    await message.reply("🔒 Сначала `/auth <PIN>`",
                                        parse_mode=enums.ParseMode.MARKDOWN)
                    return
                await message.reply("🎛 Панель:", reply_markup=main_menu_kb())
                return

            if cmd == "/addsession":
                if uid not in authed:
                    await message.reply("🔒 Сначала `/auth <PIN>`",
                                        parse_mode=enums.ParseMode.MARKDOWN)
                    return
                pending[uid] = {"action": "add_session_string"}
                await message.reply("📝 Отправь строку сессии одним сообщением.\n/cancel")
                return

            if cmd.startswith("/"):
                await message.reply(f"❓ Неизвестная команда: {cmd}\n"
                                    f"Доступно: /start /auth /panel /cancel /addsession")
                return

            if uid not in authed:
                await message.reply("🔒 Сначала авторизуйся: `/auth <PIN>`",
                                    parse_mode=enums.ParseMode.MARKDOWN)
                return

            act = pending.get(uid)
            if act:
                await _handle_pending(client, message, uid, act)
                return

            await message.reply(
                "🤔 Не понял, что делать с этим сообщением.\n"
                "Открой панель: /panel",
                reply_markup=main_menu_kb())
        except Exception as e:
            log.exception("on_admin_message")
            try:
                await message.reply(f"❌ Ошибка: {e}")
            except Exception:
                pass

    @bot.on_callback_query()
    async def on_cb(client, cb):
        uid = cb.from_user.id
        if uid != admin_id:
            await cb.answer()
            return
        if uid not in authed:
            await cb.answer("🔒 /auth <PIN>", show_alert=False)
            return
        data = cb.data or ""
        try:
            if data == "menu":
                await cb.message.edit_text("🎛 Панель:", reply_markup=main_menu_kb())
            elif data == "noop":
                await cb.answer()
            elif data == "accounts_menu":
                await cb.message.edit_text(accounts_menu_text(), reply_markup=accounts_menu_kb())
            elif data == "sub_main":
                await cb.message.edit_text(sub_main_text(), reply_markup=sub_main_kb())
            elif data == "stats_menu":
                await cb.message.edit_text(stats_menu_text(), reply_markup=stats_menu_kb())

            # ================= ПРОКСИ (глобально) =================
            elif data == "proxy_global_menu":
                await cb.message.edit_text(proxy_global_text(),
                                            reply_markup=proxy_global_kb())
            elif data == "proxy_global_toggle":
                CFG["proxy_enabled"] = not bool(CFG.get("proxy_enabled"))
                save_json(CONFIG_FILE, CFG)
                await cb.answer(f"Прокси: {'ВКЛ' if CFG['proxy_enabled'] else 'ВЫКЛ'}")
                await cb.message.edit_text(proxy_global_text(),
                                            reply_markup=proxy_global_kb())

            # ================= ОТПИСИ =================
            elif data == "incoming_menu":
                await cb.message.edit_text(incoming_menu_text(),
                                            reply_markup=incoming_menu_kb())
            elif data == "incoming_day":
                s = stats_for_period(1)
                await cb.answer(f"Сегодня: {s['new']} новых, {s['messages']} сообщений",
                                show_alert=True)
            elif data == "incoming_week":
                s = stats_for_period(7)
                await cb.answer(f"7 дней: {s['new']} новых, {s['messages']} сообщений",
                                show_alert=True)
            elif data == "incoming_month":
                s = stats_for_period(30)
                await cb.answer(f"30 дней: {s['new']} новых, {s['messages']} сообщений",
                                show_alert=True)
            elif data.startswith("incoming_last:"):
                try:
                    page = int(data.split(":", 1)[1])
                except ValueError:
                    page = 0
                await cb.message.edit_text(incoming_last_text(page),
                                            reply_markup=incoming_last_kb(page))

            elif data == "acc_add":
                pending[uid] = {"action": "acc_add_phone"}
                await cb.message.edit_text(
                    "➕ Номер в формате `+79991234567`.\n/cancel",
                    parse_mode=enums.ParseMode.MARKDOWN)

            # ================= КАРТОЧКА АККАУНТА =================
            elif data.startswith("acc_open:"):
                acc_id = data.split(":", 1)[1]
                if not get_account(acc_id):
                    await cb.answer()
                    return
                await cb.message.edit_text(account_text(acc_id), reply_markup=account_kb(acc_id))
            elif data.startswith("acc_toggle:"):
                acc_id = data.split(":", 1)[1]
                acc = get_account(acc_id)
                if not acc or not user_clients.get(acc_id):
                    await cb.answer()
                    return
                if acc.get("running"):
                    acc["running"] = False
                else:
                    if not acc.get("groups"):
                        await cb.answer()
                        return
                    has_src = bool(acc.get("source_chat_id") and acc.get("source_message_id"))
                    if not (has_src or account_has_content(acc)):
                        await cb.answer()
                        return
                    acc["running"] = True
                    start_mailing_for_account(acc_id)
                save_json(STATE_FILE, STATE)
                await cb.answer()
                await cb.message.edit_text(account_text(acc_id), reply_markup=account_kb(acc_id))

            # ================= ПРОКСИ АККАУНТА =================
            elif data.startswith("acc_proxy:"):
                acc_id = data.split(":", 1)[1]
                if not get_account(acc_id):
                    await cb.answer()
                    return
                await cb.message.edit_text(account_proxy_text(acc_id),
                                            reply_markup=account_proxy_kb(acc_id))
            elif data.startswith("acc_proxy_set:"):
                acc_id = data.split(":", 1)[1]
                pending[uid] = {"action": "acc_proxy_set", "acc_id": acc_id}
                await cb.message.edit_text(
                    "✏️ Отправь прокси одной строкой.\n"
                    "Примеры:\n"
                    "`socks5://user:pass@1.2.3.4:1080`\n"
                    "`http://1.2.3.4:8080`\n"
                    "`mtproxy://secret@1.2.3.4:443`\n"
                    "`1.2.3.4:1080`\n/cancel",
                    parse_mode=enums.ParseMode.MARKDOWN)
            elif data.startswith("acc_proxy_clear:"):
                acc_id = data.split(":", 1)[1]
                acc = get_account(acc_id)
                if acc:
                    acc["proxy"] = {"enabled": False, "type": "socks5",
                                    "host": "", "port": 0,
                                    "username": "", "password": "", "secret": ""}
                    save_json(STATE_FILE, STATE)
                await cb.answer("Прокси очищен")
                await cb.message.edit_text(account_proxy_text(acc_id),
                                            reply_markup=account_proxy_kb(acc_id))

            # ================= ПУЛ ТЕКСТОВ =================
            elif data.startswith("acc_pool:"):
                parts = data.split(":")
                acc_id = parts[1]
                page = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else 0
                if not get_account(acc_id):
                    await cb.answer()
                    return
                await cb.message.edit_text(pool_text(acc_id, page),
                                            reply_markup=pool_kb(acc_id, page))
            elif data.startswith("acc_pool_add:"):
                acc_id = data.split(":", 1)[1]
                acc = get_account(acc_id)
                if not acc:
                    await cb.answer()
                    return
                if len(acc.get("text_pool") or []) >= 8:
                    await cb.answer("Максимум 8 вариантов", show_alert=True)
                    return
                pending[uid] = {"action": "acc_pool_add", "acc_id": acc_id}
                await cb.message.edit_text(
                    "➕ Отправь текст (можно с премиум-эмодзи), "
                    "или фото/видео с подписью — пойдёт в пул.\n/cancel")
            elif data.startswith("acc_pool_del:"):
                parts = data.split(":")
                if len(parts) < 4:
                    await cb.answer()
                    return
                acc_id = parts[1]
                try:
                    idx = int(parts[2]); page = int(parts[3])
                except ValueError:
                    await cb.answer()
                    return
                acc = get_account(acc_id)
                if not acc:
                    await cb.answer()
                    return
                pool = acc.get("text_pool") or []
                if 0 <= idx < len(pool):
                    pool.pop(idx)
                    save_json(STATE_FILE, STATE)
                total = len(pool)
                pages = max(1, (total + TEXTS_PER_PAGE - 1) // TEXTS_PER_PAGE)
                page = max(0, min(page, pages - 1))
                await cb.answer("Удалено")
                await cb.message.edit_text(pool_text(acc_id, page),
                                            reply_markup=pool_kb(acc_id, page))
            elif data.startswith("acc_pool_clear:"):
                acc_id = data.split(":", 1)[1]
                acc = get_account(acc_id)
                if not acc or not acc.get("text_pool"):
                    await cb.answer("Пусто")
                    return
                await cb.message.edit_text(
                    "💣 Очистить пул текстов?",
                    reply_markup=pool_clear_confirm_kb(acc_id))
            elif data.startswith("acc_pool_clear_ok:"):
                acc_id = data.split(":", 1)[1]
                acc = get_account(acc_id)
                if acc:
                    acc["text_pool"] = []
                    acc["pool_index"] = 0
                    save_json(STATE_FILE, STATE)
                await cb.answer("Пул очищен")
                await cb.message.edit_text(pool_text(acc_id, 0),
                                            reply_markup=pool_kb(acc_id, 0))

            # ================= РОТАЦИЯ =================
            elif data.startswith("acc_rotate:"):
                acc_id = data.split(":", 1)[1]
                acc = get_account(acc_id)
                if not acc:
                    await cb.answer()
                    return
                pending[uid] = {"action": "acc_rotate_set", "acc_id": acc_id}
                await cb.message.edit_text(
                    f"🔄 Сейчас: каждые {acc.get('rotate_every', 17)} отправок.\n"
                    f"Сколько отправок между сменой текста? (5–100)\n/cancel")

            # ================= ИСТОЧНИК (copy) =================
            elif data.startswith("acc_source:"):
                acc_id = data.split(":", 1)[1]
                acc = get_account(acc_id)
                if not acc:
                    await cb.answer()
                    return
                cur = "—"
                if acc.get("source_chat_id") and acc.get("source_message_id"):
                    cur = f"chat={acc['source_chat_id']} msg={acc['source_message_id']}"
                kb = InlineKeyboardMarkup([
                    [InlineKeyboardButton("📨 Переслать сообщение боту (в пул)",
                                          callback_data=f"acc_source_forward:{acc_id}")],
                    [InlineKeyboardButton("✏️ Задать chat_id/msg_id",
                                          callback_data=f"acc_source_manual:{acc_id}")],
                    [InlineKeyboardButton("🗑 Сбросить",
                                          callback_data=f"acc_source_clear:{acc_id}")],
                    [InlineKeyboardButton("⬅️", callback_data=f"acc_open:{acc_id}")],
                ])
                await cb.message.edit_text(
                    f"📎 **Источник (copy_message)**\n\n"
                    f"Текущий: `{cur}`\n\n"
                    f"«Переслать сообщение боту» добавит содержимое в **пул текстов** "
                    f"как ещё один вариант.",
                    reply_markup=kb)
            elif data.startswith("acc_source_forward:"):
                acc_id = data.split(":", 1)[1]
                pending[uid] = {"action": "acc_source_forward", "acc_id": acc_id}
                await cb.message.edit_text(
                    "📨 Перешли сюда сообщение с премиум-эмодзи — "
                    "добавлю в пул текстов.\n/cancel")
            elif data.startswith("acc_source_manual:"):
                acc_id = data.split(":", 1)[1]
                pending[uid] = {"action": "acc_source_manual", "acc_id": acc_id}
                await cb.message.edit_text(
                    "✏️ Отправь двумя строками:\n`chat_id`\n`message_id`\n/cancel")
            elif data.startswith("acc_source_clear:"):
                acc_id = data.split(":", 1)[1]
                acc = get_account(acc_id)
                if acc:
                    acc["source_chat_id"] = None
                    acc["source_message_id"] = None
                    save_json(STATE_FILE, STATE)
                await cb.answer()
                await cb.message.edit_text(account_text(acc_id), reply_markup=account_kb(acc_id))

            elif data.startswith("acc_text:"):
                acc_id = data.split(":", 1)[1]
                pending[uid] = {"action": "acc_text", "acc_id": acc_id}
                await cb.message.edit_text(
                    "📝 Базовый текст (используется только если пул пуст).\n"
                    "Отправь текст с премиум-эмодзи.\n/cancel")
            elif data.startswith("acc_media:"):
                acc_id = data.split(":", 1)[1]
                pending[uid] = {"action": "acc_media", "acc_id": acc_id}
                await cb.message.edit_text(
                    "🖼 Базовое медиа.\nОтправь фото/видео/документ/гифку.\n/cancel")
            elif data.startswith("acc_timing:"):
                acc_id = data.split(":", 1)[1]
                acc = get_account(acc_id)
                if not acc:
                    await cb.answer()
                    return
                kb = InlineKeyboardMarkup([
                    [InlineKeyboardButton(f"⏱ Интервал: {acc.get('interval', 1800)}с",
                                          callback_data=f"acc_setint:{acc_id}")],
                    [InlineKeyboardButton(f"⏳ Мин: {acc.get('delay_min', 5)}с",
                                          callback_data=f"acc_setdmin:{acc_id}")],
                    [InlineKeyboardButton(f"⏳ Макс: {acc.get('delay_max', 15)}с",
                                          callback_data=f"acc_setdmax:{acc_id}")],
                    [InlineKeyboardButton(
                        f"⌨️ Печатает: {acc.get('typing_seconds', DEFAULT_TYPING_SECONDS)}с",
                        callback_data=f"acc_settyping:{acc_id}")],
                    [InlineKeyboardButton("⬅️", callback_data=f"acc_open:{acc_id}")],
                ])
                await cb.message.edit_text(
                    f"⏱ Тайминги **{acc.get('name')}**\n\n"
                    f"Пауза между отправками — рандом: "
                    f"{acc.get('delay_min', 5)}–{acc.get('delay_max', 15)}с "
                    f"с множителем 1.0–1.6, плюс периодически длинные паузы "
                    f"({LONG_PAUSE_MIN}–{LONG_PAUSE_MAX}с) каждые "
                    f"{LONG_PAUSE_EVERY_MIN}–{LONG_PAUSE_EVERY_MAX} сообщений.",
                    reply_markup=kb)
            elif data.startswith("acc_setint:"):
                acc_id = data.split(":", 1)[1]
                pending[uid] = {"action": "acc_setint", "acc_id": acc_id}
                await cb.message.edit_text("Интервал (>=60):\n/cancel")
            elif data.startswith("acc_setdmin:"):
                acc_id = data.split(":", 1)[1]
                pending[uid] = {"action": "acc_setdmin", "acc_id": acc_id}
                await cb.message.edit_text("Минимум (>=1):\n/cancel")
            elif data.startswith("acc_setdmax:"):
                acc_id = data.split(":", 1)[1]
                pending[uid] = {"action": "acc_setdmax", "acc_id": acc_id}
                await cb.message.edit_text("Максимум:\n/cancel")
            elif data.startswith("acc_settyping:"):
                acc_id = data.split(":", 1)[1]
                pending[uid] = {"action": "acc_settyping", "acc_id": acc_id}
                await cb.message.edit_text("Секунд «печатает…» (1–60):\n/cancel")

            elif data.startswith("acc_scan:"):
                acc_id = data.split(":", 1)[1]
                c = user_clients.get(acc_id)
                if not c:
                    await cb.answer()
                    return
                await cb.answer()
                await cb.message.edit_text("🔍 Сканирую (только группы)…")
                found, skipped = await scan_groups_for_account(acc_id)
                acc = get_account(acc_id)
                scanned = {g["id"] for g in found}
                manual = [g for g in (acc.get("groups") or [])
                          if g.get("manual") and g["id"] not in scanned]
                acc["groups"] = found + manual
                save_json(STATE_FILE, STATE)
                await cb.message.edit_text(
                    f"✅ Найдено групп: {len(found)}\n"
                    f"Всего в списке: {len(acc['groups'])}\n"
                    f"Отфильтровано (не группы): {skipped}\n"
                    f"🚫 Пропущено исключённых: {len(acc.get('excluded') or [])}",
                    reply_markup=groups_list_kb(acc_id, 0))

            elif data.startswith("acc_grp_list:"):
                _, acc_id, page_s = data.split(":", 2)
                try:
                    page = int(page_s)
                except ValueError:
                    page = 0
                if not get_account(acc_id):
                    await cb.answer()
                    return
                await cb.message.edit_text(groups_list_text(acc_id, page),
                                            reply_markup=groups_list_kb(acc_id, page))
            elif data.startswith("acc_grp_del_one:"):
                parts = data.split(":", 3)
                if len(parts) < 4:
                    await cb.answer()
                    return
                _, acc_id, gid_s, page_s = parts
                try:
                    gid = int(gid_s); page = int(page_s)
                except ValueError:
                    await cb.answer()
                    return
                acc = get_account(acc_id)
                if not acc:
                    await cb.answer()
                    return
                groups = acc.get("groups") or []
                rec = next((g for g in groups if g.get("id") == gid), None)
                acc["groups"] = [g for g in groups if g.get("id") != gid]
                if rec:
                    add_to_excluded(acc, rec)
                save_json(STATE_FILE, STATE)
                total = len(acc["groups"])
                pages = max(1, (total + GROUPS_PER_PAGE - 1) // GROUPS_PER_PAGE)
                page = max(0, min(page, pages - 1))
                await cb.answer("Удалено → в исключения")
                await cb.message.edit_text(groups_list_text(acc_id, page),
                                            reply_markup=groups_list_kb(acc_id, page))
            elif data.startswith("acc_grp_del_idx:"):
                acc_id = data.split(":", 1)[1]
                acc = get_account(acc_id)
                if not acc:
                    await cb.answer()
                    return
                pending[uid] = {"action": "acc_grp_del_idx", "acc_id": acc_id}
                total = len(acc.get("groups") or [])
                await cb.message.edit_text(
                    f"✂️ Отправь номера для удаления (всего: {total}).\n"
                    f"Пример: `1,3,5-7,10`\n/cancel",
                    parse_mode=enums.ParseMode.MARKDOWN)
            elif data.startswith("acc_grp_clear:"):
                acc_id = data.split(":", 1)[1]
                acc = get_account(acc_id)
                if not acc:
                    await cb.answer()
                    return
                total = len(acc.get("groups") or [])
                if not total:
                    await cb.answer("Уже пусто")
                    return
                await cb.message.edit_text(
                    f"💣 Все **{total}** групп → в исключения?",
                    reply_markup=groups_clear_confirm_kb(acc_id),
                    parse_mode=enums.ParseMode.MARKDOWN)
            elif data.startswith("acc_grp_clear_ok:"):
                acc_id = data.split(":", 1)[1]
                acc = get_account(acc_id)
                if not acc:
                    await cb.answer()
                    return
                removed = list(acc.get("groups") or [])
                acc["groups"] = []
                for g in removed:
                    add_to_excluded(acc, g)
                save_json(STATE_FILE, STATE)
                await cb.answer(f"Очищено: {len(removed)}")
                await cb.message.edit_text(groups_list_text(acc_id, 0),
                                            reply_markup=groups_list_kb(acc_id, 0))

            elif data.startswith("acc_grp_excl:"):
                _, acc_id, page_s = data.split(":", 2)
                try:
                    page = int(page_s)
                except ValueError:
                    page = 0
                if not get_account(acc_id):
                    await cb.answer()
                    return
                await cb.message.edit_text(excluded_text(acc_id, page),
                                            reply_markup=excluded_kb(acc_id, page))
            elif data.startswith("acc_excl_restore:"):
                parts = data.split(":", 3)
                if len(parts) < 4:
                    await cb.answer()
                    return
                _, acc_id, gid_s, page_s = parts
                try:
                    gid = int(gid_s); page = int(page_s)
                except ValueError:
                    await cb.answer()
                    return
                acc = get_account(acc_id)
                if not acc:
                    await cb.answer()
                    return
                item = next((x for x in (acc.get("excluded") or [])
                             if str(x.get("id")) == str(gid)), None)
                if item:
                    exists = any(str(g.get("id")) == str(gid)
                                 for g in (acc.get("groups") or []))
                    if not exists:
                        acc.setdefault("groups", []).append({
                            "id": item.get("id"),
                            "title": item.get("title") or str(item.get("id")),
                            "type": item.get("type") or "UNKNOWN",
                            "username": item.get("username"),
                            "link": item.get("link") or "",
                            "manual": True,
                        })
                    remove_from_excluded(acc, gid)
                    save_json(STATE_FILE, STATE)
                    await cb.answer("Возвращено")
                else:
                    await cb.answer("Не найдено")
                total = len(acc.get("excluded") or [])
                pages = max(1, (total + EXCLUDED_PER_PAGE - 1) // EXCLUDED_PER_PAGE)
                page = max(0, min(page, pages - 1))
                await cb.message.edit_text(excluded_text(acc_id, page),
                                            reply_markup=excluded_kb(acc_id, page))
            elif data.startswith("acc_excl_clear:"):
                acc_id = data.split(":", 1)[1]
                acc = get_account(acc_id)
                if not acc:
                    await cb.answer()
                    return
                acc["excluded"] = []
                save_json(STATE_FILE, STATE)
                await cb.answer("Исключения очищены")
                await cb.message.edit_text(excluded_text(acc_id, 0),
                                            reply_markup=excluded_kb(acc_id, 0))

            elif data.startswith("acc_grp_prob:"):
                _, acc_id, page_s = data.split(":", 2)
                try:
                    page = int(page_s)
                except ValueError:
                    page = 0
                if not get_account(acc_id):
                    await cb.answer()
                    return
                await cb.message.edit_text(problem_text(acc_id, page),
                                            reply_markup=problem_kb(acc_id, page))
            elif data.startswith("acc_prob_clear:"):
                parts = data.split(":", 3)
                if len(parts) < 4:
                    await cb.answer()
                    return
                _, acc_id, gid_s, page_s = parts
                try:
                    gid = int(gid_s); page = int(page_s)
                except ValueError:
                    await cb.answer()
                    return
                acc = get_account(acc_id)
                if not acc:
                    await cb.answer()
                    return
                clear_problem(acc, gid)
                save_json(STATE_FILE, STATE)
                total = len(acc.get("problem") or [])
                pages = max(1, (total + PROBLEM_PER_PAGE - 1) // PROBLEM_PER_PAGE)
                page = max(0, min(page, pages - 1))
                await cb.answer("Очищено")
                await cb.message.edit_text(problem_text(acc_id, page),
                                            reply_markup=problem_kb(acc_id, page))
            elif data.startswith("acc_prob_clear_all:"):
                acc_id = data.split(":", 1)[1]
                acc = get_account(acc_id)
                if not acc:
                    await cb.answer()
                    return
                acc["problem"] = []
                save_json(STATE_FILE, STATE)
                await cb.answer("Очищено")
                await cb.message.edit_text(problem_text(acc_id, 0),
                                            reply_markup=problem_kb(acc_id, 0))
            elif data.startswith("acc_prob_to_excl:"):
                acc_id = data.split(":", 1)[1]
                acc = get_account(acc_id)
                if not acc:
                    await cb.answer()
                    return
                prob = list(acc.get("problem") or [])
                for p in prob:
                    add_to_excluded(acc, p)
                prob_ids = {str(p.get("id")) for p in prob}
                acc["groups"] = [g for g in (acc.get("groups") or [])
                                 if str(g.get("id")) not in prob_ids]
                acc["problem"] = []
                save_json(STATE_FILE, STATE)
                await cb.answer(f"Перенесено: {len(prob)}")
                await cb.message.edit_text(account_text(acc_id),
                                            reply_markup=account_kb(acc_id))

            elif data.startswith("acc_addgrp:"):
                acc_id = data.split(":", 1)[1]
                pending[uid] = {"action": "acc_addgrp", "acc_id": acc_id}
                await cb.message.edit_text(
                    "➕ Отправь @username / t.me/... / ID группы.\n"
                    "_(каналы и личка не принимаются)_\n/cancel",
                    parse_mode=enums.ParseMode.MARKDOWN)

            elif data.startswith("acc_setmain:"):
                acc_id = data.split(":", 1)[1]
                if get_account(acc_id):
                    STATE["main_account_id"] = acc_id
                    save_json(STATE_FILE, STATE)
                await cb.answer()
                await cb.message.edit_text(account_text(acc_id), reply_markup=account_kb(acc_id))
            elif data.startswith("acc_remove:"):
                acc_id = data.split(":", 1)[1]
                kb = InlineKeyboardMarkup([
                    [InlineKeyboardButton("✅ Да", callback_data=f"acc_remove_ok:{acc_id}")],
                    [InlineKeyboardButton("❌ Нет", callback_data=f"acc_open:{acc_id}")],
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
                await cb.answer()
                await cb.message.edit_text(accounts_menu_text(),
                                            reply_markup=accounts_menu_kb())

            elif data.startswith("acc_sub_menu:"):
                acc_id = data.split(":", 1)[1]
                if not get_account(acc_id):
                    await cb.answer()
                    return
                await cb.message.edit_text(subscribe_menu_text(acc_id),
                                            reply_markup=subscribe_menu_kb(acc_id))
            elif data.startswith("acc_sub_load:"):
                acc_id = data.split(":", 1)[1]
                pending[uid] = {"action": "acc_sub_load", "acc_id": acc_id}
                await cb.message.edit_text(
                    "📋 Отправь список групп:\n"
                    "• По одной на строку: `@group1`\n"
                    "• Через запятую: `@g1, @g2`\n"
                    "• Или .txt файлом\n/cancel")
            elif data.startswith("acc_sub_delay:"):
                acc_id = data.split(":", 1)[1]
                pending[uid] = {"action": "acc_sub_delay", "acc_id": acc_id}
                await cb.message.edit_text("⏱ Мин и макс (пример: `40 120`):\n/cancel")
            elif data.startswith("acc_sub_start:"):
                acc_id = data.split(":", 1)[1]
                acc = get_account(acc_id)
                if not acc or not acc.get("subscribe_queue"):
                    await cb.answer()
                    return
                if not user_clients.get(acc_id):
                    await cb.answer()
                    return
                acc["subscribe_status"] = "running"
                save_json(STATE_FILE, STATE)
                start_subscribe(acc_id)
                await cb.answer()
                await cb.message.edit_text(subscribe_menu_text(acc_id),
                                            reply_markup=subscribe_menu_kb(acc_id))
            elif data.startswith("acc_sub_stop:"):
                acc_id = data.split(":", 1)[1]
                acc = get_account(acc_id)
                if acc:
                    acc["subscribe_status"] = "paused"
                    save_json(STATE_FILE, STATE)
                await cb.answer()
                await cb.message.edit_text(subscribe_menu_text(acc_id),
                                            reply_markup=subscribe_menu_kb(acc_id))
            elif data.startswith("acc_sub_clear:"):
                acc_id = data.split(":", 1)[1]
                acc = get_account(acc_id)
                if acc:
                    acc["subscribe_queue"] = []
                    acc["subscribe_status"] = "idle"
                    save_json(STATE_FILE, STATE)
                await cb.answer()
                await cb.message.edit_text(subscribe_menu_text(acc_id),
                                            reply_markup=subscribe_menu_kb(acc_id))
            else:
                await cb.answer()
        except Exception:
            log.exception("Ошибка callback")
            try:
                await cb.answer()
            except Exception:
                pass


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

async def main():
    global CFG, STATE, bot_client

    CFG = load_cfg()
    if not cfg_ok(CFG):
        print("Не заданы переменные: API_ID, API_HASH, BOT_TOKEN, ADMIN_ID")
        return
    CFG.setdefault("pin", DEFAULT_PIN)
    save_json(CONFIG_FILE, CFG)

    log.info(f"ADMIN_ID={CFG['admin_id']} (type={type(CFG['admin_id']).__name__})")
    log.info(f"PIN={'*' * len(str(CFG['pin']))}, proxy_enabled={CFG.get('proxy_enabled')}")

    STATE = load_state()
    bootstrap_default_session()

    await db_init()
    await start_userbot_clients()

    bot_client = Client(
        name=SESSION_BOT, api_id=CFG["api_id"], api_hash=CFG["api_hash"],
        bot_token=CFG["bot_token"])
    register_handlers(bot_client)

    log.info("Запуск бота…")
    await bot_client.start()
    bme = await bot_client.get_me()
    log.info(f"Бот запущен: @{bme.username}")

    await try_start_existing_clients()

    try:
        accounts = STATE.get("accounts") or []
        await bot_client.send_message(
            CFG["admin_id"],
            f"🤖 Бот запущен.\n• Аккаунтов: {len(accounts)}\n"
            f"🧦 Прокси: {'ВКЛ' if CFG.get('proxy_enabled') else 'ВЫКЛ'}\n"
            f"Для доступа: `/auth <PIN>`",
            parse_mode=enums.ParseMode.MARKDOWN)
    except Exception as e:
        log.warning(f"Приветствие: {e}")

    log.info("Сервис работает.")
    try:
        await asyncio.Event().wait()
    finally:
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
