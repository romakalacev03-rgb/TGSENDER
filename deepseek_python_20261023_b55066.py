# -*- coding: utf-8 -*-
"""
Мульти-аккаунт автопостер + рассылка + массовая подписка + автоответчик.
Глобальный пул текстов. SQLite-персистентность. Диагностика /data.
Не убиваем сессию при транзитных ошибках (AuthKeyDuplicated и т.п.).
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
from enum import Enum

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
#  СЕССИЯ ПО УМОЛЧАНИЮ (вставь свою из Colab)
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
MEDIA_DIR = os.path.join(DATA_DIR, "media")
DB_FILE = os.path.join(DATA_DIR, "bot_data.db")
SESSION_BOT = os.path.join(DATA_DIR, "control_bot")
MARKER_FILE = os.path.join(DATA_DIR, ".persistence_marker")
LEGACY_STATE = os.path.join(DATA_DIR, "state.json")
LEGACY_CONFIG = os.path.join(DATA_DIR, "config.json")

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
POOL_MAX = 8

INCOMING_NOTIFY_COOLDOWN_SEC = 300
INCOMING_WARMUP_SEC = 3

SHORT_PAUSE_PROB = 0.85
LONG_PAUSE_EVERY_MIN = 8
LONG_PAUSE_EVERY_MAX = 15
LONG_PAUSE_MIN = 45
LONG_PAUSE_MAX = 180

DEFAULT_REPLY_COOLDOWN = 3600
SESSION_ERRORS_TO_CLEAR = 3

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("autoposter")

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
_last_reply_by_user: dict = {}
_admin_last_seen: dict = {"ts": 0.0}
_boot_ts: float = 0.0
_persistence_ok: bool = False

DEVICE_PARAMS = {
    "app_version": "9.3.1",
    "device_model": "Samsung Galaxy S23 Ultra",
    "system_version": "Android 13",
    "lang_code": "ru",
}


# ===========================================================================
#  УТИЛИТЫ
# ===========================================================================
def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def today_str() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def _safe_json(obj):
    if obj is None:
        return None
    if isinstance(obj, Enum):
        v = obj.value
        if isinstance(v, (dict, list)):
            return _safe_json(v)
        if isinstance(v, (bool, int, float, str)) or v is None:
            return v
        return str(v)
    if isinstance(obj, bool):
        return obj
    if isinstance(obj, int):
        return int(obj)
    if isinstance(obj, float):
        return float(obj)
    if isinstance(obj, str):
        if type(obj) is str:
            return obj
        return str.__str__(obj)
    if isinstance(obj, dict):
        return {str(k): _safe_json(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple, set)):
        return [_safe_json(x) for x in obj]
    try:
        return str(obj)
    except Exception:
        return None


def entities_to_json(entities) -> list:
    if not entities:
        return []
    out = []
    for e in entities:
        try:
            et = getattr(e, "type", None)
            if et is None:
                continue
            if hasattr(et, "name"):
                etype = str(et.name).lower()
            elif hasattr(et, "value"):
                etype = str(et.value)
            else:
                etype = str(et)
            etype = etype.strip()
            if not etype:
                continue
            item = {
                "type": etype,
                "offset": int(getattr(e, "offset", 0)),
                "length": int(getattr(e, "length", 0)),
            }
            url = getattr(e, "url", None)
            if url:
                item["url"] = str(url)
            lang = getattr(e, "language", None)
            if lang:
                item["language"] = str(lang)
            ce = getattr(e, "custom_emoji_id", None)
            if ce is not None:
                try:
                    item["custom_emoji_id"] = str(int(ce))
                except Exception:
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
            kwargs = {"type": etype,
                      "offset": int(d["offset"]),
                      "length": int(d["length"])}
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


def parse_proxy_line(line: str) -> dict:
    s = (line or "").strip()
    if not s:
        raise ValueError("пустая строка")

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
                "enabled": True, "type": "mtproxy",
                "host": host, "port": port,
                "username": "", "password": "",
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
            "host": host, "port": port,
            "username": username, "password": password,
            "secret": "",
        }

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
        "enabled": True, "type": "socks5",
        "host": host, "port": port,
        "username": user, "password": pwd,
        "secret": "",
    }


def proxy_dict_for_pyrogram(proxy: dict):
    if not proxy or not proxy.get("enabled"):
        return None
    try:
        ptype = (proxy.get("type") or "socks5").lower()
        host = proxy["host"]
        port = int(proxy["port"])
        if ptype == "mtproxy":
            return {"scheme": "mtproxy", "hostname": host, "port": port,
                    "secret": proxy.get("secret") or ""}
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


def _group_key(g: dict) -> str:
    return str(g.get("id") or "")


# ===========================================================================
#  ПЕРСИСТЕНТНОСТЬ / ДИАГНОСТИКА
# ===========================================================================
def check_persistence() -> bool:
    """Проверяет, переживает ли DATA_DIR рестарт. Возвращает True если маркер уже был."""
    prev = None
    if os.path.exists(MARKER_FILE):
        try:
            with open(MARKER_FILE, "r", encoding="utf-8") as f:
                prev = f.read().strip()
        except Exception:
            pass
    try:
        with open(MARKER_FILE, "w", encoding="utf-8") as f:
            f.write(now_iso())
    except Exception as e:
        log.error(f"[PERSISTENCE] не могу записать маркер: {e}")
    if prev:
        log.info(f"[PERSISTENCE] OK — предыдущий старт был {prev}")
        return True
    log.warning("[PERSISTENCE] ЛОСТ — маркер не найден! "
                "DATA_DIR скорее всего НЕ сохраняется между рестартами.")
    return False


def db_size_bytes() -> int:
    try:
        return os.path.getsize(DB_FILE)
    except Exception:
        return 0


# ===========================================================================
#  SQLITE
# ===========================================================================
async def db_init():
    async with aiosqlite.connect(DB_FILE, timeout=15.0) as db:
        await db.executescript("""
            CREATE TABLE IF NOT EXISTS kv (
                k TEXT PRIMARY KEY,
                v TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS accounts (
                id TEXT PRIMARY KEY,
                name TEXT,
                data TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS pool (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                data TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS replies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                data TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                role TEXT NOT NULL,
                content TEXT NOT NULL,
                timestamp TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_messages_uid ON messages(user_id);
        """)
        await db.commit()


async def _db_write(sql: str, params: tuple, retries: int = 4) -> None:
    last_err = None
    for attempt in range(retries):
        try:
            async with aiosqlite.connect(DB_FILE, timeout=15.0) as db:
                await db.execute(sql, params)
                await db.commit()
            return
        except Exception as e:
            last_err = e
            s = str(e).lower()
            if "lock" in s or "busy" in s:
                await asyncio.sleep(0.4 * (attempt + 1))
                continue
            raise
    raise last_err


async def db_kv_get(key: str, default=None):
    async with aiosqlite.connect(DB_FILE, timeout=15.0) as db:
        async with db.execute("SELECT v FROM kv WHERE k=?", (key,)) as cur:
            row = await cur.fetchone()
            if not row:
                return default
            try:
                return json.loads(row[0])
            except Exception:
                return default


async def db_kv_set(key: str, value):
    payload = json.dumps(_safe_json(value), ensure_ascii=False)
    await _db_write(
        "INSERT INTO kv(k,v) VALUES(?,?) "
        "ON CONFLICT(k) DO UPDATE SET v=excluded.v",
        (key, payload))


async def db_accounts_load() -> list:
    async with aiosqlite.connect(DB_FILE, timeout=15.0) as db:
        async with db.execute("SELECT data FROM accounts ORDER BY rowid") as cur:
            rows = await cur.fetchall()
    out = []
    for r in rows:
        try:
            out.append(json.loads(r[0]))
        except Exception as e:
            log.warning(f"[LOAD] битый JSON аккаунта: {e}")
    return out


async def db_account_upsert(acc: dict):
    payload = json.dumps(_safe_json(acc), ensure_ascii=False)
    await _db_write(
        "INSERT INTO accounts(id,name,data) VALUES(?,?,?) "
        "ON CONFLICT(id) DO UPDATE SET name=excluded.name, data=excluded.data",
        (acc["id"], acc.get("name") or "", payload))


async def db_account_delete(acc_id: str):
    await _db_write("DELETE FROM accounts WHERE id=?", (acc_id,))


async def db_pool_load() -> list:
    async with aiosqlite.connect(DB_FILE, timeout=15.0) as db:
        async with db.execute("SELECT data FROM pool ORDER BY id") as cur:
            rows = await cur.fetchall()
    out = []
    for r in rows:
        try:
            out.append(json.loads(r[0]))
        except Exception:
            continue
    return out


async def db_pool_add(item: dict):
    payload = json.dumps(_safe_json(item), ensure_ascii=False)
    await _db_write("INSERT INTO pool(data) VALUES(?)", (payload,))


async def db_pool_delete(idx: int):
    async with aiosqlite.connect(DB_FILE, timeout=15.0) as db:
        async with db.execute("SELECT id FROM pool ORDER BY id LIMIT 1 OFFSET ?",
                              (idx,)) as cur:
            row = await cur.fetchone()
    if row:
        await _db_write("DELETE FROM pool WHERE id=?", (row[0],))


async def db_pool_clear():
    await _db_write("DELETE FROM pool", ())


async def db_replies_load() -> list:
    async with aiosqlite.connect(DB_FILE, timeout=15.0) as db:
        async with db.execute("SELECT data FROM replies ORDER BY id") as cur:
            rows = await cur.fetchall()
    out = []
    for r in rows:
        try:
            out.append(json.loads(r[0]))
        except Exception:
            continue
    return out


async def db_replies_add(item: dict):
    payload = json.dumps(_safe_json(item), ensure_ascii=False)
    await _db_write("INSERT INTO replies(data) VALUES(?)", (payload,))


async def db_replies_delete(idx: int):
    async with aiosqlite.connect(DB_FILE, timeout=15.0) as db:
        async with db.execute("SELECT id FROM replies ORDER BY id LIMIT 1 OFFSET ?",
                              (idx,)) as cur:
            row = await cur.fetchone()
    if row:
        await _db_write("DELETE FROM replies WHERE id=?", (row[0],))


async def db_replies_clear():
    await _db_write("DELETE FROM replies", ())


async def state_save():
    try:
        meta = {
            "main_account_id": STATE.get("main_account_id", ""),
            "global_stats": STATE.get("global_stats", {}),
            "incoming_users": STATE.get("incoming_users", {}),
            "incoming_daily": STATE.get("incoming_daily", {}),
            "incoming_log": STATE.get("incoming_log", []),
            "proxy_enabled": CFG.get("proxy_enabled", False),
            "auto_reply_enabled": STATE.get("auto_reply_enabled", False),
            "auto_reply_cooldown": STATE.get("auto_reply_cooldown", DEFAULT_REPLY_COOLDOWN),
            "selected_for_bulk": STATE.get("selected_for_bulk", {}),
            "bootstrapped": STATE.get("_bootstrapped", False),
        }
        await db_kv_set("meta", meta)
    except Exception as e:
        log.error(f"state_save: {e}")


async def state_load():
    """Атомарная загрузка. Если что-то падает — STATE не трогаем."""
    try:
        meta = await db_kv_get("meta", {}) or {}
        accounts = await db_accounts_load()
        pool = await db_pool_load()
        replies = await db_replies_load()
    except Exception as e:
        log.exception(f"[STATE_LOAD] не смог прочитать БД: {e}")
        raise  # пусть main упадёт явно, а не стартует с пустым STATE

    log.info(f"[STATE_LOAD] accounts={len(accounts)} pool={len(pool)} replies={len(replies)}")

    # Попытка миграции из старого state.json (если БД пустая)
    if not accounts and os.path.exists(LEGACY_STATE):
        try:
            with open(LEGACY_STATE, "r", encoding="utf-8") as f:
                legacy = json.load(f)
            legacy_accounts = legacy.get("accounts") or []
            if legacy_accounts:
                log.warning(f"[MIGRATE] DB пустая, нашёл state.json с "
                            f"{len(legacy_accounts)} аккаунтами — восстанавливаю")
                for old in legacy_accounts:
                    if not old.get("id"):
                        continue
                    acc = _new_account(old.get("name") or "old",
                                        old.get("session_name") or "")
                    for k in ("session_string", "user_id", "username",
                              "groups", "excluded", "problem", "stats",
                              "interval", "delay_min", "delay_max",
                              "typing_seconds", "rotate_every",
                              "source_chat_id", "source_message_id",
                              "subscribe_queue", "subscribe_status",
                              "subscribe_delay_min", "subscribe_delay_max",
                              "subscribe_stats", "proxy"):
                        if k in old:
                            acc[k] = old[k]
                    acc["running"] = False
                    acc["auto_reply_enabled"] = bool(old.get("auto_reply_enabled", False))
                    await db_account_upsert(acc)
                accounts = await db_accounts_load()
                log.info(f"[MIGRATE] восстановлено {len(accounts)} аккаунтов")
        except Exception as e:
            log.warning(f"[MIGRATE] state.json битый: {e}")

    if not STATE.get("_bootstrapped") and not meta.get("main_account_id") and accounts:
        meta["main_account_id"] = accounts[0]["id"]

    new_state = {
        "accounts": accounts,
        "main_account_id": meta.get("main_account_id", "") or (accounts[0]["id"] if accounts else ""),
        "global_stats": meta.get("global_stats", {}) or {},
        "incoming_users": meta.get("incoming_users", {}) or {},
        "incoming_daily": meta.get("incoming_daily", {}) or {},
        "incoming_log": meta.get("incoming_log", []) or [],
        "auto_reply_enabled": meta.get("auto_reply_enabled", False),
        "auto_reply_cooldown": meta.get("auto_reply_cooldown", DEFAULT_REPLY_COOLDOWN),
        "selected_for_bulk": meta.get("selected_for_bulk", {}) or {},
        "_bootstrapped": meta.get("bootstrapped", False),
        "pool": pool,
        "replies": replies,
    }

    # МИГРАЦИЯ старых per-account пулов
    if not pool:
        migrated = []
        for acc in accounts:
            for item in (acc.get("text_pool") or []):
                migrated.append(item)
                if len(migrated) >= POOL_MAX:
                    break
            if len(migrated) >= POOL_MAX:
                break
        if migrated:
            for item in migrated:
                await db_pool_add(item)
            new_state["pool"] = migrated
            log.info(f"[MIGRATE] перенесено {len(migrated)} текстов в глобальный пул")
        for acc in accounts:
            if acc.get("text_pool"):
                acc["text_pool"] = []
                await db_account_upsert(acc)

    for acc in new_state["accounts"]:
        acc.setdefault("auto_reply_enabled", False)
        acc.setdefault("session_error_count", 0)
        acc.setdefault("last_error", "")
        acc.setdefault("last_error_iso", "")

    CFG["proxy_enabled"] = meta.get("proxy_enabled", CFG.get("proxy_enabled", False))

    # ТОЛЬКО ТЕПЕРЬ меняем глобальный STATE
    STATE.clear()
    STATE.update(new_state)


# ===========================================================================
#  АККАУНТЫ
# ===========================================================================
def _new_account(name: str, session_name: str) -> dict:
    return {
        "id": f"acc_{int(datetime.now().timestamp() * 1000)}_{random.randint(100, 999)}",
        "name": name, "session_name": session_name, "session_string": "",
        "user_id": None, "username": None,
        "text": "", "text_entities": [],
        "caption": "", "caption_entities": [],
        "media_path": None, "media_type": None,
        "text_pool": [],
        "source_chat_id": None, "source_message_id": None,
        "groups": [], "excluded": [], "problem": [],
        "running": False,
        "interval": 1800, "delay_min": 5, "delay_max": 15,
        "typing_seconds": DEFAULT_TYPING_SECONDS,
        "rotate_every": 17,
        "stats": {"sent": 0, "errors": 0, "rounds": 0,
                  "last_round": None, "next_round": None},
        "subscribe_queue": [], "subscribe_status": "idle",
        "subscribe_delay_min": 40, "subscribe_delay_max": 120,
        "subscribe_stats": {"subscribed": 0, "skipped": 0, "errors": 0,
                            "started_at": None},
        "proxy": {"enabled": False, "type": "socks5",
                  "host": "", "port": 0,
                  "username": "", "password": "", "secret": ""},
        "auto_reply_enabled": False,
        "session_error_count": 0,
        "last_error": "",
        "last_error_iso": "",
    }


def get_account(acc_id: str) -> dict:
    for a in STATE.get("accounts") or []:
        if a.get("id") == acc_id:
            return a
    return {}


async def add_account_async(name: str, session_name: str) -> dict:
    acc = _new_account(name, session_name)
    STATE.setdefault("accounts", []).append(acc)
    if not STATE.get("main_account_id"):
        STATE["main_account_id"] = acc["id"]
    await db_account_upsert(acc)
    await state_save()
    return acc


async def remove_account_async(acc_id: str) -> bool:
    accounts = STATE.get("accounts") or []
    new_list = [a for a in accounts if a.get("id") != acc_id]
    if len(new_list) == len(accounts):
        return False
    STATE["accounts"] = new_list
    if STATE.get("main_account_id") == acc_id:
        STATE["main_account_id"] = new_list[0]["id"] if new_list else ""
    sel = STATE.setdefault("selected_for_bulk", {})
    sel.pop(acc_id, None)
    await db_account_delete(acc_id)
    await state_save()
    return True


async def bootstrap_default_session():
    if not DEFAULT_SESSION_STRING or DEFAULT_SESSION_STRING.startswith("ВСТАВЬ"):
        return
    if STATE.get("_bootstrapped"):
        return
    if STATE.get("accounts"):
        STATE["_bootstrapped"] = True
        await state_save()
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
    await db_account_upsert(acc)
    await state_save()
    log.info("[BOOTSTRAP] Добавлен аккаунт «Основной»")


# ===========================================================================
#  ГРУППЫ / ПРОБЛЕМЫ
# ===========================================================================
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


# ===========================================================================
#  ВХОДЯЩИЕ
# ===========================================================================
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


def account_has_content(acc: dict) -> bool:
    return bool(acc.get("text") or acc.get("media_path") or STATE.get("pool"))


def current_pool_item(acc: dict) -> dict:
    pool = STATE.get("pool") or []
    if not pool:
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
    return pool[idx]


# ===========================================================================
#  ОТПРАВКА
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


# ===========================================================================
#  РАССЫЛКА
# ===========================================================================
async def mailing_loop_for_account(acc_id: str):
    acc = get_account(acc_id)
    if not acc:
        return
    log.info(f"[{acc.get('name')}] рассылка запущена")
    while acc.get("running"):
        groups = list(acc.get("groups") or [])
        if not groups:
            await asyncio.sleep(30)
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
            try:
                await db_account_upsert(acc)
            except Exception as e:
                log.warning(f"db_account_upsert failed: {e}")

            pause = _pick_pause_seconds(acc, sent_in_round)
            try:
                await asyncio.sleep(pause)
            except Exception:
                await asyncio.sleep(5)

        acc["stats"]["rounds"] = acc["stats"].get("rounds", 0) + 1
        acc["stats"]["last_round"] = now_iso()
        STATE["global_stats"]["total_sent"] = STATE["global_stats"].get("total_sent", 0) + sent
        STATE["global_stats"]["total_errors"] = STATE["global_stats"].get("total_errors", 0) + errors
        try:
            await db_account_upsert(acc)
            await state_save()
        except Exception as e:
            log.warning(f"save after round: {e}")

        if not acc.get("running"):
            break
        interval = int(acc.get("interval", 1800))
        acc["stats"]["next_round"] = (
            datetime.now() + timedelta(seconds=interval)).isoformat(timespec="seconds")
        try:
            await db_account_upsert(acc)
        except Exception:
            pass
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


# ===========================================================================
#  ПОДПИСКА
# ===========================================================================
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
        try:
            await db_account_upsert(acc)
        except Exception:
            pass
        return
    acc["subscribe_status"] = "running"
    try:
        await db_account_upsert(acc)
    except Exception:
        pass
    while acc.get("subscribe_status") == "running":
        queue = acc.get("subscribe_queue") or []
        if not queue:
            acc["subscribe_status"] = "done"
            try:
                await db_account_upsert(acc)
            except Exception:
                pass
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
        try:
            await db_account_upsert(acc)
        except Exception:
            pass
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
#  АВТООТВЕТЧИК + ВХОДЯЩИЕ
# ===========================================================================
def _admin_online() -> bool:
    ts = _admin_last_seen.get("ts", 0)
    return (time.time() - ts) < 300


async def _auto_reply(client: Client, message, acc_id: str):
    if not STATE.get("auto_reply_enabled"):
        return
    acc = get_account(acc_id)
    if not acc.get("auto_reply_enabled"):
        return
    replies = STATE.get("replies") or []
    if not replies:
        return
    if _admin_online():
        return
    fu = message.from_user
    if not fu or getattr(fu, "is_bot", False):
        return
    if int(fu.id) == int(CFG.get("admin_id", 0)):
        return
    uid_key = str(fu.id)
    now_ts = time.time()
    last = _last_reply_by_user.get(uid_key, 0)
    cd = int(STATE.get("auto_reply_cooldown", DEFAULT_REPLY_COOLDOWN) or DEFAULT_REPLY_COOLDOWN)
    if now_ts - last < cd:
        return
    _last_reply_by_user[uid_key] = now_ts

    item = random.choice(replies)
    text = item.get("text") or ""
    ents = entities_from_json(item.get("text_entities"))
    if not text:
        return
    try:
        if ents:
            await client.send_message(message.chat.id, text, entities=ents)
        else:
            try:
                await client.send_message(message.chat.id, text,
                                          parse_mode=enums.ParseMode.MARKDOWN)
            except Exception:
                await client.send_message(message.chat.id, text)
        try:
            await bot_client.send_message(
                CFG["admin_id"],
                f"🤖 Автоответ → {fu.first_name or ''} (@{fu.username or '—'})\n"
                f"Аккаунт: {acc.get('name')}")
        except Exception:
            pass
    except Exception as e:
        log.warning(f"auto_reply send: {e}")


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

            text = message.text or message.caption or "<медиа>"
            name = (fu.first_name or "") + (f" {fu.last_name}" if fu.last_name else "")
            is_new_today = register_incoming(int(fu.id), name.strip(), fu.username or "", text)

            await _auto_reply(userbot, message, acc_id)

            last = _last_notify_by_user.get(uid_key, 0)
            if now_ts - last < INCOMING_NOTIFY_COOLDOWN_SEC:
                return
            _last_notify_by_user[uid_key] = now_ts

            username = fu.username or ""
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
#  КЛИЕНТЫ
# ===========================================================================
async def start_userbot_clients():
    accounts = STATE.get("accounts") or []
    log.info(f"[CLIENTS] создаю клиентов для {len(accounts)} аккаунтов")
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
        session_name = acc.get("session_name") or f"userbot_{acc_id}"
        try:
            c = make_client(session_name, proxy=proxy)
            user_clients[acc_id] = c
            client_locks[acc_id] = asyncio.Lock()
        except Exception as e:
            log.warning(f"client {acc_id}: {e}")


def _mark_session_error(acc: dict, err_text: str):
    """Считаем ошибку сессии. Не убиваем сессию с первого раза."""
    acc["session_error_count"] = int(acc.get("session_error_count", 0)) + 1
    acc["last_error"] = str(err_text)[:200]
    acc["last_error_iso"] = now_iso()
    cnt = acc["session_error_count"]
    log.warning(f"[{acc.get('name')}] session error #{cnt}: {err_text}")
    if cnt >= SESSION_ERRORS_TO_CLEAR:
        log.warning(f"[{acc.get('name')}] {cnt} ошибок подряд — очищаю session_string")
        acc["session_string"] = ""
        acc["session_error_count"] = 0


async def try_start_existing_clients():
    accounts = STATE.get("accounts") or []
    log.info(f"[START] пробую стартовать {len(accounts)} клиентов")
    for acc in accounts:
        acc_id = acc["id"]
        c = user_clients.get(acc_id)
        if not c:
            log.warning(f"[{acc.get('name')}] нет клиента — пропуск")
            continue
        try:
            await asyncio.wait_for(c.start(), timeout=30)
            me = await c.get_me()
            if me:
                ME_IDS[acc_id] = me.id
                acc["user_id"] = me.id
                acc["username"] = me.username
                acc["session_error_count"] = 0
                acc["last_error"] = ""
                acc["last_error_iso"] = ""
                log.info(f"✅ [{acc.get('name')}] {me.first_name} id={me.id}")

                attach_incoming_listener(c, acc_id)
                asyncio.create_task(_mark_incoming_ready(acc_id))

                if acc.get("running"):
                    start_mailing_for_account(acc_id)
                if acc.get("subscribe_status") == "running":
                    start_subscribe(acc_id)
        except asyncio.TimeoutError:
            _mark_session_error(acc, "timeout")
            try:
                c.disconnect_handler = None
                if getattr(c, "network_task", None):
                    c.network_task.cancel()
                await c.stop()
            except Exception:
                pass
            user_clients.pop(acc_id, None)
        except (AuthKeyUnregistered, AuthKeyDuplicated,
                UserDeactivated, SessionRevoked) as e:
            _mark_session_error(acc, e)
            try:
                await c.stop()
            except Exception:
                pass
            user_clients.pop(acc_id, None)
        except Exception as e:
            _mark_session_error(acc, e)
            try:
                if not c.is_connected:
                    await c.connect()
            except Exception:
                pass
        try:
            await db_account_upsert(acc)
        except Exception as ex:
            log.warning(f"save acc after start: {ex}")
    await state_save()
    ok_clients = sum(1 for a in accounts if a["id"] in user_clients)
    log.info(f"[START] клиентов в памяти: {ok_clients}/{len(accounts)}")


# ===========================================================================
#  МЕНЮ
# ===========================================================================
def main_menu_kb() -> InlineKeyboardMarkup:
    accounts = STATE.get("accounts") or []
    running_count = sum(1 for a in accounts if a.get("running"))
    subs_count = sum(1 for a in accounts if a.get("subscribe_status") == "running")
    subs_mark = f" 📥{subs_count}" if subs_count else ""
    px_mark = "🟢" if CFG.get("proxy_enabled") else "⚪️"
    reply_on = STATE.get("auto_reply_enabled")
    reply_accs = sum(1 for a in accounts if a.get("auto_reply_enabled"))
    reply_mark = "🤖" if reply_on else "💤"
    active = running_count + subs_count
    active_mark = f" ({active})" if active else ""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"📢 Рассылка ({running_count}/{len(accounts)})",
                              callback_data="accounts_menu")],
        [InlineKeyboardButton(f"🟢 Активные задачи{active_mark}",
                              callback_data="active_tasks")],
        [InlineKeyboardButton("🚀 Массовый запуск / выбор",
                              callback_data="bulk_start_menu")],
        [InlineKeyboardButton(f"📥 Массовая подписка{subs_mark}",
                              callback_data="sub_main")],
        [InlineKeyboardButton(f"🧩 Пул текстов ({len(STATE.get('pool') or [])})",
                              callback_data="pool_menu:0")],
        [InlineKeyboardButton(f"{reply_mark} Автоответчик ({reply_accs} акк.)",
                              callback_data="reply_menu:0")],
        [InlineKeyboardButton("📈 Отписи", callback_data="incoming_menu")],
        [InlineKeyboardButton(f"{px_mark} Прокси: "
                              f"{'вкл' if CFG.get('proxy_enabled') else 'выкл'}",
                              callback_data="proxy_global_menu")],
        [InlineKeyboardButton("📊 Статистика", callback_data="stats_menu")],
        [InlineKeyboardButton("🩺 Диагностика", callback_data="diag_menu")],
    ])


def diag_text() -> str:
    accounts_db = 0
    try:
        import sqlite3 as _sql
        conn = _sql.connect(DB_FILE)
        try:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM accounts")
            accounts_db = cur.fetchone()[0]
        finally:
            conn.close()
    except Exception as e:
        accounts_db = -1
        log.warning(f"diag: {e}")

    accounts_mem = len(STATE.get("accounts") or [])
    clients = len(user_clients)
    connected = 0
    for c in user_clients.values():
        try:
            if c.is_connected:
                connected += 1
        except Exception:
            pass
    uptime = int(time.time() - _boot_ts) if _boot_ts else 0
    ups = uptime // 86400
    ups_h = (uptime % 86400) // 3600
    ups_m = (uptime % 3600) // 60

    lines = [
        "🩺 **Диагностика**\n",
        f"• DATA_DIR: `{DATA_DIR}`",
        f"• DB: `{DB_FILE}`",
        f"• DB size: **{db_size_bytes()} байт**",
        f"• Маркер персистентности: "
        f"{'✅ есть' if _persistence_ok else '❌ НЕТ (потеря при рестарте!)'}",
        f"• LEGACY state.json: {'есть' if os.path.exists(LEGACY_STATE) else 'нет'}",
        "",
        f"• Аккаунтов в БД: **{accounts_db}**",
        f"• Аккаунтов в памяти: **{accounts_mem}**",
        f"• Клиентов создано: **{clients}**",
        f"• Клиентов online: **{connected}**",
        "",
        f"• Пул текстов: {len(STATE.get('pool') or [])}",
        f"• Шаблонов автоответа: {len(STATE.get('replies') or [])}",
        f"• Аптайм: {ups}д {ups_h}ч {ups_m}м",
    ]
    if accounts_db != accounts_mem:
        lines.append(
            "\n⚠️ **ВНИМАНИЕ**: количество в БД и памяти РАСХОДИТСЯ! "
            "Это баг, сообщи разработчику.")
    if not _persistence_ok:
        lines.append(
            "\n⚠️ **/data НЕ переживает рестарт!**\n"
            "Проверь `persistenceMount: /data` в amvera.yml и тариф Amvera. "
            "Без persistent volume все аккаунты будут теряться.")
    return "\n".join(lines)


def diag_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔄 Обновить", callback_data="diag_menu")],
        [InlineKeyboardButton("🧹 Сбросить ошибки сессий",
                              callback_data="diag_reset_errors")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="menu")],
    ])


def accounts_menu_kb() -> InlineKeyboardMarkup:
    accounts = STATE.get("accounts") or []
    rows = []
    for acc in accounts:
        mark = "🟢" if acc.get("running") else "⚪️"
        sub = " 📥" if acc.get("subscribe_status") == "running" else ""
        main_mark = " ⭐" if acc.get("id") == STATE.get("main_account_id") else ""
        px = "🧦" if (CFG.get("proxy_enabled") and proxy_is_active_for_account(acc)) else ""
        rr = " 🤖" if acc.get("auto_reply_enabled") else ""
        conn = "🔗" if acc["id"] in user_clients else "⚠️"
        err = ""
        if acc.get("session_error_count", 0) > 0:
            err = f" ❗{acc['session_error_count']}"
        name = (acc.get("name") or "—")[:25]
        rows.append([InlineKeyboardButton(f"{mark}{conn} {name}{main_mark}{sub}{px}{rr}{err}",
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
            rr = " 🤖" if acc.get("auto_reply_enabled") else ""
            conn = "🔗" if acc["id"] in user_clients else "⚠️ offline"
            s = acc.get("stats") or {}
            src = "✅" if (acc.get("source_chat_id") and acc.get("source_message_id")) else "❌"
            err_n = acc.get("session_error_count", 0)
            err = f" | ❗{err_n}" if err_n else ""
            px = ""
            if CFG.get("proxy_enabled") and proxy_is_active_for_account(acc):
                p = acc.get("proxy") or {}
                px = f" 🧦 {p.get('type')}://{p.get('host')}:{p.get('port')}"
            lines.append(
                f"{mark} **{i}. {acc.get('name', '?')}**{main_mark}{sub_mark}{rr} "
                f"[{conn}]{err}\n"
                f"   Групп: {len(acc.get('groups') or [])} | "
                f"Искл: {len(acc.get('excluded') or [])} | "
                f"Пробл: {len(acc.get('problem') or [])}\n"
                f"   Source: {src} | "
                f"Sent: {s.get('sent', 0)} | ❌{s.get('errors', 0)}{px}")
    return "\n".join(lines)


def active_tasks_text() -> str:
    accounts = STATE.get("accounts") or []
    lines = ["🟢 **Активные задачи**\n"]
    active_any = False
    for acc in accounts:
        running = acc.get("running")
        sub_running = acc.get("subscribe_status") == "running"
        if not running and not sub_running:
            continue
        active_any = True
        s = acc.get("stats") or {}
        conn = "🔗" if acc["id"] in user_clients else "⚠️ offline"
        lines.append(f"**{acc.get('name')}** [{conn}]")
        if running:
            nxt = s.get("next_round") or "—"
            lines.append(
                f"  📢 рассылка: ✅{s.get('sent', 0)} | ❌{s.get('errors', 0)} | "
                f"кругов {s.get('rounds', 0)}")
            lines.append(f"     групп: {len(acc.get('groups') or [])} | "
                         f"след. круг: {nxt}")
        if sub_running:
            ss = acc.get("subscribe_stats") or {}
            qlen = len(acc.get("subscribe_queue") or [])
            lines.append(
                f"  📥 подписка: ✅{ss.get('subscribed', 0)} | "
                f"⏭{ss.get('skipped', 0)} | ❌{ss.get('errors', 0)} | в очереди {qlen}")
    if not active_any:
        lines.append("_Нет активных задач. Запусти рассылку или подписку._")
    return "\n".join(lines)


def active_tasks_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔄 Обновить", callback_data="active_tasks")],
        [InlineKeyboardButton("⏸ Остановить ВСЁ", callback_data="bulk_stop_all")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="menu")],
    ])


def bulk_start_text() -> str:
    accounts = STATE.get("accounts") or []
    sel_map = STATE.get("selected_for_bulk") or {}
    lines = ["🚀 **Массовый запуск**\n",
             "_Тап по аккаунту — исключить/включить._\n"]
    if not accounts:
        lines.append("_Нет аккаунтов._")
    else:
        for i, acc in enumerate(accounts, 1):
            checked = sel_map.get(acc["id"], True)
            box = "✅" if checked else "⬜️"
            run = "🟢" if acc.get("running") else "⚪️"
            conn = "🔗" if acc["id"] in user_clients else "⚠️"
            lines.append(f"{box} {run}{conn} {i}. **{acc.get('name')}** "
                         f"(групп: {len(acc.get('groups') or [])})")
    return "\n".join(lines)


def bulk_start_kb() -> InlineKeyboardMarkup:
    accounts = STATE.get("accounts") or []
    sel_map = STATE.get("selected_for_bulk") or {}
    rows = []
    for i, acc in enumerate(accounts, 1):
        aid = acc["id"]
        checked = sel_map.get(aid, True)
        box = "✅" if checked else "⬜️"
        name = (acc.get("name") or "—")[:25]
        rows.append([InlineKeyboardButton(f"{box} {i}. {name}",
                                          callback_data=f"bulk_toggle:{aid}")])
    if accounts:
        rows.append([
            InlineKeyboardButton("✅ Выбрать все", callback_data="bulk_select_all"),
            InlineKeyboardButton("⬜️ Снять все", callback_data="bulk_deselect_all"),
        ])
    rows.append([
        InlineKeyboardButton("🚀 Запустить выбранные", callback_data="bulk_start_go"),
    ])
    rows.append([
        InlineKeyboardButton("⏸ Остановить все", callback_data="bulk_stop_all"),
    ])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="menu")])
    return InlineKeyboardMarkup(rows)


async def bulk_start_selected():
    accounts = STATE.get("accounts") or []
    sel_map = STATE.get("selected_for_bulk") or {}
    started = 0
    skipped = 0
    for acc in accounts:
        aid = acc["id"]
        if not sel_map.get(aid, True):
            continue
        if acc.get("running"):
            skipped += 1
            continue
        if not user_clients.get(aid):
            skipped += 1
            continue
        if not acc.get("groups"):
            skipped += 1
            continue
        has_src = bool(acc.get("source_chat_id") and acc.get("source_message_id"))
        if not (has_src or account_has_content(acc)):
            skipped += 1
            continue
        acc["running"] = True
        try:
            await db_account_upsert(acc)
        except Exception:
            pass
        start_mailing_for_account(aid)
        started += 1
    await state_save()
    return started, skipped


async def bulk_stop_all():
    stopped = 0
    for acc in STATE.get("accounts") or []:
        if acc.get("running"):
            acc["running"] = False
            stopped += 1
        if acc.get("subscribe_status") == "running":
            acc["subscribe_status"] = "paused"
            stopped += 1
        try:
            await db_account_upsert(acc)
        except Exception:
            pass
    await state_save()
    return stopped


def pool_text_global(page: int = 0) -> str:
    pool = STATE.get("pool") or []
    total = len(pool)
    per_page = TEXTS_PER_PAGE
    pages = max(1, (total + per_page - 1) // per_page)
    page = max(0, min(page, pages - 1))
    start = page * per_page
    end = start + per_page
    page_items = pool[start:end]

    lines = [f"🧩 **Глобальный пул текстов** — всего {total}/{POOL_MAX}\n",
             f"_Применяется ко ВСЕМ аккаунтам. "
             f"Страница {page+1}/{pages}._\n"]
    if not total:
        lines.append("_Пусто. Добавь варианты — они будут чередоваться при рассылке._")
    else:
        for i, item in enumerate(page_items, 1):
            idx = start + i
            kind = "🖼" if item.get("media_type") else "📝"
            t = (item.get("text") or item.get("caption") or "")[:160]
            ent_n = len(item.get("text_entities") or item.get("caption_entities") or [])
            lines.append(f"**{idx}.** {kind} (entities: {ent_n})\n{t or '<медиа>'}\n")
    return "\n".join(lines)


def pool_kb_global(page: int = 0) -> InlineKeyboardMarkup:
    pool = STATE.get("pool") or []
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
            callback_data=f"pool_del:{start+i-1}:{page}")])

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅️", callback_data=f"pool_menu:{page-1}"))
    nav.append(InlineKeyboardButton(f"{page+1}/{pages}", callback_data="noop"))
    if page < pages - 1:
        nav.append(InlineKeyboardButton("➡️", callback_data=f"pool_menu:{page+1}"))
    if nav:
        rows.append(nav)

    if total < POOL_MAX:
        rows.append([InlineKeyboardButton("➕ Добавить текст/медиа в пул",
                                           callback_data="pool_add")])
    if total:
        rows.append([InlineKeyboardButton("💣 Очистить пул",
                                           callback_data="pool_clear")])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="menu")])
    return InlineKeyboardMarkup(rows)


def pool_clear_confirm_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Да, очистить", callback_data="pool_clear_ok")],
        [InlineKeyboardButton("❌ Отмена", callback_data="pool_menu:0")],
    ])


def reply_text(page: int = 0) -> str:
    replies = STATE.get("replies") or []
    accounts = STATE.get("accounts") or []
    total = len(replies)
    per_page = TEXTS_PER_PAGE
    pages = max(1, (total + per_page - 1) // per_page)
    page = max(0, min(page, pages - 1))
    start = page * per_page
    end = start + per_page
    page_items = replies[start:end]

    master = "🟢 ВКЛ" if STATE.get("auto_reply_enabled") else "⚪️ ВЫКЛ"
    cd = int(STATE.get("auto_reply_cooldown", DEFAULT_REPLY_COOLDOWN) or 0)
    on_accs = [a for a in accounts if a.get("auto_reply_enabled")]
    lines = [f"🤖 **Автоответчик** (мастер: {master})\n",
             f"• Кулдаун: **{cd}с** на пользователя\n"
             f"• Отвечает когда админ не в сети (>5 мин без активности)\n"
             f"• Аккаунтов с автоответом: **{len(on_accs)}/{len(accounts)}**\n"
             f"• Шаблонов: {total}. Страница {page+1}/{pages}.\n"]
    if not total:
        lines.append("_Пусто. Добавь шаблон — выбирается случайно._")
    else:
        for i, item in enumerate(page_items, 1):
            idx = start + i
            t = (item.get("text") or "")[:200]
            ent_n = len(item.get("text_entities") or [])
            lines.append(f"**{idx}.** (entities: {ent_n})\n{t}\n")
    return "\n".join(lines)


def reply_kb(page: int = 0) -> InlineKeyboardMarkup:
    replies = STATE.get("replies") or []
    total = len(replies)
    per_page = TEXTS_PER_PAGE
    pages = max(1, (total + per_page - 1) // per_page)
    page = max(0, min(page, pages - 1))
    start = page * per_page
    end = start + per_page
    page_items = replies[start:end]

    rows = []
    master = "⚪️ Выключить мастер" if STATE.get("auto_reply_enabled") else "🟢 Включить мастер"
    rows.append([InlineKeyboardButton(master, callback_data="reply_toggle")])
    rows.append([InlineKeyboardButton("🎯 Аккаунты (вкл/выкл)",
                                       callback_data="reply_accounts")])
    rows.append([InlineKeyboardButton("⏱ Кулдаун на пользователя",
                                       callback_data="reply_cooldown")])

    for i, item in enumerate(page_items, 1):
        idx = start + i
        t = (item.get("text") or "")[:30]
        rows.append([InlineKeyboardButton(
            f"❌ {idx}. {t}",
            callback_data=f"reply_del:{start+i-1}:{page}")])

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅️", callback_data=f"reply_menu:{page-1}"))
    nav.append(InlineKeyboardButton(f"{page+1}/{pages}", callback_data="noop"))
    if page < pages - 1:
        nav.append(InlineKeyboardButton("➡️", callback_data=f"reply_menu:{page+1}"))
    if nav:
        rows.append(nav)

    rows.append([InlineKeyboardButton("➕ Добавить шаблон", callback_data="reply_add")])
    if total:
        rows.append([InlineKeyboardButton("💣 Очистить шаблоны", callback_data="reply_clear")])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="menu")])
    return InlineKeyboardMarkup(rows)


def reply_clear_confirm_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Да, очистить", callback_data="reply_clear_ok")],
        [InlineKeyboardButton("❌ Отмена", callback_data="reply_menu:0")],
    ])


def reply_accounts_text() -> str:
    accounts = STATE.get("accounts") or []
    lines = ["🎯 **Автоответчик по аккаунтам**\n",
             "_Тап по аккаунту — вкл/выкл._\n"
             "_Мастер-выключатель должен быть ВКЛ._\n"]
    if not accounts:
        lines.append("_Нет аккаунтов._")
    else:
        for i, acc in enumerate(accounts, 1):
            box = "✅" if acc.get("auto_reply_enabled") else "⬜️"
            conn = "🔗" if acc["id"] in user_clients else "⚠️"
            lines.append(f"{box}{conn} {i}. **{acc.get('name')}**")
    return "\n".join(lines)


def reply_accounts_kb() -> InlineKeyboardMarkup:
    accounts = STATE.get("accounts") or []
    rows = []
    for i, acc in enumerate(accounts, 1):
        aid = acc["id"]
        box = "✅" if acc.get("auto_reply_enabled") else "⬜️"
        name = (acc.get("name") or "—")[:25]
        rows.append([InlineKeyboardButton(f"{box} {i}. {name}",
                                          callback_data=f"reply_acc_toggle:{aid}")])
    if accounts:
        rows.append([
            InlineKeyboardButton("✅ Все", callback_data="reply_acc_all"),
            InlineKeyboardButton("⬜️ Никто", callback_data="reply_acc_none"),
        ])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="reply_menu:0")])
    return InlineKeyboardMarkup(rows)


def account_kb(acc_id: str) -> InlineKeyboardMarkup:
    acc = get_account(acc_id)
    running = acc.get("running", False)
    is_main = acc.get("id") == STATE.get("main_account_id")
    sub_status = acc.get("subscribe_status", "idle")
    sub_queue_len = len(acc.get("subscribe_queue") or [])
    excl_len = len(acc.get("excluded") or [])
    prob_len = len(acc.get("problem") or [])
    proxy_on = CFG.get("proxy_enabled")
    proxy_set = proxy_is_active_for_account(acc)
    rr = "✅" if acc.get("auto_reply_enabled") else "❌"
    connected = acc["id"] in user_clients

    rows = [
        [InlineKeyboardButton("⏸ Стоп рассылку" if running else "🚀 Запустить рассылку",
                              callback_data=f"acc_toggle:{acc_id}")],
        [InlineKeyboardButton("📎 Источник (премиум)",
                              callback_data=f"acc_source:{acc_id}")],
        [InlineKeyboardButton("📝 Базовый текст", callback_data=f"acc_text:{acc_id}"),
         InlineKeyboardButton("🖼 Медиа", callback_data=f"acc_media:{acc_id}")],
        [InlineKeyboardButton("⏱ Тайминги", callback_data=f"acc_timing:{acc_id}")],
        [InlineKeyboardButton(
            f"🧦 Прокси: {'✅ ' + str(acc.get('proxy', {}).get('host')) if proxy_on and proxy_set else ('—' if proxy_on else 'глобально off')}",
            callback_data=f"acc_proxy:{acc_id}")],
        [InlineKeyboardButton(f"🤖 Автоответ: {rr}",
                              callback_data=f"acc_autoreply:{acc_id}")],
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
    if not connected:
        rows.append([InlineKeyboardButton("🔌 Переподключить",
                                          callback_data=f"acc_reconnect:{acc_id}")])
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
    pool_n = len(STATE.get("pool") or [])
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

    rr_line = "✅ включён" if acc.get("auto_reply_enabled") else "❌ выключен"
    conn = "🔗 online" if acc["id"] in user_clients else "⚠️ offline"
    err_n = acc.get("session_error_count", 0)
    last_err = acc.get("last_error") or ""
    err_block = ""
    if err_n or last_err:
        err_block = f"\n❗ Session errors: {err_n}\n_last:_ `{last_err[:120]}`"
    return (
        f"📢 **{acc.get('name', '?')}** {is_main}\n\n"
        f"• Статус: {'🟢' if acc.get('running') else '⚪️'} | {conn}\n"
        f"• Session string: {has_string}\n"
        f"• Групп: {len(acc.get('groups') or [])} | "
        f"🚫 {len(acc.get('excluded') or [])} | "
        f"⚠️ {len(acc.get('problem') or [])}\n"
        f"• Глобальный пул: {pool_n} вариантов, ротация каждые {acc.get('rotate_every', 17)}\n"
        f"• Интервал: {acc.get('interval', 1800)}с | "
        f"Задержка: {acc.get('delay_min', 5)}–{acc.get('delay_max', 15)}с\n"
        f"• Печатает: {acc.get('typing_seconds', DEFAULT_TYPING_SECONDS)}с\n"
        f"🤖 Автоответ: {rr_line}\n"
        f"🧦 Прокси: {proxy_line}\n"
        f"📎 Источник (copy): {src_line}\n"
        f"📊 Sent: {s.get('sent', 0)} | Err: {s.get('errors', 0)} | Кругов: {s.get('rounds', 0)}\n"
        f"📥 Подписка: {sub_status} | ✅{sub_stats.get('subscribed', 0)}\n"
        f"{err_block}\n"
        f"📝 Базовый текст {has_ent} (если пул пуст):\n{preview}"
    )


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
        "_Когда глобально выключено — прокси не применяется ни к одному аккаунту._"
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
                + (f" (user: {p.get('username')})" if p.get("username") else ""))
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
async def _store_from_message(acc: dict, message, acc_id: str) -> dict:
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
            log.warning(f"download media упал: {e}")

    return {
        "text": msg_text,
        "text_entities": entities_json,
        "media_path": None,
        "media_type": None,
        "caption": "",
        "caption_entities": [],
    }


async def _handle_pending(client, message, uid, act):
    action = act.get("action")
    raw_text = message.text or ""
    text = raw_text.strip()

    if action == "pool_add":
        try:
            item = await _store_from_message({}, message, "pool")
            if not item or (not item.get("text") and not item.get("media_path")):
                pending.pop(uid, None)
                await message.reply("❌ Пустое сообщение.")
                return
            item = _safe_json(item)
            await db_pool_add(item)
            STATE["pool"] = await db_pool_load()
            pending.pop(uid, None)
            try:
                await message.react(emoji="👍")
            except Exception:
                pass
            await message.reply(
                f"✅ В пуле теперь {len(STATE['pool'])} вариантов.",
                reply_markup=pool_kb_global(0))
        except Exception as e:
            log.exception("pool_add")
            pending.pop(uid, None)
            await message.reply(f"❌ {e}")
        return

    if action == "reply_add":
        if not raw_text:
            await message.reply("❌ Пустой текст.")
            return
        try:
            item = {
                "text": raw_text,
                "text_entities": entities_to_json(message.entities or []),
            }
            item = _safe_json(item)
            await db_replies_add(item)
            STATE["replies"] = await db_replies_load()
        except Exception as e:
            log.exception("reply_add")
            pending.pop(uid, None)
            await message.reply(f"❌ {e}")
            return
        pending.pop(uid, None)
        try:
            await message.react(emoji="👍")
        except Exception:
            pass
        await message.reply(
            f"✅ Шаблонов автоответа: {len(STATE['replies'])}.",
            reply_markup=reply_kb(0))
        return

    if action == "reply_cooldown":
        try:
            v = int(text)
            if v < 60:
                raise ValueError("мин. 60")
            if v > 86400:
                raise ValueError("макс 86400")
            STATE["auto_reply_cooldown"] = v
            await state_save()
            pending.pop(uid, None)
            await message.reply(f"✅ Кулдаун: {v}с.", reply_markup=reply_kb(0))
        except Exception as e:
            await message.reply(f"❌ {e}")
        return

    if action == "acc_source_forward":
        acc_id = act.get("acc_id")
        acc = get_account(acc_id)
        if not acc:
            pending.pop(uid, None)
            return
        try:
            item = await _store_from_message(acc, message, acc_id)
            if not item:
                pending.pop(uid, None)
                await message.reply("❌ Не удалось сохранить.")
                return
            acc["source_chat_id"] = None
            acc["source_message_id"] = None
            await db_account_upsert(acc)
            if len(STATE.get("pool") or []) < POOL_MAX:
                await db_pool_add(_safe_json(item))
                STATE["pool"] = await db_pool_load()
            pending.pop(uid, None)
            try:
                await message.react(emoji="👍")
            except Exception:
                pass
            await message.reply(
                f"✅ Добавлено в глобальный пул. Всего: {len(STATE.get('pool') or [])}.",
                reply_markup=account_kb(acc_id))
        except Exception as e:
            log.exception("acc_source_forward")
            pending.pop(uid, None)
            await message.reply(f"❌ {e}")
        return

    if action == "acc_rotate_set":
        acc_id = act.get("acc_id")
        acc = get_account(acc_id)
        try:
            v = int(text)
            if v < 5:
                raise ValueError("мин. 5")
            if v > 100:
                raise ValueError("макс 100")
            acc["rotate_every"] = v
            await db_account_upsert(acc)
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
        await db_account_upsert(acc)
        pending.pop(uid, None)
        await message.reply(
            "✅ Прокси сохранён.\n"
            "⚠️ Перезапусти сервис для применения.",
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
            acc = await add_account_async(name, sname)
            acc["user_id"] = me.id
            acc["username"] = me.username
            acc["session_string"] = cleaned
            await db_account_upsert(acc)
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
            chat_id = int(parts[0])
            msg_id = int(parts[1])
        except ValueError:
            await message.reply("❌ Только числа.")
            return
        acc["source_chat_id"] = chat_id
        acc["source_message_id"] = msg_id
        await db_account_upsert(acc)
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
        phone = act["phone"]
        hash_ = act["hash"]
        sname = act["session_name"]
        new_c: Client = act["client"]
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
        acc = await add_account_async(name, sname)
        acc["user_id"] = me.id
        acc["username"] = me.username
        await db_account_upsert(acc)
        user_clients[acc["id"]] = new_c
        ME_IDS[acc["id"]] = me.id
        attach_incoming_listener(new_c, acc["id"])
        asyncio.create_task(_mark_incoming_ready(acc["id"]))
        await message.reply(f"✅ **{name}**", reply_markup=main_menu_kb())
        return

    if action == "acc_add_password":
        pending.pop(uid, None)
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
        acc = await add_account_async(name, sname)
        acc["user_id"] = me.id
        acc["username"] = me.username
        await db_account_upsert(acc)
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
        await db_account_upsert(acc)
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
            lo = int(parts[0])
            hi = int(parts[1])
            if lo < 5:
                raise ValueError("мин. 5")
            if hi < lo:
                raise ValueError("макс >= мин")
            if hi > 1800:
                raise ValueError("макс 1800")
        except Exception as e:
            await message.reply(f"❌ {e}")
            return
        acc["subscribe_delay_min"] = lo
        acc["subscribe_delay_max"] = hi
        await db_account_upsert(acc)
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
        await db_account_upsert(acc)
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
        await db_account_upsert(acc)
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
            if v < 60:
                raise ValueError("мин. 60")
            acc["interval"] = v
            await db_account_upsert(acc)
            await message.reply(f"✅ Интервал: {v}с.", reply_markup=account_kb(acc["id"]))
        except Exception as e:
            await message.reply(f"❌ {e}")
        return

    if action == "acc_setdmin":
        pending.pop(uid, None)
        acc = get_account(act["acc_id"])
        try:
            v = int(text)
            if v < 1:
                raise ValueError("мин. 1")
            acc["delay_min"] = v
            if acc["delay_max"] < v:
                acc["delay_max"] = v
            await db_account_upsert(acc)
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
            await db_account_upsert(acc)
            await message.reply(f"✅ Макс: {v}с.", reply_markup=account_kb(acc["id"]))
        except Exception as e:
            await message.reply(f"❌ {e}")
        return

    if action == "acc_settyping":
        pending.pop(uid, None)
        acc = get_account(act["acc_id"])
        try:
            v = int(text)
            if v < 1 or v > 60:
                raise ValueError("1–60")
            acc["typing_seconds"] = v
            await db_account_upsert(acc)
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
        await db_account_upsert(acc)
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
        await db_account_upsert(acc)
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
#  ХЕНДЛЕРЫ
# ===========================================================================
async def _reconnect_account(acc_id: str) -> tuple:
    """Пробует переподключить один аккаунт. Возвращает (ok: bool, msg: str)."""
    acc = get_account(acc_id)
    if not acc:
        return False, "аккаунт не найден"
    # закрываем старый клиент если был
    old = user_clients.pop(acc_id, None)
    if old:
        try:
            await old.stop()
        except Exception:
            pass
    proxy = acc.get("proxy") if CFG.get("proxy_enabled") else None
    session_str = acc.get("session_string")
    c = None
    if session_str:
        ok, _ = validate_session_string(session_str)
        if ok:
            try:
                c = make_client(session_str, proxy=proxy)
            except Exception as e:
                return False, f"make_client: {e}"
    if c is None:
        sname = acc.get("session_name") or f"userbot_{acc_id}"
        try:
            c = make_client(sname, proxy=proxy)
        except Exception as e:
            return False, f"make_client: {e}"
    try:
        await asyncio.wait_for(c.start(), timeout=30)
        me = await c.get_me()
        if not me:
            raise RuntimeError("get_me вернул None")
        user_clients[acc_id] = c
        client_locks[acc_id] = asyncio.Lock()
        ME_IDS[acc_id] = me.id
        acc["user_id"] = me.id
        acc["username"] = me.username
        acc["session_error_count"] = 0
        acc["last_error"] = ""
        acc["last_error_iso"] = ""
        await db_account_upsert(acc)
        attach_incoming_listener(c, acc_id)
        asyncio.create_task(_mark_incoming_ready(acc_id))
        if acc.get("running"):
            start_mailing_for_account(acc_id)
        if acc.get("subscribe_status") == "running":
            start_subscribe(acc_id)
        return True, f"{me.first_name} id={me.id}"
    except Exception as e:
        _mark_session_error(acc, str(e))
        try:
            await c.stop()
        except Exception:
            pass
        await db_account_upsert(acc)
        return False, str(e)[:200]


def register_handlers(bot: Client) -> None:
    admin_id = int(CFG["admin_id"])
    log.info(f"Регистрирую хендлеры для admin_id={admin_id}")

    @bot.on_message(filters.private & filters.user(admin_id))
    async def on_admin_message(client, message):
        try:
            uid = message.from_user.id
            _admin_last_seen["ts"] = time.time()
            text = (message.text or message.caption or "").strip()
            cmd = text.split(maxsplit=1)[0].lower() if text.startswith("/") else ""

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

            if cmd == "/diag":
                if uid not in authed:
                    await message.reply("🔒 Сначала `/auth <PIN>`",
                                        parse_mode=enums.ParseMode.MARKDOWN)
                    return
                await message.reply(diag_text(), reply_markup=diag_kb(),
                                    parse_mode=enums.ParseMode.MARKDOWN)
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
                                    f"Доступно: /start /auth /panel /cancel /addsession /diag")
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
        _admin_last_seen["ts"] = time.time()
        data = cb.data or ""
        try:
            if data == "menu":
                await cb.message.edit_text("🎛 Панель:", reply_markup=main_menu_kb())
            elif data == "noop":
                await cb.answer()
            elif data == "diag_menu":
                await cb.message.edit_text(diag_text(), reply_markup=diag_kb(),
                                            parse_mode=enums.ParseMode.MARKDOWN)
            elif data == "diag_reset_errors":
                for a in STATE.get("accounts") or []:
                    a["session_error_count"] = 0
                    a["last_error"] = ""
                    a["last_error_iso"] = ""
                    await db_account_upsert(a)
                await cb.answer("Сброшено")
                await cb.message.edit_text(diag_text(), reply_markup=diag_kb(),
                                            parse_mode=enums.ParseMode.MARKDOWN)
            elif data == "accounts_menu":
                await cb.message.edit_text(accounts_menu_text(), reply_markup=accounts_menu_kb())
            elif data == "active_tasks":
                await cb.message.edit_text(active_tasks_text(),
                                            reply_markup=active_tasks_kb())
            elif data == "bulk_start_menu":
                await cb.message.edit_text(bulk_start_text(),
                                            reply_markup=bulk_start_kb())
            elif data.startswith("bulk_toggle:"):
                aid = data.split(":", 1)[1]
                sel = STATE.setdefault("selected_for_bulk", {})
                sel[aid] = not sel.get(aid, True)
                await state_save()
                await cb.answer("✅" if sel[aid] else "⬜️")
                await cb.message.edit_text(bulk_start_text(),
                                            reply_markup=bulk_start_kb())
            elif data == "bulk_select_all":
                sel = STATE.setdefault("selected_for_bulk", {})
                for a in STATE.get("accounts") or []:
                    sel[a["id"]] = True
                await state_save()
                await cb.answer("Все выбраны")
                await cb.message.edit_text(bulk_start_text(),
                                            reply_markup=bulk_start_kb())
            elif data == "bulk_deselect_all":
                sel = STATE.setdefault("selected_for_bulk", {})
                for a in STATE.get("accounts") or []:
                    sel[a["id"]] = False
                await state_save()
                await cb.answer("Все сняты")
                await cb.message.edit_text(bulk_start_text(),
                                            reply_markup=bulk_start_kb())
            elif data == "bulk_start_go":
                started, skipped = await bulk_start_selected()
                await cb.answer(f"🚀 {started} | ⏭ {skipped}", show_alert=False)
                await cb.message.edit_text(
                    f"🚀 Запущено: **{started}**\n⏭ Пропущено: **{skipped}**\n\n"
                    + active_tasks_text(),
                    reply_markup=active_tasks_kb())
            elif data == "bulk_stop_all":
                n = await bulk_stop_all()
                await cb.answer(f"Остановлено: {n}", show_alert=False)
                await cb.message.edit_text(
                    f"⏸ Остановлено всего: **{n}**\n\n" + active_tasks_text(),
                    reply_markup=active_tasks_kb())

            elif data.startswith("pool_menu:"):
                try:
                    page = int(data.split(":", 1)[1])
                except Exception:
                    page = 0
                await cb.message.edit_text(pool_text_global(page),
                                            reply_markup=pool_kb_global(page))
            elif data == "pool_add":
                if len(STATE.get("pool") or []) >= POOL_MAX:
                    await cb.answer(f"Максимум {POOL_MAX}", show_alert=True)
                    return
                pending[uid] = {"action": "pool_add"}
                await cb.message.edit_text(
                    "➕ Отправь текст (можно с премиум-эмодзи), "
                    "или фото/видео с подписью — пойдёт в пул.\n/cancel")
            elif data.startswith("pool_del:"):
                parts = data.split(":")
                if len(parts) < 3:
                    await cb.answer()
                    return
                try:
                    idx = int(parts[1])
                    page = int(parts[2])
                except ValueError:
                    await cb.answer()
                    return
                await db_pool_delete(idx)
                STATE["pool"] = await db_pool_load()
                total = len(STATE["pool"])
                pages = max(1, (total + TEXTS_PER_PAGE - 1) // TEXTS_PER_PAGE)
                page = max(0, min(page, pages - 1))
                await cb.answer("Удалено")
                await cb.message.edit_text(pool_text_global(page),
                                            reply_markup=pool_kb_global(page))
            elif data == "pool_clear":
                if not STATE.get("pool"):
                    await cb.answer("Пусто")
                    return
                await cb.message.edit_text(
                    "💣 Очистить глобальный пул?",
                    reply_markup=pool_clear_confirm_kb())
            elif data == "pool_clear_ok":
                await db_pool_clear()
                STATE["pool"] = []
                await cb.answer("Пул очищен")
                await cb.message.edit_text(pool_text_global(0),
                                            reply_markup=pool_kb_global(0))

            elif data.startswith("reply_menu:"):
                try:
                    page = int(data.split(":", 1)[1])
                except Exception:
                    page = 0
                await cb.message.edit_text(reply_text(page), reply_markup=reply_kb(page))
            elif data == "reply_toggle":
                STATE["auto_reply_enabled"] = not bool(STATE.get("auto_reply_enabled"))
                await state_save()
                await cb.answer("ВКЛ" if STATE["auto_reply_enabled"] else "ВЫКЛ")
                await cb.message.edit_text(reply_text(0), reply_markup=reply_kb(0))
            elif data == "reply_accounts":
                await cb.message.edit_text(reply_accounts_text(),
                                            reply_markup=reply_accounts_kb())
            elif data.startswith("reply_acc_toggle:"):
                aid = data.split(":", 1)[1]
                acc = get_account(aid)
                if acc:
                    acc["auto_reply_enabled"] = not bool(acc.get("auto_reply_enabled"))
                    await db_account_upsert(acc)
                await cb.answer()
                await cb.message.edit_text(reply_accounts_text(),
                                            reply_markup=reply_accounts_kb())
            elif data == "reply_acc_all":
                for a in STATE.get("accounts") or []:
                    a["auto_reply_enabled"] = True
                    await db_account_upsert(a)
                await cb.answer("Все вкл")
                await cb.message.edit_text(reply_accounts_text(),
                                            reply_markup=reply_accounts_kb())
            elif data == "reply_acc_none":
                for a in STATE.get("accounts") or []:
                    a["auto_reply_enabled"] = False
                    await db_account_upsert(a)
                await cb.answer("Все выкл")
                await cb.message.edit_text(reply_accounts_text(),
                                            reply_markup=reply_accounts_kb())
            elif data == "reply_cooldown":
                pending[uid] = {"action": "reply_cooldown"}
                await cb.message.edit_text(
                    f"⏱ Кулдаун (сек) на пользователя. "
                    f"Сейчас: {STATE.get('auto_reply_cooldown')}\n/cancel")
            elif data == "reply_add":
                pending[uid] = {"action": "reply_add"}
                await cb.message.edit_text(
                    "➕ Отправь текст шаблона автоответа (можно с премиум-эмодзи).\n/cancel")
            elif data.startswith("reply_del:"):
                parts = data.split(":")
                if len(parts) < 3:
                    await cb.answer()
                    return
                try:
                    idx = int(parts[1])
                    page = int(parts[2])
                except ValueError:
                    await cb.answer()
                    return
                await db_replies_delete(idx)
                STATE["replies"] = await db_replies_load()
                total = len(STATE["replies"])
                pages = max(1, (total + TEXTS_PER_PAGE - 1) // TEXTS_PER_PAGE)
                page = max(0, min(page, pages - 1))
                await cb.answer("Удалено")
                await cb.message.edit_text(reply_text(page), reply_markup=reply_kb(page))
            elif data == "reply_clear":
                if not STATE.get("replies"):
                    await cb.answer("Пусто")
                    return
                await cb.message.edit_text(
                    "💣 Очистить шаблоны автоответа?",
                    reply_markup=reply_clear_confirm_kb())
            elif data == "reply_clear_ok":
                await db_replies_clear()
                STATE["replies"] = []
                await cb.answer("Очищено")
                await cb.message.edit_text(reply_text(0), reply_markup=reply_kb(0))

            elif data == "sub_main":
                await cb.message.edit_text(sub_main_text(), reply_markup=sub_main_kb())
            elif data == "stats_menu":
                await cb.message.edit_text(stats_menu_text(), reply_markup=stats_menu_kb())

            elif data == "proxy_global_menu":
                await cb.message.edit_text(proxy_global_text(),
                                            reply_markup=proxy_global_kb())
            elif data == "proxy_global_toggle":
                CFG["proxy_enabled"] = not bool(CFG.get("proxy_enabled"))
                await state_save()
                await cb.answer(f"Прокси: {'ВКЛ' if CFG['proxy_enabled'] else 'ВЫКЛ'}")
                await cb.message.edit_text(proxy_global_text(),
                                            reply_markup=proxy_global_kb())

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

            elif data.startswith("acc_open:"):
                acc_id = data.split(":", 1)[1]
                if not get_account(acc_id):
                    await cb.answer("Аккаунт не найден в STATE", show_alert=True)
                    return
                await cb.message.edit_text(account_text(acc_id), reply_markup=account_kb(acc_id))
            elif data.startswith("acc_toggle:"):
                acc_id = data.split(":", 1)[1]
                acc = get_account(acc_id)
                if not acc:
                    await cb.answer("Аккаунт не найден", show_alert=True)
                    return
                if not user_clients.get(acc_id):
                    await cb.answer("🔌 Клиент не подключён. Нажми «Переподключить».",
                                    show_alert=True)
                    return
                if acc.get("running"):
                    acc["running"] = False
                else:
                    if not acc.get("groups"):
                        await cb.answer("Нет групп", show_alert=True)
                        return
                    has_src = bool(acc.get("source_chat_id") and acc.get("source_message_id"))
                    if not (has_src or account_has_content(acc)):
                        await cb.answer("Нет контента (пул пуст и нет базового текста)",
                                        show_alert=True)
                        return
                    acc["running"] = True
                    start_mailing_for_account(acc_id)
                await db_account_upsert(acc)
                await cb.answer()
                await cb.message.edit_text(account_text(acc_id), reply_markup=account_kb(acc_id))
            elif data.startswith("acc_reconnect:"):
                acc_id = data.split(":", 1)[1]
                await cb.answer("Подключаюсь…")
                ok, msg = await _reconnect_account(acc_id)
                try:
                    if ok:
                        await cb.message.edit_text(
                            f"✅ Переподключён: {msg}\n\n" + account_text(acc_id),
                            reply_markup=account_kb(acc_id))
                    else:
                        await cb.message.edit_text(
                            f"❌ Не удалось: {msg}\n\n" + account_text(acc_id),
                            reply_markup=account_kb(acc_id))
                except Exception:
                    pass
            elif data.startswith("acc_autoreply:"):
                acc_id = data.split(":", 1)[1]
                acc = get_account(acc_id)
                if not acc:
                    await cb.answer()
                    return
                acc["auto_reply_enabled"] = not bool(acc.get("auto_reply_enabled"))
                await db_account_upsert(acc)
                await cb.answer("ВКЛ" if acc["auto_reply_enabled"] else "ВЫКЛ")
                await cb.message.edit_text(account_text(acc_id), reply_markup=account_kb(acc_id))

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
                    await db_account_upsert(acc)
                await cb.answer("Прокси очищен")
                await cb.message.edit_text(account_proxy_text(acc_id),
                                            reply_markup=account_proxy_kb(acc_id))

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
                    f"«Переслать сообщение боту» добавит содержимое в **глобальный пул**.",
                    reply_markup=kb)
            elif data.startswith("acc_source_forward:"):
                acc_id = data.split(":", 1)[1]
                pending[uid] = {"action": "acc_source_forward", "acc_id": acc_id}
                await cb.message.edit_text(
                    "📨 Перешли сюда сообщение с премиум-эмодзи — "
                    "добавлю в глобальный пул.\n/cancel")
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
                    await db_account_upsert(acc)
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
                    f"Пауза: {acc.get('delay_min', 5)}–{acc.get('delay_max', 15)}с "
                    f"×1.0–1.6, плюс длинные паузы "
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
                    await cb.answer("Клиент не подключён", show_alert=True)
                    return
                await cb.answer()
                await cb.message.edit_text("🔍 Сканирую (только группы)…")
                found, skipped = await scan_groups_for_account(acc_id)
                acc = get_account(acc_id)
                scanned = {g["id"] for g in found}
                manual = [g for g in (acc.get("groups") or [])
                          if g.get("manual") and g["id"] not in scanned]
                acc["groups"] = found + manual
                await db_account_upsert(acc)
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
                    gid = int(gid_s)
                    page = int(page_s)
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
                await db_account_upsert(acc)
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
                await db_account_upsert(acc)
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
                    gid = int(gid_s)
                    page = int(page_s)
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
                    await db_account_upsert(acc)
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
                await db_account_upsert(acc)
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
                    gid = int(gid_s)
                    page = int(page_s)
                except ValueError:
                    await cb.answer()
                    return
                acc = get_account(acc_id)
                if not acc:
                    await cb.answer()
                    return
                clear_problem(acc, gid)
                await db_account_upsert(acc)
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
                await db_account_upsert(acc)
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
                await db_account_upsert(acc)
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
                    await state_save()
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
                    await remove_account_async(acc_id)
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
                    await cb.answer("Очередь пуста", show_alert=True)
                    return
                if not user_clients.get(acc_id):
                    await cb.answer("Клиент не подключён", show_alert=True)
                    return
                acc["subscribe_status"] = "running"
                await db_account_upsert(acc)
                start_subscribe(acc_id)
                await cb.answer()
                await cb.message.edit_text(subscribe_menu_text(acc_id),
                                            reply_markup=subscribe_menu_kb(acc_id))
            elif data.startswith("acc_sub_stop:"):
                acc_id = data.split(":", 1)[1]
                acc = get_account(acc_id)
                if acc:
                    acc["subscribe_status"] = "paused"
                    await db_account_upsert(acc)
                await cb.answer()
                await cb.message.edit_text(subscribe_menu_text(acc_id),
                                            reply_markup=subscribe_menu_kb(acc_id))
            elif data.startswith("acc_sub_clear:"):
                acc_id = data.split(":", 1)[1]
                acc = get_account(acc_id)
                if acc:
                    acc["subscribe_queue"] = []
                    acc["subscribe_status"] = "idle"
                    await db_account_upsert(acc)
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


# ===========================================================================
#  ЗАПУСК
# ===========================================================================
def load_cfg_from_env() -> dict:
    cfg = {}
    env_map = {
        "API_ID": ("api_id", int),
        "API_HASH": ("api_hash", str),
        "BOT_TOKEN": ("bot_token", str),
        "ADMIN_ID": ("admin_id", int),
        "PIN": ("pin", str),
    }
    for env, (key, typ) in env_map.items():
        v = os.getenv(env)
        if v is None or v == "":
            continue
        try:
            cfg[key] = typ(v)
        except Exception:
            log.warning(f"Некорректное значение {env}={v!r}")
    cfg.setdefault("proxy_enabled", False)
    cfg.setdefault("pin", DEFAULT_PIN)
    return cfg


async def main():
    global CFG, STATE, bot_client, _boot_ts, _persistence_ok

    _boot_ts = time.time()
    CFG = load_cfg_from_env()
    if not all(CFG.get(k) for k in ("api_id", "api_hash", "bot_token", "admin_id")):
        print("Не заданы переменные: API_ID, API_HASH, BOT_TOKEN, ADMIN_ID")
        return

    log.info("=" * 60)
    log.info(f"STARTUP | DATA_DIR = {DATA_DIR}")
    log.info(f"STARTUP | DB = {DB_FILE}")
    _persistence_ok = check_persistence()
    log.info(f"STARTUP | ADMIN_ID={CFG['admin_id']}")
    log.info("=" * 60)

    await db_init()
    log.info(f"STARTUP | DB size = {db_size_bytes()} bytes")
    await state_load()

    await bootstrap_default_session()
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
        rr_on = sum(1 for a in accounts if a.get("auto_reply_enabled"))
        clients_on = len(user_clients)
        warn = ""
        if not _persistence_ok:
            warn = ("\n\n⚠️ **/data НЕ переживает рестарт!** Проверь amvera.yml "
                    "и тариф. Диагностика: /diag")
        await bot_client.send_message(
            CFG["admin_id"],
            f"🤖 Бот запущен.\n"
            f"• Аккаунтов в БД: **{len(accounts)}** | клиентов: **{clients_on}**\n"
            f"• Пул текстов: {len(STATE.get('pool') or [])}\n"
            f"• Шаблонов автоответа: {len(STATE.get('replies') or [])}\n"
            f"• Автоответ включён у: {rr_on} акк.\n"
            f"🧦 Прокси: {'ВКЛ' if CFG.get('proxy_enabled') else 'ВЫКЛ'}\n"
            f"Для доступа: `/auth <PIN>`"
            f"{warn}",
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