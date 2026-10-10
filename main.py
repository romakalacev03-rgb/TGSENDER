# -*- coding: utf-8 -*-
"""
Мульти-аккаунт автопостер + рассылка + массовая подписка.
Без ИИ и автоответчика.
"""

import asyncio
import json
import os
import random
import re
import logging
from datetime import datetime, timedelta

import aiosqlite

from pyrogram import Client, filters, enums
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.errors import (
    FloodWait, ChatWriteForbidden, ChatAdminRequired, UserBannedInChannel,
    PeerIdInvalid, UserIsBlocked, ChannelPrivate,
    SessionPasswordNeeded, PhoneCodeInvalid, PhoneCodeExpired, PasswordHashInvalid,
    UserAlreadyParticipant, UsernameInvalid, UsernameNotOccupied,
    InviteHashExpired, InviteHashInvalid, ChatIdInvalid,
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

user_clients: dict = {}       # {acc_id: Client}
mailing_tasks: dict = {}      # {acc_id: asyncio.Task}
subscribe_tasks: dict = {}    # {acc_id: asyncio.Task}
ME_IDS: dict = {}             # {acc_id: user_id}

authed: set = set()
pending: dict = {}

DEVICE_PARAMS = {
    "app_version": "9.3.1",
    "device_model": "Samsung Galaxy S23 Ultra",
    "system_version": "Android 13",
    "lang_code": "ru",
}


def make_client(session_name_or_string: str) -> Client:
    """Авто-определение: session_string или имя файла."""
    raw = session_name_or_string or ""
    s = re.sub(r"\s+", "", raw).strip('"').strip("'").strip("`")
    log.info(f"[CLIENT] raw_len={len(raw)} clean_len={len(s)} head={s[:30]}")

    if len(s) > 100:
        log.info("[CLIENT] session_string")
        return Client(
            name=":memory:",
            api_id=CFG["api_id"],
            api_hash=CFG["api_hash"],
            session_string=s,
            app_version=DEVICE_PARAMS["app_version"],
            device_model=DEVICE_PARAMS["device_model"],
            system_version=DEVICE_PARAMS["system_version"],
            lang_code=DEVICE_PARAMS["lang_code"],
        )

    if len(s) > 200:
        s = f"userbot_{int(datetime.now().timestamp())}"
    log.info(f"[CLIENT] Файл: {s}")
    session_path = os.path.join(DATA_DIR, s)
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


def _new_account(name: str, session_name: str) -> dict:
    return {
        "id": f"acc_{int(datetime.now().timestamp() * 1000)}_{random.randint(100, 999)}",
        "name": name, "session_name": session_name, "session_string": "",
        "user_id": None, "username": None,
        "text": "", "caption": "", "media_path": None, "media_type": None,
        "groups": [], "running": False,
        "interval": 1800, "delay_min": 5, "delay_max": 15,
        "stats": {"sent": 0, "errors": 0, "rounds": 0, "last_round": None, "next_round": None},
        "subscribe_queue": [], "subscribe_status": "idle",
        "subscribe_delay_min": 40, "subscribe_delay_max": 120,
        "subscribe_stats": {"subscribed": 0, "skipped": 0, "errors": 0, "started_at": None},
    }


def default_state() -> dict:
    return {
        "accounts": [],
        "main_account_id": "",
        "global_stats": {"total_sent": 0, "total_errors": 0, "total_subscribed": 0},
    }


def load_state() -> dict:
    raw = load_json(STATE_FILE, {}) or {}
    st = default_state()

    # Миграция со старой схемы
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

    if isinstance(raw.get("accounts"), list):
        st["accounts"] = raw["accounts"]
    st["main_account_id"] = raw.get("main_account_id") or st.get("main_account_id") or ""
    if not isinstance(raw.get("global_stats"), dict):
        st["global_stats"] = default_state()["global_stats"]
    for k in ("total_sent", "total_errors", "total_subscribed"):
        st["global_stats"].setdefault(k, 0)

    if not st.get("main_account_id") and st.get("accounts"):
        st["main_account_id"] = st["accounts"][0]["id"]

    # Доп. поля на всякий случай
    for acc in st.get("accounts") or []:
        acc.setdefault("session_string", "")
        acc.setdefault("subscribe_queue", [])
        acc.setdefault("subscribe_status", "idle")
        acc.setdefault("subscribe_delay_min", 40)
        acc.setdefault("subscribe_delay_max", 120)
        acc.setdefault("subscribe_stats", {"subscribed": 0, "skipped": 0, "errors": 0, "started_at": None})
        acc.setdefault("stats", {"sent": 0, "errors": 0, "rounds": 0})
    return st


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
    s = s.split("?")[0].split("/")[0].strip()
    if not s:
        return None
    if s.startswith("@"):
        return s
    try:
        return int(s)
    except ValueError:
        return "@" + s


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
        for g in groups:
            if not acc.get("running"):
                break
            gid = g.get("id")
            try:
                await send_post_for_account(acc_id, gid)
                sent += 1
                acc["stats"]["sent"] = acc["stats"].get("sent", 0) + 1
            except FloodWait as fw:
                await asyncio.sleep(fw.value + 2)
                try:
                    await send_post_for_account(acc_id, gid)
                    sent += 1
                    acc["stats"]["sent"] = acc["stats"].get("sent", 0) + 1
                except Exception:
                    errors += 1
                    acc["stats"]["errors"] = acc["stats"].get("errors", 0) + 1
            except (ChatWriteForbidden, ChatAdminRequired, UserBannedInChannel,
                    PeerIdInvalid, UserIsBlocked, ChannelPrivate):
                errors += 1
                acc["stats"]["errors"] = acc["stats"].get("errors", 0) + 1
            except Exception:
                errors += 1
                acc["stats"]["errors"] = acc["stats"].get("errors", 0) + 1
            try:
                lo = int(acc.get("delay_min", 5))
                hi = int(acc.get("delay_max", 15))
                if hi < lo:
                    hi = lo
                await asyncio.sleep(random.randint(lo, hi))
            except Exception:
                await asyncio.sleep(5)

        acc["stats"]["rounds"] = acc["stats"].get("rounds", 0) + 1
        acc["stats"]["last_round"] = datetime.now().isoformat(timespec="seconds")
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


# ---------------------------------------------------------------------------
# Массовая подписка
# ---------------------------------------------------------------------------

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
                exists = any(g["id"] == gid for g in (acc.get("groups") or []))
                if not exists:
                    acc.setdefault("groups", []).append({
                        "id": gid, "title": chat.title or str(gid),
                        "type": chat.type.name if chat.type else "UNKNOWN",
                        "manual": True})
            queue.pop(0)
        elif status == "already":
            stats["skipped"] += 1
            if chat:
                gid = chat.id
                exists = any(g["id"] == gid for g in (acc.get("groups") or []))
                if not exists:
                    acc.setdefault("groups", []).append({
                        "id": gid, "title": chat.title or str(gid),
                        "type": chat.type.name if chat.type else "UNKNOWN",
                        "manual": True})
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
# Логин / запуск клиентов
# ---------------------------------------------------------------------------

async def start_userbot_clients():
    accounts = STATE.get("accounts") or []
    for acc in accounts:
        acc_id = acc["id"]
        session_str = acc.get("session_string")
        if session_str:
            try:
                c = make_client(session_str)
                user_clients[acc_id] = c
                continue
            except Exception as e:
                log.warning(f"session_string {acc_id}: {e}")
        session_name = acc.get("session_name") or f"userbot_{acc_id}"
        try:
            c = make_client(session_name)
            user_clients[acc_id] = c
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
            await c.start()
            me = await c.get_me()
            if me:
                ME_IDS[acc_id] = me.id
                acc["user_id"] = me.id
                acc["username"] = me.username
                log.info(f"✅ [{acc.get('name')}] {me.first_name} id={me.id}")
                if acc.get("running"):
                    start_mailing_for_account(acc_id)
                if acc.get("subscribe_status") == "running":
                    start_subscribe(acc_id)
        except Exception as e:
            log.warning(f"[{acc.get('name')}] не залогинен: {e}")
            try:
                if not c.is_connected:
                    await c.connect()
            except Exception:
                pass
    save_json(STATE_FILE, STATE)


# ---------------------------------------------------------------------------
# Клавиатуры
# ---------------------------------------------------------------------------

def main_menu_kb() -> InlineKeyboardMarkup:
    accounts = STATE.get("accounts") or []
    running_count = sum(1 for a in accounts if a.get("running"))
    subs_count = sum(1 for a in accounts if a.get("subscribe_status") == "running")
    subs_mark = f" 📥{subs_count}" if subs_count else ""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"📢 Рассылка ({running_count}/{len(accounts)})",
                              callback_data="accounts_menu")],
        [InlineKeyboardButton(f"📥 Массовая подписка{subs_mark}",
                              callback_data="sub_main")],
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
        rows.append([InlineKeyboardButton("🔀 Пересечения",
                                          callback_data="acc_check_overlaps")])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="menu")])
    return InlineKeyboardMarkup(rows)


def accounts_menu_text() -> str:
    accounts = STATE.get("accounts") or []
    main_id = STATE.get("main_account_id") or ""
    lines = ["📢 Рассылка по аккаунтам\n"]
    if not accounts:
        lines.append("_Нет аккаунтов._")
    else:
        for i, acc in enumerate(accounts, 1):
            mark = "🟢" if acc.get("running") else "⚪️"
            main_mark = " ⭐" if acc.get("id") == main_id else ""
            sub_mark = " 📥" if acc.get("subscribe_status") == "running" else ""
            s = acc.get("stats") or {}
            lines.append(f"{mark} **{i}. {acc.get('name', '?')}**{main_mark}{sub_mark}\n"
                         f"   Групп: {len(acc.get('groups') or [])} | "
                         f"Sent: {s.get('sent', 0)} | ❌{s.get('errors', 0)}")
    return "\n".join(lines)


def account_kb(acc_id: str) -> InlineKeyboardMarkup:
    acc = get_account(acc_id)
    running = acc.get("running", False)
    is_main = acc.get("id") == STATE.get("main_account_id")
    sub_status = acc.get("subscribe_status", "idle")
    sub_queue_len = len(acc.get("subscribe_queue") or [])
    rows = [
        [InlineKeyboardButton("⏸ Стоп рассылку" if running else "🚀 Запустить рассылку",
                              callback_data=f"acc_toggle:{acc_id}")],
        [InlineKeyboardButton("📝 Текст", callback_data=f"acc_text:{acc_id}")],
        [InlineKeyboardButton("🖼 Медиа", callback_data=f"acc_media:{acc_id}")],
        [InlineKeyboardButton("⏱ Тайминги", callback_data=f"acc_timing:{acc_id}")],
        [InlineKeyboardButton("🔍 Скан групп", callback_data=f"acc_scan:{acc_id}")],
        [InlineKeyboardButton("➕ Группу", callback_data=f"acc_addgrp:{acc_id}"),
         InlineKeyboardButton("🗑 Список", callback_data=f"acc_delgrp:{acc_id}")],
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
    rows.append([InlineKeyboardButton("🗑 Удалить",
                                       callback_data=f"acc_remove:{acc_id}")])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="accounts_menu")])
    return InlineKeyboardMarkup(rows)


def account_text(acc_id: str) -> str:
    acc = get_account(acc_id)
    if not acc:
        return "Не найден."
    s = acc.get("stats") or {}
    txt = (acc.get("text") or "").strip()
    media = acc.get("media_type")
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
    return (
        f"📢 **{acc.get('name', '?')}** {is_main}\n\n"
        f"• Статус: {'🟢' if acc.get('running') else '⚪️'}\n"
        f"• Session: {has_string}\n"
        f"• Групп: {len(acc.get('groups') or [])}\n"
        f"• Интервал: {acc.get('interval', 1800)}с\n"
        f"• Задержка: {acc.get('delay_min', 5)}–{acc.get('delay_max', 15)}с\n"
        f"📊 Sent: {s.get('sent', 0)} | Err: {s.get('errors', 0)} | Кругов: {s.get('rounds', 0)}\n"
        f"📥 Подписка: {sub_status} | ✅{sub_stats.get('subscribed', 0)}\n\n"
        f"📝 Текст:\n{preview}"
    )


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
    gs = STATE.get("global_stats") or {}
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
    
# ---------------------------------------------------------------------------
# Обработчики бота
# ---------------------------------------------------------------------------

def register_handlers(bot: Client) -> None:

    @bot.on_message(filters.command("start") & filters.private)
    async def cmd_start(client, message):
        if message.from_user.id != CFG["admin_id"]:
            await message.reply("⛔")
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
            await message.reply("❌")
            return
        authed.add(message.from_user.id)
        pending.pop(message.from_user.id, None)
        await message.reply("✅", reply_markup=main_menu_kb())

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

    @bot.on_message(filters.command("addsession") & filters.private)
    async def cmd_addsession(client, message):
        if message.from_user.id != CFG["admin_id"] or message.from_user.id not in authed:
            return
        pending[message.from_user.id] = {"action": "add_session_string"}
        await message.reply(
            "📝 Отправь **строку сессии** одним сообщением.\n\n"
            "⚠️ Никому не пересылай! /cancel",
            parse_mode=enums.ParseMode.MARKDOWN)

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
            elif data == "sub_main":
                await cb.message.edit_text(sub_main_text(), reply_markup=sub_main_kb())
            elif data == "accounts_menu":
                await cb.message.edit_text(accounts_menu_text(), reply_markup=accounts_menu_kb())
            elif data == "acc_add":
                pending[uid] = {"action": "acc_add_phone"}
                await cb.message.edit_text(
                    "➕ **Добавление**\n\nНомер в формате `+79991234567`.\n\n/cancel",
                    parse_mode=enums.ParseMode.MARKDOWN)
            elif data == "acc_check_overlaps":
                overlaps = check_group_overlaps()
                if not overlaps:
                    await cb.answer("✅ Нет пересечений!", show_alert=True)
                    return
                lines = ["⚠️ Пересечения:\n"]
                id_to_name = {a["id"]: a.get("name", "?") for a in (STATE.get("accounts") or [])}
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
                        [InlineKeyboardButton("⬅️", callback_data="accounts_menu")]]))
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
                await cb.message.edit_text("📝 Отправь текст.\n/cancel")
            elif data.startswith("acc_media:"):
                acc_id = data.split(":", 1)[1]
                pending[uid] = {"action": "acc_media", "acc_id": acc_id}
                await cb.message.edit_text("🖼 Отправь фото/видео.\n/cancel")
            elif data.startswith("acc_timing:"):
                acc_id = data.split(":", 1)[1]
                acc = get_account(acc_id)
                if not acc:
                    return
                kb = InlineKeyboardMarkup([
                    [InlineKeyboardButton(f"⏱ {acc.get('interval', 1800)}с",
                                          callback_data=f"acc_setint:{acc_id}")],
                    [InlineKeyboardButton(f"⏳ Мин {acc.get('delay_min', 5)}с",
                                          callback_data=f"acc_setdmin:{acc_id}")],
                    [InlineKeyboardButton(f"⏳ Макс {acc.get('delay_max', 15)}с",
                                          callback_data=f"acc_setdmax:{acc_id}")],
                    [InlineKeyboardButton("⬅️", callback_data=f"acc_open:{acc_id}")],
                ])
                await cb.message.edit_text(f"⏱ Тайминги **{acc.get('name')}**", reply_markup=kb)
            elif data.startswith("acc_setint:"):
                acc_id = data.split(":", 1)[1]
                pending[uid] = {"action": "acc_setint", "acc_id": acc_id}
                await cb.message.edit_text("Интервал (>=60):\n/cancel")
            elif data.startswith("acc_setdmin:"):
                acc_id = data.split(":", 1)[1]
                pending[uid] = {"action": "acc_setdmin", "acc_id": acc_id}
                await cb.message.edit_text("Мин (>=1):\n/cancel")
            elif data.startswith("acc_setdmax:"):
                acc_id = data.split(":", 1)[1]
                pending[uid] = {"action": "acc_setdmax", "acc_id": acc_id}
                await cb.message.edit_text("Макс:\n/cancel")
            elif data.startswith("acc_scan:"):
                acc_id = data.split(":", 1)[1]
                c = user_clients.get(acc_id)
                if not c:
                    await cb.answer("Нет клиента.", show_alert=True)
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
                rows.append([InlineKeyboardButton("⬅️", callback_data=f"acc_open:{acc_id}")])
                await cb.message.edit_text("Удалить:", reply_markup=InlineKeyboardMarkup(rows))
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
                    await cb.answer("⭐", show_alert=True)
                await cb.message.edit_text(account_text(acc_id), reply_markup=account_kb(acc_id))
            elif data.startswith("acc_remove:"):
                acc_id = data.split(":", 1)[1]
                kb = InlineKeyboardMarkup([
                    [InlineKeyboardButton("✅ Да", callback_data=f"acc_remove_ok:{acc_id}")],
                    [InlineKeyboardButton("❌ Нет", callback_data=f"acc_open:{acc_id}")],
                ])
                await cb.message.edit_text("🗑 Удалить?", reply_markup=kb)
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
                    "📋 Отправь список групп:\n"
                    "• По одной на строку: `@group1`\n"
                    "• Через запятую: `@g1, @g2`\n"
                    "• Или .txt файлом\n\n/cancel")
            elif data.startswith("acc_sub_delay:"):
                acc_id = data.split(":", 1)[1]
                pending[uid] = {"action": "acc_sub_delay", "acc_id": acc_id}
                await cb.message.edit_text("⏱ Мин и макс (пример: `40 120`):\n/cancel")
            elif data.startswith("acc_sub_start:"):
                acc_id = data.split(":", 1)[1]
                acc = get_account(acc_id)
                if not acc or not acc.get("subscribe_queue"):
                    await cb.answer("Пусто.", show_alert=True)
                    return
                if not user_clients.get(acc_id):
                    await cb.answer("Нет клиента.", show_alert=True)
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
            elif data == "stats_menu":
                await cb.message.edit_text(stats_menu_text(), reply_markup=stats_menu_kb())
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
            if action == "add_session_string":
                raw = (text or "").strip()
                cleaned = re.sub(r"\s+", "", raw).strip('"').strip("'").strip("`")
                log.info(f"[ADDSESSION] raw_len={len(raw)} clean_len={len(cleaned)}")
                if not cleaned or len(cleaned) < 100:
                    await message.reply(
                        f"❌ Слишком короткая: **{len(cleaned)}** символов.",
                        parse_mode=enums.ParseMode.MARKDOWN)
                    return
                await message.reply("⏳ Подключаюсь к Telegram…")
                try:
                    new_c = make_client(cleaned)
                    await new_c.start()
                    me = await new_c.get_me()
                    if not me:
                        await message.reply("❌ Не удалось получить данные.")
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
                    await message.reply(
                        f"✅ Аккаунт добавлен: **{name}** (id={me.id}).",
                        reply_markup=main_menu_kb())
                except Exception as e:
                    log.exception(f"[ADDSESSION] {e}")
                    await message.reply(f"❌ Ошибка: `{e}`",
                                        parse_mode=enums.ParseMode.MARKDOWN)
                return

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
                pending[uid] = {"action": "acc_add_code", "phone": phone,
                                "hash": sent.phone_code_hash, "session_name": sname, "client": new_c}
                await message.reply("📩 Код отправлен. Введи его:")

            elif action == "acc_add_code":
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
                    await message.reply("⚠️ Код истёк. /cancel и /login заново.")
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
                await message.reply(f"✅ **{name}**", reply_markup=main_menu_kb())

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
                await message.reply(f"✅ **{name}**", reply_markup=main_menu_kb())

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
                    await message.reply("Пусто.")
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
                    f"✅ Загружено: **{added}**\nВсего: **{len(acc['subscribe_queue'])}**\n\n"
                    f"→ 📥 Массовая подписка → ▶️ Запустить",
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
                    await message.reply("❌ Нужно 2 числа.")
                    return
                try:
                    lo = int(parts[0])
                    hi = int(parts[1])
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
                await message.reply(f"✅ {lo}–{hi}с", reply_markup=account_kb(acc_id))

            elif action == "acc_text":
                acc_id = act["acc_id"]
                acc = get_account(acc_id)
                if not acc:
                    return
                if not text:
                    pending[uid] = act
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
                    await message.reply(f"✅ {v}с", reply_markup=account_kb(acc_id))
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
                    await message.reply(f"❌ {e}")
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

        except Exception as e:
            log.exception("on_admin_input")
            await message.reply(f"❌ {e}")


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
    STATE = load_state()

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
            f"🤖 Бот запущен.\n"
            f"• Аккаунтов: {len(accounts)}\n"
            f"• В рассылке: {sum(1 for a in accounts if a.get('running'))}\n"
            f"• Со session_string: {sum(1 for a in accounts if a.get('session_string'))}\n"
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
