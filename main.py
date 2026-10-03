# -*- coding: utf-8 -*-
"""
Автопостер по группам Telegram с управляющим ботом.

Стек: Pyrogram 2.x + asyncio + tgcrypto.
Первый запуск: интерактивная настройка (API_ID / API_HASH / BOT_TOKEN / ADMIN_ID),
далее данные читаются из config.json, состояние — из state.json.

Установка зависимостей:
    pip install -U pyrogram tgcrypto
"""

import asyncio
import json
import os
import random
import sys
import logging
from datetime import datetime, timedelta

from pyrogram import Client, filters, enums
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.errors import (
    FloodWait,
    ChatWriteForbidden,
    ChatAdminRequired,
    UserBannedInChannel,
    PeerIdInvalid,
    UserIsBlocked,
    ChannelPrivate,
)

# ---------------------------------------------------------------------------
# Константы и глобальное состояние
# ---------------------------------------------------------------------------

CONFIG_FILE = "config.json"
STATE_FILE = "state.json"
MEDIA_DIR = "media"
SESSION_BOT = "control_bot"      # .session управляющего бота
SESSION_USER = "userbot"         # .session юзербота (аккаунт рассылки)

DEFAULT_PIN = "2512"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("autoposter")

# Глобальные объекты (инициализируются в main)
CFG: dict = {}
STATE: dict = {}
user_client: Client = None
bot_client: Client = None
mailing_task: asyncio.Task = None

# Авторизованные пользователи (сессия в памяти — сбрасывается при рестарте)
authed: set = set()
# Ожидание ввода от админа: {user_id: {"action": "..."}}
pending: dict = {}


# ---------------------------------------------------------------------------
# Утилиты для работы с JSON
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


def default_state() -> dict:
    return {
        "text": None,
        "media_type": None,     # 'photo' | 'video' | None
        "media_path": None,
        "caption": "",
        "interval": 1800,       # интервал между кругами, сек
        "delay_min": 5,         # мин. задержка между группами, сек
        "delay_max": 15,        # макс. задержка между группами, сек
        "groups": [],           # [{"id": int, "title": str, "type": str, "manual": bool}]
        "running": False,
        "stats": {
            "sent": 0,
            "errors": 0,
            "rounds": 0,
            "last_round": None,
            "next_round": None,
        },
    }


def load_state() -> dict:
    raw = load_json(STATE_FILE, {}) or {}
    st = default_state()
    st.update(raw)
    if not isinstance(st.get("stats"), dict):
        st["stats"] = default_state()["stats"]
    return st


# ---------------------------------------------------------------------------
# Первичная интерактивная настройка
# ---------------------------------------------------------------------------

def interactive_setup() -> dict:
    print("=" * 64)
    print("   Первичная настройка Telegram-автопостера")
    print("=" * 64)
    print("Введите данные (получить можно на https://my.telegram.org и у @BotFather).")
    try:
        api_id = int(input("API_ID: ").strip())
        api_hash = input("API_HASH: ").strip()
        bot_token = input("BOT_TOKEN: ").strip()
        admin_id = int(input("ADMIN_ID (ваш Telegram ID): ").strip())
    except (KeyboardInterrupt, EOFError):
        print("\nНастройка прервана.")
        sys.exit(1)
    except ValueError:
        print("Некорректное числовое значение. Перезапустите скрипт.")
        sys.exit(1)

    cfg = {
        "api_id": api_id,
        "api_hash": api_hash,
        "bot_token": bot_token,
        "admin_id": admin_id,
        "pin": DEFAULT_PIN,
    }
    save_json(CONFIG_FILE, cfg)
    print("[+] Конфигурация сохранена в config.json")
    return cfg


# ---------------------------------------------------------------------------
# Парсинг ссылок на чаты
# ---------------------------------------------------------------------------

def parse_chat_ref(text: str):
    """Преобразует @username, https://t.me/..., t.me/..., числовой ID в ссылку для get_chat."""
    if not text:
        return None
    s = text.strip()
    if s.startswith("https://t.me/"):
        s = s[len("https://t.me/"):]
    elif s.startswith("http://t.me/"):
        s = s[len("http://t.me/"):]
    elif s.startswith("t.me/"):
        s = s[len("t.me/"):]
    s = s.split("?")[0].split("/")[0].strip()
    if not s:
        return None
    if s.startswith("@"):
        return s
    # числовой ID (может быть отрицательный)
    try:
        return int(s)
    except ValueError:
        return "@" + s


# ---------------------------------------------------------------------------
# Отправка сообщения в один чат
# ---------------------------------------------------------------------------

async def send_post(chat_id: int) -> None:
    """Отправляет подготовленное сообщение в чат от лица юзербота."""
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
        raise ValueError("Не задан текст или медиа для рассылки")


# ---------------------------------------------------------------------------
# Автоскан групп
# ---------------------------------------------------------------------------

async def scan_groups() -> list:
    """Собирает список всех групп/супергрупп, в которых состоит юзербот."""
    found = []
    try:
        async for dialog in user_client.get_dialogs():
            chat = dialog.chat
            if chat.type not in (enums.ChatType.GROUP, enums.ChatType.SUPERGROUP):
                continue
            # Пропускаем broadcast-каналы (туда нельзя писать обычным юзером)
            if getattr(chat, "is_broadcast", False):
                continue
            found.append({
                "id": chat.id,
                "title": chat.title or str(chat.id),
                "type": chat.type.name,
                "manual": False,
            })
    except FloodWait as e:
        log.warning(f"FloodWait при сканировании: {e.value}s")
        await asyncio.sleep(e.value + 2)
    except Exception as e:
        log.exception(f"Ошибка при сканировании диалогов: {e}")
    return found


# ---------------------------------------------------------------------------
# Основной цикл рассылки
# ---------------------------------------------------------------------------

async def mailing_loop():
    """Круговой обход групп с задержками и защитой от FloodWait."""
    log.info("Рассылка запущена.")
    while STATE.get("running"):
        groups = list(STATE.get("groups") or [])
        if not groups:
            log.warning("Нет групп для рассылки — ждём 60 секунд.")
            await asyncio.sleep(60)
            continue

        sent = 0
        errors = 0
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
                log.warning(f"[FLOOD] {fw.value}s на {title}")
                try:
                    await bot_client.send_message(
                        CFG["admin_id"],
                        f"⚠️ FloodWait: пауза {fw.value} сек (группа: {title})."
                    )
                except Exception:
                    pass
                await asyncio.sleep(fw.value + 2)
                try:
                    await send_post(gid)
                    sent += 1
                    STATE["stats"]["sent"] += 1
                    log.info(f"[OK/retry] {title}")
                except Exception as e:
                    errors += 1
                    STATE["stats"]["errors"] += 1
                    log.warning(f"[ERR/retry] {title}: {e}")
            except (ChatWriteForbidden, ChatAdminRequired, UserBannedInChannel,
                    PeerIdInvalid, UserIsBlocked, ChannelPrivate) as e:
                errors += 1
                STATE["stats"]["errors"] += 1
                log.warning(f"[SKIP] {title}: {type(e).__name__} — {e}")
            except Exception as e:
                errors += 1
                STATE["stats"]["errors"] += 1
                log.exception(f"[ERR] {title}: {e}")

            # Пауза между группами
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
            await bot_client.send_message(
                CFG["admin_id"],
                f"✅ Круг №{STATE['stats']['rounds']} завершён.\n"
                f"• Отправлено: {sent}\n"
                f"• Ошибок: {errors}\n"
                f"• Следующий круг через {interval // 60} мин."
            )
        except Exception:
            pass

        # Ожидание интервала с возможностью ранней остановки
        remaining = interval
        while remaining > 0 and STATE.get("running"):
            chunk = min(5, remaining)
            await asyncio.sleep(chunk)
            remaining -= chunk

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
    running = STATE.get("running")
    start_label = "🚀 Запустить рассылку" if not running else "🚀 Рассылка идёт…"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(start_label, callback_data="start")],
        [InlineKeyboardButton("⏸ Остановить рассылку", callback_data="stop")],
        [InlineKeyboardButton("📝 Изменить сообщение", callback_data="edit_msg")],
        [InlineKeyboardButton("⏱ Настройка таймингов", callback_data="timings")],
        [InlineKeyboardButton("🔍 Обновить список групп", callback_data="scan")],
        [
            InlineKeyboardButton("➕ Добавить группу", callback_data="add_grp"),
            InlineKeyboardButton("➖ Удалить группу", callback_data="del_grp"),
        ],
        [InlineKeyboardButton("📊 Статус и статистика", callback_data="status")],
    ])


def groups_kb() -> InlineKeyboardMarkup:
    rows = []
    for g in (STATE.get("groups") or [])[:30]:
        title = (g.get("title") or str(g.get("id")))[:40]
        rows.append([InlineKeyboardButton(f"❌ {title}", callback_data=f"delgrp:{g['id']}")])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="menu")])
    return InlineKeyboardMarkup(rows)


# ---------------------------------------------------------------------------
# Регистрация обработчиков управляющего бота
# ---------------------------------------------------------------------------

def register_handlers(bot: Client) -> None:

    # ----------------------- /start -----------------------
    @bot.on_message(filters.command("start") & filters.private)
    async def cmd_start(client, message):
        if message.from_user.id != CFG["admin_id"]:
            await message.reply("⛔ Доступ запрещён.")
            return
        if message.from_user.id not in authed:
            await message.reply(
                "🔒 Требуется авторизация.\nВведите: `/auth <PIN>`",
                parse_mode=enums.ParseMode.MARKDOWN,
            )
            return
        await message.reply("🎛 Панель управления:", reply_markup=main_menu_kb())

    # ----------------------- /auth ------------------------
    @bot.on_message(filters.command("auth") & filters.private)
    async def cmd_auth(client, message):
        if message.from_user.id != CFG["admin_id"]:
            return
        parts = (message.text or "").split(maxsplit=1)
        if len(parts) < 2 or parts[1].strip() != str(CFG.get("pin")):
            await message.reply("❌ Неверный ПИН-код.")
            return
        authed.add(message.from_user.id)
        pending.pop(message.from_user.id, None)
        await message.reply("✅ Авторизация успешна.", reply_markup=main_menu_kb())

    # ----------------------- /panel -----------------------
    @bot.on_message(filters.command("panel") & filters.private)
    async def cmd_panel(client, message):
        if message.from_user.id != CFG["admin_id"]:
            return
        if message.from_user.id not in authed:
            await message.reply("🔒 /auth <PIN>")
            return
        await message.reply("🎛 Панель:", reply_markup=main_menu_kb())

    # ----------------------- /cancel ----------------------
    @bot.on_message(filters.command("cancel") & filters.private)
    async def cmd_cancel(client, message):
        if message.from_user.id != CFG["admin_id"]:
            return
        pending.pop(message.from_user.id, None)
        await message.reply("Отменено.", reply_markup=main_menu_kb())

    # --------------------- Callback -----------------------
    @bot.on_callback_query()
    async def on_cb(client, cb):
        uid = cb.from_user.id
        if uid != CFG["admin_id"]:
            await cb.answer("⛔ Нет доступа.", show_alert=True)
            return
        if uid not in authed:
            await cb.answer("🔒 Сначала /auth <PIN>", show_alert=True)
            return

        data = cb.data or ""
        try:
            # ---------- Меню ----------
            if data == "menu":
                await cb.message.edit_text("🎛 Панель управления:", reply_markup=main_menu_kb())

            # ---------- Старт ----------
            elif data == "start":
                if STATE.get("running"):
                    await cb.answer("Уже запущено.")
                    return
                if not STATE.get("groups"):
                    await cb.answer("Список групп пуст — сделайте скан или добавьте вручную.", show_alert=True)
                    return
                if not (STATE.get("text") or STATE.get("media_path")):
                    await cb.answer("Не задано сообщение для рассылки!", show_alert=True)
                    return
                STATE["running"] = True
                save_json(STATE_FILE, STATE)
                start_mailing()
                await cb.message.edit_text("🚀 Рассылка запущена.", reply_markup=main_menu_kb())

            # ---------- Стоп ----------
            elif data == "stop":
                STATE["running"] = False
                save_json(STATE_FILE, STATE)
                await cb.message.edit_text("⏸ Рассылка остановлена.", reply_markup=main_menu_kb())

            # ---------- Меню редактирования сообщения ----------
            elif data == "edit_msg":
                cur = "— (пусто)"
                if STATE.get("media_path"):
                    cur = f"Медиа: {STATE.get('media_type')}\nCaption: {(STATE.get('caption') or '')[:150]}"
                elif STATE.get("text"):
                    cur = f"Текст: {STATE['text'][:200]}"
                kb = InlineKeyboardMarkup([
                    [InlineKeyboardButton("📝 Задать текст", callback_data="set_text")],
                    [InlineKeyboardButton("🖼 Задать медиа (фото/видео)", callback_data="set_media")],
                    [InlineKeyboardButton("🗑 Очистить сообщение", callback_data="clear_msg")],
                    [InlineKeyboardButton("⬅️ Назад", callback_data="menu")],
                ])
                await cb.message.edit_text(f"📝 Текущее сообщение:\n{cur}", reply_markup=kb)

            elif data == "set_text":
                pending[uid] = {"action": "set_text"}
                await cb.message.edit_text(
                    "Отправьте текст рекламного сообщения.\nДля отмены: /cancel"
                )

            elif data == "set_media":
                pending[uid] = {"action": "set_media"}
                await cb.message.edit_text(
                    "Отправьте фото или видео (опционально с подписью).\nДля отмены: /cancel"
                )

            elif data == "clear_msg":
                STATE["text"] = None
                STATE["media_path"] = None
                STATE["media_type"] = None
                STATE["caption"] = ""
                save_json(STATE_FILE, STATE)
                await cb.message.edit_text("🗑 Сообщение очищено.", reply_markup=main_menu_kb())

            # ---------- Тайминги ----------
            elif data == "timings":
                kb = InlineKeyboardMarkup([
                    [InlineKeyboardButton("⏱ Интервал круга (сек)", callback_data="set_interval")],
                    [InlineKeyboardButton("⏳ Мин. задержка (сек)", callback_data="set_delay_min")],
                    [InlineKeyboardButton("⏳ Макс. задержка (сек)", callback_data="set_delay_max")],
                    [InlineKeyboardButton("⬅️ Назад", callback_data="menu")],
                ])
                await cb.message.edit_text(
                    f"⏱ Текущие настройки:\n"
                    f"• Интервал круга: {STATE['interval']} сек (~{STATE['interval'] // 60} мин)\n"
                    f"• Задержка между группами: {STATE['delay_min']}–{STATE['delay_max']} сек",
                    reply_markup=kb,
                )

            elif data == "set_interval":
                pending[uid] = {"action": "set_interval"}
                await cb.message.edit_text(
                    "Введите интервал между кругами в секундах (минимум 60).\n/cancel для отмены"
                )
            elif data == "set_delay_min":
                pending[uid] = {"action": "set_delay_min"}
                await cb.message.edit_text(
                    "Введите минимальную задержку между группами в секундах (>= 1).\n/cancel"
                )
            elif data == "set_delay_max":
                pending[uid] = {"action": "set_delay_max"}
                await cb.message.edit_text(
                    "Введите максимальную задержку между группами в секундах.\n/cancel"
                )

            # ---------- Скан групп ----------
            elif data == "scan":
                await cb.answer("Сканирую…")
                await cb.message.edit_text("🔍 Сканирую диалоги юзербота…")
                found = await scan_groups()
                # сохраняем вручную добавленные, не вошедшие в скан
                scanned_ids = {g["id"] for g in found}
                manual_keep = [
                    g for g in (STATE.get("groups") or [])
                    if g.get("manual") and g["id"] not in scanned_ids
                ]
                STATE["groups"] = found + manual_keep
                save_json(STATE_FILE, STATE)
                await cb.message.edit_text(
                    f"✅ Найдено групп: {len(found)}\n"
                    f"• Всего в списке (с ручными): {len(STATE['groups'])}",
                    reply_markup=main_menu_kb(),
                )

            # ---------- Добавить вручную ----------
            elif data == "add_grp":
                pending[uid] = {"action": "add_group"}
                await cb.message.edit_text(
                    "Отправьте @username, ссылку `t.me/...` или числовой ID группы.\n/cancel для отмены",
                    parse_mode=enums.ParseMode.MARKDOWN,
                )

            # ---------- Удалить группу ----------
            elif data == "del_grp":
                if not STATE.get("groups"):
                    await cb.answer("Список пуст.", show_alert=True)
                    return
                await cb.message.edit_text("Выберите группу для удаления:", reply_markup=groups_kb())

            elif data.startswith("delgrp:"):
                try:
                    gid = int(data.split(":", 1)[1])
                except ValueError:
                    await cb.answer("Ошибка параметра.")
                    return
                before = len(STATE["groups"])
                STATE["groups"] = [g for g in STATE["groups"] if g["id"] != gid]
                save_json(STATE_FILE, STATE)
                if STATE["groups"]:
                    await cb.message.edit_text(
                        f"➖ Удалено. Осталось: {len(STATE['groups'])} (было {before}).",
                        reply_markup=groups_kb(),
                    )
                else:
                    await cb.message.edit_text("➖ Список групп пуст.", reply_markup=main_menu_kb())

            # ---------- Статус ----------
            elif data == "status":
                s = STATE["stats"]
                txt = (
                    f"📊 Статистика\n"
                    f"• Статус: {'🟢 работает' if STATE.get('running') else '🔴 остановлен'}\n"
                    f"• Групп в списке: {len(STATE.get('groups') or [])}\n"
                    f"• Всего отправлено: {s.get('sent', 0)}\n"
                    f"• Ошибок: {s.get('errors', 0)}\n"
                    f"• Завершённых кругов: {s.get('rounds', 0)}\n"
                    f"• Последний круг: {s.get('last_round') or '—'}\n"
                    f"• Следующий круг: {s.get('next_round') or '—'}\n"
                    f"• Интервал: {STATE['interval']} сек\n"
                    f"• Задержка: {STATE['delay_min']}–{STATE['delay_max']} сек"
                )
                kb = InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔄 Обновить", callback_data="status")],
                    [InlineKeyboardButton("⬅️ Назад", callback_data="menu")],
                ])
                await cb.message.edit_text(txt, reply_markup=kb)

            else:
                await cb.answer("Неизвестная команда.")

        except Exception as e:
            log.exception("Ошибка в обработчике callback")
            try:
                await cb.answer(f"Ошибка: {e}", show_alert=True)
            except Exception:
                pass

    # ---------------- Приём текста/медиа от админа ----------------
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
            # ---------- Задать текст ----------
            if action == "set_text":
                if not text:
                    await message.reply("Пустой текст. Отправьте снова или /cancel.")
                    pending[uid] = {"action": "set_text"}
                    return
                STATE["text"] = text
                STATE["media_path"] = None
                STATE["media_type"] = None
                STATE["caption"] = ""
                save_json(STATE_FILE, STATE)
                await message.reply("✅ Текст сохранён.", reply_markup=main_menu_kb())

            # ---------- Задать медиа ----------
            elif action == "set_media":
                if not (message.photo or message.video):
                    await message.reply("Это не фото и не видео. Пришлите медиа или /cancel.")
                    pending[uid] = {"action": "set_media"}
                    return
                os.makedirs(MEDIA_DIR, exist_ok=True)
                if message.photo:
                    ext = "jpg"
                    STATE["media_type"] = "photo"
                else:
                    ext = "mp4"
                    STATE["media_type"] = "video"
                path = os.path.join(MEDIA_DIR, f"post_media.{ext}")
                await message.download(file_name=path)
                STATE["media_path"] = path
                STATE["caption"] = message.caption or ""
                STATE["text"] = None
                save_json(STATE_FILE, STATE)
                await message.reply(
                    f"✅ Медиа сохранено ({STATE['media_type']}).\n"
                    f"Caption: {(STATE['caption'] or '')[:80] or '—'}",
                    reply_markup=main_menu_kb(),
                )

            # ---------- Добавить группу вручную ----------
            elif action == "add_group":
                ref = parse_chat_ref(text)
                if ref is None:
                    await message.reply("Не удалось распарсить ссылку. /cancel")
                    return
                try:
                    chat = await user_client.get_chat(ref)
                except Exception as e:
                    await message.reply(f"❌ Не удалось получить чат: {e}")
                    return
                gid = chat.id
                title = chat.title or str(gid)
                if any(g["id"] == gid for g in STATE["groups"]):
                    await message.reply("ℹ️ Эта группа уже в списке.", reply_markup=main_menu_kb())
                    return
                STATE["groups"].append({
                    "id": gid,
                    "title": title,
                    "type": chat.type.name if chat.type else "UNKNOWN",
                    "manual": True,
                })
                save_json(STATE_FILE, STATE)
                await message.reply(f"✅ Добавлено: {title} ({gid})", reply_markup=main_menu_kb())

            # ---------- Тайминги ----------
            elif action == "set_interval":
                try:
                    val = int(text)
                    if val < 60:
                        raise ValueError("минимум 60 секунд")
                    STATE["interval"] = val
                    save_json(STATE_FILE, STATE)
                    await message.reply(f"✅ Интервал: {val} сек (~{val // 60} мин).",
                                        reply_markup=main_menu_kb())
                except Exception as e:
                    await message.reply(f"❌ Некорректное значение: {e}")
                    pending[uid] = {"action": "set_interval"}

            elif action == "set_delay_min":
                try:
                    val = int(text)
                    if val < 1:
                        raise ValueError("минимум 1 секунда")
                    STATE["delay_min"] = val
                    if STATE["delay_max"] < val:
                        STATE["delay_max"] = val
                    save_json(STATE_FILE, STATE)
                    await message.reply(f"✅ Мин. задержка: {val} сек.", reply_markup=main_menu_kb())
                except Exception as e:
                    await message.reply(f"❌ Некорректное значение: {e}")
                    pending[uid] = {"action": "set_delay_min"}

            elif action == "set_delay_max":
                try:
                    val = int(text)
                    if val < 1:
                        raise ValueError("минимум 1 секунда")
                    if val < STATE["delay_min"]:
                        raise ValueError(f"максимум должен быть >= {STATE['delay_min']}")
                    STATE["delay_max"] = val
                    save_json(STATE_FILE, STATE)
                    await message.reply(f"✅ Макс. задержка: {val} сек.", reply_markup=main_menu_kb())
                except Exception as e:
                    await message.reply(f"❌ Некорректное значение: {e}")
                    pending[uid] = {"action": "set_delay_max"}

        except Exception as e:
            log.exception("Ошибка обработки ввода админа")
            await message.reply(f"❌ Ошибка: {e}")


# ---------------------------------------------------------------------------
# Точка входа
# ---------------------------------------------------------------------------

async def main():
    global CFG, STATE, user_client, bot_client

    # 1) Конфиг
    CFG = load_json(CONFIG_FILE, None)
    if not CFG or not all(k in CFG for k in ("api_id", "api_hash", "bot_token", "admin_id")):
        CFG = interactive_setup()
    CFG.setdefault("pin", DEFAULT_PIN)
    save_json(CONFIG_FILE, CFG)

    # 2) Состояние
    STATE = load_state()

    # 3) Клиенты
    user_client = Client(
        SESSION_USER,
        api_id=CFG["api_id"],
        api_hash=CFG["api_hash"],
    )
    bot_client = Client(
        SESSION_BOT,
        api_id=CFG["api_id"],
        api_hash=CFG["api_hash"],
        bot_token=CFG["bot_token"],
    )

    register_handlers(bot_client)

    # 4) Запуск клиентов (для юзербота при первом запуске Pyrogram спросит телефон)
    log.info("Запуск юзербота… (при первом запуске введите телефон/код/2FA в консоли)")
    await user_client.start()
    me = await user_client.get_me()
    log.info(f"Юзербот авторизован: {me.first_name} (@{me.username}) id={me.id}")

    log.info("Запуск управляющего бота…")
    await bot_client.start()
    bme = await bot_client.get_me()
    log.info(f"Бот авторизован: @{bme.username}")

    # 5) Приветствие админа
    try:
        await bot_client.send_message(
            CFG["admin_id"],
            "🤖 Автопостер запущен.\nДля доступа: `/auth <PIN>`",
            parse_mode=enums.ParseMode.MARKDOWN,
        )
    except Exception as e:
        log.warning(f"Не удалось отправить приветствие админу: {e}")

    # 6) Восстановление рассылки после рестарта
    if STATE.get("running"):
        log.info("Возобновляю рассылку после рестарта…")
        start_mailing()

    log.info("Сервис запущен. Остановка: Ctrl+C.")
    # Бесконечное ожидание
    await asyncio.Event().wait()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        print("\nОстановлено пользователем.")
