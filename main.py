import os
import asyncio
import random
import logging
from pyrogram import Client, filters
from pyrogram.errors import FloodWait, SlowmodeWait, ChatWriteForbidden
from pyrogram.raw import functions

# Включаем логирование, чтобы видеть скрытые ошибки
logging.basicConfig(level=logging.INFO)

# === НАСТРОЙКИ ===
API_ID = int(os.environ.get("API_ID", "0"))
API_HASH = os.environ.get("API_HASH", "")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
SESSION_STRING = os.environ.get("SESSION_STRING", "")
ADMIN_ID = int(os.environ.get("ADMIN_ID", "0"))

# === ИНИЦИАЛИЗАЦИЯ ===
# in_memory=True для бота, чтобы исключить конфликты файлов на хостинге
user_app = Client("user_account", session_string=SESSION_STRING, api_id=API_ID, api_hash=API_HASH)
bot_app = Client("bot_account", bot_token=BOT_TOKEN, api_id=API_ID, api_hash=API_HASH, in_memory=True)

class Config:
    is_running = False
    message_text = "Привет! Это рекламный пост."
    delay_min = 60
    delay_max = 120
    folder_name = "Пиар"
    target_chats = []
    task = None

def is_admin(_, __, message):
    return message.from_user and message.from_user.id == ADMIN_ID

admin_filter = filters.create(is_admin)

# === ФУНКЦИИ ===
async def get_chats_from_folder(folder_title):
    chats = []
    try:
        # Прогреваем кэш ТОЛЬКО при запуске рассылки, чтобы не тормозить старт бота
        print("📥 Синхронизация чатов (это может занять до минуты)...")
        async for _ in user_app.get_dialogs(limit=300):
            pass

        filters_list = await user_app.invoke(functions.messages.GetDialogFilters())
        for f in filters_list:
            if hasattr(f, 'title') and f.title.lower() == folder_title.lower():
                for peer in f.include_peers:
                    if hasattr(peer, 'channel_id'):
                        chats.append(int(f"-100{peer.channel_id}"))
                    elif hasattr(peer, 'chat_id'):
                        chats.append(int(f"-{peer.chat_id}"))
        return chats
    except Exception as e:
        print(f"❌ Ошибка парсинга папки: {e}")
        return chats

async def poster_task():
    while Config.is_running:
        if not Config.target_chats:
            await bot_app.send_message(ADMIN_ID, "⚠️ Список чатов пуст. Рассылка остановлена.")
            Config.is_running = False
            break
            
        for chat_id in Config.target_chats:
            if not Config.is_running:
                break
            
            try:
                await user_app.send_message(chat_id, Config.message_text)
                print(f"✅ Отправлено в {chat_id}")
            except SlowmodeWait as e:
                print(f"⏳ Slowmode в {chat_id}. Ждем {e.value} сек.")
                await asyncio.sleep(e.value + 2)
                await user_app.send_message(chat_id, Config.message_text)
            except FloodWait as e:
                print(f"🛑 FloodWait. Спим {e.value} сек.")
                await asyncio.sleep(e.value + 5)
                await user_app.send_message(chat_id, Config.message_text)
            except ChatWriteForbidden:
                print(f"❌ Нет прав в {chat_id}. Удаляю из списка.")
                Config.target_chats.remove(chat_id)
            except Exception as e:
                print(f"⚠️ Ошибка в {chat_id}: {e}")
            
            if Config.is_running:
                await asyncio.sleep(random.randint(Config.delay_min, Config.delay_max))
                
        if Config.is_running:
            await asyncio.sleep(60)

# === КОМАНДЫ ===

# ТЕСТОВАЯ КОМАНДА ДЛЯ ПРОВЕРКИ СВЯЗИ (Доступна всем!)
@bot_app.on_message(filters.command("ping"))
async def ping_cmd(client, message):
    await message.reply(
        f"🏓 Понг! Бот жив и моментально читает сообщения.\n\n"
        f"👤 Твой ID: <code>{message.from_user.id}</code>\n"
        f"🛠 ID Админа в настройках: <code>{ADMIN_ID}</code>"
    )

@bot_app.on_message(filters.command("start"))
async def start_cmd(client, message):
    # Теперь бот скажет, если твой ID не совпадает
    if message.from_user.id != ADMIN_ID:
        await message.reply(
            f"⛔️ Доступ запрещен!\n"
            f"В настройках Amvera указан ADMIN_ID: {ADMIN_ID}\n"
            f"А твой реальный ID: {message.from_user.id}\n"
            f"👉 Исправь переменную ADMIN_ID на хостинге!"
        )
        return
        
    await message.reply("🤖 **Панель управления**\n\n`/run` — Запуск\n`/stop` — Стоп\n`/status` — Настройки\n`/set_folder [имя]` — Папка\n`/set_msg [текст]` — Текст")

@bot_app.on_message(filters.command("set_msg") & admin_filter)
async def set_msg_cmd(client, message):
    if len(message.command) > 1:
        Config.message_text = message.text.split(None, 1)[1]
        await message.reply("✅ Текст обновлен!")

@bot_app.on_message(filters.command("set_delay") & admin_filter)
async def set_delay_cmd(client, message):
    try:
        _, min_d, max_d = message.text.split()
        Config.delay_min, Config.delay_max = int(min_d), int(max_d)
        await message.reply(f"✅ Задержка: от {min_d} до {max_d} сек.")
    except Exception:
        await message.reply("⚠️ Использование: `/set_delay 60 120`")

@bot_app.on_message(filters.command("set_folder") & admin_filter)
async def set_folder_cmd(client, message):
    try:
        Config.folder_name = message.text.split(None, 1)[1]
        await message.reply(f"✅ Папка изменена на: **{Config.folder_name}**")
    except Exception:
        pass

@bot_app.on_message(filters.command("status") & admin_filter)
async def status_cmd(client, message):
    status = "🟢 РАБОТАЕТ" if Config.is_running else "🔴 ОСТАНОВЛЕН"
    await message.reply(f"📊 **Статус:** {status}\n📁 **Папка:** {Config.folder_name}\n🎯 **Групп:** {len(Config.target_chats)}\n📝 **Текст:**\n{Config.message_text}")

@bot_app.on_message(filters.command("run") & admin_filter)
async def run_cmd(client, message):
    if Config.is_running:
        return await message.reply("⚠️ Уже работает!")

    msg = await message.reply("🔄 Синхронизирую чаты (подожди чуть-чуть)...")
    Config.target_chats = await get_chats_from_folder(Config.folder_name)
    
    if not Config.target_chats:
        return await msg.edit_text(f"❌ Чаты в папке **{Config.folder_name}** не найдены.")
        
    Config.is_running = True
    Config.task = asyncio.create_task(poster_task())
    await msg.edit_text(f"✅ **Запущено!** Групп: {len(Config.target_chats)}")

@bot_app.on_message(filters.command("stop") & admin_filter)
async def stop_cmd(client, message):
    Config.is_running = False
    if Config.task: Config.task.cancel()
    await message.reply("🛑 **Остановлено.**")

# === ЗАПУСК ===
async def main():
    print("="*40)
    print("🚀 НАЧИНАЕТСЯ ЗАПУСК СКРИПТА...")
    print("="*40)
    
    try:
        print("1. Подключаем Юзербота...")
        await user_app.start()
        print("✅ Юзербот авторизован!")

        print("2. Подключаем Бота управления...")
        await bot_app.start()
        print("✅ Бот управления в сети!")
        print("="*40)
        print("🔥 ВСЁ ГОТОВО! НАПИШИ БОТУ /ping ИЛИ /start")
        print("="*40)
        
        from pyrogram import idle
        await idle()
        
    except Exception as e:
        print(f"❌ КРИТИЧЕСКАЯ ОШИБКА ПРИ ЗАПУСКЕ: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await user_app.stop()
        await bot_app.stop()

if __name__ == "__main__":
    asyncio.run(main())
