import os
import asyncio
import random
from pyrogram import Client, filters
from pyrogram.errors import FloodWait, SlowmodeWait, ChatWriteForbidden
from pyrogram.raw import functions

# === НАСТРОЙКИ (берутся из переменных окружения Amvera) ===
API_ID = int(os.environ.get("API_ID", "0"))
API_HASH = os.environ.get("API_HASH", "")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
SESSION_STRING = os.environ.get("SESSION_STRING", "")
ADMIN_ID = int(os.environ.get("ADMIN_ID", "0"))

# === ИНИЦИАЛИЗАЦИЯ КЛИЕНТОВ ===
user_app = Client("user_account", session_string=SESSION_STRING, api_id=API_ID, api_hash=API_HASH)
bot_app = Client("bot_account", bot_token=BOT_TOKEN, api_id=API_ID, api_hash=API_HASH)

# === ГЛОБАЛЬНОЕ СОСТОЯНИЕ ===
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

# === ФУНКЦИИ АВТОПОСТИНГА ===
async def get_chats_from_folder(folder_title):
    chats = []
    try:
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
        print(f"Ошибка получения папок: {e}")
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
                print(f"✅ Отправлено в чат {chat_id}")
                
            except SlowmodeWait as e:
                print(f"⏳ Slowmode в {chat_id}. Ждем {e.value} сек.")
                await asyncio.sleep(e.value + 2)
                await user_app.send_message(chat_id, Config.message_text)
                
            except FloodWait as e:
                print(f"🛑 FloodWait. Спим {e.value} сек.")
                await asyncio.sleep(e.value + 5)
                await user_app.send_message(chat_id, Config.message_text)
                
            except ChatWriteForbidden:
                print(f"❌ Нет прав писать в {chat_id}. Удаляю из списка.")
                Config.target_chats.remove(chat_id)
                
            except Exception as e:
                print(f"⚠️ Ошибка в {chat_id}: {e}")
            
            if Config.is_running:
                delay = random.randint(Config.delay_min, Config.delay_max)
                await asyncio.sleep(delay)
                
        if Config.is_running:
            await asyncio.sleep(60)

# === УПРАВЛЕНИЕ ЧЕРЕЗ БОТА ===

@bot_app.on_message(filters.command("start") & filters.private)
async def start_cmd(client, message):
    if message.from_user.id != ADMIN_ID:
        await message.reply(f"⛔️ Доступ запрещен.\nВаш ID: <code>{message.from_user.id}</code>")
        return
        
    text = (
        "🤖 **Панель управления Автопостингом**\n\n"
        "▫️ `/run` — Запустить\n"
        "▫️ `/stop` — Остановить\n"
        "▫️ `/set_msg [текст]` — Текст рассылки\n"
        "▫️ `/set_delay [мин] [макс]` — Задержка (в сек)\n"
        "▫️ `/set_folder [имя]` — Папка с группами\n"
        "▫️ `/status` — Настройки"
    )
    await message.reply(text)

@bot_app.on_message(filters.command("set_msg") & admin_filter)
async def set_msg_cmd(client, message):
    if len(message.command) > 1:
        Config.message_text = message.text.split(None, 1)[1]
        await message.reply("✅ Текст сообщения обновлен!")
    else:
        await message.reply("⚠️ Использование: `/set_msg Текст`")

@bot_app.on_message(filters.command("set_delay") & admin_filter)
async def set_delay_cmd(client, message):
    try:
        _, min_d, max_d = message.text.split()
        Config.delay_min = int(min_d)
        Config.delay_max = int(max_d)
        await message.reply(f"✅ Задержка: от {min_d} до {max_d} сек.")
    except Exception:
        await message.reply("⚠️ Использование: `/set_delay 60 120`")

@bot_app.on_message(filters.command("set_folder") & admin_filter)
async def set_folder_cmd(client, message):
    try:
        folder = message.text.split(None, 1)[1]
        Config.folder_name = folder
        await message.reply(f"✅ Папка изменена на: **{folder}**")
    except Exception:
        await message.reply("⚠️ Использование: `/set_folder Название`")

@bot_app.on_message(filters.command("status") & admin_filter)
async def status_cmd(client, message):
    status = "🟢 РАБОТАЕТ" if Config.is_running else "🔴 ОСТАНОВЛЕН"
    text = (
        f"📊 **Статус:** {status}\n\n"
        f"📁 **Папка:** {Config.folder_name}\n"
        f"🎯 **Чат-лист:** {len(Config.target_chats)} групп\n"
        f"⏱ **Тайминги:** {Config.delay_min}-{Config.delay_max} сек\n"
        f"📝 **Текст:**\n{Config.message_text}"
    )
    await message.reply(text)

@bot_app.on_message(filters.command("run") & admin_filter)
async def run_cmd(client, message):
    if Config.is_running:
        await message.reply("⚠️ Уже работает!")
        return

    msg = await message.reply("🔄 Собираю чаты из папки...")
    Config.target_chats = await get_chats_from_folder(Config.folder_name)
    
    if not Config.target_chats:
        await msg.edit_text(f"❌ В папке **{Config.folder_name}** нет чатов или папка не найдена.")
        return
        
    Config.is_running = True
    Config.task = asyncio.create_task(poster_task())
    await msg.edit_text(f"✅ **Запущено!**\nГрупп: {len(Config.target_chats)}")

@bot_app.on_message(filters.command("stop") & admin_filter)
async def stop_cmd(client, message):
    if not Config.is_running:
        await message.reply("⚠️ Уже остановлено.")
        return
        
    Config.is_running = False
    if Config.task:
        Config.task.cancel()
    await message.reply("🛑 **Остановлено.**")

# === ЗАПУСК ===
async def main():
    print("Запуск UserBot...")
    await user_app.start()
    
    # ❗️ ВАЖНО: Прогрев кэша. Бот прочитывает диалоги, чтобы узнать access_hash групп 
    print("📥 Загрузка кэша чатов... (подожди пару секунд)")
    try:
        async for _ in user_app.get_dialogs():
            pass
        print("✅ Кэш успешно загружен!")
    except Exception as e:
        print(f"⚠️ Ошибка загрузки кэша: {e}")

    print("Запуск Bot...")
    await bot_app.start()
    print("🚀 Скрипт готов! Напиши боту /start")
    
    from pyrogram import idle
    await idle()
    
    await user_app.stop()
    await bot_app.stop()

if __name__ == "__main__":
    import logging
    logging.getLogger("pyrogram").setLevel(logging.WARNING)
    
    asyncio.run(main())
