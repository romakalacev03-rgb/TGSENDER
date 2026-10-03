import os
import asyncio
import random
import logging
from pyrogram import Client, filters, idle
from pyrogram.errors import FloodWait, SlowmodeWait, ChatWriteForbidden
from pyrogram.raw import functions

logging.basicConfig(level=logging.INFO)

# === НАСТРОЙКИ ===
API_ID = int(os.environ.get("API_ID", "1234567")) # Укажи тут свой API ID если хочешь
API_HASH = os.environ.get("API_HASH", "")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")

# SESSION_STRING удален. Теперь используется файл сессии "user_account.session"
user_app = Client("user_account", api_id=API_ID, api_hash=API_HASH)
bot_app = Client("bot_account", bot_token=BOT_TOKEN, api_id=API_ID, api_hash=API_HASH, in_memory=True)

class Config:
    is_running = False
    message_text = "Привет! Это рекламный пост."
    delay_min = 60
    delay_max = 120
    folder_name = "Пиар"
    target_chats = []
    task = None
    userbot_ready = False

# Список ID пользователей, которые ввели правильный пароль
authorized_users = set()

def is_authorized(_, __, message):
    return message.from_user and message.from_user.id in authorized_users

auth_filter = filters.create(is_authorized)

# === ФУНКЦИИ РАССЫЛКИ ===
async def get_chats_from_folder(folder_title):
    chats = []
    try:
        print("📥 Синхронизация чатов...")
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
            await bot_app.send_message(list(authorized_users)[0], "⚠️ Список чатов пуст. Рассылка остановлена.")
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

# === КОМАНДЫ БОТА ===

# Пинг доступен всем для проверки того, что бот не завис
@bot_app.on_message(filters.command("ping"))
async def ping_cmd(client, message):
    status = "✅ Готов к работе" if Config.userbot_ready else "❌ Сессия юзербота отсутствует"
    await message.reply(f"🏓 Понг! Бот моментально читает сообщения.\nЮзербот: {status}")

# ПЕРЕХВАТЧИК ДЛЯ ВВОДА ПАРОЛЯ (2 ФАКТОРКА)
@bot_app.on_message(filters.private & ~auth_filter)
async def auth_handler(client, message):
    if message.text and message.text.strip() == "2512":
        authorized_users.add(message.from_user.id)
        await message.reply("✅ Пароль верный! Доступ к боту открыт.\n\nНажми /start для вызова меню.")
    else:
        await message.reply("🔒 **Бот защищен.**\nПожалуйста, отправь пароль (4 цифры) для доступа:")

# Дальше идут команды, доступные ТОЛЬКО после ввода пароля
@bot_app.on_message(filters.command("start") & auth_filter)
async def start_cmd(client, message):
    await message.reply("🤖 **Панель управления**\n\n`/run` — Запуск\n`/stop` — Стоп\n`/status` — Настройки\n`/set_folder [имя]` — Папка\n`/set_msg [текст]` — Текст")

@bot_app.on_message(filters.command("set_msg") & auth_filter)
async def set_msg_cmd(client, message):
    if len(message.command) > 1:
        Config.message_text = message.text.split(None, 1)[1]
        await message.reply("✅ Текст обновлен!")

@bot_app.on_message(filters.command("set_delay") & auth_filter)
async def set_delay_cmd(client, message):
    try:
        _, min_d, max_d = message.text.split()
        Config.delay_min, Config.delay_max = int(min_d), int(max_d)
        await message.reply(f"✅ Задержка: от {min_d} до {max_d} сек.")
    except Exception:
        await message.reply("⚠️ Использование: `/set_delay 60 120`")

@bot_app.on_message(filters.command("set_folder") & auth_filter)
async def set_folder_cmd(client, message):
    try:
        Config.folder_name = message.text.split(None, 1)[1]
        await message.reply(f"✅ Папка изменена на: **{Config.folder_name}**")
    except Exception:
        pass

@bot_app.on_message(filters.command("status") & auth_filter)
async def status_cmd(client, message):
    status = "🟢 РАБОТАЕТ" if Config.is_running else "🔴 ОСТАНОВЛЕН"
    await message.reply(f"📊 **Статус:** {status}\n📁 **Папка:** {Config.folder_name}\n🎯 **Групп:** {len(Config.target_chats)}\n📝 **Текст:**\n{Config.message_text}")

@bot_app.on_message(filters.command("run") & auth_filter)
async def run_cmd(client, message):
    if not Config.userbot_ready:
        return await message.reply("❌ **ОШИБКА:** Юзербот не авторизован!\nСкрипт работает без `SESSION_STRING`. Загрузи файл `user_account.session` в корень с ботом.")
        
    if Config.is_running:
        return await message.reply("⚠️ Уже работает!")

    msg = await message.reply("🔄 Синхронизирую чаты (подожди чуть-чуть)...")
    Config.target_chats = await get_chats_from_folder(Config.folder_name)
    
    if not Config.target_chats:
        return await msg.edit_text(f"❌ Чаты в папке **{Config.folder_name}** не найдены.")
        
    Config.is_running = True
    Config.task = asyncio.create_task(poster_task())
    await msg.edit_text(f"✅ **Запущено!** Групп: {len(Config.target_chats)}")

@bot_app.on_message(filters.command("stop") & auth_filter)
async def stop_cmd(client, message):
    Config.is_running = False
    if Config.task: Config.task.cancel()
    await message.reply("🛑 **Остановлено.**")

# === БЕЗОПАСНЫЙ ЗАПУСК ===
async def main():
    print("="*40)
    print("🚀 НАЧИНАЕТСЯ ЗАПУСК СКРИПТА...")
    print("="*40)
    
    # 1. Сначала запускаем бота (чтобы он точно не завис и отвечал на /ping)
    print("1. Подключаем Бота управления...")
    await bot_app.start()
    print("✅ Бот управления в сети!")

    # 2. Безопасно проверяем юзербота, не вызывая консоль
    print("2. Проверяем файл сессии юзербота...")
    await user_app.connect()
    try:
        await user_app.get_me() # Проверка авторизации
        Config.userbot_ready = True
        print("✅ Юзербот успешно авторизован!")
    except Exception as e:
        print("❌ ЮЗЕРБОТ НЕ АВТОРИЗОВАН! Нет файла user_account.session")
        Config.userbot_ready = False
    finally:
        await user_app.disconnect()
        
    # Запускаем юзербота ТОЛЬКО если есть рабочая сессия, чтобы скрипт не зависал
    if Config.userbot_ready:
        await user_app.start()
        
    print("="*40)
    print("🔥 ВСЁ ГОТОВО! НАПИШИ БОТУ /ping ИЛИ ЛЮБОЕ СООБЩЕНИЕ")
    print("="*40)
    
    await idle()
    
    try:
        if Config.userbot_ready: await user_app.stop()
        await bot_app.stop()
    except: pass

if __name__ == "__main__":
    asyncio.run(main())
