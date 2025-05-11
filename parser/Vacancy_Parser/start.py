import os
import subprocess
import sys
import time

# Путь к вашему скрипту бота
bot_script_path = "/Users/andrii_zadorozhnii/MyScripts/start_bot.sh"

def start_bot():
    try:
        # Проверяем, существует ли файл скрипта
        if os.path.exists(bot_script_path):
            # Запускаем скрипт через bash
            subprocess.Popen(["/bin/bash", bot_script_path])
            print("Bot started successfully!")
        else:
            print("Bot script not found!")
    except Exception as e:
        print(f"Error starting the bot: {e}")

# Ожидаем немного для корректной загрузки системы, если нужно
time.sleep(10)

# Запускаем бота
start_bot()