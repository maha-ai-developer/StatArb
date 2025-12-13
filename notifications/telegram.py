# notifications/telegram.py
import json
import os
import requests

CONFIG_PATH = os.path.join("config", "config.json")


def _load_config():
    if not os.path.exists(CONFIG_PATH):
        return {}
    with open(CONFIG_PATH, "r") as f:
        return json.load(f)


def send_telegram_message(text: str):
    cfg = _load_config()
    bot_token = cfg.get("telegram_bot_token")
    chat_id = cfg.get("telegram_chat_id")

    if not bot_token or not chat_id:
        # silently no-op if not configured
        return

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    try:
        requests.post(url, data={"chat_id": chat_id, "text": text})
    except Exception as e:
        print(f"[Telegram] Failed to send alert: {e}")
