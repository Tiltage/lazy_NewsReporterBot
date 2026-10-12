import os, sys, requests

r = requests.post(
    f"https://api.telegram.org/bot{os.environ['TELEGRAM_BOT_TOKEN']}/sendMessage",
    json={"chat_id": os.environ["TELEGRAM_CHAT_ID"], "text": "newsbot deployed ✅"},
    timeout=15,
)
print("telegram status:", r.status_code)   # don't print the URL, because Actions logs are public on a public repo
sys.exit(0 if r.ok else 1)