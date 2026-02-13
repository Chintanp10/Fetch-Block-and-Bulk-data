# Fetch-Block-and-Bulk-data

This project scans **SME block and bulk deals** from **NSE and BSE** and posts a summary to Telegram.

## Script

- `sme_block_bulk_telegram.py`

## Setup

1. Create a Telegram bot with BotFather and get bot token.
2. Get your target `chat_id` (personal chat, group, or channel).
3. Export environment variables:

```bash
export TELEGRAM_BOT_TOKEN='123456:ABC...'
export TELEGRAM_CHAT_ID='-1001234567890'
# Optional:
export LOOKBACK_DAYS='1'
export MAX_ROWS_PER_SECTION='20'
```

## Run

```bash
python3 sme_block_bulk_telegram.py
```

## Cron example (every 30 min)

```cron
*/30 * * * * cd /workspace/Fetch-Block-and-Bulk-data && /usr/bin/python3 sme_block_bulk_telegram.py >> scan.log 2>&1
```

## Notes

- NSE/BSE endpoints occasionally change or rate-limit requests.
- The script uses a best-effort SME symbol filter for both exchanges.
