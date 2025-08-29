# MyAsianDramaBot 🇰🇷🇨🇳🇯🇵

Телеграм-бот для каталога дорам: добавление/поиск/оценка, пагинация, фильтры по странам.

## Функции
- Добавление записей через диалог (название, актёры, год, рейтинг, постер URL)
- Поиск/фильтры/пагинация, Markdown v2 экранирование
- Логи и обработка ошибок

## Технологии
Python · python-telegram-bot v20 · aiosqlite · asyncio

## Запуск
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp config.py.example config.py
python NeZabuDrama.py
```
## Demo
![Screenshot](assets/screenshot.jpg)

> See [PORTFOLIO_ONLY.md](PORTFOLIO_ONLY.md) for showcase disclaimer.
