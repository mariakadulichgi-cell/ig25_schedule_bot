# bot.py
import os
import re
import csv
import io
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from threading import Thread

import requests
from dotenv import load_dotenv

from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

# =======================
# Render "костыль": Flask, чтобы был открытый порт
# =======================
from flask import Flask

web = Flask(__name__)

@web.get("/")
def home():
    return "ok", 200

def _run_web():
    port = int(os.environ.get("PORT", "10000"))
    web.run(host="0.0.0.0", port=port)

def keep_alive():
    Thread(target=_run_web, daemon=True).start()


# =======================
# НАСТРОЙКИ
# =======================
TZ = ZoneInfo("Asia/Krasnoyarsk")  # Красноярск (+07)
DEFAULT_GROUP = os.getenv("GROUP_NAME", "ИГ25-01Б-ОМ")

# Ищем дату в формате 02.02 / 02-02 / 02/02 / 02.02.26 и т.п.
DATE_RE = re.compile(r"\b(\d{1,2})[.\-/](\d{1,2})(?:[.\-/](\d{2,4}))?\b")
# Ищем время или диапазон (8:30, 8.30, 8:30-10:05, 8.30–10.05)
TIME_RE = re.compile(r"\b(\d{1,2})[.:](\d{2})(?:\s*[-–]\s*(\d{1,2})[.:](\d{2}))?\b")

# Кэш, чтобы не дергать гугл на каждое сообщение
_CACHE_TEXT = None
_CACHE_TS = 0.0
CACHE_SECONDS = 60


# =======================
# ВСПОМОГАТЕЛЬНЫЕ
# =======================
def norm(s: str) -> str:
    return (s or "").replace("\xa0", " ").strip()

def _compact_spaces(s: str) -> str:
    s = (s or "").replace("\xa0", " ").replace("\t", " ")
    s = re.sub(r"[ ]{2,}", " ", s)
    return s.strip()

def norm_group(s: str) -> str:
    """Нормализуем название группы: разные тире/пробелы -> одинаково"""
    s = norm(s)
    s = s.replace("–", "-").replace("—", "-")
    s = re.sub(r"\s+", "", s)
    return s.upper()

def parse_ddmm(text: str) -> str | None:
    m = DATE_RE.search(text or "")
    if not m:
        return None
    dd = int(m.group(1))
    mm = int(m.group(2))
    return f"{dd:02d}.{mm:02d}"

def normalize_time(s: str) -> str:
    s = norm(s)
    m = TIME_RE.search(s)
    if not m:
        return s
    h1, m1, h2, m2 = m.groups()
    if h2 and m2:
        return f"{int(h1)}:{m1}–{int(h2)}:{m2}"
    return f"{int(h1)}:{m1}"

def fetch_csv_text(url: str) -> str:
    global _CACHE_TEXT, _CACHE_TS
    now = time.time()
    if _CACHE_TEXT and (now - _CACHE_TS) < CACHE_SECONDS:
        return _CACHE_TEXT

    r = requests.get(url, timeout=25)
    r.raise_for_status()
    _CACHE_TEXT = r.text
    _CACHE_TS = now
    return r.text


# =======================
# ЛОГИКА ПОИСКА КОЛОНОК (Дата/Часы + колонки группы)
# =======================
def find_header_and_group_cols(rows: list[list[str]], group_name: str):
    """
    Ищем строку заголовков, где есть 'Дата' и 'Часы' (в первых 60 строках),
    затем ищем колонки группы в следующих строках.
    """
    g_need = norm_group(group_name)

    header_row_i = None
    date_col = None
    time_col = None

    for i in range(min(60, len(rows))):
        row = [norm(x) for x in rows[i]]
        low = [x.lower() for x in row]
        if "дата" in low and "часы" in low:
            header_row_i = i
            date_col = low.index("дата")
            time_col = low.index("часы")
            break

    if header_row_i is None:
        raise RuntimeError("Не нашла заголовки 'Дата' и 'Часы' в таблице (CSV).")

    group_cols = []
    for i in range(header_row_i, min(header_row_i + 12, len(rows))):
        row = rows[i]
        for j, cell in enumerate(row):
            if norm_group(cell) == g_need:
                group_cols.append(j)

    group_cols = sorted(set(group_cols))
    return header_row_i, date_col, time_col, group_cols


# =======================
# СКЛЕЙКИ "пр", "лек" и прочего
# =======================
_TAG_ONLY = {"пр", "лек"}  # можно расширить: {"пр", "лек", "лаб"}

def _glue_short_tags(lines: list[str]) -> list[str]:
    """
    Склеиваем строки, которые равны "пр" или "лек" с предыдущей строкой.
    """
    out = []
    for ln in lines:
        ln = _compact_spaces(ln)
        if not ln:
            continue
        low = ln.lower()

        if out and low in _TAG_ONLY:
            out[-1] = _compact_spaces(out[-1] + " " + ln)
            continue

        # Частый случай: "пр" прилепилось с пробелом в начале
        if out and (low == "пр" or low == "лек"):
            out[-1] = _compact_spaces(out[-1] + " " + ln)
            continue

        out.append(ln)
    return out

def _glue_pr_slash_lines(lines: list[str]) -> list[str]:
    """
    Склеиваем переносы вида:
    - "пр" + "/ 3-17"
    - "пр / 3-17" (если "/ 3-17" отдельно)
    - "/ 3-17" отдельно -> приклеить к предыдущей (если есть)
    """
    out = []
    for raw in lines:
        ln = _compact_spaces(raw)
        if not ln:
            continue

        low = ln.lower()

        # Если строка просто "/ 3-17" или "/3-17" — приклеиваем к предыдущей
        if out and (ln.startswith("/") or ln.startswith("/ ")):
            out[-1] = _compact_spaces(out[-1] + " " + ln)
            continue

        # Если предыдущая строка заканчивается на "пр" или "лек" и эта начинается с "/"
        if out and out[-1].lower() in _TAG_ONLY and ln.startswith("/"):
            out[-1] = _compact_spaces(out[-1] + " " + ln)
            continue

        out.append(ln)
    return out

def _postprocess_cell_text(cell_text: str) -> list[str]:
    """
    Разбиваем текст ячейки на строки, чистим мусор, склеиваем "пр"/"лек".
    """
    txt = (cell_text or "").replace("\r", "")
    raw_lines = [l.strip() for l in txt.splitlines() if l.strip()]

    cleaned = []
    for l in raw_lines:
        low = l.lower()
        # выкидываем мусорные строки (если они вдруг попались)
        if "семестр" in low or "утверждаю" in low:
            continue
        cleaned.append(l)

    cleaned = _glue_short_tags(cleaned)
    cleaned = _glue_pr_slash_lines(cleaned)
    return cleaned


# =======================
# ВЫТАЩИТЬ РАСПИСАНИЕ НА ДАТУ
# =======================
def extract_schedule_for_date(csv_text: str, group_name: str, target_ddmm: str):
    reader = csv.reader(io.StringIO(csv_text))
    rows = list(reader)

    header_i, date_col, time_col, group_cols = find_header_and_group_cols(rows, group_name)

    if not group_cols:
        # полезный дебаг: какие группы видим
        groups_found = set()
        for i in range(min(50, len(rows))):
            for cell in rows[i]:
                c = norm(cell)
                if c.startswith("ИГ") or c.startswith("иг"):
                    groups_found.add(c)
        hint = ", ".join(sorted(groups_found)) if groups_found else "не нашла ни одной"
        raise RuntimeError(f"Не нашла колонку группы '{group_name}'. В таблице вижу: {hint}")

    items = []  # list[(time, text)]
    cur_date = None
    cur_time = None

    for r in rows[header_i + 1 :]:
        need_len = max(date_col, time_col, max(group_cols)) + 1
        if len(r) < need_len:
            r = r + [""] * (need_len - len(r))

        d_raw = norm(r[date_col])
        t_raw = norm(r[time_col])

        ddmm = parse_ddmm(d_raw)
        if ddmm:
            cur_date = ddmm

        t_norm = normalize_time(t_raw)
        if TIME_RE.search(t_raw):
            cur_time = t_norm

        if cur_date != target_ddmm:
            continue

        if not cur_time:
            continue

        # собираем текст из всех колонок группы
        parts = []
        for j in group_cols:
            v = norm(r[j])
            if not v:
                continue
            parts.append(v)

        # ✅ НОВОЕ: если в строке нет пары (пусто в колонках группы),
        # но время есть — добавляем "нет пары"
        if not parts:
            items.append((cur_time, "нет пары"))
            continue

        # объединяем, убираем дубли строк
        cell_text = "\n".join(parts).strip()
        lines = _postprocess_cell_text(cell_text)

        # если после чистки всё пропало — тоже считаем как "нет пары"
        if not lines:
            items.append((cur_time, "нет пары"))
            continue

        # убираем дубли строк (внутри пары)
        seen = set()
        uniq = []
        for ln in lines:
            key = ln.lower()
            if key in seen:
                continue
            seen.add(key)
            uniq.append(ln)

        items.append((cur_time, "\n".join(uniq)))

    # Убираем повторы по (время + текст)
    out = []
    seen = set()
    for tm, tx in items:
        key = (tm, tx)
        if key in seen:
            continue
        seen.add(key)
        out.append((tm, tx))
    return out

from collections import OrderedDict

def merge_items_by_time(items: list[tuple[str, str]]) -> list[tuple[str, str]]:
    """
    Склеивает строки с одинаковым временем в один блок.
    Убирает дубли строк внутри одного времени.
    """
    merged: "OrderedDict[str, list[str]]" = OrderedDict()

    for tm, tx in items:
        tm = (tm or "").strip()
        tx = (tx or "").strip()
        if not tm or not tx:
            continue

        lines = [l.strip() for l in tx.splitlines() if l.strip()]
        bucket = merged.setdefault(tm, [])

        for line in lines:
            if line not in bucket:
                bucket.append(line)

    result: list[tuple[str, str]] = []
    for tm, lines in merged.items():
        result.append((tm, "\n".join(lines)))

    return result

def format_schedule(group_name: str, ddmm: str, items: list[tuple[str, str]]) -> str:
    title = f"{group_name} — {ddmm}:"

    if not items:
        return title + "\nнет пар"

    # ✅ убираем повторы времени
    items = merge_items_by_time(items)

    out_lines = [title]

    for tm, text in items:
        parts = [p.strip() for p in text.split("\n") if p.strip()]
        if not parts:
            continue

        # первая строка — основной текст пары (обычно предмет)
        out_lines.append(f"• {tm} — {parts[0]}")

        # остальные строки — подробности с отступом (как во “втором примере”)
        for extra in parts[1:]:
            out_lines.append(f"  {extra}")

        out_lines.append("")  # пустая строка между парами

    while out_lines and out_lines[-1] == "":
        out_lines.pop()

    return "\n".join(out_lines)


# =======================
# TELEGRAM HANDLERS
# =======================
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Привет! Я твой виртуальный помощник ОТЕЛЬКА 🩵. Давай помогу с расписанием!\n\n"
        "Команды:\n"
        "/today — расписание на сегодня\n"
        "/tomorrow — расписание на завтра\n"
        "/day 30.01 — расписание на дату (ДД.ММ)\n\n"
        "Можно и текстом: 30.01 или «день 30.01»"
    )

async def cmd_today(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ddmm = datetime.now(TZ).strftime("%d.%m")
    await send_schedule(update, ddmm)

async def cmd_tomorrow(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ddmm = (datetime.now(TZ) + timedelta(days=1)).strftime("%d.%m")
    await send_schedule(update, ddmm)

async def cmd_day(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    if args:
        ddmm = parse_ddmm(" ".join(args))
        if not ddmm:
            await update.message.reply_text("Формат даты: /day 30.01 (ДД.ММ)")
            return
    else:
        ddmm = datetime.now(TZ).strftime("%d.%m")

    await send_schedule(update, ddmm)

async def text_day(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip().lower()
    m = re.search(r"(\d{1,2}[.\-/]\d{1,2}(?:[.\-/]\d{2,4})?)", text)
    if not m:
        return
    ddmm = parse_ddmm(m.group(1))
    if not ddmm:
        return
    await send_schedule(update, ddmm)

async def send_schedule(update: Update, ddmm: str):
    url = os.getenv("SHEET_CSV_URL", "").strip()
    group = os.getenv("GROUP_NAME", DEFAULT_GROUP).strip()

    if not url:
        await update.message.reply_text("Не задана переменная SHEET_CSV_URL.")
        return

    try:
        csv_text = fetch_csv_text(url)
        items = extract_schedule_for_date(csv_text, group, ddmm)
        msg = format_schedule(group, ddmm, items)
        await update.message.reply_text(msg)
    except Exception as e:
        await update.message.reply_text(f"Ошибка чтения расписания: {e}")


# =======================
# MAIN
# =======================
def main():
    load_dotenv()
    token = os.getenv("BOT_TOKEN", "").strip()
    if not token:
        raise RuntimeError("Нет BOT_TOKEN...")

    # запускаем мини-веб-сервер для Render
    keep_alive()

    app = Application.builder().token(token).build()

    # команды
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("today", cmd_today))
    app.add_handler(CommandHandler("tomorrow", cmd_tomorrow))
    app.add_handler(CommandHandler("day", cmd_day))

    # текстовая дата
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_day))

    print("Bot started / polling")

    # ✅ фикс конфликта getUpdates (если Render перезапустил второй процесс)
    app.run_polling(
        drop_pending_updates=True
        allowed_updates=Update.ALL_TYPES,
        close_loop=False, 
    )

if __name__ == "__main__":
    main()

