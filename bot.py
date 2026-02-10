import os
import re
import csv
import io
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

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

# =========================
# Render + Flask (keep alive)
# =========================
from threading import Thread
from flask import Flask

web = Flask(__name__)

@web.get("/")
def home():
    return "ok", 200

def run_web():
    port = int(os.environ.get("PORT", "10000"))
    web.run(host="0.0.0.0", port=port)

def keep_alive():
    Thread(target=run_web, daemon=True).start()


# =========================
# Настройки
# =========================
TZ = ZoneInfo("Asia/Krasnoyarsk")  # Красноярск (+07)
DEFAULT_GROUP = os.getenv("GROUP_NAME", "ИГ25-01Б-ОМ")

# Даты типа: 02.02 / 02-02 / 02/02 / 02.02.26
DATE_RE = re.compile(r"\b(\d{1,2})[.\-/](\d{1,2})(?:[.\-/]\d{2,4})?\b")

# Время: 8:30-10:05 / 08:30–10:05 / 8.30-10.05
TIME_RE = re.compile(r"(\d{1,2})[.:](\d{2})\s*[–—-]\s*(\d{1,2})[.:](\d{2})")

# Кэш, чтобы не дёргать гугл при каждом сообщении
_CACHE_TEXT = None
_CACHE_TS = 0
CACHE_SECONDS = 60


# =========================
# Утилиты нормализации
# =========================
def norm(s: str) -> str:
    return (s or "").replace("\xa0", " ").strip()

def norm_group(s: str) -> str:
    """Нормализуем название группы: разные тире/пробелы -> одинаково"""
    s = norm(s)
    s = s.replace("—", "-").replace("–", "-")
    s = re.sub(r"\s+", " ", s)
    return s.upper()

def parse_ddmm(text: str) -> str | None:
    m = DATE_RE.search(text or "")
    if not m:
        return None
    dd = int(m.group(1))
    mm = int(m.group(2))
    return f"{dd:02d}.{mm:02d}"

def normalize_time(text: str) -> str | None:
    """Ищем интервал времени и приводим к HH:MM–HH:MM"""
    m = TIME_RE.search(text or "")
    if not m:
        return None
    h1, m1, h2, m2 = m.groups()
    return f"{int(h1):02d}:{m1}–{int(h2):02d}:{m2}"

def fetch_csv_text(url: str) -> str:
    global _CACHE_TEXT, _CACHE_TS
    now = time.time()
    if _CACHE_TEXT and (now - _CACHE_TS) < CACHE_SECONDS:
        return _CACHE_TEXT

    r = requests.get(url, timeout=25)
    r.raise_for_status()
    _CACHE_TEXT = r.text
    _CACHE_TS = now
    return _CACHE_TEXT

def read_csv_rows(csv_text: str) -> list[list[str]]:
    # Автоматически определяем разделитель CSV (Google часто даёт ;)
    sample = csv_text[:5000]
    delim = ";" if sample.count(";") > sample.count(",") else ","

    reader = csv.reader(io.StringIO(csv_text), delimiter=delim)
    return list(reader)

# =========================
# Поиск заголовка и колонок группы
# =========================
def find_header_and_group_cols(rows: list[list[str]], group_name: str):
    """
    Ищем строку, где есть 'Дата' и 'Часы' (в первых 80 строках),
    затем ищем колонку группы рядом ниже.
    """
    g_need = norm_group(group_name)

    header_row_i = None
    date_col = None
    time_col = None

    # 1) строка заголовков
    for i in range(min(80, len(rows))):
        row = [norm(x) for x in rows[i]]
        low = [x.lower() for x in row]

        # ищем по "вхождению", а не по строгому равенству
        def find_col(keyword: str):
            for idx, cell in enumerate(low):
                if keyword in cell:
                    return idx
            return None

        dc = find_col("дата")
        tc = find_col("часы")
        if dc is not None and tc is not None:
            header_row_i = i
            date_col = dc
            time_col = tc
            break

    if header_row_i is None:
        # дебаг: покажем первые строки, чтобы понять структуру
        preview = "\n".join([" | ".join([norm(x) for x in rows[k][:8]]) for k in range(min(8, len(rows)))])
        raise RuntimeError("Не нашла заголовки 'Дата' и 'Часы' в таблице (CSV).\nПервые строки:\n" + preview)

    # 2) колонки группы — ищем ниже заголовков (в пределах 20 строк)
    group_cols = []
    for i in range(header_row_i, min(header_row_i + 20, len(rows))):
        row = rows[i]
        for j, cell in enumerate(row):
            if norm_group(cell) == g_need:
                group_cols.append(j)

    group_cols = sorted(set(group_cols))
    return header_row_i, date_col, time_col, group_cols


# =========================
# Склейки "пр/лек/лаб" и чистка строк
# =========================
def compact_spaces(s: str) -> str:
    s = (s or "").replace("\xa0", " ").replace("\t", " ")
    s = re.sub(r"[ ]{2,}", " ", s)
    return s.strip()

def glue_markers_to_prev(lines: list[str]) -> list[str]:
    """
    Приклеиваем 'пр', 'лек', 'лаб' и варианты вроде 'пр / 3-17' к предыдущей строке.
    Пример:
      "... синхронно,"  + "пр" + "/ 3-17"  -> "... синхронно, пр / 3-17"
    """
    out: list[str] = []
    for raw in lines:
        ln = compact_spaces(raw)
        if not ln:
            continue

        low = ln.lower()

        is_marker_alone = low in {"пр", "лек", "лаб", "сем"}  # если вдруг попадается
        is_marker_start = bool(re.match(r"^(пр|лек|лаб)\b", low))
        is_slash_room = ln.startswith("/")  # "/ 3-17" тоже приклеим

        if out and (is_marker_alone or is_marker_start or is_slash_room):
            out[-1] = compact_spaces(out[-1] + " " + ln)
        else:
            out.append(ln)

    return out


# =========================
# Извлечение расписания на дату
# =========================
def extract_schedule_for_date(csv_text: str, group_name: str, target_ddmm: str):
    rows = read_csv_rows(csv_text)

    header_i, date_col, time_col, group_cols = find_header_and_group_cols(rows, group_name)

    if not group_cols:
        # дебаг — какие группы видим
        groups_found = set()
        for i in range(min(40, len(rows))):
            for cell in rows[i]:
                c = norm(cell)
                if c.upper().startswith("ИГ"):
                    groups_found.add(c)
        hint = ", ".join(sorted(groups_found)) if groups_found else "не нашла ни одной"
        raise RuntimeError(f"Не нашла колонку группы '{group_name}'. В таблице вижу группы: {hint}")

    cur_date = None
    cur_time = None
    items: list[tuple[str, str]] = []

    for r in rows[header_i + 1:]:
        # защита: расширяем короткие строки
        need_len = max(date_col, time_col, max(group_cols)) + 1
        if len(r) < need_len:
            r = r + [""] * (need_len - len(r))

        d_raw = norm(r[date_col])
        t_raw = norm(r[time_col])

        ddmm = parse_ddmm(d_raw)
        if ddmm:
            cur_date = ddmm

        t_norm = normalize_time(t_raw)
        if t_norm:
            cur_time = t_norm

        if cur_date != target_ddmm:
            continue

        # собираем текст из колонок группы
        parts = []
        for j in group_cols:
            v = norm(r[j])
            if not v:
                continue

            # чистим мусорные строки (под себя можешь расширить)
            lv = v.lower()
            if "семестр" in lv or "утверждаю" in lv:
                continue

            parts.append(v)

        if not parts:
            continue

        cell_text = "\n".join(parts).strip()
        lines = [x.strip() for x in cell_text.splitlines() if x.strip()]

        # Склейки пр/лек/лаб и "/ 3-17" к предыдущей
        lines = glue_markers_to_prev(lines)

        # Убираем дубли строк в рамках одной пары
        uniq = []
        seen = set()
        for x in lines:
            if x not in seen:
                seen.add(x)
                uniq.append(x)

        text_block = "\n".join(uniq).strip()
        if cur_time and text_block:
            items.append((cur_time, text_block))

    return items


# =========================
# Объединение по времени (убираем повтор времени)
# =========================
def merge_items_by_time(items: list[tuple[str, str]]) -> list[tuple[str, str]]:
    """
    Если одно и то же время встречается несколько раз — объединяем тексты в один блок.
    """
    merged: dict[str, list[str]] = {}
    for tm, tx in items:
        tm = (tm or "").strip()
        tx = (tx or "").strip()
        if not tm or not tx:
            continue
        merged.setdefault(tm, []).append(tx)

    out: list[tuple[str, str]] = []
    for tm in sorted(merged.keys()):
        # Склеим блоки, уберём дубль блоков
        blocks = []
        seen = set()
        for b in merged[tm]:
            if b not in seen:
                seen.add(b)
                blocks.append(b)
        out.append((tm, "\n".join(blocks)))
    return out


# =========================
# Форматирование ответа
# =========================
def format_schedule(group_name: str, ddmm: str, items: list[tuple[str, str]]) -> str:
    title = f"{group_name} — {ddmm}:"
    if not items:
        return title + "\n• Нет пары"

    items = merge_items_by_time(items)

    out_lines = [title]
    for tm, tx in items:
        # Заголовок пары: время + первая строка блока
        raw_lines = [x.strip() for x in (tx or "").splitlines() if x.strip()]
        if not raw_lines:
            continue

        # на всякий случай ещё раз склеим маркеры
        raw_lines = glue_markers_to_prev(raw_lines)

        first = raw_lines[0]
        out_lines.append(f"• {tm} — {first}")

        # остальные строки — просто с отступом
        for ln in raw_lines[1:]:
            out_lines.append(f"  {ln}")

        out_lines.append("")  # пустая строка между парами

    # убираем хвостовую пустую строку
    while out_lines and out_lines[-1] == "":
        out_lines.pop()

    return "\n".join(out_lines)


# =========================
# Telegram handlers
# =========================
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


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Привет! Я твой виртуальный помощник ОТЕЛЬКА 💙\n"
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

    # принимает "30.01" или "день 30.01"
    m = re.match(r"^(?:день\s+)?(\d{1,2}[.\-/]\d{1,2}(?:[.\-/]\d{2,4})?)$", text)
    if not m:
        return

    ddmm = parse_ddmm(m.group(1))
    if not ddmm:
        return

    await send_schedule(update, ddmm)


# =========================
# Main
# =========================
def main():
    load_dotenv()

    token = os.getenv("BOT_TOKEN", "").strip()
    if not token:
        raise RuntimeError("Нет BOT_TOKEN...")

    # запускаем мини-веб-сервер, чтобы Render видел порт
    keep_alive()

    app = Application.builder().token(token).build()

    # команды
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("today", cmd_today))
    app.add_handler(CommandHandler("tomorrow", cmd_tomorrow))
    app.add_handler(CommandHandler("day", cmd_day))

    # текстовые даты
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_day))

    print("Bot started / polling")
    # ВАЖНО: одна строка run_polling, и тут корректные параметры
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()

# force redeploy

# fffff
