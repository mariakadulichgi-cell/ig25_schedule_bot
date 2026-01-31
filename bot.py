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
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters


# ----------------- НАСТРОЙКИ -----------------
TZ = ZoneInfo("Asia/Krasnoyarsk")  # Красноярск (+07)
DEFAULT_GROUP = os.getenv("GROUP_NAME", "ИГ25-01Б-ОМ")

DATE_RE = re.compile(r"\b(\d{1,2})[.\-/](\d{1,2})(?:[.\-/](\d{2,4}))?\b")  # 02.02 или 02.02.26
TIME_RE = re.compile(r"(\d{1,2})[.:](\d{2})\s*[-–—]\s*(\d{1,2})[.:](\d{2})")  # 8:30-10:05 и т.п.

# кэш CSV чтобы не долбить гугл при каждом сообщении
_CACHE_TEXT = None
_CACHE_TS = 0
CACHE_SECONDS = 60


def norm(s: str) -> str:
    return (s or "").replace("\xa0", " ").strip()


def norm_group(s: str) -> str:
    """Нормализуем название группы: разные тире/пробелы -> одинаково"""
    s = norm(s)
    s = s.replace("—", "-").replace("–", "-")
    s = re.sub(r"\s+", "", s)
    return s.upper()


def normalize_time(s: str) -> str:
    s = norm(s)
    m = TIME_RE.search(s)
    if not m:
        return s
    h1, m1, h2, m2 = m.groups()
    return f"{int(h1)}:{m1}–{int(h2)}:{m2}"


def parse_ddmm(text: str) -> str | None:
    m = DATE_RE.search(text or "")
    if not m:
        return None
    dd = int(m.group(1))
    mm = int(m.group(2))
    return f"{dd:02d}.{mm:02d}"


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


def find_header_and_group_cols(rows: list[list[str]], group_name: str):
    """
    Ищем строку, где есть 'Дата' и 'Часы' (в первых 60 строках),
    потом находим колонки группы в следующих строках.
    """
    g_need = norm_group(group_name)

    header_row_i = None
    date_col = None
    time_col = None

    # 1) строка заголовков: ищем "дата" и "часы"
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

    # 2) колонки группы: ищем рядом ниже заголовков (в пределах 12 строк)
    group_cols = []
    for i in range(header_row_i, min(header_row_i + 12, len(rows))):
        row = rows[i]
        for j, cell in enumerate(row):
            if norm_group(cell) == g_need:
                group_cols.append(j)

    group_cols = sorted(set(group_cols))
    return header_row_i, date_col, time_col, group_cols


def extract_schedule_for_date(csv_text: str, group_name: str, target_ddmm: str):
    reader = csv.reader(io.StringIO(csv_text))
    rows = list(reader)

    header_i, date_col, time_col, group_cols = find_header_and_group_cols(rows, group_name)

    if not group_cols:
        # полезный дебаг: какие группы видим в таблице
        groups_found = set()
        for i in range(min(40, len(rows))):
            for cell in rows[i]:
                c = norm(cell)
                if c.startswith("ИГ"):
                    groups_found.add(c)
        hint = ", ".join(sorted(groups_found)) if groups_found else "не нашла ни одной"
        raise RuntimeError(f"Не нашла колонку группы '{group_name}'. В таблице вижу группы: {hint}")

    cur_date = ""
    cur_time = ""
    items = []

    for r in rows[header_i + 1 :]:
        # защита: расширяем строку, если короткая
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

        # собираем текст из всех найденных колонок группы
        parts = []
        for j in group_cols:
            v = norm(r[j])
            if not v:
                continue
            # чистим мусор
            if "семестр" in v.lower() or "утверждаю" in v.lower():
                continue
            parts.append(v)

        if not parts:
            continue

        # объединяем, убираем дубли строк внутри ячейки
        cell_text = "\n".join(parts).strip()
        lines = [x.strip() for x in cell_text.splitlines() if x.strip()]
        uniq_lines = []
        seen = set()
        for x in lines:
            if x not in seen:
                seen.add(x)
                uniq_lines.append(x)

        items.append((cur_time, "\n".join(uniq_lines)))

    # убираем повторы по (время + текст)
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
    merged: "OrderedDict[str, list[str]]" = OrderedDict()

    for tm, tx in items:
        tm = (tm or "").strip()
        tx = (tx or "").strip()
        if not tm or not tx:
            continue

        # разбиваем текст на строки и чистим
        lines = [l.strip() for l in tx.splitlines() if l.strip()]

        bucket = merged.setdefault(tm, [])
        for l in lines:
            # убираем случайные "•" если вдруг попали
            l = l.lstrip("•").strip()
            if l and l not in bucket:
                bucket.append(l)

    # собираем обратно
    out = []
    for tm, lines in merged.items():
        out.append((tm, "\n".join(lines)))
    return out


def _fix_parts(parts: list[str]) -> list[str]:
    # 1) чистим и убираем пустые
    parts = [p.strip() for p in parts if p and p.strip()]

    # 2) склеиваем "пр" + следующая строка (если следующая начинается с "/")
    out = []
    i = 0
    while i < len(parts):
        p = parts[i]
        low = p.lower()

        if low in {"пр", "пр."} and i + 1 < len(parts):
            nxt = parts[i + 1].strip()
            if nxt.startswith("/"):   # было "пр" на одной строке, "/ 3-17" на другой
                out.append(f"пр {nxt}")
                i += 2
                continue

        out.append(p)
        i += 1

    # 3) приклеиваем строку "пр ..." к предыдущей (если предыдущая похожа на адрес/аудиторию)
    def should_attach(prev: str) -> bool:
        s = prev.lower()
        return any(k in s for k in ("мфк", "пр.", "стр", "ауд", "синхрон", "ул.", "корп"))

    final = []
    for p in out:
        if final and p.lower().startswith("пр") and should_attach(final[-1]):
            final[-1] = f"{final[-1]} {p}"
        else:
            final.append(p)

    return final


def _compact_spaces(s: str) -> str:
    # убираем табы/неразрывные пробелы и сжимаем множественные пробелы
    s = (s or "").replace("\u00a0", " ").replace("\t", " ")
    s = re.sub(r"[ ]{2,}", " ", s)
    return s.strip()

def _glue_pr_lines(lines: list[str]) -> list[str]:
    """
    Склеиваем строки, которые выглядят как "пр", "пр / 3-17", "/ 3-17"
    чтобы они не уезжали отдельно.
    """
    out: list[str] = []
    for raw in lines:
        ln = _compact_spaces(raw)
        if not ln:
            continue

        low = ln.lower()

        # если строка начинается с "пр" или "/" — доклеиваем к предыдущей
        if out and (low == "пр" or low.startswith("пр/") or low.startswith("пр /") or ln.startswith("/")):
            out[-1] = _compact_spaces(out[-1] + " " + ln)
            continue

        out.append(ln)

    return out

def format_schedule(group_name: str, ddmm: str, items: list[tuple[str, str]]) -> str:
    title = f"{group_name} — {ddmm}:"
    if not items:
        return title + "\n• Нет занятий / нет данных"

    items = merge_items_by_time(items)

    out_lines = [title]
    for tm, tx in items:
        tx = (tx or "").replace("\r", "")
        raw_lines = tx.splitlines()
        parts = _glue_pr_lines(raw_lines)

        if not parts:
            continue

        # первая строка — с временем
        out_lines.append(f"• {tm} — {parts[0]}")

        # остальные строки — просто с отступом (без стрелок)
        for p in parts[1:]:
            out_lines.append(f"  {p}")

        # ПУСТАЯ СТРОКА между парами (как “абзац”)
        out_lines.append("")

    # уберём последнюю пустую строку, чтобы не было лишнего хвоста
    while out_lines and out_lines[-1] == "":
        out_lines.pop()

    return "\n".join(out_lines)




# ----------------- TELEGRAM HANDLERS -----------------
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Привет! Я твой виртуальный помощник ОТЕЛЬКА 🩵. Давай помогу тебе с расписанием! Команды:\n"
        "/day 30.01 — расписание на дату (ДД.ММ)\n"
        "Можно и текстом: день 30.01"
    )


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
    m = re.match(r"^(?:день\s+)?(\d{1,2}[.\-/]\d{1,2}(?:[.\-/]\d{2,4})?)$", text)
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


def main():
    load_dotenv()  # чтобы можно было хранить токен в .env

    token = os.getenv("BOT_TOKEN", "").strip()
    if not token:
        raise RuntimeError("Нет BOT_TOKEN в переменных окружения (.env или export).")

    app = Application.builder().token(token).build()

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("day", cmd_day))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_day))

    print("Bot started / polling")
    app.run_polling()


if __name__ == "__main__":
    main()

