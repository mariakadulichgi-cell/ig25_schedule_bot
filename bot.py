import io
import os
import re
import time
from datetime import date, datetime, time as dt_time, timedelta
from threading import Thread
from typing import Dict, List, Optional, Set, Tuple
from zoneinfo import ZoneInfo

import requests
from dotenv import load_dotenv
from flask import Flask
from openpyxl import load_workbook
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
DEFAULT_TIMEZONE = "Asia/Krasnoyarsk"
DEFAULT_GROUP = "ИГ25-01Б-ОМ"

DATE_RE = re.compile(r"\b(\d{1,2})[.\-/](\d{1,2})(?:[.\-/](\d{2,4}))?\b")
TIME_RE = re.compile(r"(\d{1,2})[.:](\d{2})\s*[–—-]\s*(\d{1,2})[.:](\d{2})")

_CACHE_ROWS: Optional[List[List[str]]] = None
_CACHE_TS: float = 0.0


# =========================
# Утилиты
# =========================
def get_tz() -> ZoneInfo:
    tz_name = os.getenv("TIMEZONE", DEFAULT_TIMEZONE).strip() or DEFAULT_TIMEZONE
    return ZoneInfo(tz_name)


def norm(value: object) -> str:
    return str(value or "").replace("\xa0", " ").strip()


def norm_group(value: object) -> str:
    text = norm(value)
    text = text.replace("—", "-").replace("–", "-")
    text = re.sub(r"\s+", " ", text)
    return text.upper()


def compact_spaces(text: str) -> str:
    text = (text or "").replace("\xa0", " ").replace("\t", " ")
    text = re.sub(r"[ ]{2,}", " ", text)
    return text.strip()


def parse_ddmm(text: object) -> Optional[str]:
    raw = norm(text)

    # убираем пробелы и чиним кривые разделители
    raw = raw.replace(" ", "")
    raw = re.sub(r"\.+", ".", raw)          # 13..04 -> 13.04
    raw = re.sub(r"[-/]{2,}", "-", raw)     # 13--04 -> 13-04
    raw = re.sub(r"[.\-/]{2,}", ".", raw)   # любой повтор разделителей -> "."

    match = re.search(r"(\d{1,2})[.\-/](\d{1,2})(?:[.\-/](\d{2,4}))?", raw)
    if not match:
        return None

    day = int(match.group(1))
    month = int(match.group(2))

    if not (1 <= day <= 31 and 1 <= month <= 12):
        return None

    return f"{day:02d}.{month:02d}"

def normalize_time(text: object) -> Optional[str]:
    raw = norm(text)
    raw = raw.replace("—", "-").replace("–", "-")
    match = TIME_RE.search(raw)
    if not match:
        return None

    h1, m1, h2, m2 = match.groups()
    return f"{int(h1):02d}:{m1}–{int(h2):02d}:{m2}"


def to_xlsx_export_url(url: str) -> str:
    url = url.strip()
    match = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", url)
    if not match:
        return url
    spreadsheet_id = match.group(1)
    return f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/export?format=xlsx"


def cell_to_text(value: object) -> str:
    if value is None:
        return ""

    if isinstance(value, datetime):
        if value.hour or value.minute or value.second:
            return value.strftime("%H:%M")
        return value.strftime("%d.%m.%Y")

    if isinstance(value, date):
        return value.strftime("%d.%m.%Y")

    if isinstance(value, dt_time):
        return value.strftime("%H:%M")

    return str(value)


def worksheet_to_rows(ws) -> List[List[str]]:
    merged_values: Dict[Tuple[int, int], object] = {}

    for merged_range in ws.merged_cells.ranges:
        min_col, min_row, max_col, max_row = merged_range.bounds
        top_left_value = ws.cell(min_row, min_col).value

        for row_idx in range(min_row, max_row + 1):
            for col_idx in range(min_col, max_col + 1):
                merged_values[(row_idx, col_idx)] = top_left_value

    rows: List[List[str]] = []

    for row_idx in range(1, ws.max_row + 1):
        row: List[str] = []

        for col_idx in range(1, ws.max_column + 1):
            value = ws.cell(row_idx, col_idx).value

            if value in (None, "") and (row_idx, col_idx) in merged_values:
                value = merged_values[(row_idx, col_idx)]

            row.append(cell_to_text(value))

        rows.append(row)

    return rows


def find_col_by_keywords(row_lower: List[str], keywords: List[str]) -> Optional[int]:
    for idx, cell in enumerate(row_lower):
        for keyword in keywords:
            if keyword in cell:
                return idx
    return None


def sheet_looks_like_schedule(rows: List[List[str]], group_name: str) -> bool:
    target_group = norm_group(group_name)

    for i in range(min(120, len(rows))):
        row = [norm(cell) for cell in rows[i]]
        row_lower = [cell.lower() for cell in row]

        has_date = find_col_by_keywords(row_lower, ["дата"]) is not None
        has_time = find_col_by_keywords(row_lower, ["часы", "время"]) is not None
        has_group = any(target_group == norm_group(cell) for cell in row)

        if has_date and has_time and has_group:
            return True

    return False


def fetch_sheet_rows(url: str, group_name: str, sheet_name: Optional[str] = None) -> List[List[str]]:
    export_url = to_xlsx_export_url(url)

    response = requests.get(export_url, timeout=30)
    response.raise_for_status()

    workbook = load_workbook(io.BytesIO(response.content), data_only=True)

    # 1. Если лист указан явно — читаем его
    if sheet_name:
        if sheet_name not in workbook.sheetnames:
            available = ", ".join(workbook.sheetnames)
            raise RuntimeError(
                f"Лист '{sheet_name}' не найден. Доступные листы: {available}"
            )
        worksheet = workbook[sheet_name]
        return worksheet_to_rows(worksheet)

    # 2. Если лист не указан — ищем автоматически
    for ws in workbook.worksheets:
        rows = worksheet_to_rows(ws)
        if sheet_looks_like_schedule(rows, group_name):
            return rows

    # 3. Если не нашли — покажем доступные вкладки
    available = ", ".join(workbook.sheetnames)
    first_sheet_rows = worksheet_to_rows(workbook.worksheets[0])
    preview = "\n".join(
        " | ".join([norm(cell) for cell in first_sheet_rows[k][:12]])
        for k in range(min(10, len(first_sheet_rows)))
    )

    raise RuntimeError(
        "Не удалось автоматически найти лист с расписанием.\n"
        f"Доступные листы: {available}\n\n"
        f"Первые строки первого листа:\n{preview}"
    )


def get_rows_with_cache(url: str, group_name: str, sheet_name: Optional[str]) -> List[List[str]]:
    global _CACHE_ROWS, _CACHE_TS

    cache_seconds = int(os.getenv("CACHE_SECONDS", "60") or "60")
    now = time.time()

    if _CACHE_ROWS is not None and (now - _CACHE_TS) < cache_seconds:
        return _CACHE_ROWS

    rows = fetch_sheet_rows(url, group_name, sheet_name)
    _CACHE_ROWS = rows
    _CACHE_TS = now
    return rows


def find_header_and_group_cols(
    rows: List[List[str]],
    group_name: str,
) -> Tuple[int, int, int, List[int]]:
    target_group = norm_group(group_name)

    header_row_idx: Optional[int] = None
    date_col: Optional[int] = None
    time_col: Optional[int] = None

    for i in range(min(120, len(rows))):
        row = [norm(cell) for cell in rows[i]]
        row_lower = [cell.lower() for cell in row]

        found_date_col = find_col_by_keywords(row_lower, ["дата"])
        found_time_col = find_col_by_keywords(row_lower, ["часы", "время"])

        if found_date_col is not None and found_time_col is not None:
            header_row_idx = i
            date_col = found_date_col
            time_col = found_time_col
            break

    if header_row_idx is None or date_col is None or time_col is None:
        preview = "\n".join(
            " | ".join([norm(cell) for cell in rows[k][:12]])
            for k in range(min(10, len(rows)))
        )
        raise RuntimeError(
            "Не удалось найти заголовки 'Дата' и 'Часы/Время' в таблице.\n\n"
            f"Первые строки таблицы:\n{preview}"
        )

    group_cols: List[int] = []
    search_until = min(header_row_idx + 30, len(rows))

    for i in range(header_row_idx, search_until):
        row = rows[i]
        for j, cell in enumerate(row):
            if norm_group(cell) == target_group:
                group_cols.append(j)

    group_cols = sorted(set(group_cols))

    if not group_cols:
        for i in range(header_row_idx, search_until):
            row = rows[i]
            for j, cell in enumerate(row):
                normalized = norm_group(cell)
                if target_group and target_group in normalized:
                    group_cols.append(j)

        group_cols = sorted(set(group_cols))

    if not group_cols:
        seen_groups: Set[str] = set()
        for i in range(min(50, len(rows))):
            for cell in rows[i]:
                text = norm(cell)
                if text.upper().startswith("ИГ"):
                    seen_groups.add(text)

        hint = ", ".join(sorted(seen_groups)) if seen_groups else "ничего похожего не найдено"
        raise RuntimeError(
            f"Не нашла колонку группы '{group_name}'. В таблице нашла: {hint}"
        )

    return header_row_idx, date_col, time_col, group_cols


def should_skip_cell_text(text: str, group_name: str) -> bool:
    lowered = text.lower().strip()

    if not lowered:
        return True

    if norm_group(text) == norm_group(group_name):
        return True

    trash_fragments = (
        "утверждаю",
        "семестр",
        "расписание",
        "проректор",
        "директор",
        "учебный год",
    )
    return any(fragment in lowered for fragment in trash_fragments)


def glue_markers_to_prev(lines: List[str]) -> List[str]:
    out: List[str] = []

    for raw in lines:
        line = compact_spaces(raw)
        if not line:
            continue

        low = line.lower()

        is_marker_alone = low in {"пр", "лек", "лаб", "сем"}
        is_marker_start = bool(re.match(r"^(пр|лек|лаб|сем)\b", low))
        is_slash_room = line.startswith("/")

        if out and (is_marker_alone or is_marker_start or is_slash_room):
            out[-1] = compact_spaces(out[-1] + " " + line)
        else:
            out.append(line)

    return out


def cleanup_lines(parts: List[str], group_name: str) -> List[str]:
    text = "\n".join(parts).strip()
    raw_lines = [line.strip() for line in text.splitlines() if line.strip()]

    filtered: List[str] = []
    for line in raw_lines:
        if should_skip_cell_text(line, group_name):
            continue
        filtered.append(line)

    filtered = glue_markers_to_prev(filtered)

    uniq: List[str] = []
    seen: Set[str] = set()

    for line in filtered:
        if line not in seen:
            seen.add(line)
            uniq.append(line)

    return uniq


def time_sort_key(time_range: str) -> Tuple[int, int]:
    match = re.match(r"^(\d{2}):(\d{2})", time_range or "")
    if not match:
        return (99, 99)
    return int(match.group(1)), int(match.group(2))


def merge_items_by_time(items: List[Tuple[str, str]]) -> List[Tuple[str, str]]:
    grouped: Dict[str, List[str]] = {}
    order: List[str] = []

    for time_value, text_block in items:
        if time_value not in grouped:
            grouped[time_value] = []
            order.append(time_value)

        if text_block not in grouped[time_value]:
            grouped[time_value].append(text_block)

    ordered_times = sorted(order, key=time_sort_key)

    result: List[Tuple[str, str]] = []
    for time_value in ordered_times:
        result.append((time_value, "\n".join(grouped[time_value])))

    return result


def extract_schedule_for_date(
    rows: List[List[str]],
    group_name: str,
    target_ddmm: str,
) -> List[Tuple[str, str]]:
    header_idx, date_col, time_col, group_cols = find_header_and_group_cols(rows, group_name)

    current_date: Optional[str] = None
    current_time: Optional[str] = None
    items: List[Tuple[str, str]] = []
    seen_pairs: Set[Tuple[str, str]] = set()

    max_needed_col = max([date_col, time_col] + group_cols)

    for raw_row in rows[header_idx + 1:]:
        row = list(raw_row)
        if len(row) <= max_needed_col:
            row.extend([""] * (max_needed_col + 1 - len(row)))

        date_value = norm(row[date_col])
        time_value = norm(row[time_col])

        parsed_date = parse_ddmm(date_value)
        if parsed_date:
            current_date = parsed_date

        parsed_time = normalize_time(time_value)
        if parsed_time:
            current_time = parsed_time

        if current_date != target_ddmm:
            continue

        if not current_time:
            continue

        parts: List[str] = []
        for col in group_cols:
            value = norm(row[col])
            if not value:
                continue
            if should_skip_cell_text(value, group_name):
                continue
            parts.append(value)

        if not parts:
            continue

        lines = cleanup_lines(parts, group_name)
        if not lines:
            continue

        text_block = "\n".join(lines).strip()
        if not text_block:
            continue

        pair_key = (current_time, text_block)
        if pair_key in seen_pairs:
            continue

        seen_pairs.add(pair_key)
        items.append(pair_key)

    return merge_items_by_time(items)


def format_schedule(group_name: str, ddmm: str, items: List[Tuple[str, str]]) -> str:
    title = f"{group_name} — {ddmm}:"

    if not items:
        return title + "\n• Нет пары"

    out_lines: List[str] = [title]

    for time_value, text_block in items:
        lines = [line.strip() for line in text_block.splitlines() if line.strip()]
        lines = glue_markers_to_prev(lines)

        if not lines:
            continue

        out_lines.append(f"• {time_value} — {lines[0]}")
        for line in lines[1:]:
            out_lines.append(f"  {line}")
        out_lines.append("")

    while out_lines and out_lines[-1] == "":
        out_lines.pop()

    return "\n".join(out_lines)


def split_message(text: str, limit: int = 3800) -> List[str]:
    if len(text) <= limit:
        return [text]

    chunks: List[str] = []
    current = ""

    for line in text.splitlines(True):
        if len(current) + len(line) <= limit:
            current += line
            continue

        if current.strip():
            chunks.append(current.rstrip())
            current = ""

        if len(line) <= limit:
            current = line
            continue

        rest = line
        while len(rest) > limit:
            chunks.append(rest[:limit])
            rest = rest[limit:]
        current = rest

    if current.strip():
        chunks.append(current.rstrip())

    return chunks


async def reply_long(update: Update, text: str) -> None:
    if not update.message:
        return

    for chunk in split_message(text):
        await update.message.reply_text(chunk)


# =========================
# Telegram handlers
# =========================
async def send_schedule(update: Update, ddmm: str):
    sheet_url = os.getenv("SHEET_URL", "").strip()
    sheet_name = os.getenv("SHEET_NAME", "").strip() or None
    group_name = os.getenv("GROUP_NAME", DEFAULT_GROUP).strip() or DEFAULT_GROUP

    if not sheet_url:
        await reply_long(update, "Не задана переменная SHEET_URL.")
        return

    try:
        rows = get_rows_with_cache(sheet_url, group_name, sheet_name)
        items = extract_schedule_for_date(rows, group_name, ddmm)
        message = format_schedule(group_name, ddmm, items)
        await reply_long(update, message)
    except Exception as exc:
        await reply_long(update, f"Ошибка чтения расписания: {exc}")


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (
        "Привет! Я бот с расписанием 💙\n\n"
        "Команды:\n"
        "/today — расписание на сегодня\n"
        "/tomorrow — расписание на завтра\n"
        "/day 30.01 — расписание на дату\n\n"
        "Можно просто написать: 30.01\n"
        "Или: день 30.01"
    )
    await reply_long(update, text)


async def cmd_today(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ddmm = datetime.now(get_tz()).strftime("%d.%m")
    await send_schedule(update, ddmm)


async def cmd_tomorrow(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ddmm = (datetime.now(get_tz()) + timedelta(days=1)).strftime("%d.%m")
    await send_schedule(update, ddmm)


async def cmd_day(update: Update, context: ContextTypes.DEFAULT_TYPE):
    raw = " ".join(context.args).strip()

    if not raw:
        await reply_long(update, "Напиши так: /day 30.01")
        return

    ddmm = parse_ddmm(raw)
    if not ddmm:
        await reply_long(update, "Формат даты: /day 30.01")
        return

    await send_schedule(update, ddmm)


async def text_day(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return

    text = (update.message.text or "").strip().lower()

    match = re.match(
        r"^(?:день\s+)?(\d{1,2}[.\-/]\d{1,2}(?:[.\-/]\d{2,4})?)$",
        text,
    )
    if not match:
        return

    ddmm = parse_ddmm(match.group(1))
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
        raise RuntimeError("Нет BOT_TOKEN в .env")

    keep_alive()

    app = Application.builder().token(token).build()

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("today", cmd_today))
    app.add_handler(CommandHandler("tomorrow", cmd_tomorrow))
    app.add_handler(CommandHandler("day", cmd_day))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_day))

    print("Bot started / polling")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
