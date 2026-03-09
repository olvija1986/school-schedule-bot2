import os, json, uuid, asyncio, httpx, html, re
from datetime import datetime, timedelta, date
from fastapi import FastAPI, Request
from zoneinfo import ZoneInfo
from telegram import (
    BotCommand,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InlineQueryResultArticle,
    InputTextMessageContent,
    Update,
)
from telegram.ext import (
    ApplicationBuilder,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    InlineQueryHandler,
    MessageHandler,
    filters,
)
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

# ================== Настройки ==================
TOKEN = os.environ.get("TELEGRAM_TOKEN")
BOT_URL = os.environ.get("BOT_URL")  # например: https://school-schedule-bot.onrender.com
WEBHOOK_PATH = f"/webhook/{TOKEN}"

if not TOKEN or not BOT_URL:
    raise RuntimeError("Не заданы переменные окружения TELEGRAM_TOKEN или BOT_URL")

# Необязательно: ограничение доступа к редактированию расписания
# Формат: "12345,67890"
_ADMIN_USER_IDS_RAW = (os.environ.get("ADMIN_USER_IDS") or "").strip()
ADMIN_USER_IDS = {
    int(x.strip())
    for x in _ADMIN_USER_IDS_RAW.split(",")
    if x.strip().isdigit()
}

# ================== Загрузка расписания ==================
with open("schedule.json", "r", encoding="utf-8") as f:
    schedule = json.load(f)

TEMP_SCHEDULE_PATH = "temp_schedule.json"
temp_schedule: dict[str, list[str]] = {}

SUBSCRIPTIONS_PATH = "subscriptions.json"
subscriptions: dict[str, dict] = {}
scheduler: AsyncIOScheduler | None = None

SCHEDULE_DAYS = [
    "Понедельник",
    "Вторник",
    "Среда",
    "Четверг",
    "Пятница",
    "Суббота",
    "Воскресенье",
]

DAY_MAP = {
    "Monday": "Понедельник",
    "Tuesday": "Вторник",
    "Wednesday": "Среда",
    "Thursday": "Четверг",
    "Friday": "Пятница",
    "Saturday": "Суббота",
    "Sunday": "Воскресенье"
}

# Суббота: расписание по профилям (ключ для хранения, подпись для UI)
SATURDAY_PROFILES: list[tuple[str, str]] = [
    ("Физмат", "Физмат"),
    ("Биохим", "Биохим"),
    ("Инфотех_1", "Инфотех 1 группа"),
    ("Инфотех_2", "Инфотех 2 группа"),
    ("Общеобразовательный_3", "Общеобр-ый 3 группа"),
]
SATURDAY_PROFILE_KEYS = [k for k, _ in SATURDAY_PROFILES]
SATURDAY_PROFILE_LABELS = {k: label for k, label in SATURDAY_PROFILES}
SATURDAY_LABEL_TO_KEY = {label: key for key, label in SATURDAY_PROFILES}

def _saturday_data_to_profiles(day_data: list | dict | None) -> list[tuple[str, list[str]]]:
    """Превращает schedule['Суббота'] или temp_schedule[date] в список (подпись, уроки)."""
    if day_data is None:
        return []
    if isinstance(day_data, list):
        return [("Суббота", day_data)]  # legacy: один блок
    if isinstance(day_data, dict):
        out: list[tuple[str, list[str]]] = []
        for key in SATURDAY_PROFILE_KEYS:
            if key in day_data and isinstance(day_data[key], list):
                label = SATURDAY_PROFILE_LABELS.get(key, key)
                out.append((label, day_data[key]))
        return out
    return []

def _get_saturday_profiles_for_date(d: date) -> list[tuple[str, list[str]]]:
    """Расписание субботы по профилям на дату d (с учётом temp_schedule).
    Если temp_schedule[date] — dict, мёржим с основным: temp перекрывает только
    те профили которые в нём есть, остальные берутся из schedule.
    Если temp_schedule[date] — list, используем его целиком (legacy).
    """
    key = d.isoformat()
    base_sat = schedule.get("Суббота")

    if key in temp_schedule:
        raw = temp_schedule[key]
        if isinstance(raw, list):
            return [("Суббота", raw)]
        if isinstance(raw, dict):
            # Мёржим: для каждого профиля берём temp если есть, иначе base
            merged: dict[str, list[str]] = {}
            for pk in SATURDAY_PROFILE_KEYS:
                if pk in raw:
                    merged[pk] = raw[pk]
                elif isinstance(base_sat, dict) and pk in base_sat:
                    merged[pk] = base_sat[pk]
            return _saturday_data_to_profiles(merged)
        return []

    return _saturday_data_to_profiles(base_sat)

_LESSON_RE = re.compile(
    r"^\s*(?P<start>\d{1,2}:\d{2})\s*-\s*(?P<end>\d{1,2}:\d{2})\s+(?P<rest>.+?)\s*$"
)

def _load_temp_schedule_from_disk() -> None:
    global temp_schedule
    try:
        with open(TEMP_SCHEDULE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            temp_schedule = {}
            return
        temp_schedule = {}
        for k, v in data.items():
            if isinstance(v, list):
                temp_schedule[k] = [str(x) for x in v]
            elif isinstance(v, dict):
                # временная суббота по профилям
                temp_schedule[k] = {
                    pk: [str(x) for x in pv]
                    for pk, pv in v.items()
                    if isinstance(pv, list)
                }
    except FileNotFoundError:
        temp_schedule = {}
    except Exception:
        temp_schedule = {}

def _save_temp_schedule_to_disk() -> None:
    tmp_path = f"{TEMP_SCHEDULE_PATH}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(temp_schedule, f, ensure_ascii=False, indent=2)
        f.write("\n")
    os.replace(tmp_path, TEMP_SCHEDULE_PATH)

def _load_subscriptions_from_disk() -> None:
    global subscriptions
    try:
        with open(SUBSCRIPTIONS_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            subscriptions = data
        else:
            subscriptions = {}
    except FileNotFoundError:
        subscriptions = {}
    except Exception:
        subscriptions = {}

def _save_subscriptions_to_disk() -> None:
    tmp_path = f"{SUBSCRIPTIONS_PATH}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(subscriptions, f, ensure_ascii=False, indent=2)
        f.write("\n")
    os.replace(tmp_path, SUBSCRIPTIONS_PATH)

async def _notify_subscribers(text: str, parse_mode: str = "HTML") -> None:
    """Отправляет сообщение всем подписчикам (напоминаний)."""
    if not subscriptions:
        return
    chat_ids = set()
    for entry in subscriptions.values():
        cid = entry.get("chat_id")
        if cid is not None:
            chat_ids.add(int(cid))
    for chat_id in chat_ids:
        try:
            await bot_app.bot.send_message(
                chat_id=chat_id,
                text=text,
                parse_mode=parse_mode,
            )
            await asyncio.sleep(0.05)
        except Exception:
            pass

def _is_admin(update: Update) -> bool:
    if not ADMIN_USER_IDS:
        return True
    user = update.effective_user
    return bool(user and user.id in ADMIN_USER_IDS)

def _save_schedule_to_disk() -> None:
    tmp_path = "schedule.json.tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(schedule, f, ensure_ascii=False, indent=4)
        f.write("\n")
    os.replace(tmp_path, "schedule.json")

def _parse_lesson_line(line: str) -> dict:
    raw = (line or "").strip()
    if not raw:
        return {"start": "", "end": "", "subject": "", "room": "", "raw": ""}

    m = _LESSON_RE.match(raw)
    if m:
        start = m.group("start")
        end = m.group("end")
        rest = m.group("rest").strip()
    else:
        start = ""
        end = ""
        rest = raw

    if "/" in rest:
        parts = [p.strip() for p in rest.split("/") if p.strip()]
        subject = parts[0] if parts else rest
        room = "/".join(parts[1:]) if len(parts) > 1 else ""
    else:
        subject = rest
        room = ""

    return {"start": start, "end": end, "subject": subject, "room": room, "raw": raw}

def _truncate(text: str, width: int) -> str:
    text = text or ""
    if len(text) <= width:
        return text
    if width <= 1:
        return text[:width]
    return text[: width - 1] + "…"

def _format_day_table_html(day: str, lessons: list[str]) -> str:
    rows = []
    for idx, line in enumerate(lessons or [], start=1):
        p = _parse_lesson_line(line)
        rows.append(
            {
                "n": str(idx),
                "start": p["start"],
                "end": p["end"],
                "subject": p["subject"],
                "room": p["room"],
            }
        )

    n_w = max(1, min(2, max((len(r["n"]) for r in rows), default=1)))
    start_w = 5
    end_w = 5
    room_w = max(3, min(12, max((len(r["room"]) for r in rows), default=3)))
    subject_w = max(10, min(28, max((len(r["subject"]) for r in rows), default=10)))

    header = (
        f"{'#':<{n_w}}  "
        f"{'Нач':<{start_w}}  "
        f"{'Кон':<{end_w}}  "
        f"{'Предмет':<{subject_w}}  "
        f"{'Каб':<{room_w}}"
    )
    sep = (
        f"{'-'*n_w}  "
        f"{'-'*start_w}  "
        f"{'-'*end_w}  "
        f"{'-'*subject_w}  "
        f"{'-'*room_w}"
    )

    lines = [header, sep]
    if not rows:
        lines.append(
            f"{'':<{n_w}}  {'':<{start_w}}  {'':<{end_w}}  "
            f"{_truncate('Нет занятий', subject_w):<{subject_w}}  {'':<{room_w}}"
        )
    else:
        for r in rows:
            subj = _truncate(r["subject"], subject_w)
            room = _truncate(r["room"], room_w)
            lines.append(
                f"{r['n']:<{n_w}}  "
                f"{r['start']:<{start_w}}  "
                f"{r['end']:<{end_w}}  "
                f"{subj:<{subject_w}}  "
                f"{room:<{room_w}}"
            )

    pre = html.escape("\n".join(lines))
    return f"<b>{html.escape(day)}</b>\n<pre>{pre}</pre>"

def _get_tz() -> ZoneInfo:
    name = (os.environ.get("TZ") or "Etc/GMT-5").strip()
    try:
        return ZoneInfo(name)
    except Exception:
        return ZoneInfo("UTC")

def _parse_date_str(s: str) -> date | None:
    s = (s or "").strip().lower()
    today = datetime.now(tz=_get_tz()).date()
    if s == "сегодня":
        return today
    if s == "завтра":
        return today + timedelta(days=1)
    try:
        return datetime.strptime(s, "%d.%m.%Y").date()
    except ValueError:
        return None

def _parse_hhmm(s: str) -> tuple[int, int] | None:
    s = (s or "").strip()
    m = re.match(r"^(?P<h>\d{1,2}):(?P<m>\d{2})$", s)
    if not m:
        return None
    h = int(m.group("h"))
    mi = int(m.group("m"))
    if not (0 <= h <= 23 and 0 <= mi <= 59):
        return None
    return h, mi

def _get_lessons_for_date(d: date) -> tuple[str, list[str]]:
    """Возвращает (название_дня_по-русски, список_уроков) с учётом временного расписания."""
    key = d.isoformat()
    day_eng = d.strftime("%A")
    day_ru = DAY_MAP.get(day_eng, day_eng)

    if day_ru == "Суббота":
        if key in temp_schedule:
            raw = temp_schedule[key]
            if isinstance(raw, list):
                return day_ru, raw
            return day_ru, []  # по профилям — см. _get_saturday_profiles_for_date
        sat = schedule.get("Суббота")
        if isinstance(sat, list):
            return day_ru, sat
        return day_ru, []  # по профилям

    if key in temp_schedule:
        raw = temp_schedule[key]
        if isinstance(raw, list):
            return day_ru, raw
        return day_ru, []
    return day_ru, schedule.get(day_ru, [])

async def _send_daily_reminder(chat_id: int):
    today = datetime.now(tz=_get_tz()).date()
    day_eng = today.strftime("%A")
    day_ru = DAY_MAP.get(day_eng, day_eng)
    if day_ru == "Суббота":
        profiles = _get_saturday_profiles_for_date(today)
        if profiles:
            parts = [_format_day_table_html(f"Суббота — {label}", lessons) for label, lessons in profiles]
            text = _truncate_message("📅 Расписание на сегодня (суббота):\n\n" + "\n\n".join(parts))
        else:
            text = _format_day_table_html("Суббота", [])
    else:
        day, lessons = _get_lessons_for_date(today)
        text = _format_day_table_html(day, lessons)
    await bot_app.bot.send_message(chat_id=chat_id, text=text, parse_mode="HTML")

def _job_id_for(user_id: int) -> str:
    return f"reminder:{user_id}"

def _reschedule_user(user_id: int):
    global scheduler
    if scheduler is None:
        return
    entry = subscriptions.get(str(user_id))
    job_id = _job_id_for(user_id)
    try:
        scheduler.remove_job(job_id)
    except Exception:
        pass
    if not entry:
        return
    time_str = entry.get("time", "")
    parsed = _parse_hhmm(time_str)
    if not parsed:
        return
    hour, minute = parsed
    chat_id = int(entry.get("chat_id"))
    trigger = CronTrigger(hour=hour, minute=minute, timezone=_get_tz())
    scheduler.add_job(
        _send_daily_reminder,
        trigger=trigger,
        args=[chat_id],
        id=job_id,
        replace_existing=True,
        misfire_grace_time=3600,
        coalesce=True,
    )

_MAX_MESSAGE_LEN = 4096

def _truncate_message(text: str, max_len: int = _MAX_MESSAGE_LEN - 100) -> str:
    if len(text) <= max_len:
        return text
    return text[: max_len - 3].rstrip() + "…"

def _format_week_text() -> str:
    """Текст расписания на неделю с учётом временных замен."""
    now_tz = datetime.now(tz=_get_tz())
    blocks: list[str] = []
    for day in SCHEDULE_DAYS:
        # Ищем ближайшую дату этого дня в течение текущей недели (пн-вс)
        # Для проверки temp_schedule берём дату ближайшего такого дня
        day_idx = SCHEDULE_DAYS.index(day)  # 0=Пн, 6=Вс
        today_idx = now_tz.weekday()  # 0=Пн, 6=Вс
        delta = day_idx - today_idx
        target_date = (now_tz + timedelta(days=delta)).date()
        date_key = target_date.isoformat()

        if day == "Суббота":
            profiles = _get_saturday_profiles_for_date(target_date)
            for label, lessons in profiles:
                if lessons:
                    blocks.append(_format_day_table_html(f"Суббота — {label}", lessons))
            continue

        # Для обычных дней — temp перекрывает основное
        if date_key in temp_schedule:
            raw = temp_schedule[date_key]
            data = raw if isinstance(raw, list) else []
        else:
            data = schedule.get(day, [])

        if isinstance(data, list) and data:
            blocks.append(_format_day_table_html(day, data))

    return "\n\n".join(blocks) if blocks else _format_day_table_html("Неделя", [])

def _format_week_text_without_saturday() -> str:
    """Текст расписания на неделю без субботы, с учётом временных замен."""
    now_tz = datetime.now(tz=_get_tz())
    blocks: list[str] = []
    for day in SCHEDULE_DAYS:
        if day == "Суббота":
            continue
        if day not in schedule and day not in [d for d in SCHEDULE_DAYS]:
            continue
        day_idx = SCHEDULE_DAYS.index(day)
        today_idx = now_tz.weekday()
        delta = day_idx - today_idx
        target_date = (now_tz + timedelta(days=delta)).date()
        date_key = target_date.isoformat()

        if date_key in temp_schedule:
            raw = temp_schedule[date_key]
            data = raw if isinstance(raw, list) else []
        else:
            data = schedule.get(day, [])

        if isinstance(data, list) and data:
            blocks.append(_format_day_table_html(day, data))
    return "\n\n".join(blocks) if blocks else _format_day_table_html("Неделя", [])

def _get_saturday_inline_results_for_week() -> list[InlineQueryResultArticle]:
    """Создаёт отдельный результат для каждого профиля субботы с учётом temp_schedule."""
    results = []
    now_tz = datetime.now(tz=_get_tz())
    # Ближайшая суббота текущей недели
    today_idx = now_tz.weekday()  # 0=Пн, 6=Вс
    delta = 5 - today_idx  # 5=Суббота
    sat_date = (now_tz + timedelta(days=delta)).date()

    profiles = _get_saturday_profiles_for_date(sat_date)
    if not profiles:
        return results

    if len(profiles) == 1 and profiles[0][0] == "Суббота":
        # Единый блок без профилей
        text = _truncate_message(_format_day_table_html("Суббота", profiles[0][1]))
        results.append(InlineQueryResultArticle(
            id=str(uuid.uuid4()),
            title="Суббота",
            description="Расписание субботы",
            input_message_content=InputTextMessageContent(text, parse_mode="HTML"),
        ))
    else:
        for label, lessons in profiles:
            if lessons:
                text = _truncate_message(_format_day_table_html(f"Суббота — {label}", lessons))
                results.append(InlineQueryResultArticle(
                    id=str(uuid.uuid4()),
                    title=f"Суббота — {label}",
                    description="Расписание субботы по профилю",
                    input_message_content=InputTextMessageContent(text, parse_mode="HTML"),
                ))
        # Все профили одним сообщением
        all_text = _truncate_message("\n\n".join(
            _format_day_table_html(f"Суббота — {lbl}", lsns) for lbl, lsns in profiles if lsns
        ))
        results.append(InlineQueryResultArticle(
            id=str(uuid.uuid4()),
            title="Суббота — Все профили",
            description="Все профили субботы одним сообщением",
            input_message_content=InputTextMessageContent(all_text, parse_mode="HTML"),
        ))
    return results

# ================== Inline-запрос ==================
#
# Навигация по уровням через текст запроса:
#   (пусто)           → 3 подсказки: «Сегодня», «Завтра», «Неделя»
#   сегодня / today   → если суббота с профилями — показывает профили;
#                       иначе сразу расписание дня
#   завтра / tomorrow → аналогично
#   неделя / week     → Пн–Пт одним блоком + подсказки профилей субботы
#   суббота / saturday→ только профили субботы (текущей недели)
#
async def inline_schedule(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query_text = (update.inline_query.query or "").lower().strip()
    now = datetime.now(tz=_get_tz())
    results = []

    # ── Уровень 0: пустой запрос — сразу готовые расписания ────────────────
    if not query_text:
        tomorrow_date = (now + timedelta(days=1)).date()

        # Сегодня
        today_day, today_lessons = _get_lessons_for_date(now.date())
        if today_day == "Суббота":
            today_profiles = _get_saturday_profiles_for_date(now.date())
            for label, prof_lessons in today_profiles:
                text = _truncate_message(_format_day_table_html(f"Суббота — {label}", prof_lessons))
                results.append(InlineQueryResultArticle(
                    id=str(uuid.uuid4()),
                    title=f"Сегодня — {label}",
                    description="Суббота, сегодня",
                    input_message_content=InputTextMessageContent(text, parse_mode="HTML"),
                ))
            if today_profiles:
                all_text = _truncate_message("\n\n".join(
                    _format_day_table_html(f"Суббота — {lbl}", lsns) for lbl, lsns in today_profiles
                ))
                results.append(InlineQueryResultArticle(
                    id=str(uuid.uuid4()),
                    title="Сегодня — Все профили",
                    description="Суббота сегодня — все профили одним сообщением",
                    input_message_content=InputTextMessageContent(all_text, parse_mode="HTML"),
                ))
        else:
            text = _truncate_message(_format_day_table_html(today_day, today_lessons))
            results.append(InlineQueryResultArticle(
                id=str(uuid.uuid4()),
                title=f"Сегодня — {today_day}",
                input_message_content=InputTextMessageContent(text, parse_mode="HTML"),
            ))

        # Завтра
        tomorrow_day, tomorrow_lessons = _get_lessons_for_date(tomorrow_date)
        if tomorrow_day == "Суббота":
            tomorrow_profiles = _get_saturday_profiles_for_date(tomorrow_date)
            for label, prof_lessons in tomorrow_profiles:
                text = _truncate_message(_format_day_table_html(f"Суббота — {label}", prof_lessons))
                results.append(InlineQueryResultArticle(
                    id=str(uuid.uuid4()),
                    title=f"Завтра — {label}",
                    description="Суббота, завтра",
                    input_message_content=InputTextMessageContent(text, parse_mode="HTML"),
                ))
            if tomorrow_profiles:
                all_text = _truncate_message("\n\n".join(
                    _format_day_table_html(f"Суббота — {lbl}", lsns) for lbl, lsns in tomorrow_profiles
                ))
                results.append(InlineQueryResultArticle(
                    id=str(uuid.uuid4()),
                    title="Завтра — Все профили",
                    description="Суббота завтра — все профили одним сообщением",
                    input_message_content=InputTextMessageContent(all_text, parse_mode="HTML"),
                ))
        else:
            text = _truncate_message(_format_day_table_html(tomorrow_day, tomorrow_lessons))
            results.append(InlineQueryResultArticle(
                id=str(uuid.uuid4()),
                title=f"Завтра — {tomorrow_day}",
                input_message_content=InputTextMessageContent(text, parse_mode="HTML"),
            ))

        # Неделя Пн–Пт
        week_no_sat = _format_week_text_without_saturday()
        results.append(InlineQueryResultArticle(
            id=str(uuid.uuid4()),
            title="Неделя — Пн–Пт",
            description="Расписание на неделю без субботы",
            input_message_content=InputTextMessageContent(
                _truncate_message(week_no_sat), parse_mode="HTML"
            ),
        ))

        await update.inline_query.answer(results, cache_time=0)
        return

    # ── Уровень 1: сегодня ──────────────────────────────────────────────────
    if query_text in ["сегодня", "today"]:
        day, lessons = _get_lessons_for_date(now.date())
        if day == "Суббота":
            profiles = _get_saturday_profiles_for_date(now.date())
            # Каждый профиль отдельной кнопкой
            for label, prof_lessons in profiles:
                text = _truncate_message(_format_day_table_html(f"Суббота — {label}", prof_lessons))
                results.append(InlineQueryResultArticle(
                    id=str(uuid.uuid4()),
                    title=f"{label}",
                    description=f"Суббота, сегодня — {label}",
                    input_message_content=InputTextMessageContent(text, parse_mode="HTML"),
                ))
            # Все профили одним сообщением
            if profiles:
                all_text = _truncate_message("\n\n".join(
                    _format_day_table_html(f"Суббота — {lbl}", lsns) for lbl, lsns in profiles
                ))
                results.append(InlineQueryResultArticle(
                    id=str(uuid.uuid4()),
                    title="Все профили",
                    description="Суббота сегодня — все профили одним сообщением",
                    input_message_content=InputTextMessageContent(all_text, parse_mode="HTML"),
                ))
        else:
            text = _truncate_message(_format_day_table_html(day, lessons))
            results.append(InlineQueryResultArticle(
                id=str(uuid.uuid4()),
                title=f"Сегодня — {day}",
                input_message_content=InputTextMessageContent(text, parse_mode="HTML"),
            ))
        await update.inline_query.answer(results, cache_time=0)
        return

    # ── Уровень 1: завтра ───────────────────────────────────────────────────
    if query_text in ["завтра", "tomorrow"]:
        tomorrow_date = (now + timedelta(days=1)).date()
        day, lessons = _get_lessons_for_date(tomorrow_date)
        if day == "Суббота":
            profiles = _get_saturday_profiles_for_date(tomorrow_date)
            # Каждый профиль отдельной кнопкой
            for label, prof_lessons in profiles:
                text = _truncate_message(_format_day_table_html(f"Суббота — {label}", prof_lessons))
                results.append(InlineQueryResultArticle(
                    id=str(uuid.uuid4()),
                    title=f"{label}",
                    description=f"Суббота, завтра — {label}",
                    input_message_content=InputTextMessageContent(text, parse_mode="HTML"),
                ))
            # Все профили одним сообщением
            if profiles:
                all_text = _truncate_message("\n\n".join(
                    _format_day_table_html(f"Суббота — {lbl}", lsns) for lbl, lsns in profiles
                ))
                results.append(InlineQueryResultArticle(
                    id=str(uuid.uuid4()),
                    title="Все профили",
                    description="Суббота завтра — все профили одним сообщением",
                    input_message_content=InputTextMessageContent(all_text, parse_mode="HTML"),
                ))
        else:
            text = _truncate_message(_format_day_table_html(day, lessons))
            results.append(InlineQueryResultArticle(
                id=str(uuid.uuid4()),
                title=f"Завтра — {day}",
                input_message_content=InputTextMessageContent(text, parse_mode="HTML"),
            ))
        await update.inline_query.answer(results, cache_time=0)
        return

    # ── Уровень 1: неделя ───────────────────────────────────────────────────
    if query_text in ["неделя", "week"]:
        # Пн–Пт одним результатом
        week_no_sat = _format_week_text_without_saturday()
        results.append(InlineQueryResultArticle(
            id=str(uuid.uuid4()),
            title="Понедельник — Пятница",
            description="Расписание на неделю (без субботы)",
            input_message_content=InputTextMessageContent(
                _truncate_message(week_no_sat), parse_mode="HTML"
            ),
        ))
        # Суббота — отдельный результат или профили
        for sat_result in _get_saturday_inline_results_for_week():
            results.append(sat_result)
        await update.inline_query.answer(results, cache_time=0)
        return

    # ── Уровень 1: суббота (явный запрос профилей) ──────────────────────────
    if query_text in ["суббота", "saturday"]:
        for sat_result in _get_saturday_inline_results_for_week():
            results.append(sat_result)
        if not results:
            results.append(InlineQueryResultArticle(
                id=str(uuid.uuid4()),
                title="Суббота — нет данных",
                input_message_content=InputTextMessageContent("Расписание субботы не задано."),
            ))
        await update.inline_query.answer(results, cache_time=0)
        return

    # ── Неизвестный запрос — подсказка ──────────────────────────────────────
    results.append(InlineQueryResultArticle(
        id=str(uuid.uuid4()),
        title="Введите: сегодня / завтра / неделя / суббота",
        description="или today / tomorrow / week / saturday",
        input_message_content=InputTextMessageContent(
            "Доступные запросы: сегодня, завтра, неделя, суббота"
        ),
    ))
    await update.inline_query.answer(results, cache_time=0)

# ================== Команда /start ==================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Привет! Я бот для школьного расписания.\n"
        "Используй inline-запрос: @rasp7V_bot today / tomorrow / week\n"
        "Для админов: /edit_schedule — редактировать расписание"
    )

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Команды:\n"
        "/start — приветствие\n"
        "/help — помощь\n"
        "/edit_schedule — редактировать расписание (если разрешено)\n"
        "/cancel — отменить редактирование\n\n"
        "Напоминания:\n"
        "/subscribe 07:30 — присылать расписание каждый день в указанное время\n"
        "/unsubscribe — отключить напоминания\n\n"
        "Inline-режим:\n"
        "Набери @бота и выбери подсказку или введи: today / tomorrow / week\n\n"
    )

async def subscribe(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    if not context.args:
        await update.message.reply_text("Формат: /subscribe HH:MM (например /subscribe 07:30)")
        return
    parsed = _parse_hhmm(context.args[0])
    if not parsed:
        await update.message.reply_text("Неверное время. Формат: HH:MM (например 07:30)")
        return
    hh, mm = parsed
    t = f"{hh:02d}:{mm:02d}"
    user = update.effective_user
    chat = update.effective_chat
    if not user or not chat:
        await update.message.reply_text("Не удалось определить пользователя/чат.")
        return

    subscriptions[str(user.id)] = {"chat_id": chat.id, "time": t}
    _save_subscriptions_to_disk()
    _reschedule_user(user.id)
    await update.message.reply_text(
        f"Ок! Буду присылать расписание каждый день в {t}.\n"
    )

async def unsubscribe(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    user = update.effective_user
    if not user:
        await update.message.reply_text("Не удалось определить пользователя.")
        return
    subscriptions.pop(str(user.id), None)
    _save_subscriptions_to_disk()
    if scheduler is not None:
        try:
            scheduler.remove_job(_job_id_for(user.id))
        except Exception:
            pass
    await update.message.reply_text("Готово. Напоминания отключены.")

# ================== Редактирование расписания (/edit_schedule) ==================
EDIT_MODE, EDIT_CHOOSE_DAY, EDIT_CHOOSE_SATURDAY_PROFILE, EDIT_ENTER_DATE, EDIT_ENTER_LESSONS, EDIT_ENTER_WEEK, EDIT_CONFIRM, EDIT_ENTER_SAT_ALL = range(8)

def _day_keyboard() -> InlineKeyboardMarkup:
    rows = []
    row = []
    for i, day in enumerate(SCHEDULE_DAYS, start=1):
        row.append(InlineKeyboardButton(day, callback_data=f"edit_day:{day}"))
        if i % 2 == 0:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append(
        [
            InlineKeyboardButton(
                "Вся неделя (одним списком)", callback_data="edit_day:__WEEK__"
            )
        ]
    )
    rows.append([InlineKeyboardButton("Отмена", callback_data="edit_cancel")])
    return InlineKeyboardMarkup(rows)

def _saturday_profile_keyboard() -> InlineKeyboardMarkup:
    rows = []
    row = []
    for key, label in SATURDAY_PROFILES:
        row.append(InlineKeyboardButton(label, callback_data=f"edit_sat_profile:{key}"))
        if len(row) >= 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("📋 Все профили сразу", callback_data="edit_sat_profile:__ALL__")])
    rows.append([InlineKeyboardButton("Отмена", callback_data="edit_cancel")])
    return InlineKeyboardMarkup(rows)

async def edit_schedule_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_admin(update):
        await update.message.reply_text("У вас нет прав на редактирование расписания.")
        return ConversationHandler.END

    context.user_data.clear()

    keyboard = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "📅 Основное расписание по дням недели",
                    callback_data="edit_mode:base",
                )
            ],
            [
                InlineKeyboardButton(
                    "🕒 Временное расписание на дату",
                    callback_data="edit_mode:temp",
                )
            ],
            [InlineKeyboardButton("Отмена", callback_data="edit_cancel")],
        ]
    )

    await update.message.reply_text(
        "Что хочешь редактировать?", reply_markup=keyboard
    )
    return EDIT_MODE

async def edit_schedule_mode_chosen(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    data = query.data or ""
    if data == "edit_cancel":
        await query.edit_message_text("Редактирование отменено.")
        return ConversationHandler.END

    if data == "edit_mode:base":
        context.user_data.clear()
        context.user_data["edit_mode"] = "base"
        await query.edit_message_text(
            "Выбери день недели, который нужно изменить.",
            reply_markup=_day_keyboard(),
        )
        return EDIT_CHOOSE_DAY

    if data == "edit_mode:temp":
        context.user_data.clear()
        context.user_data["edit_mode"] = "temp"
        await query.edit_message_text(
            "Для какой даты сделать временное расписание?\n"
            "Введи дату в формате ДД.ММ.ГГГГ или напиши «сегодня» / «завтра».",
        )
        return EDIT_ENTER_DATE

    await query.edit_message_text("Не понял выбор. Попробуй ещё раз: /edit_schedule")
    return ConversationHandler.END

async def edit_schedule_date_entered(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_admin(update):
        await update.message.reply_text("У вас нет прав на редактирование расписания.")
        return ConversationHandler.END

    if context.user_data.get("edit_mode") != "temp":
        await update.message.reply_text("Сессия редактирования потеряна. Запусти заново: /edit_schedule")
        return ConversationHandler.END

    d = _parse_date_str(update.message.text or "")
    if not d:
        await update.message.reply_text(
            "Не понял дату. Формат: ДД.ММ.ГГГГ или «сегодня» / «завтра»."
        )
        return EDIT_ENTER_DATE

    key = d.isoformat()
    day_eng = d.strftime("%A")
    day_ru = DAY_MAP.get(day_eng, day_eng)
    context.user_data["edit_date"] = key
    context.user_data["edit_label"] = f"{d.strftime('%d.%m.%Y')} ({day_ru})"
    context.user_data["edit_mode"] = "temp"

    # Если суббота — сначала выбор профиля
    if day_ru == "Суббота":
        await update.message.reply_text(
            f"Дата {d.strftime('%d.%m.%Y')} — суббота.\n"
            "Выбери профиль для редактирования:",
            reply_markup=_saturday_profile_keyboard(),
        )
        return EDIT_CHOOSE_SATURDAY_PROFILE

    current = schedule.get(day_ru, [])
    if key in temp_schedule:
        raw = temp_schedule[key]
        current = raw if isinstance(raw, list) else []

    current_text = "\n".join(current) if current else "— (пусто) —"
    await update.message.reply_text(
        f"Текущее временное расписание для {context.user_data['edit_label']}:\n"
        f"{current_text}\n\n"
        "Пришли новое расписание одним сообщением: по одной строке на урок.\n"
        "Чтобы очистить — отправь слово: пусто\n"
        "Отмена — /cancel",
    )
    return EDIT_ENTER_LESSONS

async def edit_schedule_day_chosen(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    if not _is_admin(update):
        await query.edit_message_text("У вас нет прав на редактирование расписания.")
        return ConversationHandler.END

    data = query.data or ""
    if data == "edit_cancel":
        await query.edit_message_text("Редактирование отменено.")
        return ConversationHandler.END

    if not data.startswith("edit_day:"):
        await query.edit_message_text("Не понял выбор дня. Попробуй ещё раз: /edit_schedule")
        return ConversationHandler.END

    day_code = data.split("edit_day:", 1)[1].strip()

    mode = context.user_data.get("edit_mode") or "base"
    context.user_data["edit_mode"] = mode

    if day_code == "__WEEK__":
        context.user_data["edit_day"] = "__WEEK__"

        blocks = []
        for d in SCHEDULE_DAYS:
            day_data = schedule.get(d)
            if d == "Суббота" and isinstance(day_data, dict):
                for pk in SATURDAY_PROFILE_KEYS:
                    if pk in day_data and day_data[pk]:
                        label = SATURDAY_PROFILE_LABELS.get(pk, pk)
                        block = [f"Суббота {label}:"]
                        block.extend(day_data[pk])
                        blocks.append("\n".join(block))
            elif isinstance(day_data, list):
                block = [f"{d}:"]
                block.extend(day_data or ["(нет занятий)"])
                blocks.append("\n".join(block))
        current_text = "\n\n".join(blocks)

        await query.edit_message_text(
            "Текущее расписание на неделю:\n\n"
            f"{current_text}\n\n"
            "Пришли НОВОЕ расписание на всю неделю одним сообщением.\n"
            "Формат:\n"
            "Понедельник:\n"
            "13:30-14:10 ...\n\n"
            "Суббота по профилям:\n"
            "Суббота Физмат:\n...\n"
            "Суббота Инфотех 1 группа:\n...\n"
            "или один блок Суббота:\n...\n"
            "Пустые дни можно не указывать. Отмена — /cancel",
        )
        return EDIT_ENTER_WEEK

    if day_code not in SCHEDULE_DAYS:
        await query.edit_message_text("Некорректный день. Попробуй ещё раз: /edit_schedule")
        return ConversationHandler.END

    context.user_data["edit_day"] = day_code

    if day_code == "Суббота":
        await query.edit_message_text(
            "Выбери профиль для редактирования расписания в субботу.",
            reply_markup=_saturday_profile_keyboard(),
        )
        return EDIT_CHOOSE_SATURDAY_PROFILE

    current = schedule.get(day_code, [])
    if isinstance(current, dict):
        current = []
    current_text = "\n".join(current) if current else "— (пусто) —"

    await query.edit_message_text(
        f"Текущие занятия для «{day_code}»:\n{current_text}\n\n"
        "Пришли новое расписание одним сообщением: по одной строке на урок.\n"
        "Если ты делаешь это в группе и у бота включён privacy mode — отправь так:\n"
        "/set <каждая строка = один урок>\n"
        "Чтобы очистить день — отправь слово: пусто\n"
        "Чтобы отменить — /cancel",
    )
    return EDIT_ENTER_LESSONS

async def edit_schedule_saturday_profile_chosen(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    if not _is_admin(update):
        await query.edit_message_text("У вас нет прав на редактирование расписания.")
        return ConversationHandler.END

    data = query.data or ""
    if data == "edit_cancel":
        await query.edit_message_text("Редактирование отменено.")
        return ConversationHandler.END

    if not data.startswith("edit_sat_profile:"):
        await query.edit_message_text("Не понял выбор. Попробуй ещё раз: /edit_schedule")
        return ConversationHandler.END

    profile_key = data.split("edit_sat_profile:", 1)[1].strip()

    # ── Режим «все профили сразу» ────────────────────────────────────────────
    if profile_key == "__ALL__":
        mode = context.user_data.get("edit_mode", "base")
        context.user_data["edit_saturday_profile"] = "__ALL__"

        # Собираем текущее расписание всех профилей для превью
        blocks: list[str] = []
        if mode == "temp":
            edit_date = context.user_data.get("edit_date")
            raw_temp = temp_schedule.get(edit_date) if edit_date else None
            for key in SATURDAY_PROFILE_KEYS:
                label = SATURDAY_PROFILE_LABELS[key]
                if isinstance(raw_temp, dict):
                    lessons = raw_temp.get(key) or (
                        schedule.get("Суббота", {}).get(key, [])
                        if isinstance(schedule.get("Суббота"), dict) else []
                    )
                else:
                    sat = schedule.get("Суббота")
                    lessons = sat.get(key, []) if isinstance(sat, dict) else []
                if lessons:
                    blocks.append(f"Суббота {label}:\n" + "\n".join(lessons))
            date_label = context.user_data.get("edit_label", "эту субботу")
            header = f"Текущее расписание субботы для {date_label}"
        else:
            sat = schedule.get("Суббота")
            for key in SATURDAY_PROFILE_KEYS:
                label = SATURDAY_PROFILE_LABELS[key]
                lessons = sat.get(key, []) if isinstance(sat, dict) else []
                if lessons:
                    blocks.append(f"Суббота {label}:\n" + "\n".join(lessons))
            header = "Текущее расписание субботы"

        current_text = "\n\n".join(blocks) if blocks else "— (пусто) —"
        example = (
            "Суббота Физмат:\n08:30-09:05 Алгебра/211\n09:10-09:45 ...\n\n"
            "Суббота Инфотех 2 группа:\n08:30-09:05 Алгоритмика/304\n..."
        )
        await query.edit_message_text(
            f"{header}:\n\n{current_text}\n\n"
            "Пришли новое расписание всех нужных профилей одним сообщением.\n"
            "Формат:\n"
            f"{example}\n\n"
            "Профили которые не укажешь — останутся без изменений.\n"
            "Отмена — /cancel",
        )
        return EDIT_ENTER_SAT_ALL

    # ── Обычный одиночный профиль ────────────────────────────────────────────
    if profile_key not in SATURDAY_PROFILE_KEYS:
        await query.edit_message_text("Некорректный профиль. Попробуй ещё раз: /edit_schedule")
        return ConversationHandler.END

    context.user_data["edit_saturday_profile"] = profile_key
    label = SATURDAY_PROFILE_LABELS.get(profile_key, profile_key)
    mode = context.user_data.get("edit_mode", "base")

    # Берём текущее расписание: для temp — из temp_schedule, для base — из schedule
    current: list[str] = []
    if mode == "temp":
        edit_date = context.user_data.get("edit_date")
        if edit_date and edit_date in temp_schedule:
            raw = temp_schedule[edit_date]
            if isinstance(raw, dict):
                current = raw.get(profile_key, [])
            # если list — значит раньше было без профилей, считаем пустым
        if not current:
            # Подставляем основное как подсказку
            sat_data = schedule.get("Суббота")
            if isinstance(sat_data, dict):
                current = sat_data.get(profile_key, [])
        date_label = context.user_data.get("edit_label", "")
        header = f"Текущее временное расписание для «{date_label} — {label}»"
    else:
        sat_data = schedule.get("Суббота")
        if isinstance(sat_data, dict):
            current = sat_data.get(profile_key, [])
        header = f"Текущие занятия для «Суббота — {label}»"

    current_text = "\n".join(current) if current else "— (пусто) —"

    await query.edit_message_text(
        f"{header}:\n{current_text}\n\n"
        "Пришли новое расписание одним сообщением: по одной строке на урок.\n"
        "В группе с privacy mode: /set <список уроков>\n"
        "Чтобы очистить — отправь слово: пусто. Отмена — /cancel",
    )
    return EDIT_ENTER_LESSONS

def _parse_lessons_from_text(text: str) -> list[str] | None:
    text = (text or "").strip()
    if not text:
        return None
    if text.lower() in {"пусто", "нет", "clear"}:
        return []
    return [line.strip() for line in text.splitlines() if line.strip()]

def _parse_saturday_all_profiles(text: str) -> dict[str, list[str]] | None:
    """Парсит текст вида:
    Суббота Физмат:
    08:30-09:05 Алгебра/211
    ...
    Суббота Инфотех 2 группа:
    08:30-09:05 Алгоритмика/304
    ...
    Возвращает {profile_key: [уроки]} или None если не распознано ни одного профиля.
    """
    lines = (text or "").splitlines()
    result: dict[str, list[str]] = {}
    current_key: str | None = None

    for raw in lines:
        line = raw.strip()
        if not line:
            continue

        # Попытка распознать заголовок профиля: "Суббота <метка>:"
        matched_key: str | None = None
        if line.lower().startswith("суббота") and line.endswith(":"):
            rest = line[7:].strip().rstrip(":").strip()  # всё после "суббота"
            rest_lower = rest.lower()
            # Сначала ищем по label
            for key, label in SATURDAY_PROFILE_LABELS.items():
                if rest_lower == label.lower():
                    matched_key = key
                    break
            # Потом по ключу напрямую
            if matched_key is None:
                for key in SATURDAY_PROFILE_KEYS:
                    if rest_lower == key.lower():
                        matched_key = key
                        break

        if matched_key is not None:
            current_key = matched_key
            if current_key not in result:
                result[current_key] = []
            continue

        if current_key is not None:
            result[current_key].append(line)

    return result if result else None

def _parse_week_from_text(text: str) -> dict[str, list[str] | dict[str, list[str]]] | None:
    lines = (text or "").splitlines()
    current_day: str | None = None
    current_saturday_profile: str | None = None
    result: dict[str, list[str] | dict[str, list[str]]] = {d: [] for d in SCHEDULE_DAYS}
    has_any = False

    for raw in lines:
        line = raw.strip()
        if not line:
            continue

        if line.lower().startswith("суббота ") and ":" in line:
            prefix, _ = line.split(":", 1)
            rest = prefix[8:].strip().lower()
            matched_key = None
            for key, label in SATURDAY_PROFILE_LABELS.items():
                if rest == label.lower():
                    matched_key = key
                    break
            if matched_key is not None:
                has_any = True
                current_day = "Суббота"
                current_saturday_profile = matched_key
                if not isinstance(result["Суббота"], dict):
                    result["Суббота"] = {}
                result["Суббота"][matched_key] = []
                continue

        matched_day = None
        for d in SCHEDULE_DAYS:
            if line.lower() == (d.lower() + ":") or line.lower().startswith(d.lower() + ":"):
                matched_day = d
                break
        if matched_day is not None:
            has_any = True
            current_day = matched_day
            current_saturday_profile = None
            if matched_day == "Суббота" and isinstance(result["Суббота"], dict):
                result["Суббота"] = []
            continue

        if current_day is None:
            continue

        if current_day == "Суббота" and current_saturday_profile is not None:
            if isinstance(result["Суббота"], dict) and current_saturday_profile in result["Суббота"]:
                result["Суббота"][current_saturday_profile].append(line)
        elif current_day == "Суббота" and isinstance(result["Суббота"], list):
            result["Суббота"].append(line)
        elif isinstance(result.get(current_day), list):
            result[current_day].append(line)

    if not has_any:
        return None
    return result

async def edit_schedule_lessons_entered(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_admin(update):
        await update.message.reply_text("У вас нет прав на редактирование расписания.")
        return ConversationHandler.END

    mode = context.user_data.get("edit_mode") or "base"
    day = context.user_data.get("edit_day")
    edit_date = context.user_data.get("edit_date")
    if mode == "base":
        if not day or day == "__WEEK__":
            await update.message.reply_text("Сессия редактирования потеряна. Запусти заново: /edit_schedule")
            return ConversationHandler.END
    else:
        if not edit_date:
            await update.message.reply_text("Сессия редактирования потеряна. Запусти заново: /edit_schedule")
            return ConversationHandler.END

    lessons = _parse_lessons_from_text(update.message.text or "")
    if lessons is None:
        await update.message.reply_text("Сообщение пустое. Пришли список уроков или «пусто».")
        return EDIT_ENTER_LESSONS

    context.user_data["edit_lessons"] = lessons
    if context.user_data.get("edit_saturday_profile"):
        pk = context.user_data["edit_saturday_profile"]
        label = "Суббота — " + SATURDAY_PROFILE_LABELS.get(pk, pk)
    else:
        label = context.user_data.get("edit_label") or day or "день"
    preview = "\n".join(lessons) if lessons else "— (пусто) —"

    keyboard = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("✅ Сохранить", callback_data="edit_confirm"),
                InlineKeyboardButton("❌ Отмена", callback_data="edit_cancel"),
            ]
        ]
    )

    await update.message.reply_text(
        f"Проверь, что всё верно для «{label}»:\n{preview}",
        reply_markup=keyboard,
    )
    return EDIT_CONFIRM

async def edit_schedule_lessons_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_admin(update):
        await update.message.reply_text("У вас нет прав на редактирование расписания.")
        return ConversationHandler.END

    mode = context.user_data.get("edit_mode") or "base"
    day = context.user_data.get("edit_day")
    edit_date = context.user_data.get("edit_date")

    if mode == "base" and day == "__WEEK__":
        await update.message.reply_text(
            "Для редактирования всей недели используй обычное сообщение (не /set), "
            "как было показано в примере."
        )
        return EDIT_ENTER_WEEK

    if mode != "base" and not edit_date:
        await update.message.reply_text("Сессия редактирования потеряна. Запусти заново: /edit_schedule")
        return ConversationHandler.END

    raw = update.message.text or ""
    parts = raw.split(None, 1)
    payload = parts[1] if len(parts) > 1 else ""
    lessons = _parse_lessons_from_text(payload)
    if lessons is None:
        await update.message.reply_text(
            "После /set нужно прислать список уроков (каждый с новой строки) или слово «пусто».\n"
            "Пример:\n"
            "/set 13:30-14:10 Математика/211\n"
            "14:20-15:00 Информатика/304"
        )
        return EDIT_ENTER_LESSONS

    context.user_data["edit_lessons"] = lessons
    if context.user_data.get("edit_saturday_profile"):
        pk = context.user_data["edit_saturday_profile"]
        label = "Суббота — " + SATURDAY_PROFILE_LABELS.get(pk, pk)
    else:
        label = context.user_data.get("edit_label") or day or "день"
    preview = "\n".join(lessons) if lessons else "— (пусто) —"
    keyboard = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("✅ Сохранить", callback_data="edit_confirm"),
                InlineKeyboardButton("❌ Отмена", callback_data="edit_cancel"),
            ]
        ]
    )
    await update.message.reply_text(
        f"Проверь, что всё верно для «{label}»:\n{preview}",
        reply_markup=keyboard,
    )
    return EDIT_CONFIRM

async def edit_schedule_week_entered(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_admin(update):
        await update.message.reply_text("У вас нет прав на редактирование расписания.")
        return ConversationHandler.END

    if context.user_data.get("edit_day") != "__WEEK__":
        await update.message.reply_text("Сессия редактирования потеряна. Запусти заново: /edit_schedule")
        return ConversationHandler.END

    week = _parse_week_from_text(update.message.text or "")
    if week is None:
        await update.message.reply_text(
            "Не удалось распознать дни недели.\n"
            "Убедись, что используешь формат:\n"
            "Понедельник:\\n...\n\n"
            "Вторник:\\n...\n"
            "и так далее."
        )
        return EDIT_ENTER_WEEK

    context.user_data["edit_week"] = week

    blocks = []
    for d in SCHEDULE_DAYS:
        lessons = week.get(d, [])
        if not lessons:
            continue
        block = [f"{d}:"]
        block.extend(lessons)
        blocks.append("\n".join(block))
    preview = "\n\n".join(blocks) if blocks else "— все дни пустые —"

    keyboard = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("✅ Сохранить", callback_data="edit_confirm"),
                InlineKeyboardButton("❌ Отмена", callback_data="edit_cancel"),
            ]
        ]
    )
    await update.message.reply_text(
        "Проверь расписание на неделю:\n\n"
        f"{preview}",
        reply_markup=keyboard,
    )
    return EDIT_CONFIRM

async def edit_schedule_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    if not _is_admin(update):
        await query.edit_message_text("У вас нет прав на редактирование расписания.")
        return ConversationHandler.END

    data = query.data or ""
    if data == "edit_cancel":
        await query.edit_message_text("Редактирование отменено.")
        return ConversationHandler.END

    if data != "edit_confirm":
        await query.edit_message_text("Не понял ответ. Попробуй ещё раз: /edit_schedule")
        return ConversationHandler.END

    mode = context.user_data.get("edit_mode") or "base"
    day = context.user_data.get("edit_day")

    # ── Сохранение всех профилей субботы сразу ──────────────────────────────
    sat_all = context.user_data.pop("edit_sat_all_profiles", None)
    if sat_all is not None:
        if mode == "temp":
            edit_date = context.user_data.get("edit_date")
            if not edit_date:
                await query.edit_message_text("Сессия редактирования потеряна. Запусти заново: /edit_schedule")
                return ConversationHandler.END
            existing = temp_schedule.get(edit_date)
            if not isinstance(existing, dict):
                existing = {}
            existing.update(sat_all)
            temp_schedule[edit_date] = existing
            try:
                _save_temp_schedule_to_disk()
            except Exception as e:
                await query.edit_message_text(f"Не удалось сохранить: {e}")
                return ConversationHandler.END
            date_label = context.user_data.get("edit_label") or edit_date
            labels_str = ", ".join(SATURDAY_PROFILE_LABELS.get(k, k) for k in sat_all)
            notify_parts = [
                _format_day_table_html(f"Суббота — {SATURDAY_PROFILE_LABELS.get(k, k)}", v)
                for k, v in sat_all.items()
            ]
            msg = _truncate_message(f"📢 Временное расписание субботы обновлено ({date_label}):\n\n" + "\n\n".join(notify_parts))
            asyncio.create_task(_notify_subscribers(msg))
            await query.edit_message_text(f"Готово! Обновлены профили для {date_label}: {labels_str}.")
        else:
            if not isinstance(schedule.get("Суббота"), dict):
                schedule["Суббота"] = {}
            schedule["Суббота"].update(sat_all)
            try:
                _save_schedule_to_disk()
            except Exception as e:
                await query.edit_message_text(f"Не удалось сохранить: {e}")
                return ConversationHandler.END
            labels_str = ", ".join(SATURDAY_PROFILE_LABELS.get(k, k) for k in sat_all)
            notify_parts = [
                _format_day_table_html(f"Суббота — {SATURDAY_PROFILE_LABELS.get(k, k)}", v)
                for k, v in sat_all.items()
            ]
            msg = _truncate_message("📢 Обновлено расписание субботы:\n\n" + "\n\n".join(notify_parts))
            asyncio.create_task(_notify_subscribers(msg))
            await query.edit_message_text(f"Готово! Обновлены профили субботы: {labels_str}.")
        return ConversationHandler.END

    if mode == "base" and day == "__WEEK__":
        week = context.user_data.get("edit_week")
        if not isinstance(week, dict):
            await query.edit_message_text("Сессия редактирования потеряна. Запусти заново: /edit_schedule")
            return ConversationHandler.END

        for d in SCHEDULE_DAYS:
            if d in week:
                schedule[d] = week[d]

        try:
            _save_schedule_to_disk()
        except Exception as e:
            await query.edit_message_text(f"Не удалось сохранить расписание: {e}")
            return ConversationHandler.END

        week_text = "\n\n".join(
            _format_day_table_html(d, schedule.get(d, []))
            for d in SCHEDULE_DAYS
            if d in schedule
        ) or _format_day_table_html("Неделя", [])
        week_text = _truncate_message("📢 Обновлено расписание на неделю:\n\n" + week_text)
        asyncio.create_task(_notify_subscribers(week_text))

        await query.edit_message_text("Готово! Расписание на неделю обновлено.")
        return ConversationHandler.END

    lessons = context.user_data.get("edit_lessons")
    if lessons is None:
        await query.edit_message_text("Сессия редактирования потеряна. Запусти заново: /edit_schedule")
        return ConversationHandler.END

    if mode == "temp":
        edit_date = context.user_data.get("edit_date")
        if not edit_date:
            await query.edit_message_text("Сессия редактирования потеряна. Запусти заново: /edit_schedule")
            return ConversationHandler.END

        profile_key = context.user_data.get("edit_saturday_profile")
        if profile_key and profile_key in SATURDAY_PROFILE_KEYS:
            # Суббота по профилям — сохраняем как dict, не затирая другие профили
            existing = temp_schedule.get(edit_date)
            if not isinstance(existing, dict):
                existing = {}
            existing[profile_key] = lessons
            temp_schedule[edit_date] = existing
            profile_label = SATURDAY_PROFILE_LABELS.get(profile_key, profile_key)
            date_label = context.user_data.get("edit_label") or edit_date
            display_label = f"{date_label} — {profile_label}"
            notify_label = f"Суббота — {profile_label}"
        else:
            temp_schedule[edit_date] = lessons
            display_label = context.user_data.get("edit_label") or edit_date
            notify_label = display_label

        try:
            _save_temp_schedule_to_disk()
        except Exception as e:
            await query.edit_message_text(f"Не удалось сохранить временное расписание: {e}")
            return ConversationHandler.END

        msg = "📢 Временное расписание обновлено:\n\n" + _format_day_table_html(notify_label, lessons)
        asyncio.create_task(_notify_subscribers(msg))

        await query.edit_message_text(f"Готово! Временное расписание для «{display_label}» обновлено.")
        return ConversationHandler.END

    if not day:
        await query.edit_message_text("Сессия редактирования потеряна. Запусти заново: /edit_schedule")
        return ConversationHandler.END

    if day == "Суббота":
        profile_key = context.user_data.get("edit_saturday_profile")
        if not profile_key or profile_key not in SATURDAY_PROFILE_KEYS:
            await query.edit_message_text("Сессия редактирования потеряна. Запусти заново: /edit_schedule")
            return ConversationHandler.END
        if not isinstance(schedule.get("Суббота"), dict):
            schedule["Суббота"] = {}
        schedule["Суббота"][profile_key] = lessons
        label = SATURDAY_PROFILE_LABELS.get(profile_key, profile_key)
    else:
        schedule[day] = lessons
        label = day

    try:
        _save_schedule_to_disk()
    except Exception as e:
        await query.edit_message_text(f"Не удалось сохранить расписание: {e}")
        return ConversationHandler.END

    msg = "📢 Обновлено расписание:\n\n" + _format_day_table_html(f"Суббота — {label}" if day == "Суббота" else day, lessons)
    asyncio.create_task(_notify_subscribers(msg))

    await query.edit_message_text(f"Готово! Расписание для «{label}» обновлено.")
    return ConversationHandler.END

async def edit_schedule_sat_all_entered(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Получаем текст со всеми профилями субботы сразу."""
    if not _is_admin(update):
        await update.message.reply_text("У вас нет прав на редактирование расписания.")
        return ConversationHandler.END

    profiles = _parse_saturday_all_profiles(update.message.text or "")
    if not profiles:
        await update.message.reply_text(
            "Не удалось распознать профили.\n"
            "Используй формат:\n"
            "Суббота Физмат:\n08:30-09:05 Алгебра/211\n...\n\n"
            "Суббота Инфотех 2 группа:\n08:30-09:05 Алгоритмика/304\n..."
        )
        return EDIT_ENTER_SAT_ALL

    context.user_data["edit_sat_all_profiles"] = profiles

    # Превью
    blocks = []
    for key, lessons in profiles.items():
        label = SATURDAY_PROFILE_LABELS.get(key, key)
        lessons_text = "\n".join(lessons) if lessons else "— (пусто) —"
        blocks.append(f"<b>Суббота — {html.escape(label)}</b>\n{html.escape(lessons_text)}")
    preview = "\n\n".join(blocks)

    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Сохранить", callback_data="edit_confirm"),
        InlineKeyboardButton("❌ Отмена", callback_data="edit_cancel"),
    ]])
    await update.message.reply_text(
        f"Проверь расписание субботы:\n\n{preview}",
        reply_markup=keyboard,
        parse_mode="HTML",
    )
    return EDIT_CONFIRM

async def edit_schedule_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message:
        await update.message.reply_text("Ок, отменил.")
    return ConversationHandler.END

# ================== FastAPI ==================
app = FastAPI()
bot_app = ApplicationBuilder().token(TOKEN).build()
bot_app.add_handler(CommandHandler("start", start))
bot_app.add_handler(CommandHandler("help", help_command))
bot_app.add_handler(CommandHandler("subscribe", subscribe))
bot_app.add_handler(CommandHandler("unsubscribe", unsubscribe))

edit_conv = ConversationHandler(
    entry_points=[CommandHandler("edit_schedule", edit_schedule_start)],
    states={
        EDIT_MODE: [CallbackQueryHandler(edit_schedule_mode_chosen, pattern=r"^edit_")],
        EDIT_CHOOSE_DAY: [CallbackQueryHandler(edit_schedule_day_chosen, pattern=r"^edit_")],
        EDIT_CHOOSE_SATURDAY_PROFILE: [
            CallbackQueryHandler(edit_schedule_saturday_profile_chosen, pattern=r"^edit_(sat_profile:.+|cancel)$")
        ],
        EDIT_ENTER_DATE: [
            MessageHandler(filters.TEXT & ~filters.COMMAND, edit_schedule_date_entered)
        ],
        EDIT_ENTER_LESSONS: [
            CommandHandler("set", edit_schedule_lessons_command),
            MessageHandler(filters.TEXT & ~filters.COMMAND, edit_schedule_lessons_entered)
        ],
        EDIT_ENTER_WEEK: [
            MessageHandler(filters.TEXT & ~filters.COMMAND, edit_schedule_week_entered)
        ],
        EDIT_ENTER_SAT_ALL: [
            MessageHandler(filters.TEXT & ~filters.COMMAND, edit_schedule_sat_all_entered)
        ],
        EDIT_CONFIRM: [CallbackQueryHandler(edit_schedule_confirm, pattern=r"^edit_")],
    },
    fallbacks=[CommandHandler("cancel", edit_schedule_cancel)],
)
bot_app.add_handler(edit_conv)
bot_app.add_handler(InlineQueryHandler(inline_schedule))

# ================== Webhook endpoint ==================
@app.post(WEBHOOK_PATH)
async def telegram_webhook(request: Request):
    data = await request.json()
    update = Update.de_json(data, bot_app.bot)
    await bot_app.process_update(update)
    return {"ok": True}

# ================== Lifespan ==================
@app.on_event("startup")
async def startup_event():
    await bot_app.initialize()
    await bot_app.bot.set_webhook(f"{BOT_URL.rstrip('/')}{WEBHOOK_PATH}")
    await bot_app.bot.set_my_commands(
        [
            BotCommand("start", "Запуск / приветствие"),
            BotCommand("help", "Подсказки и помощь"),
            BotCommand("edit_schedule", "Редактировать расписание"),
            BotCommand("subscribe", "Ежедневное напоминание (HH:MM)"),
            BotCommand("unsubscribe", "Отключить напоминания"),
            BotCommand("cancel", "Отменить редактирование"),
        ]
    )

    global scheduler
    scheduler = AsyncIOScheduler(timezone=_get_tz())
    _load_temp_schedule_from_disk()
    _load_subscriptions_from_disk()
    for user_id_str in list(subscriptions.keys()):
        if user_id_str.isdigit():
            _reschedule_user(int(user_id_str))
    scheduler.start()

    await bot_app.start()
    print("✅ Webhook установлен, бот готов к работе")

    async def ping_self():
        async with httpx.AsyncClient(timeout=10.0) as client:
            while True:
                try:
                    resp = await client.get(BOT_URL)
                    print(f"[ping] {resp.status_code} {datetime.now().strftime('%H:%M:%S')}")
                except Exception as e:
                    print(f"[ping error] {e}")
                await asyncio.sleep(600)

    asyncio.create_task(ping_self())

@app.on_event("shutdown")
async def shutdown_event():
    await bot_app.stop()
    await bot_app.shutdown()
    print("🛑 Бот остановлен")

# ================== Стартовая страница ==================
@app.get("/")
def root():
    return {"status": "Bot is running ✅"}
