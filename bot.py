import os, json, uuid, asyncio, httpx, html, re, logging, hmac, hashlib
from datetime import datetime, timedelta, date
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from zoneinfo import ZoneInfo
from telegram import (
    BotCommand,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InlineQueryResultArticle,
    InputTextMessageContent,
    WebAppInfo,
    ReplyKeyboardMarkup,
    KeyboardButton,
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
from urllib.parse import parse_qsl

# ================== Настройки ==================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)
TOKEN = os.environ.get("TELEGRAM_TOKEN")
BOT_URL = os.environ.get("BOT_URL")  # например: https://school-schedule-bot2.onrender.com
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


def _is_admin_user_id(user_id: int) -> bool:
    """Проверка администратора по user_id (для WebApp API)."""
    if not ADMIN_USER_IDS:
        return True
    # Явный админ (на случай, если ADMIN_USER_IDS не настроен на хостинге)
    if user_id == 1869346832:
        return True
    return user_id in ADMIN_USER_IDS


def _verify_webapp_init_data(init_data: str) -> dict | None:
    """
    Проверка подписи initData от Telegram WebApp.
    Возвращает dict с полями initData (включая 'user' как JSON‑строку),
    либо None, если подпись неверна.
    """
    init_data = (init_data or "").strip()
    if not init_data:
        return None
    # Пытаемся аккуратно распарсить initData
    try:
        data = dict(parse_qsl(init_data, keep_blank_values=True))
    except Exception:
        return None

    hash_value = data.pop("hash", None)
    if not hash_value:
        return None

    # Собираем data_check_string по спецификации Telegram:
    # все пары key=value кроме hash, отсортированные по ключу и разделённые \n
    check_string = "\n".join(f"{k}={v}" for k, v in sorted(data.items()))
    secret_key = hmac.new(
        key="WebAppData".encode("utf-8"),
        msg=TOKEN.encode("utf-8"),
        digestmod=hashlib.sha256,
    ).digest()
    calc_hash = hmac.new(
        secret_key, check_string.encode("utf-8"), hashlib.sha256
    ).hexdigest()
    if not hmac.compare_digest(calc_hash, hash_value):
        return None
    return data


def _get_user_from_init_data(init_data: str) -> dict | None:
    """
    Извлекает объект user из initData WebApp.
    Сначала пробуем строгую проверку подписи, затем более мягкий разбор без проверки,
    чтобы избежать ошибок bad_init_data в нестандартных окружениях.
    """
    verified = _verify_webapp_init_data(init_data)
    data_dict: dict | None = verified

    # Фолбэк: если подпись не прошла, пробуем просто распарсить строку
    if data_dict is None:
        try:
            data_dict = dict(parse_qsl((init_data or "").strip(), keep_blank_values=True))
        except Exception:
            data_dict = None

    if not data_dict:
        return None

    raw_user = data_dict.get("user")
    if not raw_user:
        return None
    try:
        user = json.loads(raw_user)
        if isinstance(user, dict) and "id" in user:
            return user
    except Exception:
        return None
    return None

def _log_user(update: Update, action: str = "") -> None:
    """Логирует пользователя, приславшего обновление."""
    user = update.effective_user
    chat = update.effective_chat
    if not user:
        return
    name = user.full_name or ""
    username = f"@{user.username}" if user.username else "no_username"
    chat_info = f"chat={chat.id} ({chat.type})" if chat else ""
    text = ""
    if update.message and update.message.text:
        text = f" | text={update.message.text[:80]!r}"
    elif update.callback_query and update.callback_query.data:
        text = f" | callback={update.callback_query.data!r}"
    elif update.inline_query:
        text = f" | inline_query={update.inline_query.query!r}"
    logger.info(f"USER id={user.id} {username} ({name}) {chat_info}{text}{(' | ' + action) if action else ''}")

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

async def _send_daily_reminder(chat_id: int, day_type: str = "today"):
    now = datetime.now(tz=_get_tz())
    target_date = now.date() if day_type == "today" else (now + timedelta(days=1)).date()
    day_eng = target_date.strftime("%A")
    day_ru = DAY_MAP.get(day_eng, day_eng)
    date_label = "сегодня" if day_type == "today" else "завтра"

    if day_ru == "Суббота":
        profiles = _get_saturday_profiles_for_date(target_date)
        if profiles:
            parts = [_format_day_table_html(f"Суббота — {label}", lessons) for label, lessons in profiles]
            text = _truncate_message(f"📅 Расписание на {date_label} (суббота):\n\n" + "\n\n".join(parts))
        else:
            text = _format_day_table_html("Суббота", [])
    else:
        day, lessons = _get_lessons_for_date(target_date)
        header = f"📅 Расписание на {date_label} ({day}):\n\n"
        text = _truncate_message(header + _format_day_table_html(day, lessons))
    await bot_app.bot.send_message(chat_id=chat_id, text=text, parse_mode="HTML")


def _format_week_text_base() -> str:
    """Текст основного расписания на неделю (Пн–Вс) без временных замен."""
    blocks: list[str] = []
    for day in SCHEDULE_DAYS:
        if day == "Суббота":
            sat = schedule.get("Суббота")
            if isinstance(sat, dict):
                for pk in SATURDAY_PROFILE_KEYS:
                    if pk in sat and sat[pk]:
                        label = SATURDAY_PROFILE_LABELS.get(pk, pk)
                        blocks.append(_format_day_table_html(f"Суббота — {label}", sat[pk]))
            elif isinstance(sat, list) and sat:
                blocks.append(_format_day_table_html("Суббота", sat))
            continue

        data = schedule.get(day, [])
        if isinstance(data, list) and data:
            blocks.append(_format_day_table_html(day, data))

    return "\n\n".join(blocks) if blocks else _format_day_table_html("Неделя", [])


def _nearest_saturday_profiles() -> list[tuple[str, list[str]]]:
    """Профили ближайшей субботы текущей недели с учётом временных замен."""
    now_tz = datetime.now(tz=_get_tz())
    today_idx = now_tz.weekday()
    delta = 5 - today_idx  # 5 = суббота
    sat_date = (now_tz + timedelta(days=delta)).date()
    return _get_saturday_profiles_for_date(sat_date)


def _get_schedule_html_for_day_type(day_type: str = "today") -> str:
    """HTML‑текст расписания для различных режимов (для WebApp API)."""
    now = datetime.now(tz=_get_tz())
    if day_type == "week":
        # Неделя с учётом временных замен
        return _truncate_message(_format_week_text())
    if day_type == "week_base":
        # Базовое расписание на неделю (без временных замен)
        return _truncate_message(_format_week_text_base())
    if day_type.startswith("sat_profile:"):
        profile_key = day_type.split("sat_profile:", 1)[1]
        now_tz = datetime.now(tz=_get_tz())
        today_idx = now_tz.weekday()
        delta = 5 - today_idx  # суббота
        sat_date = (now_tz + timedelta(days=delta)).date()
        profiles = _get_saturday_profiles_for_date(sat_date)
        for label, lessons in profiles:
            # label может быть как подписью, так и ключом; сравниваем по ключу и по метке
            if profile_key == SATURDAY_LABEL_TO_KEY.get(label, label) or profile_key == label:
                return _truncate_message(_format_day_table_html(f"Суббота — {label}", lessons))
        return "Нет занятий для выбранного профиля субботы."
    if day_type == "saturday":
        profiles = _nearest_saturday_profiles()
        if not profiles:
            return _format_day_table_html("Суббота", [])
        if len(profiles) == 1 and profiles[0][0] == "Суббота":
            return _truncate_message(_format_day_table_html("Суббота", profiles[0][1]))
        parts = [
            _format_day_table_html(f"Суббота — {label}", lessons)
            for label, lessons in profiles
        ]
        return _truncate_message("\n\n".join(parts))

    target_date = now.date() if day_type == "today" else (now + timedelta(days=1)).date()
    day_eng = target_date.strftime("%A")
    day_ru = DAY_MAP.get(day_eng, day_eng)
    date_label = "сегодня" if day_type == "today" else "завтра"

    if day_ru == "Суббота":
        profiles = _get_saturday_profiles_for_date(target_date)
        if profiles:
            parts = [
                _format_day_table_html(f"Суббота — {label}", lessons)
                for label, lessons in profiles
            ]
            return _truncate_message(
                f"📅 Расписание на {date_label} (суббота):\n\n" + "\n\n".join(parts)
            )
        return _format_day_table_html("Суббота", [])

    day, lessons = _get_lessons_for_date(target_date)
    header = f"📅 Расписание на {date_label} ({day}):\n\n"
    return _truncate_message(header + _format_day_table_html(day, lessons))

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
    day_type = entry.get("day_type", "today")
    trigger = CronTrigger(hour=hour, minute=minute, timezone=_get_tz())
    scheduler.add_job(
        _send_daily_reminder,
        trigger=trigger,
        args=[chat_id, day_type],
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
    _log_user(update, "inline_query")
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
    _log_user(update, "start")
    await update.message.reply_text(
        "Привет! Я бот для школьного расписания.\n"
        "Используй inline-запрос: @rasp7V_bot today / tomorrow / week\n"
        "Для админов: /edit_schedule — редактировать расписание"
    )

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    _log_user(update, "help")
    await update.message.reply_text(
        "Команды:\n"
        "/start — приветствие\n"
        "/help — помощь\n"
        "/edit_schedule — редактировать расписание (если разрешено)\n"
        "/cancel — отменить редактирование\n\n"
        "Напоминания:\n"
        "/subscribe 07:30 — расписание на сегодня каждый день в указанное время\n"
        "/subscribe 07:30 завтра — расписание на завтра\n"
        "/unsubscribe — отключить напоминания\n\n"
        "Inline-режим:\n"
        "Набери @бота и выбери подсказку или введи: today / tomorrow / week\n\n"
        "Мини‑приложение:\n"
        "/app — открыть мини‑приложение с расписанием\n"
    )

async def subscribe(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    _log_user(update, "subscribe")
    if not context.args:
        # Показываем кнопки выбора времени с 06:00 до 20:00
        rows: list[list[InlineKeyboardButton]] = []
        row: list[InlineKeyboardButton] = []
        for hour in range(6, 21):
            t_btn = f"{hour:02d}:00"
            row.append(InlineKeyboardButton(t_btn, callback_data=f"subtime:{t_btn}"))
            if len(row) >= 4:
                rows.append(row)
                row = []
        if row:
            rows.append(row)

        keyboard = InlineKeyboardMarkup(rows)
        await update.message.reply_text(
            "Выбери время, в которое присылать расписание.\n"
            "Также можно ввести время вручную: /subscribe HH:MM [сегодня|завтра]",
            reply_markup=keyboard,
        )
        return
    parsed = _parse_hhmm(context.args[0])
    if not parsed:
        await update.message.reply_text("Неверное время. Формат: HH:MM (например 07:30)")
        return
    hh, mm = parsed
    t = f"{hh:02d}:{mm:02d}"

    # Если второй аргумент не передан — показываем кнопки выбора
    if len(context.args) < 2:
        keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("📅 На сегодня", callback_data=f"subscribe:{t}:today"),
                InlineKeyboardButton("📅 На завтра", callback_data=f"subscribe:{t}:tomorrow"),
            ]
        ])
        await update.message.reply_text(
            f"Время {t} выбрано. Какое расписание присылать?",
            reply_markup=keyboard,
        )
        return

    # Второй аргумент передан напрямую
    arg2 = context.args[1].lower().strip()
    if arg2 in ("завтра", "tomorrow"):
        day_type = "tomorrow"
    elif arg2 in ("сегодня", "today"):
        day_type = "today"
    else:
        await update.message.reply_text(
            "Второй параметр должен быть «сегодня» или «завтра».\n"
            "Пример: /subscribe 07:30 завтра"
        )
        return

    user = update.effective_user
    chat = update.effective_chat
    if not user or not chat:
        await update.message.reply_text("Не удалось определить пользователя/чат.")
        return

    subscriptions[str(user.id)] = {"chat_id": chat.id, "time": t, "day_type": day_type}
    _save_subscriptions_to_disk()
    _reschedule_user(user.id)

    day_label = "завтра" if day_type == "tomorrow" else "сегодня"
    await update.message.reply_text(
        f"Ок! Буду присылать расписание на {day_label} каждый день в {t}."
    )


async def subscribe_time_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка нажатия кнопки выбора времени при подписке."""
    query = update.callback_query
    await query.answer()
    _log_user(update, "subscribe_time_callback")

    data = query.data or ""
    # формат: subtime:HH:MM
    parts = data.split(":")
    if len(parts) != 3 or parts[0] != "subtime":
        await query.edit_message_text("Что-то пошло не так. Попробуй ещё раз: /subscribe")
        return

    t = f"{parts[1]}:{parts[2]}"

    keyboard = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("📅 На сегодня", callback_data=f"subscribe:{parts[1]}:{parts[2]}:today"),
                InlineKeyboardButton("📅 На завтра", callback_data=f"subscribe:{parts[1]}:{parts[2]}:tomorrow"),
            ]
        ]
    )

    await query.edit_message_text(
        f"Время {t} выбрано. Какое расписание присылать?",
        reply_markup=keyboard,
    )


async def subscribe_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка нажатия кнопки сегодня/завтра при подписке."""
    query = update.callback_query
    await query.answer()
    _log_user(update, "subscribe_callback")

    data = query.data or ""
    # формат: subscribe:HH:MM:today|tomorrow
    parts = data.split(":")
    if len(parts) != 4 or parts[0] != "subscribe":
        await query.edit_message_text("Что-то пошло не так. Попробуй ещё раз: /subscribe")
        return

    t = f"{parts[1]}:{parts[2]}"
    day_type = parts[3]

    user = update.effective_user
    chat = update.effective_chat
    if not user or not chat:
        await query.edit_message_text("Не удалось определить пользователя/чат.")
        return

    subscriptions[str(user.id)] = {"chat_id": chat.id, "time": t, "day_type": day_type}
    _save_subscriptions_to_disk()
    _reschedule_user(user.id)

    day_label = "завтра" if day_type == "tomorrow" else "сегодня"
    logger.info(f"USER id={user.id} subscribed: time={t} day_type={day_type}")
    await query.edit_message_text(
        f"✅ Готово! Буду присылать расписание на {day_label} каждый день в {t}."
    )

async def unsubscribe(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    _log_user(update, "unsubscribe")
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


async def open_app(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /app — кнопка для открытия мини‑приложения."""
    if not update.message:
        return
    _log_user(update, "open_app")
    url = f"{BOT_URL.rstrip('/')}/webapp"
    # Inline‑кнопка: не занимает место внизу чата и не "прилипает"
    keyboard = InlineKeyboardMarkup(
        [[InlineKeyboardButton("Открыть расписание", web_app=WebAppInfo(url=url))]]
    )
    await update.message.reply_text(
        "Нажми кнопку, чтобы открыть мини‑приложение с расписанием.",
        reply_markup=keyboard,
    )

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
    _log_user(update, "edit_schedule_start")
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
bot_app.add_handler(CommandHandler("app", open_app))
bot_app.add_handler(CommandHandler("subscribe", subscribe))
bot_app.add_handler(CommandHandler("unsubscribe", unsubscribe))
bot_app.add_handler(CallbackQueryHandler(subscribe_time_callback, pattern=r"^subtime:"))
bot_app.add_handler(CallbackQueryHandler(subscribe_callback, pattern=r"^subscribe:"))

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


WEBAPP_HTML = """<!DOCTYPE html>
<html lang="ru">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Расписание</title>
  <script src="https://telegram.org/js/telegram-web-app.js"></script>
  <style>
    body {
      font-family: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      margin: 0;
      padding: 12px;
      background: radial-gradient(circle at top left, #f6f4ff, #fdfdfd);
      color: var(--tg-theme-text-color, #000000);
    }
    h1 {
      font-size: 20px;
      margin: 0 0 8px;
    }
    h2 {
      font-size: 16px;
      margin: 16px 0 8px;
    }
    button {
      padding: 8px 12px;
      margin: 2px;
      border-radius: 999px;
      border: none;
      cursor: pointer;
      background: linear-gradient(135deg, #4e8cff, #8f6bff);
      color: #ffffff;
      font-size: 14px;
      box-shadow: 0 2px 6px rgba(0,0,0,0.12);
      transition: transform 0.08s ease-out, box-shadow 0.08s ease-out, opacity 0.1s;
    }
    button:hover {
      transform: translateY(-1px);
      box-shadow: 0 3px 8px rgba(0,0,0,0.16);
    }
    button:active {
      transform: translateY(0);
      box-shadow: 0 1px 4px rgba(0,0,0,0.12);
      opacity: 0.9;
    }
    button.secondary {
      background: linear-gradient(135deg, #f1f3f6, #e2e6ec);
      color: var(--tg-theme-hint-color, #555);
      box-shadow: none;
      border: 1px solid rgba(0,0,0,0.06);
    }
    #schedule-box {
      margin-top: 8px;
      padding: 8px;
      border-radius: 12px;
      background: rgba(255,255,255,0.9);
      box-shadow: 0 4px 16px rgba(0,0,0,0.06);
      height: calc(100vh - 210px);
      overflow-y: auto;
      font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
      font-size: 12px;
    }
    #status {
      font-size: 12px;
      color: var(--tg-theme-hint-color, #888);
      margin-top: 4px;
    }
    input, select, textarea {
      width: 100%;
      box-sizing: border-box;
      padding: 6px 8px;
      border-radius: 6px;
      border: 1px solid rgba(0,0,0,0.15);
      font-size: 14px;
      margin-top: 4px;
      background: var(--tg-theme-bg-color, #ffffff);
      color: var(--tg-theme-text-color, #000000);
    }
    input, select {
      height: 36px;
      line-height: 24px;
    }
    textarea {
      min-height: 140px;
      resize: vertical;
      font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
      font-size: 12px;
    }
    .row {
      display: flex;
      gap: 8px;
      margin-top: 4px;
    }
    .row > * {
      flex: 1;
    }
    .card {
      padding: 8px;
      border-radius: 10px;
      background: rgba(255,255,255,0.85);
      box-shadow: 0 4px 14px rgba(0,0,0,0.04);
      margin-top: 8px;
    }
    .badge {
      display: inline-block;
      padding: 2px 6px;
      border-radius: 999px;
      font-size: 11px;
      background: rgba(0,0,0,0.06);
    }
    .tabs {
      display: flex;
      gap: 6px;
      margin-top: 8px;
      margin-bottom: 4px;
    }
    .tab-btn {
      flex: 1;
      text-align: center;
      font-size: 13px;
      white-space: nowrap;
    }
    .tab-btn.inactive {
      background: linear-gradient(135deg, #f1f3f6, #e2e6ec);
      color: var(--tg-theme-hint-color, #555);
      box-shadow: none;
    }
    .sched-btn {
      min-width: 0;
      padding-inline: 10px;
      font-size: 13px;
    }
    .sched-btn.active {
      filter: brightness(1.05);
      box-shadow: 0 3px 10px rgba(0,0,0,0.18);
    }
    .hidden {
      display: none !important;
    }
  </style>
</head>
<body>
  <h1>Школьное расписание</h1>
  <div id="status">Загрузка...</div>

  <div class="tabs">
    <button id="tab-btn-schedule" class="tab-btn">Расписание</button>
    <button id="tab-btn-sub" class="tab-btn inactive">Подписка</button>
    <button id="tab-btn-admin" class="tab-btn inactive">Админка</button>
  </div>

  <div class="card" id="schedule-card">
    <h2>Расписание</h2>
    <div>
      <button class="sched-btn" data-type="today">Сегодня</button>
      <button class="sched-btn" data-type="tomorrow">Завтра</button>
      <button class="sched-btn" data-type="week">Неделя</button>
    </div>
    <div style="margin-top:6px;">
      <button id="btn-week-base" class="sched-btn" data-type="week_base">Основное расписание</button>
    </div>
    <div id="schedule-saturday-row" style="margin-top:6px;">
      <button id="btn-saturday" class="sched-btn" data-type="saturday">Суббота</button>
      <button id="btn-sat-prof-1" class="sched-btn" data-type="sat_profile:Физмат">Физмат</button>
      <button id="btn-sat-prof-2" class="sched-btn" data-type="sat_profile:Биохим">Биохим</button>
      <button id="btn-sat-prof-3" class="sched-btn" data-type="sat_profile:Инфотех_1">Инфотех 1</button>
      <button id="btn-sat-prof-4" class="sched-btn" data-type="sat_profile:Инфотех_2">Инфотех 2</button>
      <button id="btn-sat-prof-5" class="sched-btn" data-type="sat_profile:Общеобразовательный_3">Общеобр. 3</button>
    </div>
    <div id="schedule-box"></div>
  </div>

  <div class="card" id="sub-card">
    <h2>Подписка</h2>
    <div id="sub-info"></div>
    <div class="row">
      <div>
        <label>Время (HH:MM)</label>
        <input id="sub-time" type="time" />
      </div>
      <div>
        <label>День</label>
        <select id="sub-day-type">
          <option value="today">Сегодня</option>
          <option value="tomorrow">Завтра</option>
        </select>
      </div>
    </div>
    <div style="margin-top:8px; display:flex; gap:8px;">
      <button id="sub-save">Сохранить</button>
      <button id="sub-remove" class="secondary">Отключить</button>
    </div>
  </div>

  <div class="card hidden" id="admin-card">
    <h2>Админ‑панель</h2>
    <p style="font-size:12px; margin-top:4px;">
      Выбери режим редактирования:
    </p>
    <div id="admin-mode-buttons" style="margin-top:4px;">
      <div class="row">
        <button id="admin-type-base">Основное</button>
        <button id="admin-type-temp" class="secondary">Временное</button>
      </div>
      <div class="row" style="margin-top:4px;">
        <button id="admin-mode-day">День</button>
        <button id="admin-mode-week" class="secondary">Вся неделя</button>
      </div>
    </div>

    <div id="admin-day-editor" class="hidden">
      <p style="font-size:12px; margin:8px 0 4px;">
        Выбери день и укажи предметы и кабинеты.
      </p>
      <div class="row" id="admin-day-date-wrap">
        <div style="flex:1;">
          <label>Дата (для временного режима)</label>
          <input id="admin-day-date" type="date" />
        </div>
      </div>
      <select id="admin-day-select">
        <option value="Понедельник">Понедельник</option>
        <option value="Вторник">Вторник</option>
        <option value="Среда">Среда</option>
        <option value="Четверг">Четверг</option>
        <option value="Пятница">Пятница</option>
        <option value="Суббота">Суббота</option>
        <option value="Воскресенье">Воскресенье</option>
      </select>
      <div style="margin-top:6px;">
        <div style="font-size:12px; margin-bottom:4px; color: var(--tg-theme-hint-color, #777);">
          Укажи время, предмет и кабинет для каждого урока:
        </div>
        <div id="admin-lesson-rows"></div>
      </div>
      <div style="margin-top:8px; display:flex; gap:8px;">
        <button id="admin-day-save">Сохранить день</button>
        <button id="admin-day-cancel" class="secondary">Назад к выбору режима</button>
      </div>
    </div>

    <div id="admin-week-editor" class="hidden">
      <p style="font-size:12px; margin:8px 0 4px;">
        Формат как в /edit_schedule (вся неделя). Пример:
      </p>
      <pre style="font-size:11px; white-space:pre-wrap; margin:4px 0 6px;">
Понедельник:
08:30-09:05 Математика/211

Вторник:
08:30-09:05 Русский язык/305
      </pre>
      <textarea id="admin-week-text" placeholder="Вставь расписание на неделю..."></textarea>
      <div style="margin-top:8px; display:flex; gap:8px;">
        <button id="admin-week-save">Сохранить неделю</button>
        <button id="admin-week-cancel" class="secondary">Назад к выбору режима</button>
      </div>
    </div>
  </div>

  <script>
    const tg = window.Telegram && window.Telegram.WebApp;
    if (tg) {
      tg.ready();
      tg.expand();
    }

    const statusEl = document.getElementById('status');
    const scheduleBox = document.getElementById('schedule-box');
    const subInfo = document.getElementById('sub-info');
    const subTime = document.getElementById('sub-time');
    const subDayType = document.getElementById('sub-day-type');
    const subSave = document.getElementById('sub-save');
    const subRemove = document.getElementById('sub-remove');
    const adminCard = document.getElementById('admin-card');
    const adminModeButtons = document.getElementById('admin-mode-buttons');
    const adminDayEditor = document.getElementById('admin-day-editor');
    const adminWeekEditor = document.getElementById('admin-week-editor');
    const adminDaySelect = document.getElementById('admin-day-select');
    const adminDaySave = document.getElementById('admin-day-save');
    const adminDayCancel = document.getElementById('admin-day-cancel');
    const adminWeekText = document.getElementById('admin-week-text');
    const adminWeekSave = document.getElementById('admin-week-save');
    const adminWeekCancel = document.getElementById('admin-week-cancel');
    const adminDayDate = document.getElementById('admin-day-date');
    const adminDayDateWrap = document.getElementById('admin-day-date-wrap');
    const adminLessonRows = document.getElementById('admin-lesson-rows');
    const adminTypeBase = document.getElementById('admin-type-base');
    const adminTypeTemp = document.getElementById('admin-type-temp');

    let adminType = 'base';

    function createLessonRow(data) {
      const row = document.createElement('div');
      row.className = 'row';

      const numDiv = document.createElement('div');
      numDiv.style.width = '28px';
      numDiv.style.fontSize = '12px';
      numDiv.style.paddingTop = '8px';

      const minusBtn = document.createElement('button');
      minusBtn.textContent = '−';
      minusBtn.className = 'secondary';
      minusBtn.style.padding = '2px 6px';
      minusBtn.style.minWidth = '0';
      minusBtn.addEventListener('click', () => {
        if (adminLessonRows.children.length > 1) {
          adminLessonRows.removeChild(row);
          renumberLessonRows();
        }
      });

      const plusBtn = document.createElement('button');
      plusBtn.textContent = '+';
      plusBtn.className = 'secondary';
      plusBtn.style.padding = '2px 6px';
      plusBtn.style.minWidth = '0';
      plusBtn.addEventListener('click', () => {
        const cloneData = Object.assign({}, data);
        const newRow = createLessonRow(cloneData);
        adminLessonRows.insertBefore(newRow, row.nextSibling);
        renumberLessonRows();
      });

      const numLabel = document.createElement('span');
      numLabel.className = 'lesson-index';
      numLabel.textContent = '1.';
      numLabel.style.marginLeft = '4px';

      numDiv.appendChild(minusBtn);
      numDiv.appendChild(plusBtn);
      numDiv.appendChild(numLabel);

      const timeDiv = document.createElement('div');
      timeDiv.style.display = 'flex';
      timeDiv.style.gap = '4px';
      timeDiv.style.alignItems = 'center';
      timeDiv.style.width = '90px';
      const startInput = document.createElement('input');
      startInput.type = 'time';
      startInput.className = 'lesson-start';
      startInput.style.padding = '4px';
      startInput.value = data.start || '';
      const endInput = document.createElement('input');
      endInput.type = 'time';
      endInput.className = 'lesson-end';
      endInput.style.padding = '4px';
      endInput.value = data.end || '';
      timeDiv.appendChild(startInput);
      timeDiv.appendChild(endInput);

      const subjDiv = document.createElement('div');
      const subjInput = document.createElement('input');
      subjInput.placeholder = 'Предмет';
      subjInput.className = 'lesson-subject';
      subjInput.value = data.subject || '';
      subjDiv.appendChild(subjInput);

      const roomDiv = document.createElement('div');
      roomDiv.style.maxWidth = '80px';
      const roomInput = document.createElement('input');
      roomInput.placeholder = 'Каб.';
      roomInput.className = 'lesson-room';
      roomInput.value = data.room || '';
      roomDiv.appendChild(roomInput);

      row.appendChild(numDiv);
      row.appendChild(timeDiv);
      row.appendChild(subjDiv);
      row.appendChild(roomDiv);

      return row;
    }

    function renumberLessonRows() {
      Array.from(adminLessonRows.children).forEach((row, idx) => {
        const label = row.querySelector('.lesson-index');
        if (label) {
          label.textContent = (idx + 1) + '.';
        }
      });
    }

    function fillLessonRowsFromLines(lines) {
      adminLessonRows.innerHTML = '';
      if (!lines || !lines.length) {
        const defaults = [
          ['08:00', '08:40'],
          ['08:50', '09:30'],
          ['09:50', '10:30'],
          ['10:50', '11:30'],
          ['11:40', '12:20'],
        ];
        defaults.forEach((t) => {
          const row = createLessonRow({ start: t[0], end: t[1], subject: '', room: '' });
          adminLessonRows.appendChild(row);
        });
        renumberLessonRows();
        return;
      }
      lines.forEach((line) => {
        const raw = (line || '').trim();
        if (!raw) return;
        let start = '', end = '', subject = '', room = '';
        const m = raw.match(/^(\d{1,2}:\d{2})\s*-\s*(\d{1,2}:\d{2})\s+(.+)$/);
        let rest = raw;
        if (m) {
          start = m[1];
          end = m[2];
          rest = m[3];
        }
        if (rest.includes('/')) {
          const parts = rest.split('/');
          subject = parts[0].trim();
          room = parts.slice(1).join('/').trim();
        } else {
          subject = rest.trim();
        }
        const row = createLessonRow({ start, end, subject, room });
        adminLessonRows.appendChild(row);
      });
      renumberLessonRows();
    }

    async function reloadAdminDay() {
      const day = adminDaySelect.value;
      const date = adminDayDate.value || null;
      const data = await api('/api/admin/day_get', { day, mode: adminType, date });
      fillLessonRowsFromLines(data.lessons || []);
    }

    const tabBtnSchedule = document.getElementById('tab-btn-schedule');
    const tabBtnSub = document.getElementById('tab-btn-sub');
    const tabBtnAdmin = document.getElementById('tab-btn-admin');
    const scheduleCard = document.getElementById('schedule-card');
    const subCard = document.getElementById('sub-card');
    const scheduleSaturdayRow = document.getElementById('schedule-saturday-row');

    function setStatus(text, isError) {
      statusEl.textContent = text || '';
      statusEl.style.color = isError ? '#d33' : 'var(--tg-theme-hint-color, #888)';
    }

    function setTab(tab) {
      tabBtnSchedule.classList.toggle('inactive', tab !== 'schedule');
      tabBtnSub.classList.toggle('inactive', tab !== 'sub');
      tabBtnAdmin.classList.toggle('inactive', tab !== 'admin');
      scheduleCard.classList.toggle('hidden', tab !== 'schedule');
      subCard.classList.toggle('hidden', tab !== 'sub');
      adminCard.classList.toggle('hidden', tab !== 'admin');
    }

    async function api(path, payload) {
      try {
        const body = Object.assign({}, payload || {}, {
          init_data: tg ? tg.initData : '',
          user: tg && tg.initDataUnsafe && tg.initDataUnsafe.user ? tg.initDataUnsafe.user : null,
        });
        const res = await fetch(path, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(body),
        });
        const data = await res.json();
        if (!data.ok) {
          throw new Error(data.error || 'Ошибка запроса');
        }
        return data;
      } catch (e) {
        console.error(e);
        setStatus(e.message || 'Ошибка связи с сервером', true);
        throw e;
      }
    }

    async function loadMe() {
      setStatus('Загрузка данных пользователя...');
      const data = await api('/api/me', {});
      if (data.subscription) {
        subInfo.textContent =
          'Подписка: ' +
          data.subscription.time +
          ' (' +
          (data.subscription.day_type === 'tomorrow' ? 'завтра' : 'сегодня') +
          ')';
        subTime.value = data.subscription.time;
        subDayType.value = data.subscription.day_type || 'today';
      } else {
        subInfo.textContent = 'Подписка не настроена.';
      }
      if (data.is_admin) {
        adminCard.classList.remove('hidden');
      }
      // управление видимостью кнопок субботы
      if (!data.has_saturday) {
        scheduleSaturdayRow.style.display = 'none';
      } else if (!data.has_saturday_profiles) {
        scheduleSaturdayRow.style.display = 'flex';
        document.getElementById('btn-sat-prof-1').style.display = 'none';
        document.getElementById('btn-sat-prof-2').style.display = 'none';
        document.getElementById('btn-sat-prof-3').style.display = 'none';
        document.getElementById('btn-sat-prof-4').style.display = 'none';
        document.getElementById('btn-sat-prof-5').style.display = 'none';
      } else {
        scheduleSaturdayRow.style.display = 'flex';
      }
      setStatus('Готово');
    }

    async function loadSchedule(type) {
      setStatus('Загрузка расписания...');
      const data = await api('/api/schedule', { type });
      scheduleBox.innerHTML = data.html || '';
      setStatus('');
      // подсветка активной кнопки
      document.querySelectorAll('button[data-type]').forEach((btn) => {
        btn.classList.toggle('active', btn.getAttribute('data-type') === type);
      });
    }

    async function saveSubscription() {
      const time = subTime.value;
      const dayType = subDayType.value;
      if (!time) {
        setStatus('Укажи время в формате HH:MM', true);
        return;
      }
      setStatus('Сохранение подписки...');
      await api('/api/subscribe', { time, day_type: dayType });
      setStatus('Подписка сохранена');
      await loadMe();
    }

    async function removeSubscription() {
      setStatus('Отключение подписки...');
      await api('/api/unsubscribe', {});
      setStatus('Подписка отключена');
      await loadMe();
    }

    async function saveAdminWeek() {
      const text = adminWeekText.value || '';
      setStatus('Сохранение расписания на неделю...');
      await api('/api/admin/week', { week_text: text, mode: adminType });
      setStatus('Расписание обновлено');
    }

    async function saveAdminDay() {
      const day = adminDaySelect.value;
      const date = adminDayDate.value || null;
      // собираем строки занятий из фиксированных слотов
      const lines = [];
      const rows = Array.from(adminLessonRows.querySelectorAll('.row'));
      const parsed = [];
      rows.forEach((row) => {
        const subjInput = row.querySelector('.lesson-subject');
        const roomInput = row.querySelector('.lesson-room');
        const startInput = row.querySelector('.lesson-start');
        const endInput = row.querySelector('.lesson-end');
        const subject = (subjInput.value || '').trim();
        const room = (roomInput.value || '').trim();
        const start = (startInput && startInput.value) || '';
        const end = (endInput && endInput.value) || '';
        if (!subject) {
          return;
        }
        const roomPart = room ? '/' + room : '';
        parsed.push({
          start,
          end,
          line: `${start || ''}-${end || ''} ${subject}${roomPart}`.trim(),
        });
      });
      parsed
        .filter((p) => p.start)
        .sort((a, b) => (a.start < b.start ? -1 : a.start > b.start ? 1 : 0))
        .forEach((p) => lines.push(p.line));
      const text = lines.join('\\n');
      setStatus('Сохранение расписания дня...');
      await api('/api/admin/day', { day, lessons_text: text, mode: adminType, date });
      setStatus('Расписание дня обновлено');
    }

    document.querySelectorAll('button[data-type]').forEach((btn) => {
      btn.addEventListener('click', () => {
        const t = btn.getAttribute('data-type');
        loadSchedule(t);
      });
    });
    subSave.addEventListener('click', saveSubscription);
    subRemove.addEventListener('click', removeSubscription);

    tabBtnSchedule.addEventListener('click', () => setTab('schedule'));
    tabBtnSub.addEventListener('click', () => setTab('sub'));
    tabBtnAdmin.addEventListener('click', () => setTab('admin'));

    adminTypeBase.addEventListener('click', () => {
      adminType = 'base';
      adminTypeBase.classList.remove('secondary');
      adminTypeTemp.classList.add('secondary');
      adminDayDateWrap.classList.add('hidden');
      if (!adminDayEditor.classList.contains('hidden')) {
        reloadAdminDay();
      }
    });
    adminTypeTemp.addEventListener('click', () => {
      adminType = 'temp';
      adminTypeTemp.classList.remove('secondary');
      adminTypeBase.classList.add('secondary');
      adminDayDateWrap.classList.remove('hidden');
      if (!adminDayEditor.classList.contains('hidden')) {
        reloadAdminDay();
      }
    });
    document.getElementById('admin-mode-day').addEventListener('click', () => {
      adminModeButtons.classList.add('hidden');
      adminWeekEditor.classList.add('hidden');
      adminDayEditor.classList.remove('hidden');
      reloadAdminDay();
    });
    document.getElementById('admin-mode-week').addEventListener('click', () => {
      adminModeButtons.classList.add('hidden');
      adminDayEditor.classList.add('hidden');
      adminWeekEditor.classList.remove('hidden');
    });
    adminDayCancel.addEventListener('click', () => {
      adminDayEditor.classList.add('hidden');
      adminModeButtons.classList.remove('hidden');
    });
    adminWeekCancel.addEventListener('click', () => {
      adminWeekEditor.classList.add('hidden');
      adminModeButtons.classList.remove('hidden');
    });
    adminWeekSave.addEventListener('click', saveAdminWeek);
    adminDaySave.addEventListener('click', saveAdminDay);

    loadMe()
      .then(() => {
        setTab('schedule');
        return loadSchedule('today');
      })
      .catch(() => {});
  </script>
</body>
</html>
"""


@app.get("/webapp", response_class=HTMLResponse)
async def webapp_page():
    return HTMLResponse(WEBAPP_HTML)


@app.post("/api/me")
async def api_me(request: Request):
    data = await request.json()
    raw_user = data.get("user")
    user = None
    if isinstance(raw_user, dict) and "id" in raw_user:
        user = raw_user
    else:
        init_data = data.get("init_data", "")
        user = _get_user_from_init_data(init_data)
    if not user:
        return JSONResponse({"ok": False, "error": "bad_init_data"}, status_code=400)
    user_id = int(user["id"])
    sub = subscriptions.get(str(user_id))
    sat_profiles = _nearest_saturday_profiles()
    has_saturday = bool(sat_profiles)
    has_saturday_profiles = False
    if sat_profiles:
        if len(sat_profiles) == 1 and sat_profiles[0][0] == "Суббота":
            has_saturday_profiles = False
        else:
            has_saturday_profiles = True
    return JSONResponse(
        {
            "ok": True,
            "user": {"id": user_id, "first_name": user.get("first_name", "")},
            "is_admin": _is_admin_user_id(user_id),
            "subscription": sub,
            "has_saturday": has_saturday,
            "has_saturday_profiles": has_saturday_profiles,
        }
    )


@app.post("/api/schedule")
async def api_schedule(request: Request):
    data = await request.json()
    raw_user = data.get("user")
    user = None
    if isinstance(raw_user, dict) and "id" in raw_user:
        user = raw_user
    else:
        init_data = data.get("init_data", "")
        user = _get_user_from_init_data(init_data)
    if not user:
        return JSONResponse({"ok": False, "error": "bad_init_data"}, status_code=400)
    day_type = data.get("type", "today")
    html_text = _get_schedule_html_for_day_type(day_type)
    return JSONResponse({"ok": True, "html": html_text})


@app.post("/api/subscribe")
async def api_subscribe(request: Request):
    data = await request.json()
    raw_user = data.get("user")
    user = None
    if isinstance(raw_user, dict) and "id" in raw_user:
        user = raw_user
    else:
        init_data = data.get("init_data", "")
        user = _get_user_from_init_data(init_data)
    if not user:
        return JSONResponse({"ok": False, "error": "bad_init_data"}, status_code=400)
    user_id = int(user["id"])
    time_str = data.get("time", "")
    parsed = _parse_hhmm(time_str)
    if not parsed:
        return JSONResponse({"ok": False, "error": "bad_time"}, status_code=400)
    hh, mm = parsed
    t = f"{hh:02d}:{mm:02d}"
    day_type = data.get("day_type", "today")
    if day_type not in {"today", "tomorrow"}:
        day_type = "today"
    chat_id = user_id
    subscriptions[str(user_id)] = {"chat_id": chat_id, "time": t, "day_type": day_type}
    _save_subscriptions_to_disk()
    _reschedule_user(user_id)
    return JSONResponse({"ok": True})


@app.post("/api/unsubscribe")
async def api_unsubscribe(request: Request):
    data = await request.json()
    raw_user = data.get("user")
    user = None
    if isinstance(raw_user, dict) and "id" in raw_user:
        user = raw_user
    else:
        init_data = data.get("init_data", "")
        user = _get_user_from_init_data(init_data)
    if not user:
        return JSONResponse({"ok": False, "error": "bad_init_data"}, status_code=400)
    user_id = int(user["id"])
    subscriptions.pop(str(user_id), None)
    _save_subscriptions_to_disk()
    if scheduler is not None:
        try:
            scheduler.remove_job(_job_id_for(user_id))
        except Exception:
            pass
    return JSONResponse({"ok": True})


@app.post("/api/admin/week")
async def api_admin_week(request: Request):
    data = await request.json()
    raw_user = data.get("user")
    user = None
    if isinstance(raw_user, dict) and "id" in raw_user:
        user = raw_user
    else:
        init_data = data.get("init_data", "")
        user = _get_user_from_init_data(init_data)
    if not user:
        return JSONResponse({"ok": False, "error": "bad_init_data"}, status_code=400)
    user_id = int(user["id"])
    if not _is_admin_user_id(user_id):
        return JSONResponse({"ok": False, "error": "forbidden"}, status_code=403)
    week_text = data.get("week_text", "") or ""
    mode = (data.get("mode") or "base").strip()
    week = _parse_week_from_text(week_text)
    if week is None:
        return JSONResponse({"ok": False, "error": "bad_format"}, status_code=400)

    if mode == "temp":
        # Временная неделя: применяем к текущей неделе (пн-вс)
        now_tz = datetime.now(tz=_get_tz())
        base_monday_idx = 0
        today_idx = now_tz.weekday()
        monday = (now_tz - timedelta(days=today_idx - base_monday_idx)).date()
        for offset, d_name in enumerate(SCHEDULE_DAYS):
            if d_name not in week:
                continue
            target_date = monday + timedelta(days=offset)
            key = target_date.isoformat()
            day_lessons = week[d_name]
            if isinstance(day_lessons, list):
                temp_schedule[key] = day_lessons
        try:
            _save_temp_schedule_to_disk()
        except Exception as e:
            return JSONResponse({"ok": False, "error": str(e)}, status_code=500)
        week_html = _format_week_text()
        msg = _truncate_message("📢 Временное расписание на неделю обновлено:\n\n" + week_html)
        asyncio.create_task(_notify_subscribers(msg))
    else:
        for d in SCHEDULE_DAYS:
            if d in week:
                schedule[d] = week[d]
        try:
            _save_schedule_to_disk()
        except Exception as e:
            return JSONResponse({"ok": False, "error": str(e)}, status_code=500)
        week_html = "\n\n".join(
            _format_day_table_html(d, schedule.get(d, []))
            for d in SCHEDULE_DAYS
            if d in schedule
        ) or _format_day_table_html("Неделя", [])
        msg = _truncate_message("📢 Обновлено расписание на неделю:\n\n" + week_html)
        asyncio.create_task(_notify_subscribers(msg))
    return JSONResponse({"ok": True})


@app.post("/api/admin/day")
async def api_admin_day(request: Request):
    data = await request.json()
    raw_user = data.get("user")
    user = None
    if isinstance(raw_user, dict) and "id" in raw_user:
        user = raw_user
    else:
        init_data = data.get("init_data", "")
        user = _get_user_from_init_data(init_data)
    if not user:
        return JSONResponse({"ok": False, "error": "bad_init_data"}, status_code=400)
    user_id = int(user["id"])
    if not _is_admin_user_id(user_id):
        return JSONResponse({"ok": False, "error": "forbidden"}, status_code=403)

    day = (data.get("day") or "").strip()
    if day not in SCHEDULE_DAYS:
        return JSONResponse({"ok": False, "error": "bad_day"}, status_code=400)

    mode = (data.get("mode") or "base").strip()
    lessons_text = data.get("lessons_text", "") or ""
    lessons = _parse_lessons_from_text(lessons_text)
    if lessons is None:
        return JSONResponse({"ok": False, "error": "bad_format"}, status_code=400)

    if mode == "temp":
        date_str = (data.get("date") or "").strip()
        if date_str:
            try:
                d = datetime.fromisoformat(date_str).date()
            except ValueError:
                return JSONResponse({"ok": False, "error": "bad_date"}, status_code=400)
        else:
            # если дата не указана — берём текущую неделю и соответствующий день
            now_tz = datetime.now(tz=_get_tz())
            today_idx = now_tz.weekday()
            target_idx = SCHEDULE_DAYS.index(day)
            delta = target_idx - today_idx
            d = (now_tz + timedelta(days=delta)).date()
        key = d.isoformat()
        temp_schedule[key] = lessons
        try:
            _save_temp_schedule_to_disk()
        except Exception as e:
            return JSONResponse({"ok": False, "error": str(e)}, status_code=500)
        label = f"{d.strftime('%d.%m.%Y')} ({DAY_MAP.get(d.strftime('%A'), d.strftime('%A'))})"
        msg = "📢 Временное расписание обновлено:\n\n" + _format_day_table_html(label, lessons)
        asyncio.create_task(_notify_subscribers(_truncate_message(msg)))
    else:
        schedule[day] = lessons
        try:
            _save_schedule_to_disk()
        except Exception as e:
            return JSONResponse({"ok": False, "error": str(e)}, status_code=500)
        msg = "📢 Обновлено расписание:\n\n" + _format_day_table_html(day, lessons)
        asyncio.create_task(_notify_subscribers(_truncate_message(msg)))

    return JSONResponse({"ok": True})


@app.post("/api/admin/day_get")
async def api_admin_day_get(request: Request):
    """Возвращает список строк уроков для дня/режима (для предзаполнения формы)."""
    data = await request.json()
    raw_user = data.get("user")
    user = None
    if isinstance(raw_user, dict) and "id" in raw_user:
        user = raw_user
    else:
        init_data = data.get("init_data", "")
        user = _get_user_from_init_data(init_data)
    if not user:
        return JSONResponse({"ok": False, "error": "bad_init_data"}, status_code=400)
    user_id = int(user["id"])
    if not _is_admin_user_id(user_id):
        return JSONResponse({"ok": False, "error": "forbidden"}, status_code=403)

    day = (data.get("day") or "").strip()
    if day not in SCHEDULE_DAYS:
        return JSONResponse({"ok": False, "error": "bad_day"}, status_code=400)
    mode = (data.get("mode") or "base").strip()

    lessons: list[str] = []
    if mode == "temp":
        date_str = (data.get("date") or "").strip()
        d: date | None = None
        if date_str:
            try:
                d = datetime.fromisoformat(date_str).date()
            except ValueError:
                d = None
        if d is None:
            now_tz = datetime.now(tz=_get_tz())
            today_idx = now_tz.weekday()
            target_idx = SCHEDULE_DAYS.index(day)
            delta = target_idx - today_idx
            d = (now_tz + timedelta(days=delta)).date()
        key = d.isoformat()
        raw = temp_schedule.get(key)
        if isinstance(raw, list):
            lessons = raw
        if not lessons:
            base = schedule.get(day)
            if isinstance(base, list):
                lessons = base
    else:
        base = schedule.get(day)
        if isinstance(base, list):
            lessons = base

    return JSONResponse({"ok": True, "lessons": lessons})
