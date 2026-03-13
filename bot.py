import os, json, uuid, asyncio, httpx, html, re, logging, hmac, hashlib
try:
    import gspread
    from google.oauth2.service_account import Credentials as GCredentials
    _GSPREAD_OK = True
except ImportError:
    _GSPREAD_OK = False
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
    ReplyKeyboardRemove,
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
# ================== Google Sheets ==================
GOOGLE_SHEET_ID   = (os.environ.get("GOOGLE_SHEET_ID") or "").strip()
_GCREDS_JSON_RAW  = (os.environ.get("GOOGLE_CREDENTIALS_JSON") or "").strip()

_gs_client: "gspread.Client | None" = None
_gs_spreadsheet = None   # gspread.Spreadsheet

_GS_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
]

def _gs_connect() -> bool:
    """Открывает соединение с Google Sheets. Возвращает True при успехе."""
    global _gs_client, _gs_spreadsheet
    if not _GSPREAD_OK:
        logger.warning("gspread не установлен — работаем без Google Sheets")
        return False
    if not GOOGLE_SHEET_ID or not _GCREDS_JSON_RAW:
        logger.warning("GOOGLE_SHEET_ID или GOOGLE_CREDENTIALS_JSON не заданы — работаем без Google Sheets")
        return False
    try:
        creds_dict = json.loads(_GCREDS_JSON_RAW)
        creds = GCredentials.from_service_account_info(creds_dict, scopes=_GS_SCOPES)
        _gs_client = gspread.authorize(creds)
        _gs_spreadsheet = _gs_client.open_by_key(GOOGLE_SHEET_ID)
        logger.info("✅ Google Sheets подключён")
        return True
    except Exception as e:
        logger.error(f"Google Sheets connect error: {e}")
        return False

def _gs_sheet(name: str):
    """Возвращает лист по имени, создаёт если нет."""
    global _gs_spreadsheet
    try:
        return _gs_spreadsheet.worksheet(name)
    except gspread.WorksheetNotFound:
        return _gs_spreadsheet.add_worksheet(title=name, rows=500, cols=10)
    except Exception as e:
        logger.error(f"_gs_sheet({name}) error: {e}")
        return None

# ── Загрузка ──────────────────────────────────────────────────────────────

def _gs_load_schedule() -> dict | None:
    """Загружает основное расписание из листа schedule."""
    try:
        ws = _gs_sheet("schedule")
        if ws is None:
            return None
        rows = ws.get_all_values()
        if not rows:
            return None
        result = {}
        for row in rows:
            if len(row) < 2 or not row[0].strip():
                continue
            day, raw = row[0].strip(), row[1].strip()
            # Пропускаем заголовок и строки не являющиеся днями недели
            if day not in SCHEDULE_DAYS:
                continue
            if not raw:
                continue
            try:
                result[day] = json.loads(raw)
            except json.JSONDecodeError:
                # Пробуем исправить одинарные кавычки (питоновский repr)
                try:
                    import ast
                    result[day] = ast.literal_eval(raw)
                except Exception:
                    logger.warning(f"_gs_load_schedule: не удалось распарсить '{day}'")
        return result if result else None
    except Exception as e:
        logger.error(f"_gs_load_schedule error: {e}")
        return None

def _gs_load_temp_schedule() -> dict | None:
    """Загружает временное расписание из листа temp_schedule."""
    try:
        ws = _gs_sheet("temp_schedule")
        if ws is None:
            return None
        rows = ws.get_all_values()
        if not rows:
            return {}
        result = {}
        for row in rows:
            if len(row) < 2 or not row[0].strip():
                continue
            date_key, raw = row[0].strip(), row[1].strip()
            try:
                result[date_key] = json.loads(raw)
            except Exception:
                pass
        return result
    except Exception as e:
        logger.error(f"_gs_load_temp_schedule error: {e}")
        return None

def _gs_load_subscriptions() -> dict | None:
    """Загружает подписки из листа subscriptions.
    Формат строки: chat_id | time | day_type | notify_daily | notify_changes
    Старые строки без последних двух колонок читаются как notify_daily=True, notify_changes=False.
    """
    try:
        ws = _gs_sheet("subscriptions")
        if ws is None:
            return None
        rows = ws.get_all_values()
        if not rows:
            return {}
        result = {}
        for row in rows:
            if not row or not row[0].strip():
                continue
            chat_id_str = row[0].strip()
            try:
                chat_id = int(chat_id_str)
            except ValueError:
                continue
            time_str       = row[1].strip() if len(row) > 1 else ""
            day_type       = row[2].strip() or "today" if len(row) > 2 else "today"
            notify_daily   = (row[3].strip().lower() not in ("false", "0", "нет")) if len(row) > 3 else True
            notify_changes = (row[4].strip().lower() in ("true", "1", "да"))        if len(row) > 4 else False
            result[chat_id_str] = {
                "chat_id":        chat_id,
                "time":           time_str,
                "day_type":       day_type,
                "notify_daily":   notify_daily,
                "notify_changes": notify_changes,
            }
        return result
    except Exception as e:
        logger.error(f"_gs_load_subscriptions error: {e}")
        return None

# ── Сохранение ────────────────────────────────────────────────────────────

def _gs_save_schedule() -> None:
    """Сохраняет основное расписание в лист schedule."""
    try:
        ws = _gs_sheet("schedule")
        if ws is None:
            return
        rows = [[day, json.dumps(data, ensure_ascii=False)]
                for day, data in schedule.items()]
        ws.clear()
        if rows:
            ws.update(rows, value_input_option="RAW")
    except Exception as e:
        logger.error(f"_gs_save_schedule error: {e}")

def _gs_save_temp_schedule() -> None:
    """Сохраняет временное расписание в лист temp_schedule."""
    try:
        ws = _gs_sheet("temp_schedule")
        if ws is None:
            return
        rows = [[date_key, json.dumps(data, ensure_ascii=False)]
                for date_key, data in temp_schedule.items()]
        ws.clear()
        if rows:
            ws.update(rows, value_input_option="RAW")
    except Exception as e:
        logger.error(f"_gs_save_temp_schedule error: {e}")

def _gs_save_subscriptions() -> None:
    """Сохраняет подписки в лист subscriptions.
    Формат: chat_id | time | day_type | notify_daily | notify_changes
    """
    try:
        ws = _gs_sheet("subscriptions")
        if ws is None:
            return
        rows = [
            [
                str(entry["chat_id"]),
                entry.get("time", ""),
                entry.get("day_type", "today"),
                "true" if entry.get("notify_daily", True)   else "false",
                "true" if entry.get("notify_changes", False) else "false",
            ]
            for entry in subscriptions.values()
        ]
        ws.clear()
        if rows:
            ws.update(rows, value_input_option="RAW")
    except Exception as e:
        logger.error(f"_gs_save_subscriptions error: {e}")



# Необязательно: ограничение доступа к редактированию расписания
# Формат: "12345,67890"
_ADMIN_USER_IDS_RAW = (os.environ.get("ADMIN_USER_IDS") or "").strip()
ADMIN_USER_IDS = {
    int(x.strip())
    for x in _ADMIN_USER_IDS_RAW.split(",")
    if x.strip().isdigit()
}

# Динамические админы (добавляются через интерфейс, хранятся в файле)
ADMINS_PATH = "admins.json"
dynamic_admins: set[int] = set()


def _load_dynamic_admins() -> None:
    global dynamic_admins
    try:
        with open(ADMINS_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            dynamic_admins = {int(x) for x in data if str(x).lstrip("-").isdigit()}
        else:
            dynamic_admins = set()
    except FileNotFoundError:
        dynamic_admins = set()
    except Exception:
        dynamic_admins = set()


def _save_dynamic_admins() -> None:
    tmp = f"{ADMINS_PATH}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(sorted(dynamic_admins), f, ensure_ascii=False)
        f.write("\n")
    os.replace(tmp, ADMINS_PATH)

# ================== Загрузка расписания ==================
try:
    with open("schedule.json", "r", encoding="utf-8") as f:
        schedule = json.load(f)
except FileNotFoundError:
    logger.warning("schedule.json не найден — будет загружен из Google Sheets при старте")
    schedule = {}
except Exception as e:
    logger.error(f"Ошибка чтения schedule.json: {e}")
    schedule = {}

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
    if _gs_spreadsheet is not None:
        _gs_save_temp_schedule()

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
    if _gs_spreadsheet is not None:
        _gs_save_subscriptions()

async def _notify_subscribers(text: str, parse_mode: str = "HTML",
                              notify_type: str = "changes") -> None:
    """Отправляет сообщение подписчикам.
    notify_type='changes' — только тем у кого включены уведомления об изменениях.
    notify_type='daily'   — только тем у кого включены ежедневные напоминания (используется планировщиком).
    notify_type='all'     — всем у кого есть хоть какая-то подписка.
    """
    if not subscriptions:
        return
    chat_ids = set()
    for entry in subscriptions.values():
        cid = entry.get("chat_id")
        if cid is None:
            continue
        if notify_type == "all":
            chat_ids.add(int(cid))
        elif notify_type == "changes" and entry.get("notify_changes", True):
            chat_ids.add(int(cid))
        elif notify_type == "daily" and entry.get("notify_daily", True):
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

def _is_superadmin_user_id(user_id: int) -> bool:
    """Суперадмин — только из переменной окружения ADMIN_USER_IDS."""
    return user_id in ADMIN_USER_IDS


def _is_admin(update: Update) -> bool:
    if not ADMIN_USER_IDS and not dynamic_admins:
        return True
    user = update.effective_user
    return bool(user and _is_admin_user_id(user.id))


def _is_admin_user_id(user_id: int) -> bool:
    """Проверка администратора: env-список ИЛИ динамический список."""
    if not ADMIN_USER_IDS and not dynamic_admins:
        return True
    return user_id in ADMIN_USER_IDS or user_id in dynamic_admins


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
    if _gs_spreadsheet is not None:
        _gs_save_schedule()

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

    if day_ru == "Воскресенье":
        return  # В воскресенье уроков нет — не отправляем

    if day_ru == "Суббота":
        profiles = _get_saturday_profiles_for_date(target_date)
        # Если нет ни одного профиля с уроками — не отправляем
        has_lessons = any(lessons for _, lessons in profiles)
        if not profiles or not has_lessons:
            return
        parts = [_format_day_table_html(f"Суббота — {label}", lessons) for label, lessons in profiles]
        text = _truncate_message(f"📅 Расписание на {date_label} (суббота):\n\n" + "\n\n".join(parts))
    else:
        day, lessons = _get_lessons_for_date(target_date)
        if not lessons:
            return  # Пустой день — не отправляем
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


def _format_schedule_webapp_html(day_label: str, lessons: list[str]) -> str:
    """Красивые HTML-карточки расписания для WebApp (вкладка Расписание)."""
    rows_html = []
    for idx, line in enumerate(lessons or [], start=1):
        p = _parse_lesson_line(line)
        subject = html.escape(p["subject"] or "—")
        time_str = ""
        if p["start"] and p["end"]:
            time_str = f'{html.escape(p["start"])} – {html.escape(p["end"])}'
        elif p["start"]:
            time_str = html.escape(p["start"])
        room_html = (
            f'<span class="sc-room">{html.escape(p["room"])}</span>'
            if p["room"] else ""
        )
        rows_html.append(
            f'<div class="sc-lesson">'
            f'<div class="sc-num">{idx}</div>'
            f'<div class="sc-body">'
            f'<div class="sc-subject">{subject}</div>'
            f'<div class="sc-meta"><span class="sc-time">{time_str}</span>{room_html}</div>'
            f'</div></div>'
        )
    inner = "\n".join(rows_html) if rows_html else '<div class="sc-empty">Нет занятий</div>'
    return (
        f'<div class="sc-day-block">'
        f'<div class="sc-day-title">{html.escape(day_label)}</div>'
        f'{inner}'
        f'</div>'
    )


def _format_week_webapp_html(blocks_fn) -> str:
    """Собирает HTML для недели из функции, возвращающей список (label, lessons)."""
    parts = blocks_fn()
    return "\n".join(
        _format_schedule_webapp_html(label, lessons) for label, lessons in parts
    ) if parts else _format_schedule_webapp_html("Нет занятий", [])


def _get_schedule_html_for_day_type(day_type: str = "today") -> str:
    """HTML‑текст расписания для различных режимов (для WebApp API)."""
    now = datetime.now(tz=_get_tz())

    if day_type == "week":
        def _week_blocks():
            result = []
            now_tz = datetime.now(tz=_get_tz())
            for day in SCHEDULE_DAYS:
                day_idx = SCHEDULE_DAYS.index(day)
                today_idx = now_tz.weekday()
                target_date = (now_tz + timedelta(days=day_idx - today_idx)).date()
                if day == "Суббота":
                    for label, lessons in _get_saturday_profiles_for_date(target_date):
                        if lessons:
                            result.append((f"Суббота — {label}", lessons))
                    continue
                date_key = target_date.isoformat()
                raw = temp_schedule.get(date_key)
                data = raw if isinstance(raw, list) else schedule.get(day, [])
                if isinstance(data, list) and data:
                    result.append((day, data))
            return result
        parts = _week_blocks()
        return "\n".join(_format_schedule_webapp_html(l, ls) for l, ls in parts) if parts else _format_schedule_webapp_html("Нет занятий", [])

    if day_type == "week_base":
        def _week_base_blocks():
            result = []
            for day in SCHEDULE_DAYS:
                if day == "Суббота":
                    sat = schedule.get("Суббота")
                    if isinstance(sat, dict):
                        for pk in SATURDAY_PROFILE_KEYS:
                            if pk in sat and sat[pk]:
                                result.append((f"Суббота — {SATURDAY_PROFILE_LABELS.get(pk, pk)}", sat[pk]))
                    elif isinstance(sat, list) and sat:
                        result.append(("Суббота", sat))
                    continue
                data = schedule.get(day, [])
                if isinstance(data, list) and data:
                    result.append((day, data))
            return result
        parts = _week_base_blocks()
        return "\n".join(_format_schedule_webapp_html(l, ls) for l, ls in parts) if parts else _format_schedule_webapp_html("Нет занятий", [])

    if day_type.startswith("sat_profile:"):
        profile_key = day_type.split("sat_profile:", 1)[1]
        now_tz = datetime.now(tz=_get_tz())
        sat_date = (now_tz + timedelta(days=5 - now_tz.weekday())).date()
        profiles = _get_saturday_profiles_for_date(sat_date)
        for label, lessons in profiles:
            if profile_key == SATURDAY_LABEL_TO_KEY.get(label, label) or profile_key == label:
                return _format_schedule_webapp_html(f"Суббота — {label}", lessons)
        return _format_schedule_webapp_html("Нет занятий для выбранного профиля", [])

    if day_type == "saturday":
        profiles = _nearest_saturday_profiles()
        if not profiles:
            return _format_schedule_webapp_html("Суббота", [])
        if len(profiles) == 1 and profiles[0][0] == "Суббота":
            return _format_schedule_webapp_html("Суббота", profiles[0][1])
        return "\n".join(_format_schedule_webapp_html(f"Суббота — {label}", lessons) for label, lessons in profiles)

    target_date = now.date() if day_type == "today" else (now + timedelta(days=1)).date()
    day_eng = target_date.strftime("%A")
    day_ru = DAY_MAP.get(day_eng, day_eng)
    date_label = "сегодня" if day_type == "today" else "завтра"

    if day_ru == "Суббота":
        profiles = _get_saturday_profiles_for_date(target_date)
        if profiles:
            return "\n".join(_format_schedule_webapp_html(f"Суббота — {label}", lessons) for label, lessons in profiles)
        return _format_schedule_webapp_html("Суббота", [])

    day, lessons = _get_lessons_for_date(target_date)
    label = f"📅 {date_label.capitalize()} — {day}"
    return _format_schedule_webapp_html(label, lessons)

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
        "Для админов: /edit_schedule — редактировать расписание",
        reply_markup=ReplyKeyboardRemove(),
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

def _sub_keyboard(entry: dict | None) -> InlineKeyboardMarkup:
    """Клавиатура управления подписками с чекбоксами."""
    daily_on   = bool(entry and entry.get("notify_daily", False))
    changes_on = bool(entry and entry.get("notify_changes", False))
    time_str   = entry.get("time", "—") if entry else "—"
    day_type   = entry.get("day_type", "today") if entry else "today"
    day_label  = "завтра" if day_type == "tomorrow" else "сегодня"

    daily_icon   = "✅" if daily_on   else "☑️"
    changes_icon = "✅" if changes_on else "☑️"

    rows = [
        [InlineKeyboardButton(
            f"{daily_icon} Ежедневное расписание ({day_label} в {time_str})",
            callback_data="sub_toggle:daily"
        )],
        [InlineKeyboardButton(
            f"{changes_icon} Уведомления об изменениях расписания",
            callback_data="sub_toggle:changes"
        )],
    ]
    if daily_on:
        rows.append([
            InlineKeyboardButton("🕐 Изменить время", callback_data="sub_set_time"),
            InlineKeyboardButton(
                "📅 " + ("Завтра" if day_type == "today" else "Сегодня"),
                callback_data="sub_toggle:day_type"
            ),
        ])
    rows.append([InlineKeyboardButton("❌ Закрыть", callback_data="sub_close")])
    return InlineKeyboardMarkup(rows)


def _sub_text(entry: dict | None) -> str:
    daily_on   = bool(entry and entry.get("notify_daily", False))
    changes_on = bool(entry and entry.get("notify_changes", False))
    if not daily_on and not changes_on:
        return "🔕 Уведомления отключены. Нажми нужный пункт чтобы включить."
    parts = []
    if daily_on:
        t = entry.get("time", "—")
        dl = "завтра" if entry.get("day_type") == "tomorrow" else "сегодня"
        parts.append(f"📅 Ежедневно в {t} — расписание на {dl}")
    if changes_on:
        parts.append("🔔 Уведомления при изменении расписания")
    return "Твои подписки:\n" + "\n".join(parts)


async def subscribe(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    _log_user(update, "subscribe")
    user = update.effective_user
    if not user:
        return
    entry = subscriptions.get(str(user.id))
    await update.message.reply_text(
        _sub_text(entry),
        reply_markup=_sub_keyboard(entry),
    )


async def subscribe_manage_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка всех кнопок экрана подписок."""
    query = update.callback_query
    await query.answer()
    user = update.effective_user
    chat = update.effective_chat
    if not user or not chat:
        return

    data = query.data or ""
    uid = str(user.id)
    entry = dict(subscriptions.get(uid) or {})
    if not entry:
        entry = {"chat_id": chat.id, "notify_daily": False, "notify_changes": False,
                 "time": "07:00", "day_type": "today"}

    if data == "sub_close":
        await query.edit_message_text("Настройки подписок закрыты.")
        return

    if data == "sub_toggle:daily":
        entry["notify_daily"] = not entry.get("notify_daily", False)
        entry["chat_id"] = chat.id

    elif data == "sub_toggle:changes":
        entry["notify_changes"] = not entry.get("notify_changes", False)
        entry["chat_id"] = chat.id

    elif data == "sub_toggle:day_type":
        entry["day_type"] = "tomorrow" if entry.get("day_type", "today") == "today" else "today"

    elif data == "sub_set_time":
        # Показываем выбор времени
        rows: list[list[InlineKeyboardButton]] = []
        row: list[InlineKeyboardButton] = []
        for hour in range(6, 21):
            t_btn = f"{hour:02d}:00"
            row.append(InlineKeyboardButton(t_btn, callback_data=f"sub_time:{t_btn}"))
            if len(row) >= 4:
                rows.append(row)
                row = []
        if row:
            rows.append(row)
        rows.append([InlineKeyboardButton("↩️ Назад", callback_data="sub_back")])
        await query.edit_message_text(
            "Выбери время ежедневного напоминания:",
            reply_markup=InlineKeyboardMarkup(rows),
        )
        return

    elif data.startswith("sub_time:"):
        t = data[len("sub_time:"):]
        entry["time"] = t
        entry["chat_id"] = chat.id

    elif data == "sub_back":
        pass  # просто перерисуем экран

    # Если обе подписки выключены — удаляем запись
    if not entry.get("notify_daily") and not entry.get("notify_changes"):
        subscriptions.pop(uid, None)
        if scheduler is not None:
            try:
                scheduler.remove_job(_job_id_for(user.id))
            except Exception:
                pass
    else:
        subscriptions[uid] = entry
        if entry.get("notify_daily"):
            _reschedule_user(user.id)
        else:
            if scheduler is not None:
                try:
                    scheduler.remove_job(_job_id_for(user.id))
                except Exception:
                    pass

    _save_subscriptions_to_disk()
    entry = subscriptions.get(uid)
    await query.edit_message_text(
        _sub_text(entry),
        reply_markup=_sub_keyboard(entry),
    )


# subscribe_time_callback и subscribe_callback объединены в subscribe_manage_callback

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
    await update.message.reply_text(
        "Все подписки отключены.\n"
        "Чтобы настроить заново — /subscribe"
    )


async def chatid_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /chatid — отвечает ID текущего чата (удобно для добавления группы в подписку)."""
    if not update.message or not update.effective_chat:
        return
    chat = update.effective_chat
    chat_type = {"private": "личный", "group": "группа", "supergroup": "супергруппа", "channel": "канал"}.get(chat.type, chat.type)
    await update.message.reply_text(
        f"Chat ID этого чата:\n<code>{chat.id}</code>\n\nТип: {chat_type}",
        parse_mode="HTML",
    )


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
bot_app.add_handler(CommandHandler("chatid", chatid_command))
bot_app.add_handler(CallbackQueryHandler(subscribe_manage_callback, pattern=r"^sub_"))

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

# ================== Яндекс Алиса ==================
# Переменная окружения ALICE_SKILL_ID — опциональная проверка ID навыка.
# Если не задана — запросы принимаются без проверки (удобно при разработке).
ALICE_SKILL_ID = (os.environ.get("ALICE_SKILL_ID") or "").strip()

_ALICE_MAX_LEN = 950  # запас до лимита 1024


def _alice_truncate(text: str, max_len: int = _ALICE_MAX_LEN) -> str:
    if len(text) <= max_len:
        return text
    return text[:max_len].rstrip(" ;,.\n") + "…"


def _alice_format_screen(lessons: list[str]) -> str:
    """Экранный формат: №. ЧЧ:ММ–ЧЧ:ММ  Предмет  каб.ХХХ"""
    if not lessons:
        return "Занятий нет"
    lines = []
    for i, line in enumerate(lessons, start=1):
        p = _parse_lesson_line(line)
        subj = p["subject"] or line
        time_part = f"{p['start']}–{p['end']}" if p["start"] and p["end"] else p["start"] if p["start"] else ""
        room_part = f"  каб.{p['room']}" if p["room"] else ""
        time_str = f"  {time_part}" if time_part else ""
        lines.append(f"{i}.{time_str}  {subj}{room_part}")
    return "\n".join(lines)


# Словарь расшифровки сокращений для голосового произношения Алисы.
# Ключи — сокращения (в нижнем регистре), значения — полные названия.
# Пополняйте по мере необходимости.
_ALICE_SUBJECT_EXPAND: dict[str, str] = {
    # РОВ
    "ров":                      "Разговоры о важном",
    # ВиСТ
    "вист":                     "Вероятность и статистика",
    "вист.":                    "Вероятность и статистика",
    # Русский язык
    "рус. яз":                  "Русский язык",
    "рус.яз.":                  "Русский язык",
    "рус. яз.":                 "Русский язык",
    "рус.яз":                   "Русский язык",
    # Английский язык
    "англ. яз.":                "Английский язык",
    "англ.яз.":                 "Английский язык",
    "англ. яз":                 "Английский язык",
    "англ.яз":                  "Английский язык",
    # Физкультура
    "физ-ра":                   "Физкультура",
    "физ. культура":            "Физкультура",
    # Окружающий мир
    "окр. мир":                 "Окружающий мир",
    "окр.мир":                  "Окружающий мир",
    # Изобразительное искусство
    "изо":                      "Изобразительное искусство",
    # Дополнительные занятия
    "доп занятия (1)":          "Дополнительные занятия",
    "доп занятия (2)":          "Дополнительные занятия",
    "доп. занятия":             "Дополнительные занятия",
    # Орлята России
    "орлята":                   "Орлята России",
    # Математика / алгебра / геометрия
    "матем.":                   "Математика",
    "алг.":                     "Алгебра",
    "геом.":                    "Геометрия",
    # Практикум по математике
    "практ. по мат-ке":         "Практикум по математике",
    "практ.по мат-ке":          "Практикум по математике",
    # Олимпийская математика
    "олимп. мат-ка":            "Олимпиадная математика",
    "олимп.мат-ка":             "Олимпиадная математика",
    # Углублённая математика
    "углубл. мате-ка":          "Углублённая математика",
    "углубл.мате-ка":           "Углублённая математика",
    # Алгоритмика
    "алгоритмика":              "Алгоритмика",
    # Экология растений
    "экология растений":        "Экология растений",
    # Введение в химию
    "введение в химию":         "Введение в химию",
    # Смысловое чтение
    "смысловое чтение":         "Смысловое чтение",
    # Прочие предметы
    "обж":                      "Основы безопасности жизнедеятельности",
    "инф.":                     "Информатика",
    "биол.":                    "Биология",
    "хим.":                     "Химия",
    "физ.":                     "Физика",
    "геогр.":                   "География",
    "лит.":                     "Литература",
    "ист.":                     "История",
    "обществ.":                 "Обществознание",
    "иностр. яз.":              "Иностранный язык",
}


def _alice_expand_subject(name: str) -> str:
    """Заменяет сокращение предмета на полное название для TTS."""
    return _ALICE_SUBJECT_EXPAND.get(name.lower().strip(), name)


def _alice_clean_tts(text: str) -> str:
    """Убирает символы, которые ломают интонацию и громкость TTS Алисы."""
    # Заменяем тире и дефисы между словами на паузу-запятую
    text = re.sub(r"\s*[–—]\s*", ", ", text)
    # Убираем скобки — Алиса их иногда читает как паузу
    text = re.sub(r"[(){}\[\]]", "", text)
    # Косая черта между кабинетами → «или»
    text = re.sub(r"(\d)/(\d)", r" или ", text)
    # Точки в конце сокращений убираем (мешают интонации)
    text = re.sub(r"([А-Яа-яA-Za-z])\.", r"", text)
    # Несколько пробелов → один
    text = re.sub(r" {2,}", " ", text)
    return text.strip()


def _alice_format_tts(lessons: list[str]) -> str:
    """Голосовой формат для TTS.
    Произносим: начало первого урока, список предметов, конец последнего.
    Кабинеты и время промежуточных уроков не произносим.
    """
    if not lessons:
        return "занятий нет"
    parsed = [_parse_lesson_line(line) for line in lessons]
    subjects = [_alice_clean_tts(_alice_expand_subject(p["subject"] or lessons[i]))
                for i, p in enumerate(parsed)]
    # Фильтруем пустышки (прочерки «-»)
    subjects = [s for s in subjects if s and s.strip("-– ")]
    if not subjects:
        return "занятий нет"
    first_start = next((p["start"] for p in parsed if p["start"]), "")
    last_end = next((p["end"] for p in reversed(parsed) if p["end"]), "")
    intro = f"Начало в {first_start}. " if first_start else ""
    outro = f". Конец в {last_end}." if last_end else "."
    return f"{intro}{', '.join(subjects)}{outro}"


def _alice_day_text(day_type: str = "today") -> tuple[str, str]:
    """Возвращает (display_text, tts_text) расписания на сегодня или завтра."""
    now = datetime.now(tz=_get_tz())
    if day_type == "tomorrow":
        target_date = (now + timedelta(days=1)).date()
        prefix = "Завтра"
    else:
        target_date = now.date()
        prefix = "Сегодня"

    day_eng = target_date.strftime("%A")
    day_ru = DAY_MAP.get(day_eng, day_eng)

    if day_ru == "Суббота":
        profiles = _get_saturday_profiles_for_date(target_date)
        if not profiles:
            msg = f"{prefix}, суббота\nЗанятий нет"
            return msg, f"{prefix} суббота. Занятий нет."
        text_parts, tts_parts = [], []
        for label, lessons in profiles:
            if lessons:
                text_parts.append(f"[ {label} ]\n{_alice_format_screen(lessons)}")
                tts_parts.append(f"Профиль {label}. {_alice_format_tts(lessons)}")
        if not text_parts:
            msg = f"{prefix}, суббота\nЗанятий нет"
            return msg, f"{prefix} суббота. Занятий нет."
        header = f"{prefix}, суббота"
        return header + "\n\n" + "\n\n".join(text_parts), f"{prefix} суббота. " + " ".join(tts_parts)

    _, lessons = _get_lessons_for_date(target_date)
    if not lessons:
        return f"{prefix}, {day_ru}\nЗанятий нет", f"{prefix} {day_ru.lower()}. Занятий нет."

    header = f"{prefix}, {day_ru}"
    display = header + "\n" + _alice_format_screen(lessons)
    tts = f"{prefix} {day_ru.lower()}. {_alice_format_tts(lessons)}"
    return display, tts


_ALICE_HELP_TEXT = (
    "Расскажу расписание уроков. "
    "Скажи «на сегодня» или «на завтра»."
)

_ALICE_MAIN_BUTTONS = [
    {"title": "На сегодня", "hide": True},
    {"title": "На завтра", "hide": True},
]


def _alice_resp(text: str, tts: str, session: dict, end_session: bool = False,
                buttons: list | None = None) -> dict:
    """Конструктор ответа Алисы."""
    return {
        "version": "1.0",
        "session": session,
        "response": {
            "text": text[:1024],
            "tts": tts[:1024],
            "end_session": end_session,
            "buttons": buttons or [],
        },
    }


def _alice_handle_request(req_body: dict) -> dict:
    """Основная логика обработки запроса от Алисы."""
    session = req_body.get("session", {})
    request = req_body.get("request", {})
    command = (request.get("command") or "").lower().strip()
    original = (request.get("original_utterance") or "").lower().strip()
    text_to_check = command or original
    is_new = session.get("new", False)

    # ── Расписание на сегодня ────────────────────────────────────────────────
    # Проверяем ДО приветствия: команда может прийти сразу при запуске навыка
    # («Алиса, попроси <навык> на сегодня»)
    if any(w in text_to_check for w in [
        "сегодня", "на сегодня", "расписание на сегодня",
        "today", "сейчас", "что сегодня", "какие сегодня", "какое сегодня",
    ]):
        display, tts = _alice_day_text("today")
        return _alice_resp(_alice_truncate(display, 1020), _alice_truncate(tts),
                           session, buttons=_ALICE_MAIN_BUTTONS)

    # ── Расписание на завтра ─────────────────────────────────────────────────
    if any(w in text_to_check for w in [
        "завтра", "на завтра", "расписание на завтра",
        "tomorrow", "что завтра", "какие завтра", "какое завтра",
    ]):
        display, tts = _alice_day_text("tomorrow")
        return _alice_resp(_alice_truncate(display, 1020), _alice_truncate(tts),
                           session, buttons=_ALICE_MAIN_BUTTONS)

    # ── Общий запрос расписания → сегодня ───────────────────────────────────
    if any(w in text_to_check for w in [
        "расписание", "уроки", "занятия", "какие уроки", "что за уроки",
    ]):
        display, tts = _alice_day_text("today")
        return _alice_resp(_alice_truncate(display, 1020), _alice_truncate(tts),
                           session, buttons=_ALICE_MAIN_BUTTONS)

    # ── Приветствие / помощь — только если команды нет или явный запрос ─────
    if is_new or not command or text_to_check in {"помощь", "help", "что ты умеешь"}:
        return _alice_resp(_ALICE_HELP_TEXT, _ALICE_HELP_TEXT, session,
                           buttons=_ALICE_MAIN_BUTTONS)

    # ── Выход ────────────────────────────────────────────────────────────────
    if any(w in text_to_check for w in ["стоп", "выход", "хватит", "пока", "выйти"]):
        msg = "До свидания! Удачи в учёбе!"
        return _alice_resp(msg, msg, session, end_session=True)

    # ── Не понял ─────────────────────────────────────────────────────────────
    answer = "Не поняла запрос. " + _ALICE_HELP_TEXT
    return _alice_resp(answer, answer, session, buttons=_ALICE_MAIN_BUTTONS)


@app.post("/alice")
async def alice_webhook(request: Request):
    """Эндпоинт для навыка Яндекс Алисы. URL: https://<домен>/alice"""
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "invalid json"}, status_code=400)

    if ALICE_SKILL_ID:
        incoming_skill_id = (body.get("session") or {}).get("skill_id", "")
        if incoming_skill_id != ALICE_SKILL_ID:
            logger.warning(f"Alice: неверный skill_id: {incoming_skill_id!r}")
            return JSONResponse({"error": "forbidden"}, status_code=403)

    try:
        response = _alice_handle_request(body)
    except Exception as e:
        logger.exception(f"Alice handler error: {e}")
        err_msg = "Произошла ошибка. Попробуйте позже."
        response = _alice_resp(err_msg, err_msg, body.get("session") or {})

    return JSONResponse(response)


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
            BotCommand("chatid", "Узнать ID текущего чата"),
            BotCommand("cancel", "Отменить редактирование"),
        ]
    )

    global scheduler, schedule

    # ── Google Sheets: подключение и загрузка ────────────────────────────
    if _gs_connect():
        gs_sched = _gs_load_schedule()
        if gs_sched:
            schedule = gs_sched
            logger.info("📊 Основное расписание загружено из Google Sheets")
            # Синхронизируем локальный файл
            try:
                tmp = "schedule.json.tmp"
                with open(tmp, "w", encoding="utf-8") as _f:
                    json.dump(schedule, _f, ensure_ascii=False, indent=4)
                os.replace(tmp, "schedule.json")
            except Exception:
                pass
        else:
            logger.info("📊 Google Sheets пуст — используем локальный schedule.json, загружаем в Sheets")
            _gs_save_schedule()

        gs_temp = _gs_load_temp_schedule()
        if gs_temp is not None:
            global temp_schedule
            temp_schedule = gs_temp
            logger.info("📊 Временное расписание загружено из Google Sheets")
        
        gs_subs = _gs_load_subscriptions()
        if gs_subs is not None:
            global subscriptions
            subscriptions = gs_subs
            logger.info("📊 Подписки загружены из Google Sheets")
    else:
        # Fallback: локальные файлы
        _load_temp_schedule_from_disk()
        _load_subscriptions_from_disk()

    scheduler = AsyncIOScheduler(timezone=_get_tz())
    if _gs_spreadsheet is None:
        _load_temp_schedule_from_disk()
        _load_subscriptions_from_disk()
    _load_dynamic_admins()
    for user_id_str in list(subscriptions.keys()):
        if user_id_str.isdigit():
            _reschedule_user(int(user_id_str))
    scheduler.start()

    await bot_app.start()
    # Сбрасываем Menu Button (кнопка под полем ввода) — используем /app вместо неё
    try:
        await bot_app.bot.delete_my_commands()
    except Exception:
        pass
    try:
        await bot_app.bot.set_chat_menu_button()  # сброс на дефолт (без WebApp-кнопки)
    except Exception:
        pass
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
      background: var(--tg-theme-bg-color, #ffffff);
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
      height: calc(100vh - 210px);
      overflow-y: auto;
      padding-bottom: 16px;
    }
    /* ── Красивое расписание ── */
    .sc-day-block { margin-bottom: 14px; }
    .sc-day-title {
      font-size: 15px;
      font-weight: 700;
      margin: 0 0 6px;
      padding: 6px 10px;
      border-radius: 10px;
      background: linear-gradient(135deg, #4e8cff22, #8f6bff22);
      border-left: 3px solid #7a6fff;
      color: var(--tg-theme-text-color, #000);
    }
    .sc-empty {
      font-size: 13px;
      color: var(--tg-theme-hint-color, #888);
      padding: 4px 10px;
    }
    .sc-lesson {
      display: flex;
      align-items: stretch;
      gap: 0;
      margin-bottom: 5px;
      border-radius: 10px;
      overflow: hidden;
      background: var(--tg-theme-secondary-bg-color, #f5f5f5);
      box-shadow: 0 1px 4px rgba(0,0,0,0.07);
    }
    .sc-num {
      display: flex;
      align-items: center;
      justify-content: center;
      min-width: 32px;
      font-size: 13px;
      font-weight: 700;
      color: #fff;
      background: linear-gradient(160deg, #4e8cff, #8f6bff);
      padding: 0 6px;
    }
    .sc-body {
      flex: 1;
      padding: 7px 10px;
      min-width: 0;
    }
    .sc-subject {
      font-size: 14px;
      font-weight: 600;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
      color: var(--tg-theme-text-color, #000);
    }
    .sc-meta {
      display: flex;
      gap: 8px;
      margin-top: 2px;
      font-size: 12px;
      color: var(--tg-theme-hint-color, #777);
      align-items: center;
    }
    .sc-time { white-space: nowrap; }
    .sc-room {
      margin-left: auto;
      background: rgba(127,107,255,0.12);
      color: #7a6fff;
      border-radius: 6px;
      padding: 1px 7px;
      font-weight: 600;
      white-space: nowrap;
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
    .card {
      padding: 8px;
      border-radius: 10px;
      background: var(--tg-theme-secondary-bg-color, rgba(255,255,255,0.85));
      box-shadow: 0 4px 14px rgba(0,0,0,0.12);
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
      font-size: 13px;
    }
    .sched-btn.active {
      filter: brightness(1.05);
      box-shadow: 0 3px 10px rgba(0,0,0,0.18);
    }
    .hidden {
      display: none !important;
    }
    /* ── Редактор уроков ── */
    .lesson-entry {
      display: flex;
      align-items: stretch;
      gap: 8px;
      margin-bottom: 8px;
    }
    /* Левая полоска: [+] номер [−] */
    .lesson-btns {
      display: flex;
      flex-direction: column;
      align-items: center;
      justify-content: space-between;
      gap: 0;
      flex-shrink: 0;
      width: 28px;
      padding: 2px 0;
    }
    .lesson-index {
      font-size: 12px;
      font-weight: 700;
      color: var(--tg-theme-hint-color, #999);
      line-height: 1;
      user-select: none;
    }
    .lesson-btn-add,
    .lesson-btn-remove {
      padding: 0;
      width: 26px;
      height: 26px;
      min-width: 0;
      line-height: 26px;
      text-align: center;
      box-shadow: none;
      border-radius: 50%;
      border: none;
      color: #ffffff;
      font-size: 16px;
      flex-shrink: 0;
      margin: 0;
    }
    .lesson-btn-add  { background: linear-gradient(135deg, #24b34b, #4edc7e); }
    .lesson-btn-remove { background: linear-gradient(135deg, #e24545, #ff8a7a); }
    /* Карточка урока */
    .lesson-row {
      flex: 1;
      display: flex;
      flex-direction: column;
      gap: 5px;
      padding: 8px 10px;
      border-radius: 10px;
      background: var(--tg-theme-secondary-bg-color, #f5f5f5);
      border: 1px solid rgba(0,0,0,0.07);
      box-shadow: 0 1px 4px rgba(0,0,0,0.06);
    }
    .lesson-row-top {
      display: flex;
      align-items: center;
      gap: 6px;
    }
    .lesson-times {
      display: flex;
      gap: 6px;
      flex: 1 1 0;
    }
    .lesson-times input {
      flex: 1 1 0;
      min-width: 0;
      padding: 4px 6px;
      font-size: 13px;
      height: 32px;
      margin-top: 0;
      text-align: center;
    }
    .lesson-times-sep {
      align-self: center;
      font-size: 13px;
      color: var(--tg-theme-hint-color, #aaa);
      flex-shrink: 0;
    }
    .lesson-row-bottom {
      display: flex;
      gap: 6px;
    }
    .lesson-row-bottom .lesson-subject-wrap {
      flex: 1 1 0;
      min-width: 0;
    }
    .lesson-row-bottom .lesson-room-wrap {
      flex: 0 0 72px;
    }
    .lesson-row-bottom input {
      padding: 5px 8px;
      font-size: 13px;
      height: 34px;
      margin-top: 0;
    }
    /* Подписи полей внутри карточки */
    .lesson-field-label {
      font-size: 10px;
      color: var(--tg-theme-hint-color, #aaa);
      margin-bottom: 2px;
      padding-left: 2px;
    }
    /* ── Кнопки расписания ── */
    #schedule-card {
      padding: 0;
      background: none;
      box-shadow: none;
    }
    .sched-main-row {
      display: flex;
      gap: 8px;
      margin-bottom: 10px;
    }
    .sched-main-btn {
      flex: 1 1 0;
      display: flex;
      flex-direction: column;
      align-items: center;
      gap: 5px;
      padding: 12px 4px;
      border-radius: 14px;
      background: linear-gradient(145deg, #4e8cff, #8f6bff);
      color: #fff;
      font-size: 13px;
      font-weight: 600;
      box-shadow: 0 3px 10px rgba(100,80,255,0.22);
      border: none;
      cursor: pointer;
      transition: transform 0.08s, box-shadow 0.08s;
      margin: 0;
    }
    .sched-main-btn:active {
      transform: scale(0.96);
      box-shadow: 0 1px 4px rgba(100,80,255,0.18);
    }
    .sched-main-btn.active {
      background: linear-gradient(145deg, #3a76f0, #7a52f0);
      box-shadow: 0 4px 14px rgba(100,80,255,0.35);
    }
    .sched-btn-icon { font-size: 20px; line-height: 1; }
    .sched-btn-label { font-size: 12px; }
    .sched-chips-row {
      display: flex;
      flex-wrap: wrap;
      gap: 6px;
      margin-bottom: 8px;
    }
    .sched-chip {
      padding: 5px 12px;
      border-radius: 999px;
      font-size: 12px;
      font-weight: 500;
      background: var(--tg-theme-secondary-bg-color, #f0f0f0);
      color: var(--tg-theme-hint-color, #555);
      border: 1px solid rgba(0,0,0,0.07);
      box-shadow: none;
      margin: 0;
      cursor: pointer;
      transition: background 0.12s, color 0.12s;
    }
    .sched-chip.active {
      background: linear-gradient(135deg, #4e8cff, #8f6bff);
      color: #fff;
      border-color: transparent;
      box-shadow: 0 2px 8px rgba(100,80,255,0.25);
    }
    /* ── Подписка ── */
    #sub-card { padding: 12px; }
    .sub-status-block {
      display: flex;
      align-items: center;
      gap: 12px;
      padding: 14px 16px;
      border-radius: 16px;
      background: linear-gradient(135deg, rgba(78,140,255,0.10), rgba(143,107,255,0.10));
      border: 1px solid rgba(122,111,255,0.18);
      margin-bottom: 16px;
    }
    .sub-status-left {
      width: 44px;
      height: 44px;
      border-radius: 50%;
      background: linear-gradient(135deg, #4e8cff22, #8f6bff33);
      display: flex;
      align-items: center;
      justify-content: center;
      flex-shrink: 0;
    }
    .sub-status-icon { font-size: 22px; }
    .sub-status-right { flex: 1; min-width: 0; }
    .sub-status-title {
      font-size: 13px;
      font-weight: 700;
      color: var(--tg-theme-text-color, #000);
      margin-bottom: 2px;
    }
    .sub-status-text {
      font-size: 12px;
      color: var(--tg-theme-hint-color, #777);
      line-height: 1.4;
    }
    .sub-settings {
      display: flex;
      gap: 10px;
      margin-bottom: 16px;
    }
    .sub-field {
      flex: 1 1 0;
      display: flex;
      flex-direction: column;
      gap: 5px;
    }
    .sub-label {
      font-size: 11px;
      font-weight: 600;
      color: var(--tg-theme-hint-color, #888);
      text-transform: uppercase;
      letter-spacing: 0.05em;
      padding-left: 3px;
    }
    .sub-input {
      width: 100%;
      box-sizing: border-box;
      height: 44px;
      padding: 8px 12px;
      border-radius: 12px;
      border: 1.5px solid rgba(0,0,0,0.10);
      font-size: 15px;
      background: var(--tg-theme-secondary-bg-color, #f5f5f5);
      color: var(--tg-theme-text-color, #000);
      margin: 0;
    }
    .sub-actions { display: flex; gap: 8px; }
    .sub-btn-save {
      flex: 1;
      padding: 13px;
      font-size: 14px;
      font-weight: 700;
      border-radius: 14px;
      background: linear-gradient(135deg, #4e8cff, #8f6bff);
      color: #fff;
      border: none;
      box-shadow: 0 3px 10px rgba(100,80,255,0.25);
      cursor: pointer;
      margin: 0;
    }
    .sub-btn-remove {
      flex: 0 0 auto;
      padding: 13px 18px;
      font-size: 13px;
      font-weight: 600;
      border-radius: 14px;
      background: rgba(220,50,50,0.07);
      color: #c0392b;
      border: 1.5px solid rgba(220,50,50,0.18);
      box-shadow: none;
      cursor: pointer;
      margin: 0;
    }
    /* ── Управление админами ── */
    .admins-section { margin-top: 16px; }
    .admins-list { display: flex; flex-direction: column; gap: 6px; margin-bottom: 10px; }
    .admins-item {
      display: flex;
      align-items: center;
      gap: 8px;
      padding: 9px 12px;
      border-radius: 10px;
      background: var(--tg-theme-secondary-bg-color, #f5f5f5);
      border: 1px solid rgba(0,0,0,0.06);
    }
    .admins-item-info { flex: 1; min-width: 0; }
    .admins-item-id {
      font-size: 12px;
      font-weight: 600;
      font-family: ui-monospace, monospace;
    }
    .admins-item-badge {
      font-size: 10px;
      padding: 2px 7px;
      border-radius: 999px;
      background: rgba(122,111,255,0.13);
      color: #7a6fff;
      font-weight: 700;
      flex-shrink: 0;
    }
    .admins-item-del {
      padding: 5px 11px;
      font-size: 12px;
      border-radius: 999px;
      background: linear-gradient(135deg, #e24545, #ff8a7a);
      color: #fff;
      border: none;
      cursor: pointer;
      box-shadow: none;
      min-width: 0;
      flex-shrink: 0;
      margin: 0;
    }
    .admins-empty {
      font-size: 13px;
      color: var(--tg-theme-hint-color, #888);
      padding: 4px 2px;
    }
    .admins-add-row {
      display: flex;
      gap: 8px;
      align-items: flex-end;
    }
    .admins-add-row .sub-field { flex: 1; }
    .admins-add-row button { flex-shrink: 0; height: 42px; padding: 0 16px; border-radius: 10px; }
    /* ── Fullscreen редакторы ── */
    .admin-fullscreen {
      display: flex;
      flex-direction: column;
      position: fixed;
      inset: 0;
      z-index: 100;
      background: var(--tg-theme-bg-color, #fff);
      padding: 0;
    }
    .admin-fullscreen.hidden { display: none !important; }
    .editor-topbar {
      display: flex;
      align-items: center;
      gap: 8px;
      padding: 10px 12px 8px;
      border-bottom: 1px solid rgba(0,0,0,0.08);
      background: var(--tg-theme-secondary-bg-color, #f5f5f5);
      flex-shrink: 0;
    }
    .editor-back-btn {
      padding: 6px 12px;
      font-size: 13px;
      flex-shrink: 0;
    }
    .editor-topbar-right {
      display: flex;
      gap: 6px;
      flex: 1;
      min-width: 0;
      justify-content: flex-end;
    }
    .editor-topbar-right input,
    .editor-topbar-right select {
      width: auto;
      flex: 1 1 0;
      min-width: 0;
      max-width: 160px;
    }
    .editor-scroll {
      flex: 1;
      overflow-y: auto;
      padding: 10px 12px;
    }
    .editor-textarea {
      flex: 1;
      width: 100%;
      box-sizing: border-box;
      resize: none;
      border: none;
      border-radius: 0;
      padding: 12px;
      font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
      font-size: 13px;
      line-height: 1.6;
      background: var(--tg-theme-bg-color, #fff);
      color: var(--tg-theme-text-color, #000);
      outline: none;
      margin: 0;
      height: auto;
    }
    .editor-bottombar {
      display: flex;
      gap: 8px;
      padding: 10px 12px;
      border-top: 1px solid rgba(0,0,0,0.08);
      background: var(--tg-theme-secondary-bg-color, #f5f5f5);
      flex-shrink: 0;
    }
    /* ── Групповые подписки ── */
    .sub-section-divider {
      font-size: 11px;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0.06em;
      color: var(--tg-theme-hint-color, #888);
      margin: 16px 0 10px;
      display: flex;
      align-items: center;
      gap: 8px;
    }
    .sub-section-divider::after {
      content: '';
      flex: 1;
      height: 1px;
      background: rgba(0,0,0,0.08);
    }
    .sub-group-list {
      display: flex;
      flex-direction: column;
      gap: 6px;
      margin-bottom: 10px;
    }
    .sub-group-empty {
      font-size: 13px;
      color: var(--tg-theme-hint-color, #888);
      padding: 4px 2px;
    }
    .sub-group-item {
      display: flex;
      align-items: center;
      gap: 8px;
      padding: 9px 12px;
      border-radius: 10px;
      background: var(--tg-theme-secondary-bg-color, #f5f5f5);
      border: 1px solid rgba(0,0,0,0.06);
    }
    .sub-group-item-info { flex: 1; min-width: 0; }
    .sub-group-item-id {
      font-size: 11px;
      font-weight: 600;
      color: var(--tg-theme-hint-color, #888);
      margin-bottom: 1px;
      font-family: ui-monospace, monospace;
    }
    .sub-group-item-time { font-size: 13px; font-weight: 500; }
    .sub-group-item-del {
      padding: 5px 11px;
      font-size: 12px;
      border-radius: 999px;
      background: linear-gradient(135deg, #e24545, #ff8a7a);
      color: #fff;
      border: none;
      cursor: pointer;
      box-shadow: none;
      min-width: 0;
      flex-shrink: 0;
      margin: 0;
    }
    .sub-group-form { display: flex; flex-direction: column; gap: 8px; }
    .sub-group-inputs {
      display: flex;
      gap: 8px;
    }
    .sub-group-inputs .sub-field:first-child { flex: 2 1 0; }
    .sub-group-inputs .sub-field { flex: 1 1 0; min-width: 0; }
    @media (max-width: 480px) {
      h1 { font-size: 18px; }
      h2 { font-size: 14px; }
      button { font-size: 13px; }
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

  <div id="schedule-card">
    <!-- Три главных кнопки -->
    <div class="sched-main-row">
      <button class="sched-main-btn" data-type="today">
        <span class="sched-btn-icon">📅</span>
        <span class="sched-btn-label">Сегодня</span>
      </button>
      <button class="sched-main-btn" data-type="tomorrow">
        <span class="sched-btn-icon">🌅</span>
        <span class="sched-btn-label">Завтра</span>
      </button>
      <button class="sched-main-btn" data-type="week">
        <span class="sched-btn-icon">📆</span>
        <span class="sched-btn-label">Неделя</span>
      </button>
    </div>
    <!-- Дополнительные чипсы: Основное + Суббота в одном ряду -->
    <div class="sched-chips-row" id="schedule-secondary-row">
      <button id="btn-week-base" class="sched-chip" data-type="week_base">Основное</button>
      <button id="btn-saturday" class="sched-chip" data-type="saturday">Суббота</button>
    </div>
    <!-- Профили субботы (отдельная строка, скрывается если нет профилей) -->
    <div id="schedule-saturday-row" class="sched-chips-row">
      <button id="btn-sat-prof-1" class="sched-chip" data-type="sat_profile:Физмат">Физмат</button>
      <button id="btn-sat-prof-2" class="sched-chip" data-type="sat_profile:Биохим">Биохим</button>
      <button id="btn-sat-prof-3" class="sched-chip" data-type="sat_profile:Инфотех_1">Инфотех 1</button>
      <button id="btn-sat-prof-4" class="sched-chip" data-type="sat_profile:Инфотех_2">Инфотех 2</button>
      <button id="btn-sat-prof-5" class="sched-chip" data-type="sat_profile:Общеобразовательный_3">Общеобр. 3</button>
    </div>
    <div id="schedule-box"></div>
  </div>

  <div class="card hidden" id="sub-card">
    <!-- Чекбоксы подписок -->
    <div style="display:flex;flex-direction:column;gap:10px;margin-bottom:14px;">
      <!-- Ежедневное расписание -->
      <div id="sub-daily-card" style="border-radius:14px;border:1.5px solid rgba(0,0,0,0.08);padding:14px;background:var(--tg-theme-secondary-bg-color,#f5f5f5);cursor:pointer;" onclick="toggleDaily()">
        <div style="display:flex;align-items:center;gap:12px;">
          <div id="sub-daily-toggle" style="width:44px;height:26px;border-radius:13px;background:#ccc;position:relative;flex-shrink:0;transition:background 0.2s;">
            <div style="position:absolute;top:3px;left:3px;width:20px;height:20px;border-radius:50%;background:#fff;transition:left 0.2s;box-shadow:0 1px 4px rgba(0,0,0,0.18);" id="sub-daily-knob"></div>
          </div>
          <div>
            <div style="font-size:14px;font-weight:700;">📅 Ежедневное расписание</div>
            <div id="sub-daily-desc" style="font-size:12px;color:var(--tg-theme-hint-color,#888);margin-top:2px;">Выключено</div>
          </div>
        </div>
        <!-- Настройки времени — показываются когда включено -->
        <div id="sub-daily-settings" style="display:none;margin-top:12px;padding-top:10px;border-top:1px solid rgba(0,0,0,0.07);" onclick="event.stopPropagation()">
          <div style="display:flex;gap:10px;">
            <div class="sub-field">
              <label class="sub-label">Время</label>
              <input id="sub-time" type="time" class="sub-input" />
            </div>
            <div class="sub-field">
              <label class="sub-label">Расписание на</label>
              <select id="sub-day-type" class="sub-input">
                <option value="today">Сегодня</option>
                <option value="tomorrow">Завтра</option>
              </select>
            </div>
          </div>
        </div>
      </div>
      <!-- Уведомления об изменениях -->
      <div id="sub-changes-card" style="border-radius:14px;border:1.5px solid rgba(0,0,0,0.08);padding:14px;background:var(--tg-theme-secondary-bg-color,#f5f5f5);cursor:pointer;" onclick="toggleChanges()">
        <div style="display:flex;align-items:center;gap:12px;">
          <div id="sub-changes-toggle" style="width:44px;height:26px;border-radius:13px;background:#ccc;position:relative;flex-shrink:0;transition:background 0.2s;">
            <div style="position:absolute;top:3px;left:3px;width:20px;height:20px;border-radius:50%;background:#fff;transition:left 0.2s;box-shadow:0 1px 4px rgba(0,0,0,0.18);" id="sub-changes-knob"></div>
          </div>
          <div>
            <div style="font-size:14px;font-weight:700;">🔔 Изменения расписания</div>
            <div style="font-size:12px;color:var(--tg-theme-hint-color,#888);margin-top:2px;">Уведомление при обновлении</div>
          </div>
        </div>
      </div>
    </div>
    <button id="sub-save" class="sub-btn-save">Сохранить настройки</button>

    <!-- Групповые подписки — только для админов -->
    <div id="sub-group-section" class="hidden">
      <div class="sub-section-divider">Групповые чаты</div>
      <div id="sub-group-list" class="sub-group-list">
        <div class="sub-group-empty">Загрузка...</div>
      </div>
      <div class="sub-group-form">
        <div class="sub-group-inputs">
          <div class="sub-field" style="flex:2 1 0;">
            <label class="sub-label">Chat ID группы</label>
            <input id="sub-group-chatid" type="text" class="sub-input" placeholder="-100123456789" />
          </div>
        </div>
        <!-- Чекбоксы типа подписки -->
        <div style="display:flex;flex-direction:column;gap:8px;margin:4px 0;">
          <!-- Ежедневное -->
          <div style="border-radius:12px;border:1.5px solid rgba(0,0,0,0.08);padding:12px;background:var(--tg-theme-secondary-bg-color,#f5f5f5);cursor:pointer;" onclick="toggleGroupDaily()">
            <div style="display:flex;align-items:center;gap:10px;">
              <div id="grp-daily-toggle" style="width:40px;height:24px;border-radius:12px;background:#ccc;position:relative;flex-shrink:0;transition:background 0.2s;">
                <div id="grp-daily-knob" style="position:absolute;top:2px;left:2px;width:20px;height:20px;border-radius:50%;background:#fff;transition:left 0.2s;box-shadow:0 1px 3px rgba(0,0,0,0.2);"></div>
              </div>
              <div style="font-size:13px;font-weight:700;">📅 Ежедневное расписание</div>
            </div>
            <div id="grp-daily-settings" style="display:none;margin-top:10px;padding-top:8px;border-top:1px solid rgba(0,0,0,0.07);" onclick="event.stopPropagation()">
              <div style="display:flex;gap:8px;">
                <div class="sub-field">
                  <label class="sub-label">Время</label>
                  <input id="sub-group-time" type="time" class="sub-input" />
                </div>
                <div class="sub-field">
                  <label class="sub-label">На</label>
                  <select id="sub-group-daytype" class="sub-input">
                    <option value="today">Сегодня</option>
                    <option value="tomorrow">Завтра</option>
                  </select>
                </div>
              </div>
            </div>
          </div>
          <!-- Изменения -->
          <div style="border-radius:12px;border:1.5px solid rgba(0,0,0,0.08);padding:12px;background:var(--tg-theme-secondary-bg-color,#f5f5f5);cursor:pointer;" onclick="toggleGroupChanges()">
            <div style="display:flex;align-items:center;gap:10px;">
              <div id="grp-changes-toggle" style="width:40px;height:24px;border-radius:12px;background:#ccc;position:relative;flex-shrink:0;transition:background 0.2s;">
                <div id="grp-changes-knob" style="position:absolute;top:2px;left:2px;width:20px;height:20px;border-radius:50%;background:#fff;transition:left 0.2s;box-shadow:0 1px 3px rgba(0,0,0,0.2);"></div>
              </div>
              <div style="font-size:13px;font-weight:700;">🔔 Изменения расписания</div>
            </div>
          </div>
        </div>
        <button id="sub-group-save" style="width:100%;">➕ Добавить подписку для группы</button>
      </div>
    </div>
  </div>

  <div class="card hidden" id="admin-card">
    <div id="admin-mode-buttons">
      <div class="row">
        <button id="admin-type-base">Основное</button>
        <button id="admin-type-temp" class="secondary">Временное</button>
      </div>
      <div class="row" style="margin-top:6px;">
        <button id="admin-mode-day">Редактировать день</button>
        <button id="admin-mode-week" class="secondary">Редактировать неделю</button>
      </div>

      <!-- Управление админами — только для суперадмина -->
      <div id="admins-section" class="hidden admins-section">
        <div class="sub-section-divider">Администраторы</div>
        <div id="admins-list" class="admins-list">
          <div class="admins-empty">Загрузка...</div>
        </div>
        <div class="admins-add-row">
          <div class="sub-field">
            <label class="sub-label">User ID нового админа</label>
            <input id="admins-add-input" type="text" class="sub-input" placeholder="123456789" />
          </div>
          <button id="admins-add-btn">➕ Добавить</button>
        </div>
      </div>
    </div>

    <!-- Редактор дня -->
    <div id="admin-day-editor" class="hidden admin-fullscreen">
      <div class="editor-topbar">
        <button id="admin-day-cancel" class="secondary editor-back-btn">← Назад</button>
        <div class="editor-topbar-right">
          <div class="row" id="admin-day-date-wrap" style="margin:0;">
            <input id="admin-day-date" type="date" style="height:32px;margin:0;font-size:12px;" />
          </div>
          <select id="admin-day-select" style="height:32px;margin:0;font-size:12px;width:auto;">
            <option value="Понедельник">Понедельник</option>
            <option value="Вторник">Вторник</option>
            <option value="Среда">Среда</option>
            <option value="Четверг">Четверг</option>
            <option value="Пятница">Пятница</option>
            <option value="Суббота">Суббота</option>
            <option value="Воскресенье">Воскресенье</option>
          </select>
        </div>
      </div>
      <!-- Выбор профиля субботы — показывается только когда выбрана Суббота -->
      <div id="admin-sat-profile-bar" style="display:none;padding:8px 12px;border-bottom:1px solid rgba(0,0,0,0.08);background:var(--tg-theme-secondary-bg-color,#f5f5f5);gap:6px;flex-wrap:wrap;align-items:center;">
        <span style="font-size:12px;color:var(--tg-theme-hint-color,#888);flex-shrink:0;">Профиль:</span>
        <button class="sched-chip sat-prof-btn active" data-profile="Физмат">Физмат</button>
        <button class="sched-chip sat-prof-btn" data-profile="Биохим">Биохим</button>
        <button class="sched-chip sat-prof-btn" data-profile="Инфотех_1">Инфотех 1</button>
        <button class="sched-chip sat-prof-btn" data-profile="Инфотех_2">Инфотех 2</button>
        <button class="sched-chip sat-prof-btn" data-profile="Общеобразовательный_3">Общеобр. 3</button>
      </div>
      <div class="editor-scroll">
        <div id="admin-lesson-rows"></div>
      </div>
      <div class="editor-bottombar">
        <button id="admin-day-save" style="flex:1;">Сохранить день</button>
      </div>
    </div>

    <!-- Редактор недели -->
    <div id="admin-week-editor" class="hidden admin-fullscreen">
      <div class="editor-topbar">
        <button id="admin-week-cancel" class="secondary editor-back-btn">← Назад</button>
        <button id="admin-week-load" class="secondary" style="font-size:12px;padding:6px 10px;">📥 Загрузить</button>
      </div>
      <textarea id="admin-week-text" class="editor-textarea" placeholder="Понедельник:&#10;08:00-08:40 Математика/211&#10;&#10;Вторник:&#10;08:00-08:40 Русский яз./305"></textarea>
      <div class="editor-bottombar">
        <button id="admin-week-save" style="flex:1;">Сохранить неделю</button>
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
    let isSuperAdmin = false;

    function createLessonRow(data) {
      // Внешняя обёртка: [полоска управления] + [карточка урока]
      const entry = document.createElement('div');
      entry.className = 'lesson-entry';

      // --- Левая полоска: [+] номер [−] ---
      const btnsDiv = document.createElement('div');
      btnsDiv.className = 'lesson-btns';

      const plusBtn = document.createElement('button');
      plusBtn.textContent = '+';
      plusBtn.className = 'lesson-btn-add';
      plusBtn.title = 'Добавить урок после';
      plusBtn.addEventListener('click', () => {
        const snapshot = {
          start: entry.querySelector('.lesson-start').value,
          end: entry.querySelector('.lesson-end').value,
          subject: '',
          room: '',
        };
        const newEntry = createLessonRow(snapshot);
        adminLessonRows.insertBefore(newEntry, entry.nextSibling);
        renumberLessonRows();
      });

      const numLabel = document.createElement('span');
      numLabel.className = 'lesson-index';
      numLabel.textContent = '1';

      const minusBtn = document.createElement('button');
      minusBtn.textContent = '\u2212';
      minusBtn.className = 'lesson-btn-remove';
      minusBtn.title = 'Удалить урок';
      minusBtn.addEventListener('click', () => {
        if (adminLessonRows.children.length > 1) {
          adminLessonRows.removeChild(entry);
          renumberLessonRows();
        }
      });

      btnsDiv.appendChild(plusBtn);
      btnsDiv.appendChild(numLabel);
      btnsDiv.appendChild(minusBtn);

      // --- Карточка урока ---
      const row = document.createElement('div');
      row.className = 'lesson-row';

      // Строка 1: время начала → время конца
      const topDiv = document.createElement('div');
      topDiv.className = 'lesson-row-top';

      const timesDiv = document.createElement('div');
      timesDiv.className = 'lesson-times';

      const startInput = document.createElement('input');
      startInput.type = 'time';
      startInput.className = 'lesson-start';
      startInput.value = data.start || '';
      startInput.title = 'Начало';

      const sep = document.createElement('span');
      sep.className = 'lesson-times-sep';
      sep.textContent = '\u2192';

      const endInput = document.createElement('input');
      endInput.type = 'time';
      endInput.className = 'lesson-end';
      endInput.value = data.end || '';
      endInput.title = 'Конец';

      timesDiv.appendChild(startInput);
      timesDiv.appendChild(sep);
      timesDiv.appendChild(endInput);
      topDiv.appendChild(timesDiv);

      // Строка 2: предмет + кабинет с подписями
      const bottomDiv = document.createElement('div');
      bottomDiv.className = 'lesson-row-bottom';

      const subjWrap = document.createElement('div');
      subjWrap.className = 'lesson-subject-wrap';
      const subjLabel = document.createElement('div');
      subjLabel.className = 'lesson-field-label';
      subjLabel.textContent = '\u041f\u0440\u0435\u0434\u043c\u0435\u0442';
      const subjInput = document.createElement('input');
      subjInput.placeholder = '\u041d\u0430\u0437\u0432\u0430\u043d\u0438\u0435';
      subjInput.className = 'lesson-subject';
      subjInput.value = data.subject || '';
      subjWrap.appendChild(subjLabel);
      subjWrap.appendChild(subjInput);

      const roomWrap = document.createElement('div');
      roomWrap.className = 'lesson-room-wrap';
      const roomLabel = document.createElement('div');
      roomLabel.className = 'lesson-field-label';
      roomLabel.textContent = '\u041a\u0430\u0431\u0438\u043d\u0435\u0442';
      const roomInput = document.createElement('input');
      roomInput.placeholder = '\u2014';
      roomInput.className = 'lesson-room';
      roomInput.value = data.room || '';
      roomWrap.appendChild(roomLabel);
      roomWrap.appendChild(roomInput);

      bottomDiv.appendChild(subjWrap);
      bottomDiv.appendChild(roomWrap);

      row.appendChild(topDiv);
      row.appendChild(bottomDiv);

      entry.appendChild(btnsDiv);
      entry.appendChild(row);

      return entry;
    }

    function renumberLessonRows() {
      Array.from(adminLessonRows.children).forEach((entry, idx) => {
        const label = entry.querySelector('.lesson-index');
        if (label) label.textContent = idx + 1;
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

    let currentSatProfile = 'Физмат';

    function updateSatProfileBar() {
      const bar = document.getElementById('admin-sat-profile-bar');
      if (!bar) return;
      if (adminDaySelect.value === 'Суббота') {
        bar.style.display = 'flex';
      } else {
        bar.style.display = 'none';
      }
    }

    // Переключение профилей субботы
    document.querySelectorAll('.sat-prof-btn').forEach(btn => {
      btn.addEventListener('click', async (e) => {
        e.stopPropagation();
        currentSatProfile = btn.getAttribute('data-profile');
        document.querySelectorAll('.sat-prof-btn').forEach(b =>
          b.classList.toggle('active', b === btn));
        await reloadAdminDay();
      });
    });

    async function reloadAdminDay() {
      const day  = adminDaySelect.value;
      const date = adminDayDate.value || null;
      updateSatProfileBar();
      if (day === 'Суббота') {
        const data = await api('/api/admin/sat_profile_get',
          { profile: currentSatProfile, mode: adminType, date });
        fillLessonRowsFromLines(data.lessons || []);
      } else {
        const data = await api('/api/admin/day_get', { day, mode: adminType, date });
        fillLessonRowsFromLines(data.lessons || []);
      }
    }

    const tabBtnSchedule = document.getElementById('tab-btn-schedule');
    const tabBtnSub = document.getElementById('tab-btn-sub');
    const tabBtnAdmin = document.getElementById('tab-btn-admin');
    const scheduleCard = document.getElementById('schedule-card');
    const subCard = document.getElementById('sub-card');
    const scheduleSaturdayRow = document.getElementById('schedule-saturday-row');

    let isAdmin = false;

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
      // Админ‑карточка видна только на вкладке "Админка" и только для админов
      const showAdmin = tab === 'admin' && isAdmin;
      adminCard.classList.toggle('hidden', !showAdmin);
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
      renderSubState(data.subscription || null);
      isAdmin = !!data.is_admin;
      isSuperAdmin = !!data.is_superadmin;
      // Управление видимостью: кнопка «Суббота» всегда в ряду с «Основное»
      // Строка профилей показывается только если есть профили
      const btnSaturday = document.getElementById('btn-saturday');
      if (!data.has_saturday) {
        if (btnSaturday) btnSaturday.style.display = 'none';
        scheduleSaturdayRow.style.display = 'none';
      } else if (!data.has_saturday_profiles) {
        if (btnSaturday) btnSaturday.style.display = '';
        scheduleSaturdayRow.style.display = 'none';
      } else {
        if (btnSaturday) btnSaturday.style.display = '';
        scheduleSaturdayRow.style.display = 'flex';
      }
      setStatus('Готово');
      if (isAdmin) {
        await loadGroupSubscriptions();
      }
      if (isSuperAdmin) {
        const adminsSection = document.getElementById('admins-section');
        if (adminsSection) adminsSection.classList.remove('hidden');
        await loadAdminsList();
      }
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

    // ── Состояние тоглов подписки ──────────────────────────────────────────
    let subDailyOn = false;
    let subChangesOn = false;

    function setToggle(toggleEl, knobEl, on) {
      toggleEl.style.background = on ? 'linear-gradient(135deg,#4e8cff,#8f6bff)' : '#ccc';
      knobEl.style.left = on ? '21px' : '3px';
    }

    function renderSubState(sub) {
      subDailyOn   = !!(sub && sub.notify_daily);
      subChangesOn = !!(sub && sub.notify_changes);

      const dailyToggle  = document.getElementById('sub-daily-toggle');
      const dailyKnob    = document.getElementById('sub-daily-knob');
      const changesToggle= document.getElementById('sub-changes-toggle');
      const changesKnob  = document.getElementById('sub-changes-knob');
      const dailySettings= document.getElementById('sub-daily-settings');
      const dailyDesc    = document.getElementById('sub-daily-desc');
      const subTime      = document.getElementById('sub-time');
      const subDayType   = document.getElementById('sub-day-type');

      setToggle(dailyToggle, dailyKnob, subDailyOn);
      setToggle(changesToggle, changesKnob, subChangesOn);

      dailySettings.style.display = subDailyOn ? 'block' : 'none';

      if (subDailyOn && sub) {
        const dl = sub.day_type === 'tomorrow' ? 'завтра' : 'сегодня';
        dailyDesc.textContent = 'Каждый день в ' + sub.time + ' — ' + dl;
        if (subTime) subTime.value = sub.time || '07:00';
        if (subDayType) subDayType.value = sub.day_type || 'today';
      } else {
        dailyDesc.textContent = 'Выключено';
        if (!subTime.value) subTime.value = '07:00';
      }
    }

    function toggleDaily() {
      subDailyOn = !subDailyOn;
      const sub = { notify_daily: subDailyOn, notify_changes: subChangesOn,
                    time: document.getElementById('sub-time').value || '07:00',
                    day_type: document.getElementById('sub-day-type').value || 'today' };
      renderSubState(sub);
    }

    function toggleChanges() {
      subChangesOn = !subChangesOn;
      const sub = { notify_daily: subDailyOn, notify_changes: subChangesOn,
                    time: document.getElementById('sub-time').value || '07:00',
                    day_type: document.getElementById('sub-day-type').value || 'today' };
      renderSubState(sub);
    }

    async function saveSubscription() {
      const time    = document.getElementById('sub-time').value || '07:00';
      const dayType = document.getElementById('sub-day-type').value || 'today';
      setStatus('Сохранение подписок...');
      const res = await api('/api/subscribe', {
        notify_daily: subDailyOn, notify_changes: subChangesOn,
        time, day_type: dayType
      });
      setStatus(subDailyOn || subChangesOn ? 'Подписки сохранены' : 'Подписки отключены');
      renderSubState(res.subscription || null);
    }

    async function removeSubscription() {
      setStatus('Отключение подписок...');
      await api('/api/unsubscribe', {});
      subDailyOn = false;
      subChangesOn = false;
      renderSubState(null);
      setStatus('Подписки отключены');
    }

    async function saveAdminWeek() {
      const text = adminWeekText.value || '';
      setStatus('Сохранение расписания на неделю...');
      await api('/api/admin/week', { week_text: text, mode: adminType });
      setStatus('Расписание обновлено');
    }

    function collectLessonsText() {
      const rows = Array.from(adminLessonRows.querySelectorAll('.lesson-entry'));
      const parsed = [];
      rows.forEach((row) => {
        const subject = (row.querySelector('.lesson-subject').value || '').trim();
        const room    = (row.querySelector('.lesson-room').value   || '').trim();
        const start   = (row.querySelector('.lesson-start').value  || '');
        const end     = (row.querySelector('.lesson-end').value    || '');
        if (!subject) return;
        const roomPart = room ? '/' + room : '';
        parsed.push({ start, line: `${start}-${end} ${subject}${roomPart}`.trim() });
      });
      return parsed
        .filter(p => p.start)
        .sort((a,b) => a.start < b.start ? -1 : a.start > b.start ? 1 : 0)
        .map(p => p.line)
        .join('\\n');
    }

    async function saveAdminDay() {
      const day  = adminDaySelect.value;
      const date = adminDayDate.value || null;
      const text = collectLessonsText();
      setStatus('Сохранение расписания дня...');
      if (day === 'Суббота') {
        await api('/api/admin/sat_profile', {
          profile: currentSatProfile, lessons_text: text, mode: adminType, date
        });
        setStatus('Профиль субботы обновлён');
      } else {
        await api('/api/admin/day', { day, lessons_text: text, mode: adminType, date });
        setStatus('Расписание дня обновлено');
      }
    }

    document.querySelectorAll('button[data-type]').forEach((btn) => {
      btn.addEventListener('click', () => {
        const t = btn.getAttribute('data-type');
        loadSchedule(t);
      });
    });
    document.getElementById('sub-save').addEventListener('click', saveSubscription);

    // ── Групповые подписки (только для админов) ──
    let grpDailyOn = false;
    let grpChangesOn = false;

    function toggleGroupDaily() {
      grpDailyOn = !grpDailyOn;
      const t = document.getElementById('grp-daily-toggle');
      const k = document.getElementById('grp-daily-knob');
      const s = document.getElementById('grp-daily-settings');
      t.style.background = grpDailyOn ? 'linear-gradient(135deg,#4e8cff,#8f6bff)' : '#ccc';
      k.style.left = grpDailyOn ? '18px' : '2px';
      s.style.display = grpDailyOn ? 'block' : 'none';
    }

    function toggleGroupChanges() {
      grpChangesOn = !grpChangesOn;
      const t = document.getElementById('grp-changes-toggle');
      const k = document.getElementById('grp-changes-knob');
      t.style.background = grpChangesOn ? 'linear-gradient(135deg,#4e8cff,#8f6bff)' : '#ccc';
      k.style.left = grpChangesOn ? '18px' : '2px';
    }

    async function loadGroupSubscriptions() {
      const section = document.getElementById('sub-group-section');
      if (!section) return;
      section.classList.remove('hidden');
      const list = document.getElementById('sub-group-list');
      list.innerHTML = '<div class="sub-group-empty">Загрузка...</div>';
      try {
        const data = await api('/api/admin/subscriptions_list', {});
        list.innerHTML = '';
        if (!data.subscriptions || !data.subscriptions.length) {
          list.innerHTML = '<div class="sub-group-empty">Нет активных групповых подписок</div>';
          return;
        }
        data.subscriptions.forEach(sub => {
          const tags = [];
          if (sub.notify_daily)   tags.push('📅 ' + sub.time + ' · ' + (sub.day_type === 'tomorrow' ? 'завтра' : 'сегодня'));
          if (sub.notify_changes) tags.push('🔔 изменения');
          if (!tags.length)       tags.push('—');
          const item = document.createElement('div');
          item.className = 'sub-group-item';
          item.innerHTML =
            '<div class="sub-group-item-info">' +
              '<div class="sub-group-item-id">ID: ' + sub.chat_id + '</div>' +
              '<div class="sub-group-item-time">' + tags.join(' &nbsp;·&nbsp; ') + '</div>' +
            '</div>' +
            '<button class="sub-group-item-del">\u2715</button>';
          item.querySelector('.sub-group-item-del').addEventListener('click', async () => {
            try {
              await api('/api/admin/unsubscribe_chat', { chat_id: sub.chat_id });
              await loadGroupSubscriptions();
            } catch(e) {}
          });
          list.appendChild(item);
        });
      } catch(e) {
        list.innerHTML = '<div class="sub-group-empty">Ошибка загрузки</div>';
      }
    }

    document.getElementById('sub-group-save').addEventListener('click', async () => {
      const chatIdInput  = document.getElementById('sub-group-chatid');
      const timeInput    = document.getElementById('sub-group-time');
      const dayTypeInput = document.getElementById('sub-group-daytype');
      const chatId = (chatIdInput.value || '').trim();
      if (!chatId) { setStatus('Укажи Chat ID группы', true); return; }
      if (!/^-?\d+$/.test(chatId)) { setStatus('Chat ID должен быть числом, например -100123456789', true); return; }
      if (!grpDailyOn && !grpChangesOn) { setStatus('Выбери хотя бы один тип уведомлений', true); return; }
      if (grpDailyOn && !timeInput.value) { setStatus('Укажи время для ежедневных уведомлений', true); return; }
      try {
        await api('/api/admin/subscribe_chat', {
          chat_id: chatId,
          notify_daily:   grpDailyOn,
          notify_changes: grpChangesOn,
          time:     timeInput.value || '07:00',
          day_type: dayTypeInput.value || 'today',
        });
        chatIdInput.value = '';
        setStatus('Подписка для группы добавлена');
        await loadGroupSubscriptions();
      } catch(e) {}
    });

    // ── Управление админами (только суперадмин) ──
    async function loadAdminsList() {
      const list = document.getElementById('admins-list');
      if (!list) return;
      list.innerHTML = '<div class="admins-empty">Загрузка...</div>';
      try {
        const data = await api('/api/admin/admins_list', {});
        list.innerHTML = '';
        if (!data.admins || !data.admins.length) {
          list.innerHTML = '<div class="admins-empty">Нет дополнительных администраторов</div>';
          return;
        }
        data.admins.forEach(uid => {
          const item = document.createElement('div');
          item.className = 'admins-item';
          item.innerHTML =
            '<div class="admins-item-info">' +
              '<div class="admins-item-id">' + uid + '</div>' +
            '</div>' +
            '<span class="admins-item-badge">админ</span>' +
            '<button class="admins-item-del" data-uid="' + uid + '">\u2715</button>';
          item.querySelector('.admins-item-del').addEventListener('click', async () => {
            try {
              await api('/api/admin/admin_remove', { target_user_id: uid });
              await loadAdminsList();
            } catch(e) {}
          });
          list.appendChild(item);
        });
      } catch(e) {
        list.innerHTML = '<div class="admins-empty">Ошибка загрузки</div>';
      }
    }

    document.getElementById('admins-add-btn').addEventListener('click', async () => {
      const input = document.getElementById('admins-add-input');
      const uid = (input.value || '').trim();
      if (!uid || !/^\d+$/.test(uid)) { setStatus('User ID должен быть числом', true); return; }
      try {
        await api('/api/admin/admin_add', { target_user_id: uid });
        input.value = '';
        setStatus('Администратор добавлен');
        await loadAdminsList();
      } catch(e) {}
    });

    tabBtnSchedule.addEventListener('click', () => setTab('schedule'));
    tabBtnSub.addEventListener('click', () => setTab('sub'));
    tabBtnAdmin.addEventListener('click', () => setTab('admin'));

    adminTypeBase.addEventListener('click', () => {
      adminType = 'base';
      adminTypeBase.classList.remove('secondary');
      adminTypeTemp.classList.add('secondary');
      adminDayDateWrap.classList.add('hidden');
      const loadBtn = document.getElementById('admin-week-load');
      if (loadBtn) loadBtn.textContent = '📥 Загрузить основное';
      if (!adminDayEditor.classList.contains('hidden')) {
        reloadAdminDay();
      }
    });
    adminTypeTemp.addEventListener('click', () => {
      adminType = 'temp';
      adminTypeTemp.classList.remove('secondary');
      adminTypeBase.classList.add('secondary');
      adminDayDateWrap.classList.remove('hidden');
      const loadBtn = document.getElementById('admin-week-load');
      if (loadBtn) loadBtn.textContent = '📥 Загрузить текущее';
      if (!adminDayEditor.classList.contains('hidden')) {
        reloadAdminDay();
      }
    });
    document.getElementById('admin-mode-day').addEventListener('click', () => {
      adminModeButtons.classList.add('hidden');
      adminWeekEditor.classList.add('hidden');
      adminDayEditor.classList.remove('hidden');
      if (adminType === 'temp' && !adminDayDate.value) {
        const today = new Date();
        adminDayDate.value = today.toISOString().split('T')[0];
        const ruDays = ['Воскресенье','Понедельник','Вторник','Среда','Четверг','Пятница','Суббота'];
        adminDaySelect.value = ruDays[today.getDay()];
      }
      updateSatProfileBar();
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
    adminDaySelect.addEventListener('change', () => {
      updateSatProfileBar();
      if (adminType === 'temp') {
        const ruDays = ['Воскресенье','Понедельник','Вторник','Среда','Четверг','Пятница','Суббота'];
        const targetIdx = ruDays.indexOf(adminDaySelect.value);
        if (targetIdx >= 0) {
          const today = new Date();
          const delta = targetIdx - today.getDay();
          const target = new Date(today);
          target.setDate(today.getDate() + delta);
          adminDayDate.value = target.toISOString().split('T')[0];
        }
      }
      if (!adminDayEditor.classList.contains('hidden')) {
        reloadAdminDay();
      }
    });
    adminDayDate.addEventListener('change', () => {
      // Синхронизируем день недели с выбранной датой
      if (adminDayDate.value) {
        const ruDays = ['Воскресенье','Понедельник','Вторник','Среда','Четверг','Пятница','Суббота'];
        const d = new Date(adminDayDate.value + 'T12:00:00');
        const dayName = ruDays[d.getDay()];
        if (adminDaySelect.value !== dayName) {
          adminDaySelect.value = dayName;
        }
      }
      if (!adminDayEditor.classList.contains('hidden')) {
        reloadAdminDay();
      }
    });
    adminWeekCancel.addEventListener('click', () => {
      adminWeekEditor.classList.add('hidden');
      adminModeButtons.classList.remove('hidden');
    });
    document.getElementById('admin-week-load').addEventListener('click', async () => {
      try {
        setStatus('Загрузка расписания...');
        const data = await api('/api/admin/week_get', { mode: adminType });
        adminWeekText.value = data.week_text || '';
        setStatus('Расписание загружено — можешь редактировать');
      } catch (e) {
        // ошибка уже показана внутри api()
      }
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
            "is_superadmin": _is_superadmin_user_id(user_id),
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
        user = _get_user_from_init_data(data.get("init_data", ""))
    if not user:
        return JSONResponse({"ok": False, "error": "bad_init_data"}, status_code=400)
    user_id = int(user["id"])
    uid = str(user_id)

    notify_daily   = bool(data.get("notify_daily", False))
    notify_changes = bool(data.get("notify_changes", False))

    entry = dict(subscriptions.get(uid) or {})
    entry["chat_id"]        = user_id
    entry["notify_daily"]   = notify_daily
    entry["notify_changes"] = notify_changes

    if notify_daily:
        time_str = data.get("time", entry.get("time", "07:00"))
        parsed = _parse_hhmm(time_str)
        if not parsed:
            return JSONResponse({"ok": False, "error": "bad_time"}, status_code=400)
        hh, mm = parsed
        entry["time"] = f"{hh:02d}:{mm:02d}"
        day_type = data.get("day_type", entry.get("day_type", "today"))
        entry["day_type"] = day_type if day_type in {"today", "tomorrow"} else "today"

    if not notify_daily and not notify_changes:
        subscriptions.pop(uid, None)
        if scheduler is not None:
            try: scheduler.remove_job(_job_id_for(user_id))
            except Exception: pass
    else:
        subscriptions[uid] = entry
        if notify_daily:
            _reschedule_user(user_id)
        else:
            if scheduler is not None:
                try: scheduler.remove_job(_job_id_for(user_id))
                except Exception: pass

    _save_subscriptions_to_disk()
    return JSONResponse({"ok": True, "subscription": subscriptions.get(uid)})


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


@app.post("/api/admin/sat_profile_get")
async def api_admin_sat_profile_get(request: Request):
    """Возвращает уроки одного профиля субботы."""
    data = await request.json()
    raw_user = data.get("user")
    user = None
    if isinstance(raw_user, dict) and "id" in raw_user:
        user = raw_user
    else:
        user = _get_user_from_init_data(data.get("init_data", ""))
    if not user:
        return JSONResponse({"ok": False, "error": "bad_init_data"}, status_code=400)
    if not _is_admin_user_id(int(user["id"])):
        return JSONResponse({"ok": False, "error": "forbidden"}, status_code=403)

    profile_key = (data.get("profile") or "").strip()
    mode = (data.get("mode") or "base").strip()
    date_str = (data.get("date") or "").strip()

    lessons: list[str] = []
    if mode == "temp" and date_str:
        try:
            d = datetime.fromisoformat(date_str).date()
        except ValueError:
            return JSONResponse({"ok": False, "error": "bad_date"}, status_code=400)
        raw = temp_schedule.get(d.isoformat())
        if isinstance(raw, dict):
            lessons = raw.get(profile_key, [])
        if not lessons:
            sat = schedule.get("Суббота")
            if isinstance(sat, dict):
                lessons = sat.get(profile_key, [])
    else:
        sat = schedule.get("Суббота")
        if isinstance(sat, dict):
            lessons = sat.get(profile_key, [])

    return JSONResponse({"ok": True, "lessons": lessons})


@app.post("/api/admin/sat_profile")
async def api_admin_sat_profile(request: Request):
    """Сохраняет уроки одного профиля субботы."""
    data = await request.json()
    raw_user = data.get("user")
    user = None
    if isinstance(raw_user, dict) and "id" in raw_user:
        user = raw_user
    else:
        user = _get_user_from_init_data(data.get("init_data", ""))
    if not user:
        return JSONResponse({"ok": False, "error": "bad_init_data"}, status_code=400)
    user_id = int(user["id"])
    if not _is_admin_user_id(user_id):
        return JSONResponse({"ok": False, "error": "forbidden"}, status_code=403)

    profile_key = (data.get("profile") or "").strip()
    if profile_key not in SATURDAY_PROFILE_KEYS:
        return JSONResponse({"ok": False, "error": "bad_profile"}, status_code=400)

    mode = (data.get("mode") or "base").strip()
    lessons_text = data.get("lessons_text", "") or ""
    lessons = _parse_lessons_from_text(lessons_text)
    if lessons is None:
        return JSONResponse({"ok": False, "error": "bad_format"}, status_code=400)

    label = SATURDAY_PROFILE_LABELS.get(profile_key, profile_key)

    if mode == "temp":
        date_str = (data.get("date") or "").strip()
        if not date_str:
            return JSONResponse({"ok": False, "error": "date_required"}, status_code=400)
        try:
            d = datetime.fromisoformat(date_str).date()
        except ValueError:
            return JSONResponse({"ok": False, "error": "bad_date"}, status_code=400)
        key = d.isoformat()
        existing = temp_schedule.get(key)
        if isinstance(existing, dict):
            existing[profile_key] = lessons
        else:
            # Инициализируем все профили из основного расписания
            sat_base = schedule.get("Суббота")
            base_dict = sat_base if isinstance(sat_base, dict) else {}
            new_dict = {pk: list(base_dict.get(pk, [])) for pk in SATURDAY_PROFILE_KEYS}
            new_dict[profile_key] = lessons
            temp_schedule[key] = new_dict
        _save_temp_schedule_to_disk()
        msg = f"📢 Временное расписание субботы ({d.strftime('%d.%m.%Y')}) — {label} обновлено:\n\n"
        msg += _format_day_table_html(label, lessons)
    else:
        sat = schedule.get("Суббота")
        if not isinstance(sat, dict):
            sat = {}
        sat[profile_key] = lessons
        schedule["Суббота"] = sat
        _save_schedule_to_disk()
        msg = f"📢 Расписание субботы — {label} обновлено:\n\n"
        msg += _format_day_table_html(label, lessons)

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


@app.post("/api/admin/week_get")
async def api_admin_week_get(request: Request):
    """Возвращает расписание недели в текстовом формате для предзаполнения формы."""
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

    mode = (data.get("mode") or "base").strip()
    lines: list[str] = []

    now_tz = datetime.now(tz=_get_tz())
    monday = (now_tz - timedelta(days=now_tz.weekday())).date()

    for offset, day in enumerate(SCHEDULE_DAYS):
        target_date = monday + timedelta(days=offset)
        key = target_date.isoformat()

        if day == "Суббота":
            # Суббота — всегда по профилям
            if mode == "temp":
                raw = temp_schedule.get(key)
                if isinstance(raw, dict):
                    sat_data = raw
                elif isinstance(raw, list):
                    # legacy list — выводим как обычный день
                    if raw:
                        lines.append(f"{day}:")
                        lines.extend(raw)
                        lines.append("")
                    continue
                else:
                    sat_data = schedule.get("Суббота")
            else:
                sat_data = schedule.get("Суббота")

            if isinstance(sat_data, dict):
                for pk in SATURDAY_PROFILE_KEYS:
                    profile_lessons = sat_data.get(pk, [])
                    if profile_lessons:
                        label = SATURDAY_PROFILE_LABELS.get(pk, pk)
                        lines.append(f"Суббота {label}:")
                        lines.extend(profile_lessons)
                        lines.append("")
            elif isinstance(sat_data, list) and sat_data:
                lines.append(f"{day}:")
                lines.extend(sat_data)
                lines.append("")
            continue

        # Обычный день
        lessons: list[str] = []
        if mode == "temp":
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

        if lessons:
            lines.append(f"{day}:")
            lines.extend(lessons)
            lines.append("")

    return JSONResponse({"ok": True, "week_text": "\n".join(lines).strip()})


@app.post("/api/admin/subscribe_chat")
async def api_admin_subscribe_chat(request: Request):
    """Добавляет или обновляет подписку для группового чата."""
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

    chat_id_raw = str(data.get("chat_id") or "").strip()
    if not chat_id_raw or not re.match(r"^-?\d+$", chat_id_raw):
        return JSONResponse({"ok": False, "error": "bad_chat_id"}, status_code=400)
    chat_id = int(chat_id_raw)

    notify_daily   = bool(data.get("notify_daily", True))
    notify_changes = bool(data.get("notify_changes", False))

    entry: dict = {"chat_id": chat_id, "notify_daily": notify_daily, "notify_changes": notify_changes}

    if notify_daily:
        time_str = data.get("time", "07:00")
        parsed = _parse_hhmm(time_str)
        if not parsed:
            return JSONResponse({"ok": False, "error": "bad_time"}, status_code=400)
        hh, mm = parsed
        entry["time"] = f"{hh:02d}:{mm:02d}"
        day_type = data.get("day_type", "today")
        entry["day_type"] = day_type if day_type in {"today", "tomorrow"} else "today"

    subscriptions[str(chat_id)] = entry
    _save_subscriptions_to_disk()
    if notify_daily:
        _reschedule_user(chat_id)
    else:
        if scheduler is not None:
            try:
                scheduler.remove_job(_job_id_for(chat_id))
            except Exception:
                pass
    return JSONResponse({"ok": True})


@app.post("/api/admin/subscriptions_list")
async def api_admin_subscriptions_list(request: Request):
    """Возвращает список всех подписок кроме личной подписки текущего пользователя."""
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

    result = []
    for key, entry in subscriptions.items():
        cid = entry.get("chat_id")
        if cid is not None and int(cid) != user_id:
            result.append({
                "chat_id": cid,
                "time":           entry.get("time", ""),
                "day_type":       entry.get("day_type", "today"),
                "notify_daily":   entry.get("notify_daily", True),
                "notify_changes": entry.get("notify_changes", False),
            })
    return JSONResponse({"ok": True, "subscriptions": result})


@app.post("/api/admin/unsubscribe_chat")
async def api_admin_unsubscribe_chat(request: Request):
    """Удаляет подписку для группового чата."""
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

    chat_id_raw = str(data.get("chat_id") or "").strip()
    if not chat_id_raw or not re.match(r"^-?\d+$", chat_id_raw):
        return JSONResponse({"ok": False, "error": "bad_chat_id"}, status_code=400)
    chat_id = int(chat_id_raw)

    subscriptions.pop(str(chat_id), None)
    _save_subscriptions_to_disk()
    if scheduler is not None:
        try:
            scheduler.remove_job(_job_id_for(chat_id))
        except Exception:
            pass
    return JSONResponse({"ok": True})


@app.post("/api/admin/admins_list")
async def api_admin_admins_list(request: Request):
    """Список динамических админов (только для суперадмина)."""
    data = await request.json()
    raw_user = data.get("user")
    user = None
    if isinstance(raw_user, dict) and "id" in raw_user:
        user = raw_user
    else:
        user = _get_user_from_init_data(data.get("init_data", ""))
    if not user:
        return JSONResponse({"ok": False, "error": "bad_init_data"}, status_code=400)
    user_id = int(user["id"])
    if not _is_superadmin_user_id(user_id):
        return JSONResponse({"ok": False, "error": "forbidden"}, status_code=403)
    return JSONResponse({"ok": True, "admins": sorted(dynamic_admins)})


@app.post("/api/admin/admin_add")
async def api_admin_admin_add(request: Request):
    """Добавить динамического админа (только для суперадмина)."""
    data = await request.json()
    raw_user = data.get("user")
    user = None
    if isinstance(raw_user, dict) and "id" in raw_user:
        user = raw_user
    else:
        user = _get_user_from_init_data(data.get("init_data", ""))
    if not user:
        return JSONResponse({"ok": False, "error": "bad_init_data"}, status_code=400)
    user_id = int(user["id"])
    if not _is_superadmin_user_id(user_id):
        return JSONResponse({"ok": False, "error": "forbidden"}, status_code=403)
    target_raw = str(data.get("target_user_id") or "").strip()
    if not target_raw or not re.match(r"^\d+$", target_raw):
        return JSONResponse({"ok": False, "error": "bad_user_id"}, status_code=400)
    target_id = int(target_raw)
    if _is_superadmin_user_id(target_id):
        return JSONResponse({"ok": False, "error": "already_superadmin"}, status_code=400)
    dynamic_admins.add(target_id)
    _save_dynamic_admins()
    return JSONResponse({"ok": True})


@app.post("/api/admin/admin_remove")
async def api_admin_admin_remove(request: Request):
    """Удалить динамического админа (только для суперадмина)."""
    data = await request.json()
    raw_user = data.get("user")
    user = None
    if isinstance(raw_user, dict) and "id" in raw_user:
        user = raw_user
    else:
        user = _get_user_from_init_data(data.get("init_data", ""))
    if not user:
        return JSONResponse({"ok": False, "error": "bad_init_data"}, status_code=400)
    user_id = int(user["id"])
    if not _is_superadmin_user_id(user_id):
        return JSONResponse({"ok": False, "error": "forbidden"}, status_code=403)
    target_raw = str(data.get("target_user_id") or "").strip()
    if not target_raw or not re.match(r"^\d+$", target_raw):
        return JSONResponse({"ok": False, "error": "bad_user_id"}, status_code=400)
    target_id = int(target_raw)
    dynamic_admins.discard(target_id)
    _save_dynamic_admins()
    return JSONResponse({"ok": True})
