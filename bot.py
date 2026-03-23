import os
import html
import logging
import re
import sqlite3
from collections import defaultdict
from datetime import datetime, time, timedelta
from io import BytesIO
from zoneinfo import ZoneInfo

from openpyxl import Workbook
from openpyxl.styles import Alignment, Font
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, InputFile, Update
from telegram.constants import ChatType, ParseMode
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    Defaults,
    MessageHandler,
    filters,
)

# =========================
# 基础配置
# =========================
BOT_TOKEN = os.environ.get("BOT_TOKEN", "").strip()
if not BOT_TOKEN:
    raise ValueError("缺少环境变量 BOT_TOKEN")

DB_FILE = "enterprise_checkin.db"

# 泰国时间
LOCAL_TZ = ZoneInfo("Asia/Bangkok")

# 打卡时间：泰国时间 20:00 - 次日 11:30
WORK_START = time(21, 0)
WORK_END = time(10, 0)

# 每天中午 12:00 作为统计周期切换点
RESET_TIME = time(12, 0)

# 21:10 检查未打上班卡成员
ABSENCE_CHECK_TIME = time(21, 10)

# 群成员都可打卡
ADMIN_ONLY = False

# 只有管理员可导出
EXPORT_ADMIN_ONLY = True

# 默认超时设置（分钟）
DEFAULT_TIMEOUTS = {
    "吃饭": 15,
    "上厕所": 10,
    "抽烟": 10,
}

# 超时前多久提醒本人（秒）
WARNING_BEFORE_SECONDS = 60

BREAK_ACTIONS = ["吃饭", "上厕所", "抽烟"]

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# =========================
# 中英文识别
# =========================
ACTION_KEYWORDS = {
    "上班": [
        "上班", "到岗", "到了", "开工", "打卡上班",
        "on", "on duty", "start work", "clock in", "check in", "go work", "at work",
    ],
    "下班": [
        "下班", "收工", "走了", "打卡下班",
        "off", "off duty", "off work", "end work", "clock out", "check out", "go home",
    ],
    "吃饭": [
        "吃饭", "去吃饭", "干饭", "吃个饭", "吃东西", "吃午饭", "吃晚饭",
        "eat", "eating", "meal", "lunch", "dinner", "breakfast",
        "go eat", "go to eat", "having lunch", "having dinner",
    ],
    "上厕所": [
        "上厕所", "去厕所", "厕所", "洗手间", "卫生间", "方便一下",
        "toilet", "bathroom", "restroom", "wc", "washroom",
        "go toilet", "go bathroom",
    ],
    "抽烟": [
        "抽烟", "去抽烟", "抽一根", "来一根", "吸烟",
        "smoke", "smoking", "cigarette", "nicotine",
        "go smoke", "have a smoke",
    ],
}

RETURN_WORDS = [
    "回座", "已回座", "回到座位", "到座", "坐回来了",
    "back", "im back", "i'm back", "iam back", "returned",
    "done", "ok", "okay", "finish", "finished", "return",
]

# =========================
# Inline 按钮
# =========================
def get_main_inline_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("上班 On", callback_data="action:上班"),
            InlineKeyboardButton("下班 Off", callback_data="action:下班"),
        ],
        [
            InlineKeyboardButton("吃饭 Eat", callback_data="action:吃饭"),
            InlineKeyboardButton("上厕所 Toilet", callback_data="action:上厕所"),
        ],
        [
            InlineKeyboardButton("抽烟 Smoke", callback_data="action:抽烟"),
            InlineKeyboardButton("回座 Back", callback_data="action:回座"),
        ],
        [
            InlineKeyboardButton("我的离岗 /me", callback_data="cmd:me"),
            InlineKeyboardButton("今日统计 /today", callback_data="cmd:today"),
        ],
        [
            InlineKeyboardButton("上下班 /attendance", callback_data="cmd:attendance"),
            InlineKeyboardButton("导出 /export", callback_data="cmd:export"),
        ],
    ])


# =========================
# 数据库
# =========================
def get_conn():
    conn = sqlite3.connect(DB_FILE)
    return conn


def init_db():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS settings (
            chat_id INTEGER NOT NULL,
            action TEXT NOT NULL,
            timeout_minutes INTEGER NOT NULL,
            PRIMARY KEY (chat_id, action)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS chats (
            chat_id INTEGER PRIMARY KEY,
            title TEXT,
            registered_at TEXT NOT NULL
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS members (
            chat_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            username TEXT,
            full_name TEXT NOT NULL,
            last_seen_at TEXT NOT NULL,
            PRIMARY KEY (chat_id, user_id)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS checkins (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            username TEXT,
            full_name TEXT NOT NULL,
            mention_name TEXT,
            action TEXT NOT NULL,
            start_time TEXT NOT NULL,
            end_time TEXT,
            duration_seconds INTEGER,
            period_key TEXT NOT NULL,
            status TEXT NOT NULL,
            timed_out INTEGER NOT NULL DEFAULT 0,
            warning_sent INTEGER NOT NULL DEFAULT 0
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS attendance (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            username TEXT,
            full_name TEXT NOT NULL,
            period_key TEXT NOT NULL,
            on_duty_time TEXT,
            off_duty_time TEXT,
            work_seconds INTEGER DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'off'
        )
    """)

    conn.commit()
    conn.close()


# =========================
# 时间工具
# =========================
def now_dt() -> datetime:
    return datetime.now(LOCAL_TZ)


def format_seconds(total_seconds: int) -> str:
    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    seconds = total_seconds % 60
    if hours > 0:
        return f"{hours}小时{minutes}分钟{seconds}秒"
    if minutes > 0:
        return f"{minutes}分钟{seconds}秒"
    return f"{seconds}秒"


def fmt_dt(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def current_period_key(dt: datetime | None = None) -> str:
    dt = dt or now_dt()
    if dt.time() < RESET_TIME:
        target_date = dt.date() - timedelta(days=1)
    else:
        target_date = dt.date()
    return target_date.strftime("%Y-%m-%d")


def previous_period_key(dt: datetime | None = None) -> str:
    dt = dt or now_dt()
    return current_period_key(dt - timedelta(seconds=1))


def in_working_hours(dt: datetime | None = None) -> bool:
    dt = dt or now_dt()
    current = dt.time()
    if WORK_START <= WORK_END:
        return WORK_START <= current <= WORK_END
    return current >= WORK_START or current <= WORK_END


def diff_seconds(start_iso: str, end_dt: datetime) -> int:
    start_dt = datetime.fromisoformat(start_iso)
    return max(int((end_dt - start_dt).total_seconds()), 0)


def seconds_between(start_iso: str, end_iso: str) -> int:
    start_dt = datetime.fromisoformat(start_iso)
    end_dt = datetime.fromisoformat(end_iso)
    return max(int((end_dt - start_dt).total_seconds()), 0)


# =========================
# 工具函数
# =========================
def normalize_text(text: str) -> str:
    text = (text or "").strip().lower()
    text = text.replace("’", "'").replace("`", "'")
    text = re.sub(r"[-_.,!?;:，。！？；：/\\()\\[\\]{}]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def contains_phrase(text: str, phrase: str) -> bool:
    nt = normalize_text(text)
    np = normalize_text(phrase)
    if not np:
        return False
    return np in nt


def extract_action(text: str):
    text = normalize_text(text)

    button_map = {
        "上班 on": "上班",
        "下班 off": "下班",
        "吃饭 eat": "吃饭",
        "上厕所 toilet": "上厕所",
        "抽烟 smoke": "抽烟",
        "回座 back": "回座",
    }
    if text in button_map:
        return button_map[text]

    for action, keywords in ACTION_KEYWORDS.items():
        for kw in keywords:
            if contains_phrase(text, kw):
                return action
    return None


def is_return_text(text: str) -> bool:
    text = normalize_text(text)
    if text == "回座 back":
        return True
    return any(contains_phrase(text, word) for word in RETURN_WORDS)


def safe_text(value: str) -> str:
    return html.escape(value or "")


def mention_html(user_id: int, name: str) -> str:
    return f'<a href="tg://user?id={user_id}">{safe_text(name)}</a>'


def build_mention_name(user) -> str:
    if user.username:
        return f"@{user.username}"
    return user.full_name or str(user.id)


async def is_group_admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    chat = update.effective_chat
    user = update.effective_user

    if chat.type == ChatType.PRIVATE:
        return True

    member = await context.bot.get_chat_member(chat.id, user.id)
    return member.status in ("creator", "administrator")


async def get_admin_mentions(chat_id: int, context: ContextTypes.DEFAULT_TYPE) -> str:
    admins = await context.bot.get_chat_administrators(chat_id)
    parts = []
    for admin in admins:
        user = admin.user
        if user.is_bot:
            continue
        parts.append(mention_html(user.id, user.full_name or str(user.id)))
    return " ".join(parts) if parts else "管理员"


# =========================
# 数据登记
# =========================
def ensure_chat_registered(chat_id: int, title: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO chats(chat_id, title, registered_at)
        VALUES (?, ?, ?)
        ON CONFLICT(chat_id) DO UPDATE SET title=excluded.title
    """, (chat_id, title or "", now_dt().isoformat()))
    conn.commit()
    conn.close()


def ensure_member_seen(chat_id: int, user_id: int, username: str, full_name: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO members(chat_id, user_id, username, full_name, last_seen_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(chat_id, user_id) DO UPDATE SET
            username=excluded.username,
            full_name=excluded.full_name,
            last_seen_at=excluded.last_seen_at
    """, (chat_id, user_id, username or "", full_name, now_dt().isoformat()))
    conn.commit()
    conn.close()


def get_registered_chats():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT chat_id FROM chats")
    rows = [r[0] for r in cur.fetchall()]
    conn.close()
    return rows


def get_known_members(chat_id: int):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT user_id, username, full_name
        FROM members
        WHERE chat_id = ?
        ORDER BY full_name ASC
    """, (chat_id,))
    rows = cur.fetchall()
    conn.close()
    return rows


# =========================
# 设置
# =========================
def ensure_default_settings(chat_id: int):
    conn = get_conn()
    cur = conn.cursor()
    for action, minutes in DEFAULT_TIMEOUTS.items():
        cur.execute("""
            INSERT OR IGNORE INTO settings(chat_id, action, timeout_minutes)
            VALUES (?, ?, ?)
        """, (chat_id, action, minutes))
    conn.commit()
    conn.close()


def get_timeout(chat_id: int, action: str) -> int:
    ensure_default_settings(chat_id)
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT timeout_minutes
        FROM settings
        WHERE chat_id = ? AND action = ?
    """, (chat_id, action))
    row = cur.fetchone()
    conn.close()
    return int(row[0]) if row else DEFAULT_TIMEOUTS[action]


# =========================
# attendance
# =========================
def get_attendance(chat_id: int, user_id: int, period_key: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT id, on_duty_time, off_duty_time, work_seconds, status
        FROM attendance
        WHERE chat_id = ? AND user_id = ? AND period_key = ?
        ORDER BY id DESC
        LIMIT 1
    """, (chat_id, user_id, period_key))
    row = cur.fetchone()
    conn.close()
    return row


def create_or_update_on_duty(chat_id: int, user_id: int, username: str, full_name: str, on_duty_time: str, period_key: str):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT id
        FROM attendance
        WHERE chat_id = ? AND user_id = ? AND period_key = ?
        ORDER BY id DESC
        LIMIT 1
    """, (chat_id, user_id, period_key))
    row = cur.fetchone()

    if row:
        att_id = row[0]
        cur.execute("""
            UPDATE attendance
            SET username = ?, full_name = ?, on_duty_time = ?, off_duty_time = NULL, work_seconds = 0, status = 'on'
            WHERE id = ?
        """, (username, full_name, on_duty_time, att_id))
    else:
        cur.execute("""
            INSERT INTO attendance (
                chat_id, user_id, username, full_name, period_key, on_duty_time, work_seconds, status
            ) VALUES (?, ?, ?, ?, ?, ?, 0, 'on')
        """, (chat_id, user_id, username, full_name, period_key, on_duty_time))

    conn.commit()
    conn.close()


def set_off_duty(chat_id: int, user_id: int, off_duty_time: str, work_seconds: int, period_key: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        UPDATE attendance
        SET off_duty_time = ?, work_seconds = ?, status = 'off'
        WHERE chat_id = ? AND user_id = ? AND period_key = ?
    """, (off_duty_time, work_seconds, chat_id, user_id, period_key))
    conn.commit()
    conn.close()


def is_on_duty(chat_id: int, user_id: int, period_key: str) -> bool:
    row = get_attendance(chat_id, user_id, period_key)
    return bool(row and row[4] == "on")


def get_attendance_rows(chat_id: int, period_key: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT full_name, on_duty_time, off_duty_time, work_seconds, status
        FROM attendance
        WHERE chat_id = ? AND period_key = ?
        ORDER BY full_name ASC
    """, (chat_id, period_key))
    rows = cur.fetchall()
    conn.close()
    return rows


def get_attended_user_ids(chat_id: int, period_key: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT DISTINCT user_id
        FROM attendance
        WHERE chat_id = ? AND period_key = ? AND on_duty_time IS NOT NULL
    """, (chat_id, period_key))
    rows = {r[0] for r in cur.fetchall()}
    conn.close()
    return rows


# =========================
# checkins
# =========================
def get_open_checkin(chat_id: int, user_id: int, period_key: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT id, action, start_time, mention_name
        FROM checkins
        WHERE chat_id = ? AND user_id = ? AND period_key = ? AND status = 'open'
        ORDER BY id DESC
        LIMIT 1
    """, (chat_id, user_id, period_key))
    row = cur.fetchone()
    conn.close()
    return row


def create_checkin(chat_id: int, user_id: int, username: str, full_name: str, mention_name: str, action: str, start_time: str, period_key: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO checkins (
            chat_id, user_id, username, full_name, mention_name,
            action, start_time, period_key, status
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'open')
    """, (
        chat_id, user_id, username, full_name, mention_name,
        action, start_time, period_key,
    ))
    checkin_id = cur.lastrowid
    conn.commit()
    conn.close()
    return checkin_id


def close_checkin(checkin_id: int, end_time: str, duration_seconds: int):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        UPDATE checkins
        SET end_time = ?, duration_seconds = ?, status = 'closed'
        WHERE id = ?
    """, (end_time, duration_seconds, checkin_id))
    conn.commit()
    conn.close()


def cancel_checkin(checkin_id: int):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        UPDATE checkins
        SET status = 'cancelled'
        WHERE id = ?
    """, (checkin_id,))
    conn.commit()
    conn.close()


def mark_warning_sent(checkin_id: int):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("UPDATE checkins SET warning_sent = 1 WHERE id = ?", (checkin_id,))
    conn.commit()
    conn.close()


def mark_timed_out(checkin_id: int):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("UPDATE checkins SET timed_out = 1 WHERE id = ?", (checkin_id,))
    conn.commit()
    conn.close()


def close_previous_period_open_records(chat_id: int, period_key: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        UPDATE checkins
        SET status = 'cancelled'
        WHERE chat_id = ? AND period_key = ? AND status = 'open'
    """, (chat_id, period_key))
    conn.commit()
    conn.close()


def get_action_count(chat_id: int, user_id: int, action: str, period_key: str) -> int:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT COUNT(*)
        FROM checkins
        WHERE chat_id = ?
          AND user_id = ?
          AND action = ?
          AND period_key = ?
          AND status IN ('open', 'closed')
    """, (chat_id, user_id, action, period_key))
    row = cur.fetchone()
    conn.close()
    return int(row[0]) if row else 0


def get_stats(chat_id: int, period_key: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT full_name, action, COUNT(*), COALESCE(SUM(duration_seconds), 0)
        FROM checkins
        WHERE chat_id = ?
          AND period_key = ?
          AND status = 'closed'
        GROUP BY full_name, action
        ORDER BY full_name, action
    """, (chat_id, period_key))
    rows = cur.fetchall()
    conn.close()
    return rows


def get_records(chat_id: int, period_key: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT full_name, action, start_time, end_time, duration_seconds,
               status, timed_out
        FROM checkins
        WHERE chat_id = ? AND period_key = ?
        ORDER BY id ASC
    """, (chat_id, period_key))
    rows = cur.fetchall()
    conn.close()
    return rows


def get_user_break_summary(chat_id: int, user_id: int, period_key: str):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT action, COUNT(*), COALESCE(SUM(duration_seconds), 0)
        FROM checkins
        WHERE chat_id = ?
          AND user_id = ?
          AND period_key = ?
          AND status = 'closed'
        GROUP BY action
    """, (chat_id, user_id, period_key))
    rows = cur.fetchall()

    cur.execute("""
        SELECT COUNT(*), COALESCE(SUM(duration_seconds), 0)
        FROM checkins
        WHERE chat_id = ?
          AND user_id = ?
          AND period_key = ?
          AND status = 'closed'
    """, (chat_id, user_id, period_key))
    total_row = cur.fetchone()
    conn.close()

    per_action = {action: {"count": 0, "seconds": 0} for action in BREAK_ACTIONS}
    for action, count, seconds in rows:
        per_action[action] = {"count": count, "seconds": seconds}

    total_count = int(total_row[0]) if total_row else 0
    total_seconds = int(total_row[1]) if total_row else 0
    return per_action, total_count, total_seconds


def get_me_summary(chat_id: int, user_id: int, period_key: str):
    per_action, total_count, total_seconds = get_user_break_summary(chat_id, user_id, period_key)
    attendance = None
    row = get_attendance(chat_id, user_id, period_key)
    if row:
        attendance = (row[1], row[2], row[3], row[4])
    return per_action, total_count, total_seconds, attendance


# =========================
# 展示文本
# =========================
def format_stats(rows, period_key: str):
    if not rows:
        return f"📅 <b>{period_key} 周期离岗统计</b>\n\n暂无记录。"

    grouped = defaultdict(dict)
    for full_name, action, count, total_seconds in rows:
        grouped[full_name][action] = {"count": count, "seconds": total_seconds}

    lines = [f"📅 <b>{period_key} 周期离岗统计</b>\n"]
    for full_name, data in grouped.items():
        lines.append(f"👤 <b>{safe_text(full_name)}</b>")
        for action in BREAK_ACTIONS:
            item = data.get(action, {"count": 0, "seconds": 0})
            lines.append(f"• {action}：{item['count']} 次，合计 {format_seconds(item['seconds'])}")
        lines.append("")
    return "\n".join(lines).strip()


def format_attendance(rows, period_key: str):
    if not rows:
        return f"🕘 <b>{period_key} 周期上下班</b>\n\n暂无记录。"

    lines = [f"🕘 <b>{period_key} 周期上下班</b>\n"]
    for full_name, on_duty_time, off_duty_time, work_seconds, status in rows:
        on_show = on_duty_time[11:19] if on_duty_time else "-"
        off_show = off_duty_time[11:19] if off_duty_time else "-"
        status_text = "在岗" if status == "on" else "离岗/已下班"
        lines.append(
            f"👤 <b>{safe_text(full_name)}</b>\n"
            f"• 上班：{on_show}\n"
            f"• 下班：{off_show}\n"
            f"• 在岗时长：{format_seconds(work_seconds or 0)}\n"
            f"• 状态：{status_text}\n"
        )
    return "\n".join(lines).strip()


def format_me(full_name: str, per_action: dict, total_count: int, total_seconds: int, attendance, period_key: str):
    work_seconds = 0
    on_show = "-"
    off_show = "-"
    status_show = "未上班"

    if attendance:
        on_duty_time, off_duty_time, saved_work_seconds, status = attendance
        on_show = on_duty_time[11:19] if on_duty_time else "-"
        off_show = off_duty_time[11:19] if off_duty_time else "-"
        work_seconds = saved_work_seconds or 0
        status_show = "在岗" if status == "on" else "已下班"

    return (
        f"👤 <b>{safe_text(full_name)} {period_key} 我的离岗明细</b>\n\n"
        f"🕘 上班：{on_show}\n"
        f"🕘 下班：{off_show}\n"
        f"⏱ 上班时长：{format_seconds(work_seconds)}\n"
        f"📊 离岗总次数：{total_count} 次\n"
        f"⌛ 离岗总时长：{format_seconds(total_seconds)}\n\n"
        f"🍚 吃饭：{per_action['吃饭']['count']} 次，{format_seconds(per_action['吃饭']['seconds'])}\n"
        f"🚽 上厕所：{per_action['上厕所']['count']} 次，{format_seconds(per_action['上厕所']['seconds'])}\n"
        f"🚬 抽烟：{per_action['抽烟']['count']} 次，{format_seconds(per_action['抽烟']['seconds'])}\n"
        f"📍 当前状态：{status_show}"
    )


def build_excel(chat_id: int, period_key: str) -> BytesIO:
    wb = Workbook()

    ws1 = wb.active
    ws1.title = "上下班日报"
    ws1.append(["姓名", "上班时间", "下班时间", "上班时长(秒)", "状态"])
    for cell in ws1[1]:
        cell.font = Font(bold=True)
        cell.alignment = Alignment(horizontal="center")

    for full_name, on_duty_time, off_duty_time, work_seconds, status in get_attendance_rows(chat_id, period_key):
        ws1.append([
            full_name,
            on_duty_time or "",
            off_duty_time or "",
            work_seconds or 0,
            "在岗" if status == "on" else "离岗/已下班",
        ])

    ws2 = wb.create_sheet("离岗明细")
    ws2.append(["姓名", "项目", "开始时间", "结束时间", "用时(秒)", "状态", "是否超时"])
    for cell in ws2[1]:
        cell.font = Font(bold=True)
        cell.alignment = Alignment(horizontal="center")

    for row in get_records(chat_id, period_key):
        full_name, action, start_time, end_time, duration_seconds, status, timed_out = row
        ws2.append([
            full_name,
            action,
            start_time,
            end_time or "",
            duration_seconds if duration_seconds is not None else "",
            status,
            "是" if timed_out else "否",
        ])

    ws3 = wb.create_sheet("个人汇总")
    ws3.append(["姓名", "项目", "次数", "总时长(秒)"])
    for cell in ws3[1]:
        cell.font = Font(bold=True)
        cell.alignment = Alignment(horizontal="center")

    for full_name, action, count, total_seconds in get_stats(chat_id, period_key):
        ws3.append([full_name, action, count, total_seconds])

    for ws in [ws1, ws2, ws3]:
        for col in ["A", "B", "C", "D", "E", "F", "G", "H"]:
            ws.column_dimensions[col].width = 22

    bio = BytesIO()
    wb.save(bio)
    bio.seek(0)
    return bio


# =========================
# 统一回复函数
# =========================
async def send_reply(update: Update, text: str, parse_mode=ParseMode.HTML):
    if update.callback_query:
        await update.callback_query.message.reply_text(
            text,
            parse_mode=parse_mode,
            reply_markup=get_main_inline_keyboard(),
        )
    else:
        await update.message.reply_text(
            text,
            parse_mode=parse_mode,
            reply_markup=get_main_inline_keyboard(),
        )


# =========================
# 定时任务
# =========================
def schedule_chat_jobs(app: Application, chat_id: int):
    scheduled = app.bot_data.setdefault("scheduled_chats", set())
    if chat_id in scheduled:
        return

    app.job_queue.run_daily(
        auto_export_job,
        time=time(12, 0, tzinfo=LOCAL_TZ),
        days=(0, 1, 2, 3, 4, 5, 6),
        data={"chat_id": chat_id},
        name=f"auto_export_{chat_id}",
    )

    app.job_queue.run_daily(
        absence_check_job,
        time=time(21, 10, tzinfo=LOCAL_TZ),
        days=(0, 1, 2, 3, 4, 5, 6),
        data={"chat_id": chat_id},
        name=f"absence_check_{chat_id}",
    )

    scheduled.add(chat_id)


async def post_init(app: Application):
    for chat_id in get_registered_chats():
        schedule_chat_jobs(app, chat_id)


async def auto_export_job(context: ContextTypes.DEFAULT_TYPE):
    chat_id = context.job.data["chat_id"]
    period_key = previous_period_key(now_dt())

    close_previous_period_open_records(chat_id, period_key)

    bio = build_excel(chat_id, period_key)
    filename = f"打卡记录_{period_key}.xlsx"

    await context.bot.send_document(
        chat_id=chat_id,
        document=InputFile(bio, filename=filename),
        caption=f"📄 {period_key} 打卡记录已自动导出（泰国时间中午12点结算）",
        reply_markup=get_main_inline_keyboard(),
    )


async def absence_check_job(context: ContextTypes.DEFAULT_TYPE):
    chat_id = context.job.data["chat_id"]
    period_key = current_period_key()
    attended = get_attended_user_ids(chat_id, period_key)
    members = get_known_members(chat_id)

    missing = []
    for user_id, username, full_name in members:
        if user_id not in attended:
            if username:
                missing.append(f"@{username}")
            else:
                missing.append(mention_html(user_id, full_name))

    if not missing:
        return

    text = (
        f"🚨 <b>21:10 未打上班卡名单（已登记成员）</b>\n\n"
        f"周期：<b>{period_key}</b>\n"
        + "\n".join(missing)
    )

    await context.bot.send_message(
        chat_id=chat_id,
        text=text,
        parse_mode=ParseMode.HTML,
        reply_markup=get_main_inline_keyboard(),
    )


async def timeout_warning_job(context: ContextTypes.DEFAULT_TYPE):
    data = context.job.data
    chat_id = data["chat_id"]
    user_id = data["user_id"]
    checkin_id = data["checkin_id"]
    action = data["action"]
    mention_name = data["mention_name"]
    period_key = data["period_key"]

    open_row = get_open_checkin(chat_id, user_id, period_key)
    if not open_row:
        return

    open_id, open_action, _, _ = open_row
    if open_id != checkin_id or open_action != action:
        return

    mark_warning_sent(checkin_id)

    await context.bot.send_message(
        chat_id=chat_id,
        text=(
            f"⚠️ <b>即将超时提醒</b>\n\n"
            f"{safe_text(mention_name)}\n"
            f"项目：<b>{action}</b>\n"
            f"请尽快回座并发送：<b>回座 / BACK / done</b>"
        ),
        parse_mode=ParseMode.HTML,
    )


async def timeout_over_job(context: ContextTypes.DEFAULT_TYPE):
    data = context.job.data
    chat_id = data["chat_id"]
    user_id = data["user_id"]
    checkin_id = data["checkin_id"]
    action = data["action"]
    mention_name = data["mention_name"]
    timeout_minutes = data["timeout_minutes"]
    period_key = data["period_key"]

    open_row = get_open_checkin(chat_id, user_id, period_key)
    if not open_row:
        return

    open_id, open_action, _, _ = open_row
    if open_id != checkin_id or open_action != action:
        return

    mark_timed_out(checkin_id)
    admin_mentions = await get_admin_mentions(chat_id, context)

    await context.bot.send_message(
        chat_id=chat_id,
        text=(
            f"🚨 <b>离岗超时</b>\n\n"
            f"{safe_text(mention_name)}\n"
            f"项目：<b>{action}</b>\n"
            f"已超过 <b>{timeout_minutes}</b> 分钟\n\n"
            f"{admin_mentions}"
        ),
        parse_mode=ParseMode.HTML,
    )


# =========================
# 命令
# =========================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    user = update.effective_user

    ensure_chat_registered(chat.id, chat.title or chat.full_name or "")
    ensure_member_seen(chat.id, user.id, user.username or "", user.full_name or str(user.id))
    ensure_default_settings(chat.id)
    schedule_chat_jobs(context.application, chat.id)

    msg = (
        "<b>泰国时间打卡机器人</b>\n\n"
        "• 上班时间：21:00 - 次日10:00（泰国时间）\n"
        "• 统计周期：每天中午12:00切换\n"
        "• 每天中午12:00自动导出上一周期表格\n"
        "• 每晚21:10自动检查未打上班卡成员（已登记成员）\n"
        "• 吃饭15分钟 / 上厕所10分钟 / 抽烟10分钟\n"
        "• 离岗前会提醒本人，超时会@管理员\n\n"
        "命令：\n"
        "/today 查看本周期离岗统计\n"
        "/attendance 查看本周期上下班\n"
        "/me 查看我自己的离岗次数和时长\n"
        "/status 查看当前设置\n"
        "/export 导出本周期 Excel（日常只有管理员能导出）\n"
        "/cancel 取消当前离岗"
    )
    await send_reply(update, msg)


async def today_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    period_key = current_period_key()
    rows = get_stats(update.effective_chat.id, period_key)
    await send_reply(update, format_stats(rows, period_key))


async def attendance_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    period_key = current_period_key()
    rows = get_attendance_rows(update.effective_chat.id, period_key)
    await send_reply(update, format_attendance(rows, period_key))


async def me_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    period_key = current_period_key()
    chat_id = update.effective_chat.id
    user_id = update.effective_user.id
    full_name = update.effective_user.full_name or str(user_id)
    per_action, total_count, total_seconds, attendance = get_me_summary(chat_id, user_id, period_key)
    await send_reply(update, format_me(full_name, per_action, total_count, total_seconds, attendance, period_key))


async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    ensure_default_settings(chat_id)

    lines = [
        "<b>当前配置</b>\n",
        "时区：Asia/Bangkok",
        f"上班时间：{WORK_START.strftime('%H:%M')} - 次日{WORK_END.strftime('%H:%M')}",
        f"周期切换：每天 {RESET_TIME.strftime('%H:%M')}",
        f"未打卡检查：每天 {ABSENCE_CHECK_TIME.strftime('%H:%M')}",
        f"导出仅管理员：{'开启' if EXPORT_ADMIN_ONLY else '关闭'}\n",
        "离岗超时设置：",
    ]
    for action in BREAK_ACTIONS:
        lines.append(f"• {action}：{get_timeout(chat_id, action)} 分钟")

    await send_reply(update, "\n".join(lines))


async def export_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if EXPORT_ADMIN_ONLY and not await is_group_admin(update, context):
        await send_reply(update, "只有管理员可以导出本周期表格。")
        return

    period_key = current_period_key()
    bio = build_excel(update.effective_chat.id, period_key)
    filename = f"打卡记录_{period_key}.xlsx"

    if update.callback_query:
        await update.callback_query.message.reply_document(
            document=InputFile(bio, filename=filename),
            caption=f"📄 {period_key} 打卡记录",
            reply_markup=get_main_inline_keyboard(),
        )
    else:
        await update.message.reply_document(
            document=InputFile(bio, filename=filename),
            caption=f"📄 {period_key} 打卡记录",
            reply_markup=get_main_inline_keyboard(),
        )


async def cancel_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    period_key = current_period_key()
    chat_id = update.effective_chat.id
    user_id = update.effective_user.id

    open_row = get_open_checkin(chat_id, user_id, period_key)
    if not open_row:
        await send_reply(update, "你当前没有进行中的离岗记录。")
        return

    checkin_id, action, _, _ = open_row
    cancel_checkin(checkin_id)
    await send_reply(update, f"已取消当前离岗：{action}")


# =========================
# 核心业务逻辑
# =========================
async def process_action(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
    chat = update.effective_chat
    user = update.effective_user
    chat_id = chat.id
    user_id = user.id
    username = user.username or ""
    full_name = user.full_name or str(user.id)
    mention_name = build_mention_name(user)

    ensure_chat_registered(chat_id, chat.title or chat.full_name or "")
    ensure_member_seen(chat_id, user_id, username, full_name)
    ensure_default_settings(chat_id)
    schedule_chat_jobs(context.application, chat_id)

    if ADMIN_ONLY and not await is_group_admin(update, context):
        await send_reply(update, "当前群只允许管理员打卡。")
        return

    period_key = current_period_key()
    action = extract_action(text)

    # 回座
    if is_return_text(text):
        open_row = get_open_checkin(chat_id, user_id, period_key)
        if not open_row:
            await send_reply(update, "你当前没有进行中的离岗记录。")
            return

        checkin_id, break_action, start_iso, _ = open_row
        end_dt = now_dt()
        seconds = diff_seconds(start_iso, end_dt)
        close_checkin(checkin_id, end_dt.isoformat(), seconds)

        per_action, total_break_count, total_break_seconds = get_user_break_summary(chat_id, user_id, period_key)

        await send_reply(
            update,
            (
                f"✅ <b>回座成功</b>\n\n"
                f"👤 {safe_text(full_name)}\n"
                f"📌 项目：{break_action}\n"
                f"🕒 本次离岗时长：{format_seconds(seconds)}\n"
                f"🔢 本周期离岗总次数：{total_break_count} 次\n"
                f"⌛ 本周期离岗总时长：{format_seconds(total_break_seconds)}\n"
                f"📍 回座时间：{fmt_dt(end_dt)}"
            ),
        )
        return

    if not action:
        return

    # 上下班时间限制
    if action in ["上班", "下班"] and not in_working_hours():
        await send_reply(
            update,
            f"当前不在上班打卡时间内。\n允许时间：{WORK_START.strftime('%H:%M')} - 次日{WORK_END.strftime('%H:%M')}（泰国时间）",
        )
        return

    # 上班
    if action == "上班":
        if is_on_duty(chat_id, user_id, period_key):
            await send_reply(update, "你本周期已经是上班状态。")
            return

        now = now_dt()
        create_or_update_on_duty(chat_id, user_id, username, full_name, now.isoformat(), period_key)
        await send_reply(
            update,
            (
                f"🟢 <b>上班打卡成功</b>\n\n"
                f"👤 {safe_text(full_name)}\n"
                f"🕒 上班时间：{fmt_dt(now)}\n"
                f"📅 周期：{period_key}"
            ),
        )
        return

    # 下班
    if action == "下班":
        if not is_on_duty(chat_id, user_id, period_key):
            await send_reply(update, "你当前不是上班状态，无法下班打卡。")
            return

        open_row = get_open_checkin(chat_id, user_id, period_key)
        if open_row:
            _, open_action, _, _ = open_row
            await send_reply(update, f"你还有未结束的离岗记录：{open_action}\n请先发送：回座 / BACK / done")
            return

        att_row = get_attendance(chat_id, user_id, period_key)
        now = now_dt()
        work_seconds = 0

        if att_row and att_row[1]:
            work_seconds = seconds_between(att_row[1], now.isoformat())

        set_off_duty(chat_id, user_id, now.isoformat(), work_seconds, period_key)

        per_action, total_break_count, total_break_seconds = get_user_break_summary(chat_id, user_id, period_key)

        await send_reply(
            update,
            (
                f"🔴 <b>下班打卡成功</b>\n\n"
                f"👤 {safe_text(full_name)}\n"
                f"🕒 下班时间：{fmt_dt(now)}\n"
                f"⏱ 本周期上班时长：{format_seconds(work_seconds)}\n"
                f"📊 本周期离岗总次数：{total_break_count} 次\n"
                f"🧾 吃饭：{per_action['吃饭']['count']} 次，{format_seconds(per_action['吃饭']['seconds'])}\n"
                f"🧾 上厕所：{per_action['上厕所']['count']} 次，{format_seconds(per_action['上厕所']['seconds'])}\n"
                f"🧾 抽烟：{per_action['抽烟']['count']} 次，{format_seconds(per_action['抽烟']['seconds'])}\n"
                f"⌛ 本周期离岗总时长：{format_seconds(total_break_seconds)}"
            ),
        )
        return

    # 离岗项目
    if action in BREAK_ACTIONS:
        if not is_on_duty(chat_id, user_id, period_key):
            await send_reply(update, "请先上班打卡，再进行离岗打卡。")
            return

        open_row = get_open_checkin(chat_id, user_id, period_key)
        if open_row:
            _, open_action, _, _ = open_row
            await send_reply(update, f"你当前还有未结束离岗：{open_action}\n请先发送：回座 / BACK / done")
            return

        previous_count = get_action_count(chat_id, user_id, action, period_key)
        current_count = previous_count + 1
        _, total_break_count, total_break_seconds = get_user_break_summary(chat_id, user_id, period_key)

        start_dt = now_dt()
        timeout_minutes = get_timeout(chat_id, action)

        checkin_id = create_checkin(
            chat_id=chat_id,
            user_id=user_id,
            username=username,
            full_name=full_name,
            mention_name=mention_name,
            action=action,
            start_time=start_dt.isoformat(),
            period_key=period_key,
        )

        warning_delay = max(timeout_minutes * 60 - WARNING_BEFORE_SECONDS, 5)

        context.job_queue.run_once(
            timeout_warning_job,
            when=warning_delay,
            data={
                "chat_id": chat_id,
                "user_id": user_id,
                "checkin_id": checkin_id,
                "action": action,
                "mention_name": mention_name,
                "period_key": period_key,
            },
            name=f"warn_{chat_id}_{user_id}_{checkin_id}",
        )

        context.job_queue.run_once(
            timeout_over_job,
            when=timeout_minutes * 60,
            data={
                "chat_id": chat_id,
                "user_id": user_id,
                "checkin_id": checkin_id,
                "action": action,
                "mention_name": mention_name,
                "timeout_minutes": timeout_minutes,
                "period_key": period_key,
            },
            name=f"over_{chat_id}_{user_id}_{checkin_id}",
        )

        await send_reply(
            update,
            (
                f"🟡 <b>离岗成功</b>\n\n"
                f"👤 {safe_text(full_name)}\n"
                f"📌 项目：{action}\n"
                f"🕒 开始时间：{fmt_dt(start_dt)}\n"
                f"🔢 本周期该项目第 {current_count} 次\n"
                f"📊 本周期离岗总次数：{total_break_count + 1} 次（含本次进行中）\n"
                f"⌛ 已完成离岗总时长：{format_seconds(total_break_seconds)}\n"
                f"⏰ 超时上限：{timeout_minutes} 分钟\n\n"
                f"完成后请发送：<b>回座 / BACK / done</b>"
            ),
        )
        return


# =========================
# 按钮处理
# =========================
async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    data = query.data or ""

    if data.startswith("action:"):
        action = data.split(":", 1)[1]
        await process_action(update, context, action if action != "回座" else "回座")
        return

    if data == "cmd:me":
        await me_command(update, context)
        return

    if data == "cmd:today":
        await today_command(update, context)
        return

    if data == "cmd:attendance":
        await attendance_command(update, context)
        return

    if data == "cmd:export":
        await export_command(update, context)
        return


# =========================
# 文本消息处理
# =========================
async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip()
    await process_action(update, context, text)


# =========================
# 主程序
# =========================
def main():
    init_db()

    defaults = Defaults(tzinfo=LOCAL_TZ)
    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .defaults(defaults)
        .post_init(post_init)
        .build()
    )

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("today", today_command))
    app.add_handler(CommandHandler("attendance", attendance_command))
    app.add_handler(CommandHandler("me", me_command))
    app.add_handler(CommandHandler("status", status_command))
    app.add_handler(CommandHandler("export", export_command))
    app.add_handler(CommandHandler("cancel", cancel_command))
    app.add_handler(CallbackQueryHandler(button_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))

    logger.info("Thailand enterprise checkin bot is running...")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
