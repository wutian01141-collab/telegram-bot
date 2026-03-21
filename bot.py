import html
import logging
import re
import sqlite3
from collections import defaultdict
from datetime import datetime, time
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
BOT_TOKEN = "8778975882:AAG6wlgNeu4I5dvtex3en5p7M3ulzlNC3wk"
DB_FILE = "enterprise_checkin.db"

# 美国东部时间
LOCAL_TZ = ZoneInfo("America/New_York")

# 上下班有效时间：美国东部时间 09:00 - 22:00
WORK_START = time(9, 0)
WORK_END = time(22, 0)

# 群成员都可打卡
ADMIN_ONLY = False

# 只有管理员可导出
EXPORT_ADMIN_ONLY = True

# 连续超时报警阈值
OVERTIME_ALERT_THRESHOLD = 2

# 重复提醒间隔（分钟）
REMINDER_REPEAT_MINUTES = 10

# 默认超时设置（分钟）
DEFAULT_TIMEOUTS = {
    "吃饭": 30,
    "上厕所": 15,
    "抽烟": 10,
}

BREAK_ACTIONS = ["吃饭", "上厕所", "抽烟"]

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
        "brb eat", "brb eating",
    ],
    "上厕所": [
        "上厕所", "去厕所", "厕所", "洗手间", "卫生间", "方便一下",
        "toilet", "bathroom", "restroom", "wc", "washroom",
        "go toilet", "go bathroom", "brb toilet", "brb bathroom",
    ],
    "抽烟": [
        "抽烟", "去抽烟", "抽一根", "来一根", "吸烟",
        "smoke", "smoking", "cigarette", "nicotine",
        "go smoke", "brb smoke", "have a smoke",
    ],
}

RETURN_WORDS = [
    "回座", "已回座", "回到座位", "到座", "坐回来了",
    "back", "im back", "i'm back", "iam back", "returned",
    "done", "ok", "okay", "finish", "finished", "return",
]

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


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
            InlineKeyboardButton("我的统计 /me", callback_data="cmd:me"),
            InlineKeyboardButton("今日统计 /today", callback_data="cmd:today"),
        ],
        [
            InlineKeyboardButton("上下班 /attendance", callback_data="cmd:attendance"),
            InlineKeyboardButton("排行 /ranking", callback_data="cmd:ranking"),
        ],
    ])


# =========================
# 数据库
# =========================
def get_conn():
    return sqlite3.connect(DB_FILE)


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
            period_day TEXT NOT NULL,
            status TEXT NOT NULL,
            timed_out INTEGER NOT NULL DEFAULT 0,
            overtime_count INTEGER NOT NULL DEFAULT 0
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS attendance (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            username TEXT,
            full_name TEXT NOT NULL,
            period_day TEXT NOT NULL,
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


def today_str() -> str:
    return now_dt().strftime("%Y-%m-%d")


def fmt_dt(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def in_working_hours() -> bool:
    current = now_dt().time()
    return WORK_START <= current <= WORK_END


def diff_seconds(start_iso: str, end_dt: datetime) -> int:
    start_dt = datetime.fromisoformat(start_iso)
    return max(int((end_dt - start_dt).total_seconds()), 0)


def seconds_between(start_iso: str, end_iso: str) -> int:
    start_dt = datetime.fromisoformat(start_iso)
    end_dt = datetime.fromisoformat(end_iso)
    return max(int((end_dt - start_dt).total_seconds()), 0)


def format_seconds(total_seconds: int) -> str:
    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    seconds = total_seconds % 60
    if hours > 0:
        return f"{hours}小时{minutes}分钟{seconds}秒"
    if minutes > 0:
        return f"{minutes}分钟{seconds}秒"
    return f"{seconds}秒"


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


def set_timeout(chat_id: int, action: str, minutes: int):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO settings(chat_id, action, timeout_minutes)
        VALUES (?, ?, ?)
        ON CONFLICT(chat_id, action)
        DO UPDATE SET timeout_minutes = excluded.timeout_minutes
    """, (chat_id, action, minutes))
    conn.commit()
    conn.close()


# =========================
# attendance
# =========================
def get_today_attendance(chat_id: int, user_id: int):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT id, on_duty_time, off_duty_time, work_seconds, status
        FROM attendance
        WHERE chat_id = ? AND user_id = ? AND period_day = ?
        ORDER BY id DESC
        LIMIT 1
    """, (chat_id, user_id, today_str()))
    row = cur.fetchone()
    conn.close()
    return row


def create_or_update_on_duty(chat_id: int, user_id: int, username: str, full_name: str, on_duty_time: str):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT id
        FROM attendance
        WHERE chat_id = ? AND user_id = ? AND period_day = ?
        ORDER BY id DESC
        LIMIT 1
    """, (chat_id, user_id, today_str()))
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
                chat_id, user_id, username, full_name, period_day, on_duty_time, work_seconds, status
            ) VALUES (?, ?, ?, ?, ?, ?, 0, 'on')
        """, (chat_id, user_id, username, full_name, today_str(), on_duty_time))

    conn.commit()
    conn.close()


def set_off_duty(chat_id: int, user_id: int, off_duty_time: str, work_seconds: int):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        UPDATE attendance
        SET off_duty_time = ?, work_seconds = ?, status = 'off'
        WHERE chat_id = ? AND user_id = ? AND period_day = ?
    """, (off_duty_time, work_seconds, chat_id, user_id, today_str()))
    conn.commit()
    conn.close()


def is_on_duty(chat_id: int, user_id: int) -> bool:
    row = get_today_attendance(chat_id, user_id)
    return bool(row and row[4] == "on")


def get_today_attendance_rows(chat_id: int):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT full_name, on_duty_time, off_duty_time, work_seconds, status
        FROM attendance
        WHERE chat_id = ? AND period_day = ?
        ORDER BY full_name ASC
    """, (chat_id, today_str()))
    rows = cur.fetchall()
    conn.close()
    return rows


# =========================
# checkins
# =========================
def get_open_checkin(chat_id: int, user_id: int):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT id, action, start_time, mention_name
        FROM checkins
        WHERE chat_id = ? AND user_id = ? AND status = 'open'
        ORDER BY id DESC
        LIMIT 1
    """, (chat_id, user_id))
    row = cur.fetchone()
    conn.close()
    return row


def create_checkin(chat_id: int, user_id: int, username: str, full_name: str, mention_name: str, action: str, start_time: str, period_day: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO checkins (
            chat_id, user_id, username, full_name, mention_name,
            action, start_time, period_day, status
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'open')
    """, (
        chat_id, user_id, username, full_name, mention_name,
        action, start_time, period_day,
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


def mark_timed_out(checkin_id: int, overtime_count: int):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        UPDATE checkins
        SET timed_out = 1, overtime_count = ?
        WHERE id = ?
    """, (overtime_count, checkin_id))
    conn.commit()
    conn.close()


def get_user_consecutive_overtime(chat_id: int, user_id: int) -> int:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT timed_out
        FROM checkins
        WHERE chat_id = ? AND user_id = ? AND status = 'closed'
        ORDER BY id DESC
        LIMIT 20
    """, (chat_id, user_id))
    rows = cur.fetchall()
    conn.close()

    count = 0
    for (timed_out,) in rows:
        if timed_out == 1:
            count += 1
        else:
            break
    return count


def get_today_action_count(chat_id: int, user_id: int, action: str) -> int:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT COUNT(*)
        FROM checkins
        WHERE chat_id = ?
          AND user_id = ?
          AND action = ?
          AND period_day = ?
          AND status IN ('open', 'closed')
    """, (chat_id, user_id, action, today_str()))
    row = cur.fetchone()
    conn.close()
    return int(row[0]) if row else 0


def get_today_stats(chat_id: int):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT full_name, action, COUNT(*), COALESCE(SUM(duration_seconds), 0)
        FROM checkins
        WHERE chat_id = ?
          AND period_day = ?
          AND status = 'closed'
        GROUP BY full_name, action
        ORDER BY full_name, action
    """, (chat_id, today_str()))
    rows = cur.fetchall()
    conn.close()
    return rows


def get_today_records(chat_id: int):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT full_name, action, start_time, end_time, duration_seconds,
               status, timed_out, overtime_count
        FROM checkins
        WHERE chat_id = ? AND period_day = ?
        ORDER BY id ASC
    """, (chat_id, today_str()))
    rows = cur.fetchall()
    conn.close()
    return rows


def get_ranking(chat_id: int):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT full_name, action, COUNT(*), COALESCE(SUM(duration_seconds), 0)
        FROM checkins
        WHERE chat_id = ?
          AND period_day = ?
          AND status = 'closed'
        GROUP BY full_name, action
        ORDER BY action, COUNT(*) DESC, COALESCE(SUM(duration_seconds), 0) DESC, full_name ASC
    """, (chat_id, today_str()))
    rows = cur.fetchall()
    conn.close()
    return rows


def get_user_today_break_summary(chat_id: int, user_id: int):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT action, COUNT(*), COALESCE(SUM(duration_seconds), 0)
        FROM checkins
        WHERE chat_id = ?
          AND user_id = ?
          AND period_day = ?
          AND status = 'closed'
        GROUP BY action
    """, (chat_id, user_id, today_str()))
    rows = cur.fetchall()

    cur.execute("""
        SELECT COUNT(*), COALESCE(SUM(duration_seconds), 0)
        FROM checkins
        WHERE chat_id = ?
          AND user_id = ?
          AND period_day = ?
          AND status = 'closed'
    """, (chat_id, user_id, today_str()))
    total_row = cur.fetchone()

    conn.close()

    per_action = {action: {"count": 0, "seconds": 0} for action in BREAK_ACTIONS}
    for action, count, seconds in rows:
        per_action[action] = {"count": count, "seconds": seconds}

    total_count = int(total_row[0]) if total_row else 0
    total_seconds = int(total_row[1]) if total_row else 0
    return per_action, total_count, total_seconds


def get_me_summary(chat_id: int, user_id: int):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT action, COUNT(*), COALESCE(SUM(duration_seconds), 0)
        FROM checkins
        WHERE chat_id = ?
          AND user_id = ?
          AND period_day = ?
          AND status = 'closed'
        GROUP BY action
    """, (chat_id, user_id, today_str()))
    rows = cur.fetchall()

    cur.execute("""
        SELECT on_duty_time, off_duty_time, work_seconds, status
        FROM attendance
        WHERE chat_id = ? AND user_id = ? AND period_day = ?
        ORDER BY id DESC
        LIMIT 1
    """, (chat_id, user_id, today_str()))
    attendance = cur.fetchone()
    conn.close()

    per_action = {action: {"count": 0, "seconds": 0} for action in BREAK_ACTIONS}
    for action, count, seconds in rows:
        per_action[action] = {"count": count, "seconds": seconds}

    return per_action, attendance


# =========================
# 文本识别
# =========================
def normalize_text(text: str) -> str:
    text = (text or "").strip().lower()
    text = text.replace("’", "'")
    text = text.replace("`", "'")
    text = re.sub(r"[_\-.,!?;:，。！？；：/\\()\[\]{}]+", " ", text)
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


# =========================
# 工具函数
# =========================
def build_mention_name(user) -> str:
    if user.username:
        return f"@{user.username}"
    return user.full_name or str(user.id)


def safe_text(value: str) -> str:
    return html.escape(value or "")


async def is_group_admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    chat = update.effective_chat
    user = update.effective_user

    if chat.type == ChatType.PRIVATE:
        return True

    member = await context.bot.get_chat_member(chat.id, user.id)
    return member.status in ("creator", "administrator")


# =========================
# 文本展示
# =========================
def format_stats(rows):
    if not rows:
        return f"📅 <b>{today_str()} 今日离岗统计</b>\n\n暂无记录。"

    grouped = defaultdict(dict)
    for full_name, action, count, total_seconds in rows:
        grouped[full_name][action] = {
            "count": count,
            "seconds": total_seconds,
        }

    lines = [f"📅 <b>{today_str()} 今日离岗统计</b>\n"]
    for full_name, data in grouped.items():
        lines.append(f"👤 <b>{safe_text(full_name)}</b>")
        for action in BREAK_ACTIONS:
            item = data.get(action, {"count": 0, "seconds": 0})
            lines.append(f"• {action}：{item['count']} 次，合计 {format_seconds(item['seconds'])}")
        lines.append("")
    return "\n".join(lines).strip()


def format_ranking(rows):
    if not rows:
        return "📊 今天还没有排行数据。"

    grouped = defaultdict(list)
    for full_name, action, cnt, total_seconds in rows:
        if action in BREAK_ACTIONS:
            grouped[action].append((full_name, cnt, total_seconds))

    emoji_map = {"吃饭": "🍚", "上厕所": "🚽", "抽烟": "🚬"}
    lines = [f"🏆 <b>{today_str()} 排行榜</b>\n"]

    for action in BREAK_ACTIONS:
        lines.append(f"{emoji_map[action]} <b>{action}排行榜</b>")
        ranking = sorted(grouped[action], key=lambda x: (-x[1], -x[2], x[0]))
        if not ranking:
            lines.append("暂无数据")
        else:
            for idx, (name, cnt, total_seconds) in enumerate(ranking[:10], start=1):
                lines.append(f"{idx}. {safe_text(name)} - {cnt} 次，{format_seconds(total_seconds)}")
        lines.append("")
    return "\n".join(lines).strip()


def format_attendance(rows):
    if not rows:
        return f"🕘 <b>{today_str()} 今日上下班</b>\n\n暂无记录。"

    lines = [f"🕘 <b>{today_str()} 今日上下班</b>\n"]
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


def format_me(full_name: str, per_action: dict, attendance):
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

    total_count = sum(per_action[a]["count"] for a in BREAK_ACTIONS)
    total_seconds = sum(per_action[a]["seconds"] for a in BREAK_ACTIONS)

    return (
        f"👤 <b>{safe_text(full_name)} 今日明细</b>\n\n"
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


def build_excel(chat_id: int) -> BytesIO:
    wb = Workbook()

    ws1 = wb.active
    ws1.title = "上下班日报"
    ws1.append(["姓名", "上班时间", "下班时间", "上班时长(秒)", "状态"])
    for cell in ws1[1]:
        cell.font = Font(bold=True)
        cell.alignment = Alignment(horizontal="center")

    for full_name, on_duty_time, off_duty_time, work_seconds, status in get_today_attendance_rows(chat_id):
        ws1.append([
            full_name,
            on_duty_time or "",
            off_duty_time or "",
            work_seconds or 0,
            "在岗" if status == "on" else "离岗/已下班",
        ])

    ws2 = wb.create_sheet("离岗明细")
    ws2.append(["姓名", "项目", "开始时间", "结束时间", "用时(秒)", "状态", "是否超时", "连续超时次数"])
    for cell in ws2[1]:
        cell.font = Font(bold=True)
        cell.alignment = Alignment(horizontal="center")

    for row in get_today_records(chat_id):
        full_name, action, start_time, end_time, duration_seconds, status, timed_out, overtime_count = row
        ws2.append([
            full_name,
            action,
            start_time,
            end_time or "",
            duration_seconds if duration_seconds is not None else "",
            status,
            "是" if timed_out else "否",
            overtime_count,
        ])

    ws3 = wb.create_sheet("个人汇总")
    ws3.append(["姓名", "项目", "次数", "总时长(秒)"])
    for cell in ws3[1]:
        cell.font = Font(bold=True)
        cell.alignment = Alignment(horizontal="center")

    for full_name, action, count, total_seconds in get_today_stats(chat_id):
        ws3.append([full_name, action, count, total_seconds])

    for ws in [ws1, ws2, ws3]:
        for col in ["A", "B", "C", "D", "E", "F", "G", "H"]:
            ws.column_dimensions[col].width = 20

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
# 定时提醒
# =========================
async def timeout_reminder_job(context: ContextTypes.DEFAULT_TYPE):
    data = context.job.data
    chat_id = data["chat_id"]
    user_id = data["user_id"]
    checkin_id = data["checkin_id"]
    action = data["action"]
    mention_name = data["mention_name"]
    timeout_minutes = data["timeout_minutes"]

    open_row = get_open_checkin(chat_id, user_id)
    if not open_row:
        return

    open_id, open_action, _, _ = open_row
    if open_id != checkin_id or open_action != action:
        return

    consecutive = get_user_consecutive_overtime(chat_id, user_id) + 1
    mark_timed_out(checkin_id, consecutive)

    await context.bot.send_message(
        chat_id=chat_id,
        text=(
            f"⏰ <b>超时提醒</b>\n\n"
            f"{safe_text(mention_name)}\n"
            f"项目：<b>{action}</b>\n"
            f"已超过 <b>{timeout_minutes}</b> 分钟，请尽快回座并发送：<b>回座 / BACK / done</b>"
        ),
        parse_mode=ParseMode.HTML,
    )

    if consecutive >= OVERTIME_ALERT_THRESHOLD:
        await context.bot.send_message(
            chat_id=chat_id,
            text=(
                f"🚨 <b>连续超时报警</b>\n\n"
                f"{safe_text(mention_name)}\n"
                f"已连续超时 <b>{consecutive}</b> 次，请管理注意。"
            ),
            parse_mode=ParseMode.HTML,
        )

    context.job_queue.run_once(
        repeated_timeout_reminder_job,
        when=REMINDER_REPEAT_MINUTES * 60,
        data=data,
        name=f"repeat_timeout_{chat_id}_{user_id}_{checkin_id}",
    )


async def repeated_timeout_reminder_job(context: ContextTypes.DEFAULT_TYPE):
    data = context.job.data
    chat_id = data["chat_id"]
    user_id = data["user_id"]
    checkin_id = data["checkin_id"]
    action = data["action"]
    mention_name = data["mention_name"]

    open_row = get_open_checkin(chat_id, user_id)
    if not open_row:
        return

    open_id, open_action, _, _ = open_row
    if open_id != checkin_id or open_action != action:
        return

    await context.bot.send_message(
        chat_id=chat_id,
        text=(
            f"🔔 <b>再次提醒</b>\n\n"
            f"{safe_text(mention_name)}\n"
            f"你的 <b>{action}</b> 仍未结束，请立即回座并发送：<b>回座 / BACK</b>"
        ),
        parse_mode=ParseMode.HTML,
    )

    context.job_queue.run_once(
        repeated_timeout_reminder_job,
        when=REMINDER_REPEAT_MINUTES * 60,
        data=data,
        name=f"repeat_timeout_{chat_id}_{user_id}_{checkin_id}_{int(datetime.now().timestamp())}",
    )


# =========================
# 命令
# =========================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ensure_default_settings(update.effective_chat.id)

    msg = (
        "<b>群成员打卡机器人</b>\n\n"
        "支持 Inline 按钮和中英文识别：\n"
        "• 上班 / On / clock in\n"
        "• 下班 / Off / clock out\n"
        "• 吃饭 / Eat / lunch\n"
        "• 上厕所 / Toilet / bathroom / wc\n"
        "• 抽烟 / Smoke / cigarette\n"
        "• 回座 / Back / done\n\n"
        f"上下班有效时间：<b>{WORK_START.strftime('%H:%M')} - {WORK_END.strftime('%H:%M')} (ET)</b>\n"
        "每天 0 点按美国东部时间自动切换新一天\n"
        "导出权限：<b>仅管理员</b>\n\n"
        "命令：\n"
        "/today 查看今日离岗统计\n"
        "/attendance 查看今日上下班\n"
        "/me 查看我自己的次数和时长\n"
        "/ranking 查看排行榜\n"
        "/status 查看当前设置\n"
        "/settimeout 吃饭 30\n"
        "/export 导出 Excel 日报\n"
        "/cancel 取消当前离岗打卡"
    )
    await send_reply(update, msg)


async def today_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    rows = get_today_stats(update.effective_chat.id)
    await send_reply(update, format_stats(rows))


async def attendance_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    rows = get_today_attendance_rows(update.effective_chat.id)
    await send_reply(update, format_attendance(rows))


async def me_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    user_id = update.effective_user.id
    full_name = update.effective_user.full_name or str(user_id)
    per_action, attendance = get_me_summary(chat_id, user_id)
    await send_reply(update, format_me(full_name, per_action, attendance))


async def ranking_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    rows = get_ranking(update.effective_chat.id)
    await send_reply(update, format_ranking(rows))


async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    ensure_default_settings(chat_id)

    lines = [
        "<b>当前配置</b>\n",
        "时区：America/New_York",
        f"上下班时间：{WORK_START.strftime('%H:%M')} - {WORK_END.strftime('%H:%M')}",
        f"群成员打卡：{'开启' if not ADMIN_ONLY else '关闭'}",
        f"导出仅管理员：{'开启' if EXPORT_ADMIN_ONLY else '关闭'}",
        f"连续超时报警阈值：{OVERTIME_ALERT_THRESHOLD} 次",
        f"重复提醒间隔：{REMINDER_REPEAT_MINUTES} 分钟\n",
        "离岗超时设置：",
    ]
    for action in BREAK_ACTIONS:
        lines.append(f"• {action}：{get_timeout(chat_id, action)} 分钟")

    await send_reply(update, "\n".join(lines))


async def settimeout_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if len(context.args) != 2:
        await send_reply(update, "用法：/settimeout 吃饭 30")
        return

    action = context.args[0].strip()
    minutes_text = context.args[1].strip()

    if action not in BREAK_ACTIONS:
        await send_reply(update, "项目只能是：吃饭 / 上厕所 / 抽烟")
        return

    if not minutes_text.isdigit() or int(minutes_text) <= 0:
        await send_reply(update, "分钟必须是大于 0 的数字")
        return

    set_timeout(update.effective_chat.id, action, int(minutes_text))
    await send_reply(update, f"✅ 已设置：{action} 超时 {minutes_text} 分钟")


async def export_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if EXPORT_ADMIN_ONLY and not await is_group_admin(update, context):
        await send_reply(update, "只有管理员可以导出日报。")
        return

    bio = build_excel(update.effective_chat.id)
    filename = f"打卡日报_{today_str()}.xlsx"

    if update.callback_query:
        await update.callback_query.message.reply_document(
            document=InputFile(bio, filename=filename),
            caption=f"📄 {today_str()} 打卡日报",
            reply_markup=get_main_inline_keyboard(),
        )
    else:
        await update.message.reply_document(
            document=InputFile(bio, filename=filename),
            caption=f"📄 {today_str()} 打卡日报",
            reply_markup=get_main_inline_keyboard(),
        )


async def cancel_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    user_id = update.effective_user.id

    open_row = get_open_checkin(chat_id, user_id)
    if not open_row:
        await send_reply(update, "你当前没有进行中的离岗打卡。")
        return

    checkin_id, action, _, _ = open_row
    cancel_checkin(checkin_id)
    await send_reply(update, f"已取消当前离岗打卡：{action}")


# =========================
# 核心业务逻辑
# =========================
async def process_action(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
    chat_id = update.effective_chat.id
    user = update.effective_user
    user_id = user.id
    username = user.username or ""
    full_name = user.full_name or str(user.id)
    mention_name = build_mention_name(user)

    ensure_default_settings(chat_id)

    if ADMIN_ONLY and not await is_group_admin(update, context):
        await send_reply(update, "当前群只允许管理员打卡。")
        return

    action = extract_action(text)

    # 回座
    if is_return_text(text):
        open_row = get_open_checkin(chat_id, user_id)
        if not open_row:
            await send_reply(update, "你当前没有进行中的离岗打卡。")
            return

        checkin_id, break_action, start_iso, _ = open_row
        end_dt = now_dt()
        seconds = diff_seconds(start_iso, end_dt)
        close_checkin(checkin_id, end_dt.isoformat(), seconds)

        await send_reply(
            update,
            (
                f"✅ <b>回座成功</b>\n\n"
                f"👤 {safe_text(full_name)}\n"
                f"📌 项目：{break_action}\n"
                f"🕒 本次用时：{format_seconds(seconds)}\n"
                f"📍 回座时间：{fmt_dt(end_dt)} (ET)"
            ),
        )
        return

    if not action:
        return

    # 上下班时间限制
    if action in ["上班", "下班"] and not in_working_hours():
        await send_reply(
            update,
            f"当前不在上下班打卡时间内。\n允许时间：{WORK_START.strftime('%H:%M')} - {WORK_END.strftime('%H:%M')} (ET)",
        )
        return

    # 上班
    if action == "上班":
        if is_on_duty(chat_id, user_id):
            await send_reply(update, "你今天已经是上班状态。")
            return

        now = now_dt()
        create_or_update_on_duty(chat_id, user_id, username, full_name, now.isoformat())
        await send_reply(
            update,
            (
                f"🟢 <b>上班打卡成功</b>\n\n"
                f"👤 {safe_text(full_name)}\n"
                f"🕒 上班时间：{fmt_dt(now)} (ET)"
            ),
        )
        return

    # 下班
    if action == "下班":
        if not is_on_duty(chat_id, user_id):
            await send_reply(update, "你当前不是上班状态，无法下班打卡。")
            return

        open_row = get_open_checkin(chat_id, user_id)
        if open_row:
            _, open_action, _, _ = open_row
            await send_reply(update, f"你还有未结束的离岗记录：{open_action}\n请先发送：回座 / BACK / done")
            return

        att_row = get_today_attendance(chat_id, user_id)
        now = now_dt()
        work_seconds = 0

        if att_row and att_row[1]:
            work_seconds = seconds_between(att_row[1], now.isoformat())

        set_off_duty(chat_id, user_id, now.isoformat(), work_seconds)

        per_action, total_break_count, total_break_seconds = get_user_today_break_summary(chat_id, user_id)

        await send_reply(
            update,
            (
                f"🔴 <b>下班打卡成功</b>\n\n"
                f"👤 {safe_text(full_name)}\n"
                f"🕒 下班时间：{fmt_dt(now)} (ET)\n"
                f"⏱ 今日上班时长：{format_seconds(work_seconds)}\n"
                f"📊 今日离岗总次数：{total_break_count} 次\n"
                f"🧾 吃饭：{per_action['吃饭']['count']} 次，{format_seconds(per_action['吃饭']['seconds'])}\n"
                f"🧾 上厕所：{per_action['上厕所']['count']} 次，{format_seconds(per_action['上厕所']['seconds'])}\n"
                f"🧾 抽烟：{per_action['抽烟']['count']} 次，{format_seconds(per_action['抽烟']['seconds'])}\n"
                f"⌛ 今日离岗总时长：{format_seconds(total_break_seconds)}"
            ),
        )
        return

    # 离岗项目
    if action in BREAK_ACTIONS:
        if not is_on_duty(chat_id, user_id):
            await send_reply(update, "请先上班打卡，再进行离岗打卡。")
            return

        open_row = get_open_checkin(chat_id, user_id)
        if open_row:
            _, open_action, _, _ = open_row
            await send_reply(update, f"你当前还有未结束打卡：{open_action}\n请先发送：回座 / BACK / done")
            return

        previous_count = get_today_action_count(chat_id, user_id, action)
        current_count = previous_count + 1

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
            period_day=today_str(),
        )

        context.job_queue.run_once(
            timeout_reminder_job,
            when=timeout_minutes * 60,
            data={
                "chat_id": chat_id,
                "user_id": user_id,
                "checkin_id": checkin_id,
                "action": action,
                "mention_name": mention_name,
                "timeout_minutes": timeout_minutes,
            },
            name=f"timeout_{chat_id}_{user_id}_{checkin_id}",
        )

        await send_reply(
            update,
            (
                f"🟡 <b>离岗打卡成功</b>\n\n"
                f"👤 {safe_text(full_name)}\n"
                f"📌 项目：{action}\n"
                f"🕒 开始时间：{fmt_dt(start_dt)} (ET)\n"
                f"🔢 你今天已经{action} {previous_count} 次了，本次是第 {current_count} 次\n"
                f"⏰ 超时提醒：{timeout_minutes} 分钟\n\n"
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
        if action == "回座":
            await process_action(update, context, "回座")
        else:
            await process_action(update, context, action)
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

    if data == "cmd:ranking":
        await ranking_command(update, context)
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
    app = Application.builder().token(BOT_TOKEN).defaults(defaults).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("today", today_command))
    app.add_handler(CommandHandler("attendance", attendance_command))
    app.add_handler(CommandHandler("me", me_command))
    app.add_handler(CommandHandler("ranking", ranking_command))
    app.add_handler(CommandHandler("status", status_command))
    app.add_handler(CommandHandler("settimeout", settimeout_command))
    app.add_handler(CommandHandler("export", export_command))
    app.add_handler(CommandHandler("cancel", cancel_command))
    app.add_handler(CallbackQueryHandler(button_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))

    logger.info("Enterprise checkin bot is running...")
    app.run_polling()


if __name__ == "__main__":
    main()