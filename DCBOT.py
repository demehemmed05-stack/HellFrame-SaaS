# ====================================================================
# 🔥 HELLFRAME QUANT ENGINE v5.0.1 (Güvenli .env Yükleme + Katı Doğrulama)
# Global SaaS Discord Trade Bot – İngilizce Çıktı, Türkçe Yorum
# ====================================================================
# GÜNCELLEME: .env yüklemesi güçlendirildi, kritik değişkenler için
#             zorunlu kontrol ve os._exit(1) ile güvenli kapatma eklendi.
#             YOUR_SERVER_ID, rol ID'leri artık .env'den okunuyor.
# ====================================================================

import os, json, asyncio, logging, difflib, re, sqlite3
from pathlib import Path
from asyncio import Lock, Semaphore
from threading import Thread
from typing import Optional, Dict, Any, Union, List, Set, Tuple
from datetime import datetime, timezone, timedelta

import discord
from discord.ext import commands, tasks
from discord.ui import View, Button
from dotenv import load_dotenv
from solana.rpc.async_api import AsyncClient
from solders.pubkey import Pubkey
import yfinance as yf
import pandas as pd
import numpy as np
from flask import Flask, request, jsonify
import stripe

# ====================================================================
# 🔐 .env YÜKLEME VE SIKI GÜVENLİK KONTROLÜ (GÜNCELLENDİ)
# ====================================================================
load_dotenv(override=True)

# Ana değişkenleri .env'den çek
TOKEN = os.getenv("DISCORD_BOT_TOKEN")
SOLANA_RPC_URLS = [u.strip() for u in os.getenv("SOLANA_RPC_URLS", "").split(",") if u.strip()]
MY_WALLET_STR = os.getenv("SOLANA_WALLET_ADDRESS")
ADMIN_ID_RAW = os.getenv("ADMIN_ID")
STRIPE_SECRET_KEY = os.getenv("STRIPE_SECRET_KEY")
STRIPE_WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET")
PROXY_URL = os.getenv("PROXY_URL")
YOUR_SERVER_ID = os.getenv("YOUR_SERVER_ID")
OMEGA_ROLE_ID = os.getenv("OMEGA_ROLE_ID")
VECTOR_ROLE_ID = os.getenv("VECTOR_ROLE_ID")
ALPHA_ROLE_ID = os.getenv("ALPHA_ROLE_ID")

# Stripe Price ID'leri (opsiyonel)
STRIPE_PRICE_DAILY = os.getenv("STRIPE_PRICE_DAILY")
STRIPE_PRICE_WEEKLY = os.getenv("STRIPE_PRICE_WEEKLY")
STRIPE_PRICE_MONTHLY = os.getenv("STRIPE_PRICE_MONTHLY")

# Kritik değişkenlerin tanımlı olduğunu doğrula
REQUIRED_VARS = {
    "DISCORD_BOT_TOKEN": TOKEN,
    "YOUR_SERVER_ID": YOUR_SERVER_ID,
    "OMEGA_ROLE_ID": OMEGA_ROLE_ID,
    "VECTOR_ROLE_ID": VECTOR_ROLE_ID,
    "ALPHA_ROLE_ID": ALPHA_ROLE_ID,
}

missing_vars = [key for key, value in REQUIRED_VARS.items() if not value]

if missing_vars:
    print(f"[🚨 SECURITY ERROR] Missing essential .env variables: {', '.join(missing_vars)}")
    os._exit(1)

# Kritik ID'leri tam sayıya çevir
YOUR_SERVER_ID = int(YOUR_SERVER_ID)
OMEGA_ROLE_ID = int(OMEGA_ROLE_ID)
VECTOR_ROLE_ID = int(VECTOR_ROLE_ID)
ALPHA_ROLE_ID = int(ALPHA_ROLE_ID)

# Opsiyonel olanları dönüştür (None kalabilir)
if ADMIN_ID_RAW:
    try:
        ADMIN_ID = int(ADMIN_ID_RAW)
    except ValueError:
        print("[🚨 SECURITY ERROR] ADMIN_ID must be a numeric Discord user ID.")
        os._exit(1)
else:
    ADMIN_ID = 0  # Fallback, fakat is_admin fonksiyonu çalışmaz

if MY_WALLET_STR:
    MY_WALLET = Pubkey.from_string(MY_WALLET_STR)
else:
    MY_WALLET = None  # SOL ödemeleri çalışmaz

if STRIPE_SECRET_KEY:
    stripe.api_key = STRIPE_SECRET_KEY

# Solana RPC URL'leri boş ise varsayılan ekle
if not SOLANA_RPC_URLS:
    SOLANA_RPC_URLS = ["https://api.mainnet-beta.solana.com"]

# ====================================================================
# 📊 LOGGING
# ====================================================================
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
                    handlers=[logging.FileHandler("bot_production.log", encoding="utf-8"), logging.StreamHandler()])
logger = logging.getLogger("HellFrameQuant")

# ====================================================================
# 🗄️ SQLITE VERİTABANI
# ====================================================================
DB_FILE = "hellframe_licenses.db"

def init_db():
    with sqlite3.connect(DB_FILE) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS used_txids (
                txid TEXT PRIMARY KEY
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS licenses (
                discord_id TEXT PRIMARY KEY,
                role_id INTEGER,
                expire_time TEXT
            )
        """)
        conn.commit()
init_db()

def db_add_txid(txid: str) -> bool:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.execute("INSERT INTO used_txids VALUES (?)", (txid,))
            conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False

def db_has_txid(txid: str) -> bool:
    with sqlite3.connect(DB_FILE) as conn:
        row = conn.execute("SELECT 1 FROM used_txids WHERE txid = ?", (txid,)).fetchone()
        return row is not None

def db_get_license(discord_id: str):
    with sqlite3.connect(DB_FILE) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT * FROM licenses WHERE discord_id = ?", (discord_id,)).fetchone()
        return dict(row) if row else None

def db_set_license(discord_id: str, role_id: int, expire_time: str):
    with sqlite3.connect(DB_FILE) as conn:
        conn.execute("""
            INSERT OR REPLACE INTO licenses (discord_id, role_id, expire_time)
            VALUES (?, ?, ?)
        """, (discord_id, role_id, expire_time))
        conn.commit()

def db_delete_license(discord_id: str):
    with sqlite3.connect(DB_FILE) as conn:
        conn.execute("DELETE FROM licenses WHERE discord_id = ?", (discord_id,))
        conn.commit()

def db_get_all_licenses():
    with sqlite3.connect(DB_FILE) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute("SELECT * FROM licenses").fetchall()
        return [dict(r) for r in rows]

# ====================================================================
# 📁 DİĞER VERİLER (JSON)
# ====================================================================
DATA_FILE, ALERTS_FILE = "user_subscriptions.json", "price_alerts.json"
DB_LOCK, ALERT_LOCK = Lock(), Lock()
analysis_semaphore = Semaphore(5)

PLAN_CONFIG = {
    "daily":   (ALPHA_ROLE_ID,  1,  3.0),
    "weekly":  (VECTOR_ROLE_ID, 7,  18.0),
    "monthly": (OMEGA_ROLE_ID,  30, 54.0),
}

PLAN_ASSET_LIMIT = {"daily": 1, "weekly": 2, "monthly": 3}
PLAN_BENEFITS = {
    "daily": "Standard daily rate.",
    "weekly": "6+1 Deal: Pay for 6 days, get 7 days! Save $3 vs daily.",
    "monthly": "3+1 Deal: Pay for 3 weeks, get 4 weeks! Save $18 vs weekly."
}

def number_emoji(num: int) -> str:
    emoji_map = {'0':'0️⃣','1':'1️⃣','2':'2️⃣','3':'3️⃣','4':'4️⃣','5':'5️⃣','6':'6️⃣','7':'7️⃣','8':'8️⃣','9':'9️⃣'}
    return ''.join(emoji_map.get(d, d) for d in str(num))

RSI_OVERSOLD, RSI_OVERBOUGHT = 30.0, 70.0
EMA_FAST, EMA_SLOW = 50, 200
SIGNAL_COOLDOWN = 3600

def ensure_files():
    for f, d in [(DATA_FILE, {}), (ALERTS_FILE, {})]:
        if not os.path.exists(f):
            with open(f, "w", encoding="utf-8") as fh:
                json.dump(d, fh)
ensure_files()

def load_data():
    try:
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except: return {}

async def save_data(data):
    async with DB_LOCK:
        t = f"{DATA_FILE}.tmp"
        try:
            with open(t, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=4, ensure_ascii=False)
            os.replace(t, DATA_FILE)
        except Exception as e:
            logger.error(f"DB save error: {e}")

def load_alerts():
    try:
        with open(ALERTS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except: return {}

async def save_alerts(data):
    async with ALERT_LOCK:
        t = f"{ALERTS_FILE}.tmp"
        try:
            with open(t, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=4, ensure_ascii=False)
            os.replace(t, ALERTS_FILE)
        except Exception as e:
            logger.error(f"Alert save error: {e}")

def is_admin(ctx):
    return ctx.author.id == ADMIN_ID

# ====================================================================
# 📈 PİYASA VERİSİ
# ====================================================================
CRYPTO_MAP = {"BTC":"BTC-USD","ETH":"ETH-USD","SOL":"SOL-USD","XRP":"XRP-USD","DOGE":"DOGE-USD","ADA":"ADA-USD","AVAX":"AVAX-USD","DOT":"DOT-USD","MATIC":"MATIC-USD","LINK":"LINK-USD","LTC":"LTC-USD","BNB":"BNB-USD"}
COMMODITY_MAP = {"GOLD":"GC=F","SILVER":"SI=F","OIL":"CL=F","COPPER":"HG=F","NATGAS":"NG=F","BRENT":"BZ=F"}
FX_MAP = {"EURUSD":"EURUSD=X","GBPUSD":"GBPUSD=X","USDJPY":"USDJPY=X","USDTRY":"USDTRY=X","EURTRY":"EURTRY=X","AUDUSD":"AUDUSD=X","USDCHF":"USDCHF=X","NZDUSD":"NZDUSD=X","USDCAD":"USDCAD=X"}

ALIASES = {"XAU":"GOLD","XAG":"SILVER","WTI":"OIL","EUR":"EURUSD","EURO":"EURUSD","GBP":"GBPUSD","TRY":"USDTRY","LIRA":"USDTRY","TL":"USDTRY","JPY":"USDJPY","CHF":"USDCHF","AUD":"AUDUSD","CAD":"USDCAD","NZD":"NZDUSD","BNB":"BNB-USD","BTC":"BTC-USD","ETH":"ETH-USD","XRP":"XRP-USD"}

def resolve_alias(ticker):
    t = ticker.upper().strip()
    if t in ALIASES: return ALIASES[t]
    if "-USD" in t or "=X" in t or "=F" in t: return t
    return CRYPTO_MAP.get(t) or COMMODITY_MAP.get(t) or FX_MAP.get(t) or t

def normalize(t):
    t = resolve_alias(t)
    if "-USD" in t or "=X" in t or "=F" in t: return t
    return CRYPTO_MAP.get(t) or COMMODITY_MAP.get(t) or FX_MAP.get(t) or t

def get_price(t):
    try:
        h = yf.Ticker(normalize(t)).history(period="1d", interval="1m")
        return float(h["Close"].iloc[-1]) if not h.empty else None
    except: return None

def get_sol_price():
    return get_price("SOL")

def fetch_ohlcv(t, period="5d", interval="15m"):
    try:
        df = yf.download(normalize(t), period=period, interval=interval, progress=False, auto_adjust=True)
        return df if not df.empty and len(df) >= 50 else None
    except: return None

def rsi(s, p=14):
    d = s.diff(); g = d.clip(lower=0); l = -d.clip(upper=0)
    ag = g.ewm(com=p-1, min_periods=p).mean(); al = l.ewm(com=p-1, min_periods=p).mean()
    return (100 - (100 / (1 + ag / al.replace(0, np.nan)))).fillna(50)

def macd(s, f=12, sl=26, sg=9):
    ef = s.ewm(span=f, adjust=False).mean(); es = s.ewm(span=sl, adjust=False).mean()
    m = ef - es; sig = m.ewm(span=sg, adjust=False).mean()
    return m, sig, m - sig

def bb(s, p=20, n=2.0):
    mid = s.rolling(window=p).mean(); std = s.rolling(window=p).std()
    return mid + n*std, mid, mid - n*std

def ema(s, p):
    return s.ewm(span=p, adjust=False).mean()

def analyze(t):
    df = fetch_ohlcv(t)
    if df is None: return None
    c = df["Close"].squeeze()
    r = rsi(c); m, sig, h = macd(c); u, mid, l = bb(c); e50 = ema(c, 50); e200 = ema(c, 200)
    lp = float(c.iloc[-1]); lr = float(r.iloc[-1]); lm = float(m.iloc[-1])
    lsig = float(sig.iloc[-1]); lh = float(h.iloc[-1]); ph = float(h.iloc[-2])
    lu = float(u.iloc[-1]); ll = float(l.iloc[-1]); le50 = float(e50.iloc[-1])
    le200 = float(e200.iloc[-1]); pe50 = float(e50.iloc[-2]); pe200 = float(e200.iloc[-2])
    bull = le50 > le200; gold = pe50 <= pe200 and le50 > le200; death = pe50 >= pe200 and le50 < le200
    bbpos = (lp - ll) / (lu - ll) if lu != ll else 0.5
    score, conf, reasons = 0, 0, []
    if bull: score += 2; conf += 1; reasons.append("Bullish Trend (EMA50 > EMA200)")
    else: score -= 2; conf += 1; reasons.append("Bearish Trend (EMA50 < EMA200)")
    if gold: score += 3; conf += 2; reasons.append("Golden Cross Detected")
    if death: score -= 3; conf += 2; reasons.append("Death Cross Detected")
    if lr < RSI_OVERSOLD: score += 2 if bull else 1; conf += 1; reasons.append(f"RSI Oversold ({lr:.1f})")
    elif lr > RSI_OVERBOUGHT: score -= 2 if not bull else 1; conf += 1; reasons.append(f"RSI Overbought ({lr:.1f})")
    else: reasons.append(f"RSI Neutral ({lr:.1f})")
    if lh > 0 and ph <= 0: score += 2; conf += 1; reasons.append("MACD Bullish Cross")
    elif lh < 0 and ph >= 0: score -= 2; conf += 1; reasons.append("MACD Bearish Cross")
    elif lh > 0: score += 1; reasons.append("MACD Positive Momentum")
    else: score -= 1; reasons.append("MACD Negative Momentum")
    if bbpos < 0.15: score += 1; reasons.append("Price near lower Bollinger Band")
    elif bbpos > 0.85: score -= 1; reasons.append("Price near upper Bollinger Band")
    if score >= 5: sig, key = "🟢 STRONG BUY", "STRONG_BUY"
    elif score >= 3: sig, key = "🟢 BUY", "BUY"
    elif score <= -5: sig, key = "🔴 STRONG SELL", "STRONG_SELL"
    elif score <= -3: sig, key = "🔴 SELL", "SELL"
    else: sig, key = "🟡 HOLD", "HOLD"
    return {"ticker":t,"price":lp,"rsi":lr,"macd":lm,"macd_signal":lsig,"macd_hist":lh,
            "ema50":le50,"ema200":le200,"golden_cross":gold,"death_cross":death,
            "bb_upper":lu,"bb_middle":mid.iloc[-1],"bb_lower":ll,
            "score":score,"confidence":conf,"signal":sig,"signal_key":key,"reasons":reasons}

# ====================================================================
# 🔗 SOLANA DOĞRULAMA (SQLite + Rol Atama Entegre)
# ====================================================================
async def verify_solana_payment(ctx, tx_id: str, plan: str) -> bool:
    if not MY_WALLET:
        await ctx.send("❌ Solana wallet not configured.")
        return False
    if not tx_id or len(tx_id) < 32:
        await ctx.send("❌ Invalid transaction ID.")
        return False
    if db_has_txid(tx_id):
        await ctx.send("🚫 **Double-Spending Alert:** This transaction ID has already been used!")
        return False

    _, _, usd_price = PLAN_CONFIG.get(plan, (None, None, None))
    if usd_price is None:
        await ctx.send("❌ Invalid plan.")
        return False

    sol_price = get_sol_price()
    if sol_price is None or sol_price <= 0:
        await ctx.send("⏳ Could not fetch SOL price. Try again later.")
        return False

    required_sol = usd_price / sol_price

    for rpc in SOLANA_RPC_URLS:
        try:
            async with AsyncClient(rpc) as c:
                resp = await asyncio.wait_for(
                    c.get_transaction(tx_id, encoding="json", max_supported_transaction_version=0), 10
                )
                if not resp or not resp.value:
                    continue
                meta = resp.value.meta
                if meta and meta.err:
                    await ctx.send("❌ Transaction failed on-chain.")
                    return False

                keys = resp.value.transaction.message.account_keys
                if MY_WALLET not in keys:
                    await ctx.send("❌ Payment not sent to the correct wallet.")
                    return False

                idx = keys.index(MY_WALLET)
                received_sol = (meta.post_balances[idx] - meta.pre_balances[idx]) / 1_000_000_000

                if abs(received_sol - required_sol) > 0.0001:
                    received_usd = received_sol * sol_price
                    await ctx.send(
                        f"❌ Incorrect amount received.\n"
                        f"You sent: **{received_sol:.4f} SOL** (≈ **${received_usd:.2f}**).\n"
                        f"Required: **${usd_price:.2f}** (≈ **{required_sol:.4f} SOL**)."
                    )
                    return False

                if not db_add_txid(tx_id):
                    await ctx.send("🚫 Transaction ID already used (race condition).")
                    return False

                role_id, days, _ = PLAN_CONFIG[plan]
                now = datetime.now(timezone.utc)
                expire = now + timedelta(days=days)
                db_set_license(str(ctx.author.id), role_id, expire.strftime('%Y-%m-%d %H:%M:%S'))

                guild = bot.get_guild(YOUR_SERVER_ID)
                if guild:
                    member = guild.get_member(ctx.author.id)
                    if member:
                        role = guild.get_role(role_id)
                        if role:
                            await member.add_roles(role)
                            logger.info(f"Rol verildi: {member.display_name} → {role.name}")

                embed = discord.Embed(
                    title="✅ Payment Verified & License Activated",
                    description=f"Your **{plan.capitalize()}** plan is now active.",
                    color=discord.Color.green()
                )
                embed.add_field(name="Transaction ID", value=f"`{tx_id}`", inline=False)
                embed.add_field(name="Expiry Date", value=expire.strftime('%Y-%m-%d %H:%M UTC'), inline=True)
                embed.add_field(name="Role", value=f"<@&{role_id}>", inline=True)
                embed.set_footer(text="HellFrame Quant Engine v5.0.1")
                await ctx.send(embed=embed)
                return True

        except asyncio.TimeoutError:
            continue
        except Exception as e:
            logger.error(f"RPC error {rpc}: {e}")
            continue

    await ctx.send("⏳ All RPC nodes unresponsive. Admin will check manually.")
    return False

# ====================================================================
# 💳 STRIPE
# ====================================================================
def create_stripe_session(uid, plan):
    if not STRIPE_SECRET_KEY: return None
    try:
        _, _, usd_price = PLAN_CONFIG[plan]
        session = stripe.checkout.Session.create(
            payment_method_types=['card'],
            line_items=[{'price_data': {
                'currency': 'usd',
                'product_data': {'name': f'HellFrame - {plan.capitalize()}'},
                'unit_amount': int(usd_price * 100),
                'recurring': {'interval': 'day'} if plan=='daily' else {'interval': 'week'} if plan=='weekly' else {'interval': 'month'}
            }, 'quantity': 1}],
            mode='subscription',
            metadata={'user_id': uid, 'plan': plan},
            success_url='https://discord.com/channels/@me',
            cancel_url='https://discord.com/channels/@me'
        )
        return session.url
    except Exception as e:
        logger.error(f"Stripe session error: {e}")
        return None

# ====================================================================
# 🌐 FLASK
# ====================================================================
app = Flask(__name__)
@app.route('/')
def home(): return "HellFrame Quant Engine v5.0.1 online!"

@app.route('/webhook', methods=['POST'])
def webhook():
    if not STRIPE_WEBHOOK_SECRET: return jsonify({'err':'no secret'}), 500
    p = request.get_data(as_text=True); sig = request.headers.get('Stripe-Signature')
    try: ev = stripe.Webhook.construct_event(p, sig, STRIPE_WEBHOOK_SECRET)
    except: return jsonify({'err':'invalid'}), 400
    if ev['type'] == 'invoice.paid':
        obj = ev['data']['object']
        uid, plan = obj.get('metadata',{}).get('user_id'), obj.get('metadata',{}).get('plan')
        if uid and plan:
            role_id, days, _ = PLAN_CONFIG[plan]
            expire = (datetime.now(timezone.utc) + timedelta(days=days)).strftime('%Y-%m-%d %H:%M:%S')
            db_set_license(uid, role_id, expire)
            guild = bot.get_guild(YOUR_SERVER_ID)
            if guild:
                member = guild.get_member(int(uid))
                if member:
                    role = guild.get_role(role_id)
                    if role: asyncio.run_coroutine_threadsafe(member.add_roles(role), bot.loop)
    return jsonify({'ok':True}), 200

def run_flask():
    app.run(host='0.0.0.0', port=int(os.getenv("PORT",8080)), debug=False)

# ====================================================================
# 🤖 BOT
# ====================================================================
intents = discord.Intents.default(); intents.message_content = True; intents.members = True
if PROXY_URL:
    proxy = discord.Proxy(url=PROXY_URL, proxy_type=discord.ProxyType.http)
    bot = commands.Bot(command_prefix="!", intents=intents, case_insensitive=True, help_command=None, proxy=proxy)
else:
    bot = commands.Bot(command_prefix="!", intents=intents, case_insensitive=True, help_command=None)

@bot.event
async def on_ready():
    logger.info(f"Online: {bot.user}")
    Thread(target=run_flask, daemon=True).start()
    for loop in [check_user_alerts, check_price_alerts, check_intervals, check_reminders, cleanup_expired_roles]:
        if not loop.is_running(): loop.start()
    await bot.change_presence(activity=discord.Activity(type=discord.ActivityType.watching, name="EMA · RSI · MACD · BB"))

def build_embed(r):
    cmap = {"BUY":discord.Color.green(),"SELL":discord.Color.red(),"HOLD":discord.Color.yellow(),"STRONG_BUY":discord.Color.dark_green(),"STRONG_SELL":discord.Color.dark_red()}
    e = discord.Embed(title=f"{r['signal']} | {r['ticker']}", color=cmap.get(r["signal_key"], discord.Color.blurple()))
    e.add_field(name="💰 Price", value=f"**${r['price']:,.4f}**", inline=True)
    e.add_field(name="📊 RSI", value=f"`{r['rsi']:.2f}`", inline=True)
    e.add_field(name="🎯 Confidence", value=f"`{r['confidence']} / 3`", inline=True)
    e.add_field(name="📈 EMA50/200", value=f"EMA50: `${r['ema50']:,.2f}`\nEMA200: `${r['ema200']:,.2f}`", inline=False)
    e.add_field(name="📉 MACD", value=f"MACD: `{r['macd']:.4f}`  Signal: `{r['macd_signal']:.4f}`  Hist: `{r['macd_hist']:.4f}`", inline=False)
    e.add_field(name="🧠 Analysis", value="\n".join(f"• {x}" for x in r["reasons"]), inline=False)
    e.set_footer(text="HellFrame Quant Engine v5.0.1")
    return e

# ====================================================================
# 🔄 ARKAPLAN DÖNGÜLERİ
# ====================================================================
@tasks.loop(seconds=60)
async def check_user_alerts():
    db = load_data()
    if not hasattr(check_user_alerts, "last"): check_user_alerts.last = {}
    now = datetime.now(timezone.utc).timestamp()
    tasks = []
    for uid, p in db.items():
        if p.get("status") != "Active": continue
        for t in p.get("assets", []):
            tasks.append(asyncio.create_task(process_asset(uid, t, f"{uid}:{t}:", now)))
    if tasks: await asyncio.gather(*tasks)

async def process_asset(uid, ticker, base, now):
    async with analysis_semaphore:
        try:
            r = await asyncio.get_event_loop().run_in_executor(None, analyze, ticker)
            if not r or r["signal_key"] == "HOLD": return
            key = f"{base}{r['signal_key']}"
            if now - check_user_alerts.last.get(key, 0) < SIGNAL_COOLDOWN: return
            u = await bot.fetch_user(int(uid))
            if u:
                await u.send(embed=build_embed(r))
                check_user_alerts.last[key] = now
        except: pass

@tasks.loop(seconds=30)
async def check_price_alerts():
    alerts = load_alerts()
    if not alerts: return
    trig = {}
    for uid, alist in alerts.items():
        idxs = []
        for i, a in enumerate(alist):
            pr = get_price(a.get("ticker"))
            if pr and ((a["direction"]=="above" and pr>=a["price"]) or (a["direction"]=="below" and pr<=a["price"])):
                try:
                    u = await bot.fetch_user(int(uid))
                    if u: await u.send(embed=discord.Embed(title="🚨 Price Alert", description=f"**{a['ticker']}** is now **${pr:,.4f}**", color=discord.Color.orange()))
                except: pass
                idxs.append(i)
        if idxs: trig[uid] = idxs
    if trig:
        for uid, idxs in trig.items():
            if uid in alerts:
                for i in sorted(idxs, reverse=True):
                    if i < len(alerts[uid]): del alerts[uid][i]
                if not alerts[uid]: del alerts[uid]
        await save_alerts(alerts)

@tasks.loop(seconds=60)
async def check_intervals():
    db = load_data()
    if not hasattr(check_intervals, "last_sent"): check_intervals.last_sent = {}
    now = datetime.now(timezone.utc)
    for uid, profile in db.items():
        if profile.get("status") != "Active": continue
        intervals = profile.get("intervals", {})
        if not intervals: continue
        for ticker, mins in intervals.items():
            try: mins = max(1, int(mins))
            except: continue
            key = f"{uid}:{ticker}"
            last_time = check_intervals.last_sent.get(key, datetime.min.replace(tzinfo=timezone.utc))
            if now - last_time >= timedelta(minutes=mins):
                price = get_price(ticker)
                if price:
                    try:
                        u = await bot.fetch_user(int(uid))
                        if u:
                            embed = discord.Embed(title="⏰ Periodic Price Update", description=f"**{ticker}** is now **${price:,.4f}**", color=discord.Color.blue(), timestamp=now)
                            embed.set_footer(text=f"Interval: every {mins} minute(s)")
                            await u.send(embed=embed)
                            check_intervals.last_sent[key] = now
                    except: pass

@tasks.loop(seconds=300)
async def check_reminders():
    db = load_data(); now = datetime.now(timezone.utc); sent_key = "reminder_sent"
    for uid, p in db.items():
        if p.get("status") != "Active": continue
        try: exp = datetime.fromisoformat(p["expiry"])
        except: continue
        rem = exp - now; plan = p.get("plan","daily")
        if (plan == "daily" and timedelta(0) < rem <= timedelta(hours=1)) or (plan != "daily" and timedelta(0) < rem <= timedelta(hours=24)):
            if not p.get(sent_key):
                try:
                    u = await bot.fetch_user(int(uid))
                    if u:
                        emb = discord.Embed(title="⏳ Subscription Renewal", description=f"Your **{plan.capitalize()}** plan expires in {rem}.", color=discord.Color.gold())
                        await u.send(embed=emb)
                        db[uid][sent_key] = True
                        await save_data(db)
                except: pass

# ====================================================================
# ⏰ OTTOMATİK SÜRE KONTROLÜ (HER 30 DAKİKADA BİR)
# ====================================================================
@tasks.loop(minutes=30)
async def cleanup_expired_roles():
    now = datetime.now(timezone.utc)
    expired = []
    for lic in db_get_all_licenses():
        expire_time = datetime.strptime(lic['expire_time'], '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
        if now >= expire_time:
            expired.append(lic)

    for lic in expired:
        discord_id = lic['discord_id']
        role_id = lic['role_id']

        guild = bot.get_guild(YOUR_SERVER_ID)
        if guild:
            member = guild.get_member(int(discord_id))
            if member:
                role = guild.get_role(role_id)
                if role and role in member.roles:
                    await member.remove_roles(role)
                    logger.info(f"Rol kaldırıldı: {member.display_name} → {role.name}")

        try:
            user = await bot.fetch_user(int(discord_id))
            if user:
                embed = discord.Embed(
                    title="⌛ License Expired",
                    description="Your premium access has ended. The corresponding role has been removed.",
                    color=discord.Color.red()
                )
                embed.add_field(name="Next Step", value="To renew, use `!subscribe` or contact support.")
                embed.set_footer(text="HellFrame Quant Engine v5.0.1")
                await user.send(embed=embed)
        except:
            pass

        db_delete_license(discord_id)

    if expired:
        logger.info(f"Cleaned up {len(expired)} expired license(s).")

# ====================================================================
# 📚 KOMUT YARDIM
# ====================================================================
COMMAND_HELP = {
    "ping": {"desc":"Check bot latency.","use":"!ping","ex":"!ping"},
    "info": {"desc":"Show bot information.","use":"!info","ex":"!info"},
    "mystatus": {"desc":"Check your subscription status.","use":"!mystatus","ex":"!mystatus"},
    "plans": {"desc":"View subscription plans.","use":"!plans","ex":"!plans"},
    "subscribe": {"desc":"View plans or subscribe.","use":"!subscribe [plan]","ex":"!subscribe daily"},
    "verify": {"desc":"Verify SOL payment & get role.","use":"!verify <tx_id> <plan>","ex":"!verify ABC123 daily"},
    "price": {"desc":"Live price of a ticker.","use":"!price <ticker>","ex":"!price XAU"},
    "analyze": {"desc":"Full technical analysis.","use":"!analyze <ticker>","ex":"!analyze SOL"},
    "addasset": {"desc":"Add to watchlist.","use":"!addasset <ticker>","ex":"!addasset GOLD"},
    "removeasset": {"desc":"Remove from watchlist.","use":"!removeasset <ticker>","ex":"!removeasset BTC"},
    "myassets": {"desc":"Show your watchlist.","use":"!myassets","ex":"!myassets"},
    "addinterval": {"desc":"Periodic price updates.","use":"!addinterval <ticker> <minutes>","ex":"!addinterval ETH 5"},
    "removeinterval": {"desc":"Stop price updates.","use":"!removeinterval <ticker>","ex":"!removeinterval ETH"},
    "myintervals": {"desc":"List active intervals.","use":"!myintervals","ex":"!myintervals"},
    "help": {"desc":"Show this help.","use":"!help [command]","ex":"!help analyze"}
}

@bot.command(name="help")
async def help_command(ctx, *, command_name: str = None):
    if command_name:
        cmd = command_name.lower().strip().lstrip("!")
        if cmd in COMMAND_HELP:
            info = COMMAND_HELP[cmd]
            embed = discord.Embed(title=f"📖 `!{cmd}`", description=info["desc"], color=discord.Color.blue())
            embed.add_field(name="Usage", value=f"`{info['use']}`", inline=False)
            embed.add_field(name="Example", value=f"`{info['ex']}`", inline=False)
            await ctx.send(embed=embed)
        else:
            matches = difflib.get_close_matches(cmd, list(COMMAND_HELP.keys()), n=1, cutoff=0.5)
            if matches: await ctx.send(f"❓ Did you mean `!{matches[0]}`?")
            else: await ctx.send(f"❓ Unknown command. Use `!help`.")
    else:
        embed = discord.Embed(title="📚 Command List", description="Use `!help <command>` for details.", color=discord.Color.gold())
        for cmd, info in COMMAND_HELP.items():
            embed.add_field(name=f"`!{cmd}`", value=info["desc"], inline=False)
        await ctx.send(embed=embed)

@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, commands.MissingRequiredArgument):
        cmd = ctx.command.name if ctx.command else "unknown"
        if cmd in COMMAND_HELP:
            await ctx.send(f"❌ Missing argument.\n**Usage:** `{COMMAND_HELP[cmd]['use']}`")
    elif isinstance(error, commands.CommandNotFound):
        wrong = ctx.message.content.split()[0].lstrip("!").lower()
        all_cmds = [c.name for c in bot.commands] + list(COMMAND_HELP.keys())
        matches = difflib.get_close_matches(wrong, list(set(all_cmds)), n=1, cutoff=0.5)
        if matches: await ctx.send(f"❓ `!{wrong}` not found. Did you mean `!{matches[0]}`?")
        else: await ctx.send(f"❓ `!{wrong}` not found. Use `!help`.")
    elif isinstance(error, commands.CheckFailure):
        await ctx.send("🔒 You need an active subscription.")
    else:
        logger.error(f"Unhandled error: {error}")
        await ctx.send("❌ An unexpected error occurred.")

# ====================================================================
# 🧪 KOMUTLAR
# ====================================================================
@bot.command()
async def ping(ctx): await ctx.send(f"Pong! {round(bot.latency*1000)}ms")

@bot.command()
async def info(ctx):
    embed = discord.Embed(title="🤖 HellFrame Quant Engine", description="Advanced trading analysis bot.", color=discord.Color.blurple())
    embed.add_field(name="Version", value="v5.0.1", inline=True)
    embed.add_field(name="Plans", value="Daily $3 | Weekly $18 | Monthly $54", inline=False)
    await ctx.send(embed=embed)

@bot.command()
async def mystatus(ctx):
    lic = db_get_license(str(ctx.author.id))
    if not lic:
        await ctx.send("❌ No active license. Use `!subscribe`.")
        return
    expire = datetime.strptime(lic['expire_time'], '%Y-%m-%d %H:%M:%S')
    now = datetime.now()
    remaining = expire - now
    embed = discord.Embed(title="📊 Your License", color=discord.Color.green())
    embed.add_field(name="Role", value=f"<@&{lic['role_id']}>", inline=True)
    embed.add_field(name="Expiry", value=expire.strftime('%Y-%m-%d %H:%M UTC'), inline=True)
    embed.add_field(name="Remaining", value=str(remaining).split('.')[0])
    await ctx.send(embed=embed)

@bot.command(name="plans")
async def plans_cmd(ctx):
    embed = discord.Embed(title="💎 Subscription Plans", color=discord.Color.gold())
    for plan, (role_id, days, price) in PLAN_CONFIG.items():
        benefit = PLAN_BENEFITS.get(plan, "")
        embed.add_field(name=f"{plan.capitalize()} – ${int(price)}", value=f"{benefit}\nRole: <@&{role_id}>", inline=False)
    embed.add_field(name="How to Subscribe", value="Use `!subscribe daily`, `!subscribe weekly`, or `!subscribe monthly`.")
    await ctx.send(embed=embed)

@bot.command()
async def subscribe(ctx, *, plan=None):
    if plan not in PLAN_CONFIG:
        await plans_cmd(ctx)
        return
    _, _, usd_price = PLAN_CONFIG[plan]
    sol_price = get_sol_price()
    emb = discord.Embed(title=f"Subscribe {plan.capitalize()}", color=discord.Color.blue())
    if sol_price:
        required = usd_price / sol_price
        emb.add_field(name="🪙 Pay with SOL", value=f"Send **{required:.4f} SOL** (≈ ${int(usd_price)}) to:\n`{MY_WALLET_STR}`\nThen `!verify <tx_id> {plan}`", inline=False)
    else:
        emb.add_field(name="🪙 SOL", value="Could not fetch SOL price. Try again later.", inline=False)
    if STRIPE_SECRET_KEY:
        url = create_stripe_session(str(ctx.author.id), plan)
        if url: emb.add_field(name="💳 Card", value=f"[Click here]({url})", inline=False)
    await ctx.send(embed=emb)

@bot.command()
async def verify(ctx, tx_id: str, plan: str):
    if plan not in PLAN_CONFIG:
        return await ctx.send("❌ Invalid plan.")
    await verify_solana_payment(ctx, tx_id, plan)

@bot.command()
async def addasset(ctx, *, t):
    t = resolve_alias(t)
    db = load_data(); uid = str(ctx.author.id); profile = db.get(uid, {})
    lic = db_get_license(uid)
    if not is_admin(ctx) and not lic:
        return await ctx.send("🔒 Active subscription required.")
    plan = "monthly" if lic is None else [k for k,v in PLAN_CONFIG.items() if v[0] == lic['role_id']][0] if lic else "daily"
    limit = PLAN_ASSET_LIMIT.get(plan, 1)
    assets = profile.get("assets", [])
    if t in assets: return await ctx.send(f"⚠️ `{t}` already in watchlist.")
    if len(assets) >= limit and not is_admin(ctx):
        return await ctx.send(f"❌ Max {limit} asset(s). Remove one first.")
    assets.append(t); profile["assets"] = assets
    if "status" not in profile: profile["status"] = "Active"
    db[uid] = profile; await save_data(db)
    await ctx.send(f"✅ Added `{t}` ({len(assets)}/{limit}).")

@bot.command()
async def removeasset(ctx, *, t):
    db = load_data(); uid = str(ctx.author.id)
    if uid not in db: return await ctx.send("❌ No subscription found.")
    t = resolve_alias(t)
    assets = db[uid].get("assets", [])
    if t in assets: assets.remove(t); await save_data(db); await ctx.send(f"✅ Removed `{t}`.")
    else: await ctx.send(f"⚠️ `{t}` not in watchlist.")

@bot.command()
async def myassets(ctx):
    db = load_data(); uid = str(ctx.author.id); profile = db.get(uid, {})
    assets = profile.get("assets", [])
    if not assets: return await ctx.send("📋 Empty.")
    lic = db_get_license(uid)
    plan = "monthly" if lic is None else [k for k,v in PLAN_CONFIG.items() if v[0] == lic['role_id']][0] if lic else "daily"
    limit = PLAN_ASSET_LIMIT.get(plan, 1)
    lines = [f"**{i}.** {t} – ${get_price(t):,.4f}" if get_price(t) else f"**{i}.** {t} – N/A" for i,t in enumerate(assets,1)]
    await ctx.send(f"📋 **Watchlist ({len(assets)}/{limit}):**\n"+"\n".join(lines))

@bot.command(name="price")
async def price_cmd(ctx, *, ticker: str = None):
    if ticker is None:
        await ctx.send("Usage: `!price <ticker>` (e.g., `!price BTC`)")
        return
    if not is_admin(ctx) and not db_get_license(str(ctx.author.id)):
        return await ctx.send("🔒 Active subscription required.")
    t = resolve_alias(ticker); p = get_price(t)
    if p is None: await ctx.send(f"❌ Could not fetch price for `{t}`.")
    else: await ctx.send(f"💰 **{t.upper()}**: **${p:,.4f}**")

@bot.command(name="analyze")
async def analyze_cmd(ctx, *, t):
    if not is_admin(ctx) and not db_get_license(str(ctx.author.id)):
        return await ctx.send("🔒 Active subscription required.")
    t = resolve_alias(t)
    async with ctx.typing():
        r = await asyncio.get_event_loop().run_in_executor(None, analyze, t)
        if r is None: await ctx.send(f"❌ Insufficient data for `{t}`.")
        else: await ctx.send(embed=build_embed(r))

@bot.command()
async def addinterval(ctx, ticker: str, minutes: int):
    if minutes < 1: return await ctx.send("❌ Minimum interval is 1 minute.")
    ticker = resolve_alias(ticker); db = load_data(); uid = str(ctx.author.id)
    if not is_admin(ctx) and not db_get_license(uid):
        return await ctx.send("🔒 Active subscription required.")
    profile = db.get(uid, {})
    profile.setdefault("intervals", {})[ticker] = minutes
    if "status" not in profile: profile["status"] = "Active"
    db[uid] = profile; await save_data(db)
    await ctx.send(f"✅ Updates for `{ticker}` every {minutes} minute(s).")

@bot.command()
async def removeinterval(ctx, *, ticker: str):
    ticker = resolve_alias(ticker); db = load_data(); uid = str(ctx.author.id)
    if "intervals" not in db.get(uid,{}): return await ctx.send("❌ No intervals set.")
    if ticker in db[uid]["intervals"]:
        del db[uid]["intervals"][ticker]
        if not db[uid]["intervals"]: del db[uid]["intervals"]
        await save_data(db); await ctx.send(f"✅ Removed interval for `{ticker}`.")
    else: await ctx.send(f"⚠️ No interval set for `{ticker}`.")

@bot.command()
async def myintervals(ctx):
    db = load_data(); uid = str(ctx.author.id)
    if not is_admin(ctx) and not db_get_license(uid):
        return await ctx.send("🔒 Active subscription required.")
    intervals = db.get(uid,{}).get("intervals",{})
    if not intervals: return await ctx.send("📋 No intervals.")
    await ctx.send("📋 **Intervals:**\n"+"\n".join(f"• **{t}**: every {m} min" for t,m in intervals.items()))

if __name__ == "__main__":
    Thread(target=run_flask, daemon=True).start()
    bot.run(TOKEN)