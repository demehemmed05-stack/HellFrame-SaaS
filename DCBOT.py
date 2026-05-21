# ====================================================================
# 🔥 HELLFRAME QUANT ENGINE v4.8.0 (Nihai Sürüm – Yeni Fiyatlandırma)
# Global SaaS Discord Trade Bot – İngilizce Çıktı, Türkçe Yorum
# ====================================================================
# YENİ:
#   • Fiyatlar: Günlük $3, Haftalık $18 (6+1), Aylık $54 (3+1)
#   • !plans komutu eklendi – avantajlı plan detaylarıyla birlikte
#   • SOL ödeme doğrulaması yeni fiyatlara göre güncellendi
# ====================================================================

import os, json, asyncio, logging, difflib
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
# 🔐 ENV
# ====================================================================
load_dotenv()
TOKEN = os.getenv("DISCORD_BOT_TOKEN")
SOLANA_RPC_URLS = [u.strip() for u in os.getenv("SOLANA_RPC_URLS", os.getenv("SOLANA_RPC_URL", "https://api.mainnet-beta.solana.com")).split(",") if u.strip()]
MY_WALLET_STR = os.getenv("SOLANA_WALLET_ADDRESS")
ADMIN_ID_RAW = os.getenv("ADMIN_ID")
STRIPE_SECRET_KEY = os.getenv("STRIPE_SECRET_KEY")
STRIPE_WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET")
STRIPE_PRICE_IDS = {
    "daily": os.getenv("STRIPE_PRICE_DAILY"),
    "weekly": os.getenv("STRIPE_PRICE_WEEKLY"),
    "monthly": os.getenv("STRIPE_PRICE_MONTHLY")
}
PROXY_URL = os.getenv("PROXY_URL")

if not TOKEN or not MY_WALLET_STR or not ADMIN_ID_RAW:
    raise ValueError("Missing essential .env variables")
if STRIPE_SECRET_KEY:
    stripe.api_key = STRIPE_SECRET_KEY
try:
    ADMIN_ID = int(ADMIN_ID_RAW)
except ValueError:
    raise ValueError("ADMIN_ID must be numeric")
MY_WALLET = Pubkey.from_string(MY_WALLET_STR)

# ====================================================================
# 📊 LOGGING
# ====================================================================
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
                    handlers=[logging.FileHandler("bot_production.log", encoding="utf-8"), logging.StreamHandler()])
logger = logging.getLogger("HellFrameQuant")

# ====================================================================
# 📁 VERİTABANI
# ====================================================================
DATA_FILE, ALERTS_FILE, USED_TX_FILE = "user_subscriptions.json", "price_alerts.json", "used_transactions.json"
DB_LOCK, ALERT_LOCK, USED_TX_LOCK = Lock(), Lock(), Lock()
analysis_semaphore = Semaphore(5)

# YENİ FİYATLANDIRMA: Günlük $3, Haftalık $18 (6+1), Aylık $54 (3+1)
TIER_PRICES_USD = {"daily": 3.0, "weekly": 18.0, "monthly": 54.0}
PLAN_DURATION = {"daily": 1, "weekly": 7, "monthly": 30}
PLAN_ASSET_LIMIT = {"daily": 1, "weekly": 2, "monthly": 3}
# Plan avantaj açıklamaları (İngilizce)
PLAN_BENEFITS = {
    "daily": "Standard daily rate.",
    "weekly": "6+1 Deal: Pay for 6 days, get 7! Save $3 vs daily.",
    "monthly": "3+1 Deal: Pay for 3 weeks, get 4! Save $18 vs weekly."
}

RSI_OVERSOLD, RSI_OVERBOUGHT = 30.0, 70.0
EMA_FAST, EMA_SLOW = 50, 200
SIGNAL_COOLDOWN = 3600

def ensure_files():
    for f, d in [(DATA_FILE, {}), (ALERTS_FILE, {}), (USED_TX_FILE, [])]:
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
            asyncio.create_task(backup_db("Subs", data))
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

async def load_used_txs():
    async with USED_TX_LOCK:
        try:
            with open(USED_TX_FILE, "r", encoding="utf-8") as f:
                return set(json.load(f))
        except: return set()

async def add_used_tx(tx):
    async with USED_TX_LOCK:
        used = await load_used_txs()
        used.add(tx)
        with open(USED_TX_FILE, "w", encoding="utf-8") as f:
            json.dump(list(used), f)

async def is_tx_used(tx):
    return tx in await load_used_txs()

async def backup_db(ctx, data):
    await bot.wait_until_ready()
    try:
        u = await bot.fetch_user(ADMIN_ID)
        if not u: return
        j = json.dumps(data, indent=2, ensure_ascii=False)
        p, s = f"📦 **[BACKUP {ctx}]**\n```json\n", "\n```"
        sz = 1900 - len(p) - len(s)
        for i in range(0, len(j), sz):
            await u.send(f"{p}{j[i:i+sz]}{s}")
    except: pass

def is_admin(ctx):
    return ctx.author.id == ADMIN_ID

# ====================================================================
# 📈 PİYASA VERİSİ
# ====================================================================
CRYPTO_MAP = {"BTC":"BTC-USD","ETH":"ETH-USD","SOL":"SOL-USD","XRP":"XRP-USD","DOGE":"DOGE-USD","ADA":"ADA-USD","AVAX":"AVAX-USD","DOT":"DOT-USD","MATIC":"MATIC-USD","LINK":"LINK-USD","LTC":"LTC-USD","BNB":"BNB-USD"}
COMMODITY_MAP = {"GOLD":"GC=F","SILVER":"SI=F","OIL":"CL=F","COPPER":"HG=F","NATGAS":"NG=F","BRENT":"BZ=F"}
FX_MAP = {"EURUSD":"EURUSD=X","GBPUSD":"GBPUSD=X","USDJPY":"USDJPY=X","USDTRY":"USDTRY=X","EURTRY":"EURTRY=X","AUDUSD":"AUDUSD=X","USDCHF":"USDCHF=X","NZDUSD":"NZDUSD=X","USDCAD":"USDCAD=X"}

ALIASES = {
    "XAU": "GOLD", "XAG": "SILVER", "WTI": "OIL", "EUR": "EURUSD", "EURO": "EURUSD",
    "GBP": "GBPUSD", "TRY": "USDTRY", "LIRA": "USDTRY", "TL": "USDTRY",
    "JPY": "USDJPY", "CHF": "USDCHF", "AUD": "AUDUSD", "CAD": "USDCAD", "NZD": "NZDUSD",
    "BNB": "BNB-USD", "BTC": "BTC-USD", "ETH": "ETH-USD", "XRP": "XRP-USD",
}

def resolve_alias(ticker):
    t = ticker.upper().strip()
    if t in ALIASES:
        return ALIASES[t]
    if "-USD" in t or "=X" in t or "=F" in t:
        return t
    return CRYPTO_MAP.get(t) or COMMODITY_MAP.get(t) or FX_MAP.get(t) or t

def normalize(t):
    t = resolve_alias(t)
    if "-USD" in t or "=X" in t or "=F" in t:
        return t
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
# 🔗 SOLANA DOĞRULAMA (Yeni fiyatlarla uyumlu)
# ====================================================================
async def verify_solana_payment(tx_id: str, plan: str) -> Union[Tuple[bool, str], str]:
    if not tx_id or len(tx_id) < 32:
        return (False, "Invalid transaction ID.")
    if await is_tx_used(tx_id):
        return (False, "This transaction has already been used.")

    usd_price = TIER_PRICES_USD.get(plan)
    if usd_price is None:
        return (False, "Invalid plan.")

    sol_price = get_sol_price()
    if sol_price is None or sol_price <= 0:
        return "FALLBACK"

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
                    return (False, "Transaction failed on-chain.")

                keys = resp.value.transaction.message.account_keys
                if MY_WALLET not in keys:
                    return (False, "Payment not sent to the correct wallet.")

                idx = keys.index(MY_WALLET)
                received_lamports = meta.post_balances[idx] - meta.pre_balances[idx]
                received_sol = received_lamports / 1_000_000_000

                if abs(received_sol - required_sol) <= 0.0001:
                    await add_used_tx(tx_id)
                    return (True, "")
                else:
                    received_usd = received_sol * sol_price
                    msg = (
                        f"❌ Incorrect amount received.\n"
                        f"You sent: **{received_sol:.4f} SOL** (≈ **${received_usd:.2f}**).\n"
                        f"Required: **${usd_price:.2f}** (≈ **{required_sol:.4f} SOL**).\n"
                        f"Please send the exact amount and try again."
                    )
                    return (False, msg)
        except asyncio.TimeoutError:
            continue
        except Exception as e:
            logger.error(f"RPC error {rpc}: {e}")
            continue
    return "FALLBACK"

# ====================================================================
# 💳 STRIPE (Yeni fiyatlarla uyumlu)
# ====================================================================
def create_stripe_session(uid, plan):
    if not STRIPE_SECRET_KEY: return None
    try:
        pid = STRIPE_PRICE_IDS.get(plan)
        if pid:
            session = stripe.checkout.Session.create(
                payment_method_types=['card'],
                line_items=[{'price': pid, 'quantity': 1}],
                mode='subscription',
                metadata={'user_id': uid, 'plan': plan},
                success_url='https://discord.com/channels/@me',
                cancel_url='https://discord.com/channels/@me'
            )
        else:
            session = stripe.checkout.Session.create(
                payment_method_types=['card'],
                line_items=[{'price_data': {
                    'currency': 'usd',
                    'product_data': {'name': f'HellFrame - {plan.capitalize()}'},
                    'unit_amount': int(TIER_PRICES_USD[plan] * 100),
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

async def add_subscription_time(user_id: str, plan: str, method: str = "solana"):
    db = load_data()
    uid = str(user_id)
    now = datetime.now(timezone.utc)
    duration = timedelta(days=PLAN_DURATION[plan])
    current_expiry = now
    if uid in db:
        try:
            cur = datetime.fromisoformat(db[uid].get("expiry", now.isoformat()))
            if cur > now:
                current_expiry = cur
        except: pass
    new_expiry = current_expiry + duration
    db[uid] = {
        "status": "Active", "plan": plan,
        "expiry": new_expiry.isoformat(),
        "assets": db.get(uid, {}).get("assets", []),
        "intervals": db.get(uid, {}).get("intervals", {}),
        "payment_method": method, "last_paid": now.isoformat()
    }
    await save_data(db)
    try:
        user = await bot.fetch_user(int(user_id))
        if user:
            embed = discord.Embed(
                title="✅ Subscription Updated",
                description=f"**{plan.capitalize()}** plan activated.\nTotal expiry: **{new_expiry.strftime('%Y-%m-%d %H:%M UTC')}**",
                color=discord.Color.green()
            )
            embed.add_field(name="Remaining", value=f"{new_expiry - now}")
            embed.set_footer(text="HellFrame Quant Engine v4.8.0")
            await user.send(embed=embed)
    except: pass

async def cancel_sub(user_id, reason):
    db = load_data()
    uid = str(user_id)
    if uid in db:
        db[uid]["status"] = "Cancelled"
        db[uid]["cancel_reason"] = reason
        await save_data(db)

# ====================================================================
# 🧩 VIEWLAR (Yeni fiyatlar butonlara yansıtıldı)
# ====================================================================
class RenewView(View):
    def __init__(self, uid, plan):
        super().__init__(timeout=86400)
        self.uid, self.plan = uid, plan
        b1 = Button(label="Yes, renew", style=discord.ButtonStyle.green)
        b1.callback = self.yes; self.add_item(b1)
        b2 = Button(label="No, let it expire", style=discord.ButtonStyle.red)
        b2.callback = self.no; self.add_item(b2)
        b3 = Button(label="Change plan", style=discord.ButtonStyle.blurple)
        b3.callback = self.change; self.add_item(b3)

    async def yes(self, i):
        if i.user.id != self.uid: return await i.response.send_message("Not yours", ephemeral=True)
        db = load_data()
        method = db.get(str(self.uid), {}).get("payment_method", "solana")
        if method == "stripe":
            await add_subscription_time(str(self.uid), self.plan, "stripe")
            await i.response.send_message("Renewed via card!", ephemeral=True)
        else:
            sol_price = get_sol_price()
            if sol_price:
                required = TIER_PRICES_USD[self.plan] / sol_price
                await i.response.send_message(
                    f"Send **{required:.4f} SOL** (≈ ${TIER_PRICES_USD[self.plan]:.2f}) to `{MY_WALLET_STR}` then `!verify <tx_id> {self.plan}`",
                    ephemeral=True
                )
            else:
                await i.response.send_message("Could not fetch SOL price, try again later.", ephemeral=True)
        self.stop()

    async def no(self, i):
        if i.user.id != self.uid: return await i.response.send_message("Not yours", ephemeral=True)
        await i.response.send_message("Okay, it will expire.", ephemeral=True)
        self.stop()

    async def change(self, i):
        if i.user.id != self.uid: return await i.response.send_message("Not yours", ephemeral=True)
        await i.response.send_message(view=PlanSelectView(self.uid), ephemeral=True)
        self.stop()

class PlanSelectView(View):
    def __init__(self, uid):
        super().__init__(timeout=300)
        self.uid = uid
        for p, pr in TIER_PRICES_USD.items():
            b = Button(label=f"{p.capitalize()} (${pr})", style=discord.ButtonStyle.grey)
            b.callback = self._cb(p)
            self.add_item(b)
    def _cb(self, plan):
        async def f(i):
            if i.user.id != self.uid: return await i.response.send_message("Not yours", ephemeral=True)
            await i.response.send_message(f"Switch to **{plan.capitalize()}** for **${TIER_PRICES_USD[plan]}**?", view=ConfirmView(self.uid, plan), ephemeral=True)
        return f

class ConfirmView(View):
    def __init__(self, uid, plan):
        super().__init__(timeout=120)
        self.uid, self.plan = uid, plan
        y = Button(label="Yes, switch", style=discord.ButtonStyle.green)
        y.callback = self.yes; self.add_item(y)
        n = Button(label="No", style=discord.ButtonStyle.red)
        n.callback = self.no; self.add_item(n)
    async def yes(self, i):
        if i.user.id != self.uid: return await i.response.send_message("Not yours", ephemeral=True)
        db = load_data()
        method = db.get(str(self.uid), {}).get("payment_method", "solana")
        if method == "stripe":
            await add_subscription_time(str(self.uid), self.plan, "stripe")
            await i.response.send_message(f"Switched to {self.plan.capitalize()}!", ephemeral=True)
        else:
            sol_price = get_sol_price()
            if sol_price:
                required = TIER_PRICES_USD[self.plan] / sol_price
                await i.response.send_message(
                    f"Send **{required:.4f} SOL** (≈ ${TIER_PRICES_USD[self.plan]:.2f}) to `{MY_WALLET_STR}` then `!verify <tx_id> {self.plan}`",
                    ephemeral=True
                )
            else:
                await i.response.send_message("Could not fetch SOL price, try again later.", ephemeral=True)
    async def no(self, i):
        if i.user.id != self.uid: return await i.response.send_message("Not yours", ephemeral=True)
        await i.response.send_message("Keeping current plan.", ephemeral=True)

# ====================================================================
# 🌐 FLASK
# ====================================================================
app = Flask(__name__)
@app.route('/')
def home(): return "HellFrame Quant Engine v4.8.0 online!"

@app.route('/webhook', methods=['POST'])
def webhook():
    if not STRIPE_WEBHOOK_SECRET: return jsonify({'err': 'no secret'}), 500
    p = request.get_data(as_text=True)
    sig = request.headers.get('Stripe-Signature')
    try: ev = stripe.Webhook.construct_event(p, sig, STRIPE_WEBHOOK_SECRET)
    except: return jsonify({'err': 'invalid'}), 400
    if ev['type'] == 'invoice.paid':
        obj = ev['data']['object']
        uid, plan = obj.get('metadata', {}).get('user_id'), obj.get('metadata', {}).get('plan')
        if uid and plan:
            asyncio.run_coroutine_threadsafe(add_subscription_time(uid, plan, "stripe"), bot.loop)
    elif ev['type'] in ['charge.dispute.created', 'charge.dispute.updated']:
        ch = stripe.Charge.retrieve(ev['data']['object']['charge'])
        uid = ch.get('metadata', {}).get('user_id')
        if uid:
            asyncio.run_coroutine_threadsafe(cancel_sub(uid, "dispute"), bot.loop)
    return jsonify({'ok': True}), 200

def run_flask():
    port = int(os.getenv("PORT", 8080))
    app.run(host='0.0.0.0', port=port, debug=False)

# ====================================================================
# 🤖 BOT
# ====================================================================
intents = discord.Intents.default()
intents.message_content = True

if PROXY_URL:
    proxy = discord.Proxy(url=PROXY_URL, proxy_type=discord.ProxyType.http)
    bot = commands.Bot(command_prefix="!", intents=intents, case_insensitive=True, help_command=None, proxy=proxy)
else:
    bot = commands.Bot(command_prefix="!", intents=intents, case_insensitive=True, help_command=None)

@bot.event
async def on_ready():
    logger.info(f"Online: {bot.user}")
    for loop in [check_user_alerts, check_price_alerts, check_intervals, check_reminders, cleanup_expired]:
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
    e.set_footer(text="HellFrame Quant Engine v4.8.0")
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
    db = load_data()
    now = datetime.now(timezone.utc)
    sent_key = "reminder_sent"
    for uid, p in db.items():
        if p.get("status") != "Active": continue
        try: exp = datetime.fromisoformat(p["expiry"])
        except: continue
        rem = exp - now
        plan = p.get("plan", "daily")
        if (plan == "daily" and timedelta(0) < rem <= timedelta(hours=1)) or (plan != "daily" and timedelta(0) < rem <= timedelta(hours=24)):
            if not p.get(sent_key):
                try:
                    u = await bot.fetch_user(int(uid))
                    if u:
                        emb = discord.Embed(title="⏳ Subscription Renewal", description=f"Your **{plan.capitalize()}** plan expires in {rem}.", color=discord.Color.gold())
                        await u.send(embed=emb, view=RenewView(int(uid), plan))
                        db[uid][sent_key] = True
                        await save_data(db)
                except: pass

@tasks.loop(hours=6)
async def cleanup_expired():
    db = load_data()
    now = datetime.now(timezone.utc)
    changed = False
    for uid, p in db.items():
        if p.get("status") != "Active": continue
        try: exp = datetime.fromisoformat(p["expiry"])
        except: continue
        if now >= exp:
            p["status"] = "Expired"; changed = True
            try: await (await bot.fetch_user(int(uid))).send(embed=discord.Embed(title="⌛ Expired", description="Renew with `!subscribe`", color=discord.Color.light_grey()))
            except: pass
    if changed: await save_data(db)

# ====================================================================
# 📚 KOMUT YARDIM SÖZLÜĞÜ (Güncellendi)
# ====================================================================
COMMAND_HELP = {
    "ping": {"desc": "Check bot latency.", "use": "!ping", "ex": "!ping"},
    "info": {"desc": "Show bot information and features.", "use": "!info", "ex": "!info"},
    "mystatus": {"desc": "Check your subscription status and watchlist.", "use": "!mystatus", "ex": "!mystatus"},
    "plans": {"desc": "View subscription plans with benefits.", "use": "!plans", "ex": "!plans"},
    "subscribe": {"desc": "View plans or subscribe. Add a plan name.", "use": "!subscribe [plan]", "ex": "!subscribe daily"},
    "verify": {"desc": "Verify a SOL payment. Provide tx_id and plan.", "use": "!verify <tx_id> <plan>", "ex": "!verify 5Bmz... daily"},
    "price": {"desc": "Live price of a ticker. Aliases: XAU, XAG, WTI, EUR, GBP, TRY, BNB.", "use": "!price <ticker>", "ex": "!price XAU"},
    "analyze": {"desc": "Full technical analysis.", "use": "!analyze <ticker>", "ex": "!analyze SOL"},
    "addasset": {"desc": "Add to watchlist (plan limits apply).", "use": "!addasset <ticker>", "ex": "!addasset GOLD"},
    "removeasset": {"desc": "Remove from watchlist.", "use": "!removeasset <ticker>", "ex": "!removeasset BTC"},
    "myassets": {"desc": "Show watchlist with live prices.", "use": "!myassets", "ex": "!myassets"},
    "addinterval": {"desc": "Price updates every X min (min 1).", "use": "!addinterval <ticker> <minutes>", "ex": "!addinterval ETH 5"},
    "removeinterval": {"desc": "Stop price updates for a ticker.", "use": "!removeinterval <ticker>", "ex": "!removeinterval ETH"},
    "myintervals": {"desc": "List active price intervals.", "use": "!myintervals", "ex": "!myintervals"},
    "help": {"desc": "Show this help. !help <cmd> for details.", "use": "!help [command]", "ex": "!help analyze"}
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
            all_cmds = list(COMMAND_HELP.keys())
            matches = difflib.get_close_matches(cmd, all_cmds, n=1, cutoff=0.5)
            if matches:
                await ctx.send(f"❓ Unknown command `!{cmd}`. Did you mean `!{matches[0]}`? Use `!help {matches[0]}` for details.")
            else:
                await ctx.send(f"❓ Unknown command `!{cmd}`. Use `!help` to see all commands.")
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
            await ctx.send(f"❌ Missing argument for `!{cmd}`.\n**Usage:** `{COMMAND_HELP[cmd]['use']}`\n**Example:** `{COMMAND_HELP[cmd]['ex']}`")
        else:
            await ctx.send(f"❌ Missing argument. Use `!help {cmd}` for details.")
    elif isinstance(error, commands.BadArgument):
        await ctx.send(f"❌ Invalid argument type. Use `!help` for command details.")
    elif isinstance(error, commands.CommandNotFound):
        wrong = ctx.message.content.split()[0].lstrip("!").lower()
        all_cmds = [c.name for c in bot.commands] + list(COMMAND_HELP.keys())
        matches = difflib.get_close_matches(wrong, list(set(all_cmds)), n=1, cutoff=0.5)
        if matches:
            await ctx.send(f"❓ `!{wrong}` not found. Did you mean `!{matches[0]}`? Use `!help {matches[0]}` to learn more.")
        else:
            await ctx.send(f"❓ `!{wrong}` not found. Use `!help` to see all available commands.")
    elif isinstance(error, commands.CheckFailure):
        await ctx.send("🔒 You need an active subscription to use this command. Use `!subscribe` to get started.")
    else:
        logger.error(f"Unhandled error: {error}")
        await ctx.send("❌ An unexpected error occurred. The admin has been notified.")

# ====================================================================
# 🧪 KOMUTLAR (YENİ FİYATLAR VE !plans DAHİL)
# ====================================================================
@bot.command()
async def ping(ctx): await ctx.send(f"Pong! {round(bot.latency*1000)}ms")

@bot.command()
async def info(ctx):
    embed = discord.Embed(title="🤖 HellFrame Quant Engine", description="Advanced trading analysis bot.", color=discord.Color.blurple())
    embed.add_field(name="Version", value="v4.8.0", inline=True)
    embed.add_field(name="Prefix", value="`!`", inline=True)
    embed.add_field(name="Plans", value="Daily $3 | Weekly $18 | Monthly $54", inline=False)
    embed.add_field(name="Get Started", value="Use `!plans` to see details or `!help` for commands.", inline=False)
    await ctx.send(embed=embed)

@bot.command()
async def mystatus(ctx):
    if is_admin(ctx):
        return await ctx.send(embed=discord.Embed(title="👑 Admin", description="Full access.", color=discord.Color.purple()))
    db = load_data(); uid = str(ctx.author.id); p = db.get(uid)
    if not p or p.get("status") != "Active":
        return await ctx.send("❌ No active subscription. Use `!plans` to choose a plan.")
    embed = discord.Embed(title="📊 Your Status", color=discord.Color.green())
    embed.add_field(name="Plan", value=p.get("plan","daily").capitalize())
    embed.add_field(name="Expiry", value=p.get("expiry","?")[:19].replace("T"," "))
    embed.add_field(name="Assets", value=f"{len(p.get('assets',[]))}/{PLAN_ASSET_LIMIT.get(p.get('plan','daily'),1)}")
    if p.get("assets"): embed.add_field(name="Watchlist", value=", ".join(p["assets"]), inline=False)
    if p.get("intervals"): embed.add_field(name="Intervals", value=", ".join(f"{t} ({m}m)" for t,m in p["intervals"].items()), inline=False)
    await ctx.send(embed=embed)

@bot.command(name="plans")
async def plans_cmd(ctx):
    """Detaylı plan bilgilerini gösterir."""
    embed = discord.Embed(
        title="💎 Subscription Plans",
        description="Choose the plan that fits your trading style.",
        color=discord.Color.gold()
    )
    embed.add_field(
        name=f"1️⃣ Daily – ${TIER_PRICES_USD['daily']:.2f}",
        value=PLAN_BENEFITS["daily"],
        inline=False
    )
    embed.add_field(
        name=f"7️⃣ Weekly – ${TIER_PRICES_USD['weekly']:.2f}",
        value=PLAN_BENEFITS["weekly"],
        inline=False
    )
    embed.add_field(
        name=f"30️⃣ Monthly – ${TIER_PRICES_USD['monthly']:.2f}",
        value=PLAN_BENEFITS["monthly"],
        inline=False
    )
    embed.add_field(
        name="How to Subscribe",
        value="Use `!subscribe daily`, `!subscribe weekly`, or `!subscribe monthly`.",
        inline=False
    )
    embed.set_footer(text="HellFrame Quant Engine v4.8.0")
    await ctx.send(embed=embed)

@bot.command()
async def subscribe(ctx, *, plan=None):
    if plan not in TIER_PRICES_USD:
        return await ctx.invoke(plans_cmd)  # Plans komutunu göster

    sol_price = get_sol_price()
    emb = discord.Embed(title=f"Subscribe {plan.capitalize()}", color=discord.Color.blue())
    emb.add_field(name="📌 Plan", value=f"**{plan.capitalize()}** – **${TIER_PRICES_USD[plan]:.2f}**\n{PLAN_BENEFITS.get(plan, '')}", inline=False)

    if sol_price:
        required = TIER_PRICES_USD[plan] / sol_price
        emb.add_field(
            name="🪙 Pay with SOL",
            value=f"Send **{required:.4f} SOL** (≈ ${TIER_PRICES_USD[plan]:.2f}) to:\n`{MY_WALLET_STR}`\nThen `!verify <tx_id> {plan}`",
            inline=False
        )
    else:
        emb.add_field(name="🪙 SOL", value="Could not fetch SOL price. Try again later.", inline=False)

    if STRIPE_SECRET_KEY:
        url = create_stripe_session(str(ctx.author.id), plan)
        if url: emb.add_field(name="💳 Card", value=f"[Click here]({url})", inline=False)

    await ctx.send(embed=emb)

@bot.command()
async def verify(ctx, tx_id: str, plan: str):
    if plan not in TIER_PRICES_USD:
        return await ctx.send("❌ Invalid plan. Use daily, weekly, or monthly.")
    async with ctx.typing():
        res = await verify_solana_payment(tx_id, plan)
        if res == "FALLBACK":
            await ctx.send("⏳ RPC nodes unresponsive. The admin will check manually.")
        elif isinstance(res, tuple):
            success, msg = res
            if success:
                await add_subscription_time(str(ctx.author.id), plan, "solana")
                await ctx.send("✅ Payment verified! Subscription active.")
            else:
                await ctx.send(msg or "❌ Verification failed.")
        else:
            await ctx.send("❌ Verification failed. Check your TX ID and try again.")

@bot.command()
async def addasset(ctx, *, t):
    t = resolve_alias(t)
    if is_admin(ctx):
        db = load_data(); uid = str(ctx.author.id)
        profile = db.get(uid, {})
        assets = profile.get("assets", [])
        if t in assets: return await ctx.send(f"⚠️ `{t}` already in watchlist.")
        assets.append(t)
        profile["assets"] = assets
        profile.setdefault("status","Active"); profile.setdefault("plan","daily")
        profile.setdefault("expiry", (datetime.now(timezone.utc)+timedelta(days=36500)).isoformat())
        db[uid] = profile
        await save_data(db)
        return await ctx.send(f"✅ Added `{t}` (admin mode).")
    db = load_data(); uid = str(ctx.author.id); profile = db.get(uid)
    if not profile or profile.get("status") != "Active":
        return await ctx.send("🔒 Active subscription required. Use `!subscribe`.")
    plan = profile.get("plan","daily"); limit = PLAN_ASSET_LIMIT.get(plan, 1)
    assets = profile.get("assets", [])
    if t in assets: return await ctx.send(f"⚠️ `{t}` already in watchlist.")
    if len(assets) >= limit:
        return await ctx.send(f"❌ Your {plan} plan allows max {limit} asset(s). Remove one first with `!removeasset`.")
    assets.append(t); profile["assets"] = assets
    await save_data(db)
    await ctx.send(f"✅ Added `{t}` ({len(assets)}/{limit}).")

@bot.command()
async def removeasset(ctx, *, t):
    t = resolve_alias(t); db = load_data(); uid = str(ctx.author.id)
    if uid not in db: return await ctx.send("❌ No subscription found.")
    assets = db[uid].get("assets", [])
    if t in assets:
        assets.remove(t)
        await save_data(db)
        await ctx.send(f"✅ Removed `{t}`.")
    else:
        await ctx.send(f"⚠️ `{t}` not in your watchlist.")

@bot.command()
async def myassets(ctx):
    db = load_data(); uid = str(ctx.author.id); profile = db.get(uid)
    if is_admin(ctx):
        assets = profile.get("assets",[]) if profile else []
        if not assets: return await ctx.send("📋 Empty. Use `!addasset <ticker>`.")
        return await ctx.send("📋 **Admin Watchlist:**\n"+"\n".join(f"**{i}.** {t}" for i,t in enumerate(assets,1)))
    if not profile or profile.get("status") != "Active":
        return await ctx.send("🔒 Active subscription required. Use `!subscribe`.")
    assets = profile.get("assets",[])
    if not assets: return await ctx.send("📋 Empty. Use `!addasset <ticker>`.")
    plan = profile.get("plan","daily"); limit = PLAN_ASSET_LIMIT.get(plan,1)
    lines = [f"**{i}.** {t} – ${get_price(t):,.4f}" if get_price(t) else f"**{i}.** {t} – N/A" for i,t in enumerate(assets,1)]
    await ctx.send(f"📋 **Watchlist ({len(assets)}/{limit}):**\n"+"\n".join(lines))

@bot.command()
async def addinterval(ctx, ticker: str, minutes: int):
    if minutes < 1: return await ctx.send("❌ Minimum interval is 1 minute.")
    ticker = resolve_alias(ticker); db = load_data(); uid = str(ctx.author.id)
    if not is_admin(ctx) and db.get(uid,{}).get("status") != "Active":
        return await ctx.send("🔒 Active subscription required. Use `!subscribe`.")
    profile = db.get(uid, {})
    profile.setdefault("intervals", {})[ticker] = minutes
    if is_admin(ctx) and "status" not in profile:
        profile["status"] = "Active"; profile["plan"] = "daily"
        profile["expiry"] = (datetime.now(timezone.utc)+timedelta(days=36500)).isoformat()
    db[uid] = profile
    await save_data(db)
    await ctx.send(f"✅ Updates for `{ticker}` every {minutes} minute(s).")

@bot.command()
async def removeinterval(ctx, *, ticker: str):
    ticker = resolve_alias(ticker); db = load_data(); uid = str(ctx.author.id)
    if "intervals" not in db.get(uid,{}): return await ctx.send("❌ No intervals set.")
    if ticker in db[uid]["intervals"]:
        del db[uid]["intervals"][ticker]
        if not db[uid]["intervals"]: del db[uid]["intervals"]
        await save_data(db)
        await ctx.send(f"✅ Removed interval for `{ticker}`.")
    else:
        await ctx.send(f"⚠️ No interval set for `{ticker}`.")

@bot.command()
async def myintervals(ctx):
    db = load_data(); uid = str(ctx.author.id)
    if not is_admin(ctx) and db.get(uid,{}).get("status") != "Active":
        return await ctx.send("🔒 Active subscription required.")
    intervals = db.get(uid,{}).get("intervals",{})
    if not intervals: return await ctx.send("📋 No intervals. Use `!addinterval <ticker> <minutes>`.")
    await ctx.send("📋 **Intervals:**\n"+"\n".join(f"• **{t}**: every {m} min" for t,m in intervals.items()))

if __name__ == "__main__":
    Thread(target=run_flask, daemon=True).start()
    bot.run(TOKEN)