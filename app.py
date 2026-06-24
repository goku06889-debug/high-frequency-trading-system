#!/usr/bin/env python3
"""GOD-MODE v7.2 — Telegram via requests+thread (fix HF network issue)"""

import subprocess, sys
for pkg in ["fastapi","uvicorn[standard]","aiohttp","nest_asyncio","pandas","numpy","requests","websockets"]:
    try: __import__(pkg.split("[")[0])
    except: subprocess.check_call([sys.executable,"-m","pip","install","-q",pkg])

import os, gc, json, time, asyncio, random, logging, statistics
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple, Any
from concurrent.futures import ThreadPoolExecutor

import requests as req_lib
import aiohttp
import numpy as np
import pandas as pd
import nest_asyncio
from fastapi import FastAPI
from fastapi.responses import JSONResponse
import uvicorn

nest_asyncio.apply()
logging.basicConfig(level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger("GOD-MODE-v7.2")

# ── CREDENTIALS ───────────────────────────────────────────────────────────────
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN","8737317449:AAH7y1Kc7_fn3YxqCK2VfgWAKGjbIScm_js")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID",  "8558439530")
ARKHAM_API_KEY     = os.getenv("ARKHAM_API_KEY",     "3ebf1081-1a99-4b81-9f8c-e990fa9318c1")

# ── CONFIG ────────────────────────────────────────────────────────────────────
class Config:
    MIN_VOLUME_24H_USD  = 2_000_000
    MAX_SPREAD_PCT      = 0.0015
    COMPOSITE_THRESHOLD = 60.0
    MIN_CONTRIBUTING    = 5
    OFUI_URGENT         = 3.0
    T1_ALLOCATION_PCT   = 0.40
    T2_ALLOCATION_PCT   = 0.60
    T1_BUFFER_PCT       = 0.0005
    T2_ICEBERG_BUFFER   = 0.0008
    STOP_LOSS_PCT       = 0.015
    TAKE_PROFIT_PCT     = 0.042
    PURGE_EVERY_N       = 5
    MAX_HISTORY_ROWS    = 1000
    SCAN_INTERVAL       = 45
    TOP_CANDIDATES      = 15
    BINANCE_FAPI        = "https://fapi.binance.com"
    BINANCE_API         = "https://api.binance.com"
    HF_HOST             = "0.0.0.0"
    HF_PORT             = int(os.getenv("PORT", 7860))  # Railway inject PORT otomatis

ENGINE_FAMILY = {
    "multi_cvd":"FLOW","ob_toxicity":"FLOW","block_trades":"FLOW",
    "vmc_ratio":"FLOW","l2_dex_velocity":"FLOW",
    "oi_velocity":"POSITIONING","funding_premium":"POSITIONING",
    "spot_futures_div":"POSITIONING","liq_cartography":"POSITIONING",
    "fractal_confluence":"STRUCTURE","stat_zscore":"STRUCTURE","beta_correlation":"STRUCTURE",
    "liq_gaps":"LIQUIDITY","arkham_flow":"ONCHAIN","session_profiles":"CONTEXT",
}
FAMILY_WEIGHTS = {"FLOW":0.34,"POSITIONING":0.26,"STRUCTURE":0.22,
                  "LIQUIDITY":0.07,"ONCHAIN":0.06,"CONTEXT":0.05}

# ── GLOBAL STATE ──────────────────────────────────────────────────────────────
_status = {"status":"INIT","signals_fired":0,"scan_cycle":0,
           "last_scan":"N/A","ram_purge":"OPERATIONAL","engine_health":"EXCELLENT"}
_history: Dict[str,pd.DataFrame] = {}
_loop_counter = 0
_watchlist: List[dict] = []
_tele_executor = ThreadPoolExecutor(max_workers=3)

# ══════════════════════════════════════════════════════════════════════════════
#  TELEGRAM — uses requests in thread (bypasses aiohttp network issues on HF)
# ══════════════════════════════════════════════════════════════════════════════
def _tele_send_sync(text: str) -> bool:
    """Synchronous Telegram send via requests — works on HF where aiohttp fails."""
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    clean = text
    for ch in ["*","_","`","[","]","~"]:
        clean = clean.replace(ch,"")
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": clean[:4000],
               "disable_web_page_preview": True}
    for attempt in range(5):
        try:
            r = req_lib.post(url, json=payload, timeout=20)
            data = r.json()
            if data.get("ok"):
                log.info(f"Telegram SENT OK (attempt {attempt+1})")
                return True
            if r.status_code == 429:
                wait = data.get("parameters",{}).get("retry_after", 10)
                log.warning(f"Telegram 429 wait {wait}s")
                time.sleep(wait); continue
            log.warning(f"Telegram FAIL {r.status_code}: {str(data)[:200]}")
            return False
        except Exception as e:
            log.warning(f"Telegram sync err attempt {attempt+1}: {e}")
            time.sleep(4*(attempt+1))
    return False

async def tele_send(text: str) -> bool:
    """Async wrapper — runs sync requests call in thread executor."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(_tele_executor, _tele_send_sync, text)

# ══════════════════════════════════════════════════════════════════════════════
#  FASTAPI
# ══════════════════════════════════════════════════════════════════════════════
app = FastAPI(title="GOD-MODE v7.2")

@app.get("/")
async def root():
    return JSONResponse({"status":"GOD-MODE v7.2 ACTIVE",
                         "engine_health":_status["engine_health"],
                         "signals_fired":_status["signals_fired"],
                         "scan_cycle":_status["scan_cycle"],
                         "last_scan_utc":_status["last_scan"]})

@app.get("/health")
async def health():
    return JSONResponse({"alive":True,"cycle":_status["scan_cycle"]})

@app.get("/watchlist")
async def watchlist():
    return JSONResponse({"candidates":_watchlist,
                         "gate":Config.COMPOSITE_THRESHOLD})

@app.on_event("startup")
async def startup():
    asyncio.create_task(scanner_main_loop())
    log.info("GOD-MODE v7.2 background task launched")

# ══════════════════════════════════════════════════════════════════════════════
#  HTTP UTIL (aiohttp — for Binance only)
# ══════════════════════════════════════════════════════════════════════════════
async def _fetch(session, url, params=None, headers=None, retries=4):
    delay = 2.0
    for attempt in range(retries):
        try:
            async with session.get(url, params=params, headers=headers,
                                   timeout=aiohttp.ClientTimeout(total=12)) as r:
                if r.status == 429:
                    await asyncio.sleep(delay*(2**attempt)); continue
                if r.status != 200: return None
                return await r.json()
        except Exception:
            await asyncio.sleep(delay*(2**attempt))
    return None

# ══════════════════════════════════════════════════════════════════════════════
#  BINANCE
# ══════════════════════════════════════════════════════════════════════════════
class BC:
    F = Config.BINANCE_FAPI; S = Config.BINANCE_API
    @staticmethod
    async def tickers(s): d=await _fetch(s,f"{BC.F}/fapi/v1/ticker/24hr"); return d if isinstance(d,list) else []
    @staticmethod
    async def spot(s,sym): return await _fetch(s,f"{BC.S}/api/v3/ticker/24hr",params={"symbol":sym})
    @staticmethod
    async def ob(s,sym,lim=50): return await _fetch(s,f"{BC.F}/fapi/v1/depth",params={"symbol":sym,"limit":lim})
    @staticmethod
    async def klines(s,sym,iv="1m",lim=100): return await _fetch(s,f"{BC.F}/fapi/v1/klines",params={"symbol":sym,"interval":iv,"limit":lim})
    @staticmethod
    async def oi_hist(s,sym): return await _fetch(s,f"{BC.F}/futures/data/openInterestHist",params={"symbol":sym,"period":"5m","limit":30})
    @staticmethod
    async def funding(s,sym): return await _fetch(s,f"{BC.F}/fapi/v1/fundingRate",params={"symbol":sym,"limit":5})
    @staticmethod
    async def trades(s,sym): return await _fetch(s,f"{BC.F}/fapi/v1/aggTrades",params={"symbol":sym,"limit":300})
    @staticmethod
    async def liqs(s,sym): return await _fetch(s,f"{BC.F}/fapi/v1/allForceOrders",params={"symbol":sym,"limit":50})
    @staticmethod
    async def premium(s,sym): return await _fetch(s,f"{BC.F}/fapi/v1/premiumIndex",params={"symbol":sym})

# ══════════════════════════════════════════════════════════════════════════════
#  UTILS
# ══════════════════════════════════════════════════════════════════════════════
def _cvd_kl(kl):
    c,cu=[], 0.0
    for k in kl:
        o,h,l,cv,v=float(k[1]),float(k[2]),float(k[3]),float(k[4]),float(k[5])
        d=v*(cv-o)/(h-l+1e-9) if cv>o else (-v*(o-cv)/(h-l+1e-9) if cv<o else 0.0)
        cu+=d; c.append(cu)
    return c

def _cvd_tr(tr):
    bv=sv=0.0
    for t in tr:
        q=float(t.get("q",0))
        if t.get("m",False): sv+=q
        else: bv+=q
    return bv,sv,bv-sv

def _vpin(tr):
    if len(tr)<50: return 0.5
    bv,sv,_=_cvd_tr(tr); return min(abs(bv-sv)/(bv+sv+1e-9),1.0)

def _zs(s,w=20):
    if len(s)<w: return 0.0
    sub=s[-w:]; mu=statistics.mean(sub)
    try: sd=statistics.stdev(sub)
    except: return 0.0
    return (sub[-1]-mu)/(sd+1e-9)

def _ema(p,n):
    if not p: return 0.0
    if len(p)<n: return p[-1]
    k=2.0/(n+1); v=statistics.mean(p[:n])
    for x in p[n:]: v=x*k+v*(1-k)
    return v

def _iceberg(ob):
    try: bids=[(float(p),float(q)) for p,q in ob.get("bids",[])[:30]]; return max(bids,key=lambda x:x[1])[0] if bids else 0.0
    except: return 0.0

def _gap(ob):
    try:
        asks=[(float(p),float(q)) for p,q in ob.get("asks",[])[:50]]
        if len(asks)<2: return 0.0,0.0
        gs=[(asks[i][0]-asks[i-1][0],asks[i][0]) for i in range(1,len(asks))]
        return max(gs,key=lambda x:x[0])[1],asks[0][0]
    except: return 0.0,0.0

# ══════════════════════════════════════════════════════════════════════════════
#  15 ENGINES
# ══════════════════════════════════════════════════════════════════════════════
def e_oi(oi,px):
    if not oi or len(oi)<6: return 5.0
    try:
        v=[float(x["sumOpenInterest"]) for x in oi]
        roc=(statistics.mean(v[-5:])-statistics.mean(v[-10:-5] if len(v)>=10 else v[:max(1,len(v)-5)]))/(statistics.mean(v[:5])+1e-9)
        return 10.0 if roc>0.08 else 8.5 if roc>0.04 else 7.0 if roc>0.02 else 5.5 if roc>0.01 else 4.0 if roc>0 else 3.0 if roc>-0.01 else 1.5
    except: return 5.0

def e_cvd(k1,k5,tr):
    try:
        bv,sv,_=_cvd_tr(tr); t1=2*(bv/(bv+sv+1e-9))-1
        c1=_cvd_kl(k1); r1=abs(c1[-1])+1e-9 if c1 else 1e-9
        t2=max(-1.0,min(1.0,((c1[-1]-c1[-min(5,len(c1))])/r1)*10)) if c1 else 0.0
        c5=_cvd_kl(k5); r5=abs(c5[-1])+1e-9 if c5 else 1e-9
        t3=max(-1.0,min(1.0,((c5[-1]-c5[-min(5,len(c5))])/r5)*6)) if c5 else 0.0
        s=max(0.0,min(10.0,(t1*0.5+t2*0.3+t3*0.2+1)*5))
        lbl=("VERTICAL AGGRESSIVE BUYING" if s>=8.5 else "BULLISH CVD EXPANSION" if s>=7.0
             else "MODERATE ACCUMULATION" if s>=5.5 else "NEUTRAL" if s>=4.0
             else "MILD DISTRIBUTION" if s>=2.5 else "AGGRESSIVE SELL PRESSURE")
        return s,lbl
    except: return 5.0,"NEUTRAL"

def e_obt(tr,ob):
    try:
        vp=_vpin(tr)*10
        bv=sum(float(q) for _,q in ob.get("bids",[])[:10])
        av=sum(float(q) for _,q in ob.get("asks",[])[:10])
        obs=max(0.0,min(10.0,((bv-av)/(bv+av+1e-9)+1)*5))
        return max(0.0,min(10.0,vp*0.5+obs*0.5))
    except: return 5.0

def e_liq(lq,px):
    if not lq: return 5.0,px*1.02
    try:
        sh=[{"p":float(l.get("price",0)),"q":float(l.get("origQty",0))} for l in lq if l.get("side")=="SELL" and float(l.get("price",0))>px]
        dv=sum(x["q"]*x["p"] for x in sh)
        s=10.0 if dv>1e6 else 8.0 if dv>5e5 else 6.5 if dv>1e5 else 4.5 if dv>1e4 else 2.0
        cp=min(sh,key=lambda x:abs(x["p"]-px))["p"] if sh else px*1.015
        return s,cp
    except: return 5.0,px*1.02

def e_arkham(ak):
    if not ak: return 5.0
    try:
        tr=ak.get("transfers",[]); 
        if not tr: return 5.0
        inf=sum(float(t.get("valueUSD",0)) for t in tr if t.get("type","").lower() in ["receive","in"])
        out=sum(float(t.get("valueUSD",0)) for t in tr if t.get("type","").lower() in ["send","out"])
        r=(inf-out)/(inf+out+1e-9)
        return 10.0 if r>0.7 else 8.0 if r>0.5 else 6.5 if r>0.3 else 5.0 if r>0 else 3.5 if r>-0.3 else 1.5
    except: return 5.0

def e_fund(fd,pm):
    if not fd: return 5.0
    try:
        lt=float(fd[-1]["fundingRate"]) if fd else 0.0
        s=9.0 if lt<-0.001 else 7.5 if lt<-0.0003 else 6.5 if abs(lt)<0.0001 else 5.0 if lt<0.0003 else 3.5 if lt<0.001 else 1.5
        if pm:
            mk=float(pm.get("markPrice",0)); ix=float(pm.get("indexPrice",1))
            pr=(mk-ix)/(ix+1e-9)
            if pr<-0.001: s=min(10.0,s+1.5)
            elif pr>0.002: s=max(0.0,s-1.0)
        return max(0.0,min(10.0,s))
    except: return 5.0

def e_gaps(ob,px):
    try:
        tg,sw=_gap(ob)
        if tg<=0: return 5.0,px*1.02
        gp=(tg-px)/(px+1e-9)*100
        asks=[(float(p),float(q)) for p,q in ob.get("asks",[])[:20]]
        thin=statistics.mean([q for _,q in asks])<1.0 if asks else False
        s=9.5 if gp<0.5 and thin else 8.0 if gp<1.0 and thin else 6.5 if gp<1.5 else 5.0 if gp<2.5 else 3.0
        return max(0.0,min(10.0,s)),tg
    except: return 5.0,px*1.02

def e_l2(k1):
    try:
        if len(k1)<10: return 5.0
        vl=[float(k[5]) for k in k1]
        r=statistics.mean(vl[-3:])/(statistics.mean(vl[-20:-3] if len(vl)>=20 else vl[:-3])+1e-9)
        return 10.0 if r>5 else 8.5 if r>3 else 7.0 if r>2 else 5.5 if r>1.5 else 4.0 if r>1 else 2.0
    except: return 5.0

def e_beta(ka,kb):
    try:
        if len(ka)<10 or len(kb)<10: return 5.0
        n=min(len(ka),len(kb),20)
        ar=[(float(ka[i][4])-float(ka[i-1][4]))/float(ka[i-1][4]) for i in range(-n,0)]
        br=[(float(kb[i][4])-float(kb[i-1][4]))/float(kb[i-1][4]) for i in range(-n,0)]
        ma,mb=statistics.mean(ar),statistics.mean(br)
        cov=statistics.mean([(a-ma)*(b-mb) for a,b in zip(ar,br)])
        beta=cov/(statistics.variance(br) if len(br)>1 else 1e-9+1e-9)
        alpha=sum(ar[-5:])-beta*sum(br[-5:])
        return 10.0 if alpha>0.05 else 8.0 if alpha>0.02 else 6.5 if alpha>0.005 else 4.5 if alpha>-0.005 else 2.0
    except: return 5.0

def e_vmc(k1):
    try:
        if len(k1)<15: return 5.0
        cl=[float(k[4]) for k in k1]; vl=[float(k[5]) for k in k1]
        pm=(cl[-1]-cl[-6])/(cl[-6]+1e-9)
        vr=statistics.mean(vl[-5:])/(statistics.mean(vl[-15:-5])+1e-9)
        return(10.0 if pm>0.01 and vr>1.5 else 8.0 if pm>0.005 and vr>1.2 else
               6.5 if pm>0.002 and vr>1 else 3.5 if pm>0 and vr<0.8 else 2.0 if pm<0 and vr>1.5 else 4.5)
    except: return 5.0

def e_zs(k1,k1h):
    try:
        z1h=_zs([float(k[4]) for k in k1h],20) if k1h else 0.0
        z1m=_zs([float(k[4]) for k in k1],30) if k1 else 0.0
        s=(9.0 if 0.3<z1h<2 and 0.2<z1m<1.5 else 7.0 if 0<z1h<2.5 and z1m>0
           else 3.5 if z1h>2.5 else 2.5 if z1h<0 else 4.5)
        return max(0.0,min(10.0,s)),z1m
    except: return 5.0,0.0

def e_blk(tr,px):
    try:
        if len(tr)<20: return 5.0
        sz=[float(t.get("q",0)) for t in tr]; thr=statistics.median(sz)*5
        bb=sum(float(t.get("q",0)) for t in tr if float(t.get("q",0))>=thr and not t.get("m",True))
        bs=sum(float(t.get("q",0)) for t in tr if float(t.get("q",0))>=thr and t.get("m",True))
        tot=bb+bs
        if tot<1e-9: return 5.0
        d=bb/tot
        return 10.0 if d>0.75 else 8.0 if d>0.6 else 6.0 if d>0.5 else 4.5 if d>0.4 else 2.5
    except: return 5.0

def e_sess():
    h=datetime.now(timezone.utc).hour
    if 13<=h<17: return 10.0,"LONDON-NY OVERLAP"
    if 8<=h<13: return 7.5,"LONDON SESSION"
    if 17<=h<22: return 7.5,"NY SESSION"
    if 22<=h or h<2: return 5.0,"NY CLOSE"
    if 2<=h<8: return 4.0,"TOKYO SESSION"
    return 6.0,"TRANSITION"

def e_sfd(sp,ft,px):
    if not sp: return 5.0,0.0
    try:
        bp=(float(ft.get("lastPrice",px))-float(sp.get("lastPrice",px)))/(float(sp.get("lastPrice",px))+1e-9)*100
        s=9.0 if bp>0.5 else 7.5 if bp>0.2 else 6.0 if bp>0 else 4.5 if bp>-0.2 else 3.0 if bp>-0.5 else 1.5
        return max(0.0,min(10.0,s)),bp
    except: return 5.0,0.0

def e_frac(k1,k5,k1h):
    def tf(kl):
        if not kl or len(kl)<22: return 5.0
        cl=[float(k[4]) for k in kl]; e9=_ema(cl,9); e21=_ema(cl,21); p=cl[-1]
        return 10.0 if p>e9>e21 else 7.0 if p>e21 else 5.5 if p>e9 else 4.0 if e9>e21 else 2.0
    try:
        s=tf(k1)*0.2+tf(k5)*0.3+tf(k1h)*0.5
        return max(0.0,min(10.0,s)),float(k1[-1][4]) if k1 else 0.0
    except: return 5.0,0.0

# ══════════════════════════════════════════════════════════════════════════════
#  COMPOSITE (data-gated + family-grouped)
# ══════════════════════════════════════════════════════════════════════════════
def ofui(cvd,obt,blk,vmc):
    return round(((cvd/10)*0.35+(obt/10)*0.25+(blk/10)*0.25+(vmc/10)*0.15)*5.0,2)

def composite(scores,data_ok):
    fam={}
    for f in FAMILY_WEIGHTS:
        mb=[e for e,ff in ENGINE_FAMILY.items() if ff==f and data_ok.get(e) and e in scores]
        if mb: fam[f]=sum(scores[e] for e in mb)/len(mb)
    if not fam: return 0.0,0,{}
    aw=sum(FAMILY_WEIGHTS[f] for f in fam)
    c=sum(FAMILY_WEIGHTS[f]*s for f,s in fam.items())/aw
    n=sum(1 for e in ENGINE_FAMILY if data_ok.get(e))
    return round(c*10,1),n,{f:round(s,1) for f,s in fam.items()}

def exec_matrix(px,ib,of_val):
    t1c=px*(1+Config.T1_BUFFER_PCT); ib=ib or px*0.995
    t2=ib*(1+Config.T2_ICEBERG_BUFFER); bl=Config.T1_ALLOCATION_PCT*t1c+Config.T2_ALLOCATION_PCT*t2
    sl=bl*(1-Config.STOP_LOSS_PCT); tp=bl*(1+Config.TAKE_PROFIT_PCT*(1+min(of_val/3,2.5)*0.3))
    return {"t1":round(px,6),"t1c":round(t1c,6),"t2":round(t2,6),"ib":round(ib,6),
            "sl":round(sl,6),"tp":round(tp,6),"rr":round((tp-bl)/(bl-sl+1e-9),2)}

def purge():
    global _history
    for s in list(_history.keys()):
        if len(_history[s])>Config.MAX_HISTORY_ROWS:
            _history[s]=_history[s].tail(Config.MAX_HISTORY_ROWS).copy()
    freed=gc.collect(); _status["ram_purge"]=f"PURGED freed={freed}"
    log.info(f"RAM purge: freed {freed} objects")

# ══════════════════════════════════════════════════════════════════════════════
#  PRE-SCREEN + ANALYZE
# ══════════════════════════════════════════════════════════════════════════════
async def build_candidates(session,all_tickers):
    usdt=[t for t in all_tickers if t.get("symbol","").endswith("USDT")
          and float(t.get("quoteVolume",0))>=Config.MIN_VOLUME_24H_USD]
    usdt.sort(key=lambda x:float(x.get("quoteVolume",0)),reverse=True)
    passed=[]
    for tk in usdt[:Config.TOP_CANDIDATES]:
        ob=await BC.ob(session,tk["symbol"],20)
        if not ob: continue
        try:
            bb=float(ob["bids"][0][0]); ba=float(ob["asks"][0][0])
            if (ba-bb)/(bb+1e-9)<=Config.MAX_SPREAD_PCT:
                tk["_ob"]=ob; passed.append(tk)
        except: pass
        await asyncio.sleep(0.03)
    log.info(f"Candidates: {len(passed)}/{Config.TOP_CANDIDATES}")
    return passed

async def analyze(session,ticker,btc1m):
    sym=ticker["symbol"]; px=float(ticker.get("lastPrice",0)); ob=ticker.get("_ob",{})
    if px<=0: return None
    k1,k5,k1h,oi,fd,tr,lq,pm,sp=await asyncio.gather(
        BC.klines(session,sym,"1m",100),BC.klines(session,sym,"5m",100),
        BC.klines(session,sym,"1h",50),BC.oi_hist(session,sym),
        BC.funding(session,sym),BC.trades(session,sym),
        BC.liqs(session,sym),BC.premium(session,sym),BC.spot(session,sym))
    k1=k1 or[]; k5=k5 or[]; k1h=k1h or[]; oi=oi or[]; tr=tr or[]

    s2,lbl=e_cvd(k1,k5,tr); s3=e_obt(tr,ob)
    s4,lcp=e_liq(lq,px); s5=e_arkham(None)
    s6=e_fund(fd,pm); s7,tg=e_gaps(ob,px)
    s8=e_l2(k1); s9=e_beta(k1,btc1m); s10=e_vmc(k1)
    s11,z1m=e_zs(k1,k1h); s12=e_blk(tr,px)
    s13,sl=e_sess(); s14,bp=e_sfd(sp,ticker,px)
    s15,_=e_frac(k1,k5,k1h); s1=e_oi(oi,px)

    scores={"oi_velocity":s1,"multi_cvd":s2,"ob_toxicity":s3,"liq_cartography":s4,
            "arkham_flow":s5,"funding_premium":s6,"liq_gaps":s7,"l2_dex_velocity":s8,
            "beta_correlation":s9,"vmc_ratio":s10,"stat_zscore":s11,"block_trades":s12,
            "session_profiles":s13,"spot_futures_div":s14,"fractal_confluence":s15}
    data_ok={"oi_velocity":len(oi)>=6,"multi_cvd":len(tr)>=50 or len(k1)>=10,
             "ob_toxicity":len(tr)>=50 and bool(ob.get("bids")),
             "liq_cartography":bool(lq),"arkham_flow":False,"funding_premium":bool(fd),
             "liq_gaps":bool(ob.get("asks")),"l2_dex_velocity":len(k1)>=10,
             "beta_correlation":len(k1)>=10 and len(btc1m)>=10,"vmc_ratio":len(k1)>=15,
             "stat_zscore":len(k1h)>=20 and len(k1)>=30,"block_trades":len(tr)>=20,
             "session_profiles":True,"spot_futures_div":bool(sp),
             "fractal_confluence":len(k1)>=22 or len(k5)>=22 or len(k1h)>=22}

    cs,n,fam=composite(scores,data_ok)
    of=ofui(s2,s3,s12,s10)
    ib=_iceberg(ob); _,sw=_gap(ob)
    em=exec_matrix(px,ib,of)
    urg=("URGENT RALLY" if of>=Config.OFUI_URGENT else
         "ELEVATED MOMENTUM" if of>=2 else "BUILDING PRESSURE" if of>=1 else "CONSOLIDATION")
    return {"symbol":sym,"price":px,"composite":cs,"ofui":of,"contributing":n,
            "families":fam,"cvd_label":lbl,"target_gap":round(tg,6),
            "sweep_price":round(sw,6),"ib_price":round(ib,6),
            "exec_matrix":em,"urgency":urg,"scores":scores}

# ══════════════════════════════════════════════════════════════════════════════
#  SIGNAL ALERT FORMATTER
# ══════════════════════════════════════════════════════════════════════════════
def build_alert(r):
    em=r["exec_matrix"]
    return (
        f"SINYAL GOD-MODE v7.2\n"
        f"{'='*30}\n"
        f"ASSET: {r['symbol']}\n"
        f"HARGA: ${r['price']}\n"
        f"SCORE: {r['composite']}/100\n"
        f"OFUI: {r['ofui']}x | {r['urgency']}\n"
        f"DATA: {r['contributing']}/15 engine\n"
        f"\n--- ENTRY SPLIT ---\n"
        f"TRANCHE 1 (40% MARKET): ${em['t1']} - ${em['t1c']}\n"
        f"TRANCHE 2 (60% LIMIT): ${em['t2']}\n"
        f"ICEBERG WALL: ${em['ib']}\n"
        f"\n--- RISK ---\n"
        f"STOP LOSS: ${em['sl']}\n"
        f"TAKE PROFIT: ${em['tp']}\n"
        f"R:R = 1:{em['rr']}\n"
        f"\n--- TAPE ---\n"
        f"CVD: {r['cvd_label']}\n"
        f"GAP TARGET: ${r['target_gap']}\n"
        f"SWEEP: ${r['sweep_price']}\n"
        f"\n[PAPER MODE - verifikasi dulu sebelum eksekusi]"
    )

# ══════════════════════════════════════════════════════════════════════════════
#  MAIN LOOP
# ══════════════════════════════════════════════════════════════════════════════
async def scanner_main_loop():
    global _loop_counter
    _status["status"]="SCANNING"

    # ── test Telegram DULU sebelum apapun ────────────────────────────────────
    log.info("Testing Telegram connection...")
    tele_ok=await tele_send(
        f"GOD-MODE v7.2 ONLINE\n"
        f"Waktu: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC\n"
        f"Telegram: OK (via requests/thread)\n"
        f"Binance: connecting...\n"
        f"Gate: score >= {Config.COMPOSITE_THRESHOLD} + {Config.MIN_CONTRIBUTING} engine berdata\n"
        f"Scan pertama dalam 45 detik."
    )
    log.info(f"Startup Telegram test: {'SENT OK' if tele_ok else 'FAILED'}")
    if not tele_ok:
        log.error("TELEGRAM TIDAK BISA KIRIM DARI SERVER INI. Cek token/chat_id.")

    connector=aiohttp.TCPConnector(limit=30,ttl_dns_cache=300)
    async with aiohttp.ClientSession(connector=connector) as session:
        while True:
            try:
                _loop_counter+=1; t0=time.time()
                _status["scan_cycle"]=_loop_counter
                ts=datetime.utcnow().strftime("%H:%M:%S")
                _status["last_scan"]=datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
                log.info(f"{'='*50}\nSCAN #{_loop_counter} | {ts} UTC\n{'='*50}")

                if _loop_counter%Config.PURGE_EVERY_N==0:
                    purge()

                # fetch tickers
                all_tickers=await BC.tickers(session)
                if not all_tickers:
                    msg=f"SCAN #{_loop_counter} | {ts}\nBinance ticker GAGAL. Retry..."
                    log.warning(msg)
                    await tele_send(msg)
                    await asyncio.sleep(Config.SCAN_INTERVAL); continue

                btc1m=await BC.klines(session,"BTCUSDT","1m",50) or []
                candidates=await build_candidates(session,all_tickers)

                if not candidates:
                    msg=f"SCAN #{_loop_counter} | {ts}\n0 kandidat lolos filter.\nTotal ticker: {len(all_tickers)}"
                    log.warning(msg); await tele_send(msg)
                    await asyncio.sleep(Config.SCAN_INTERVAL); continue

                # analyze
                results=[]
                for tk in candidates:
                    try:
                        res=await analyze(session,tk,btc1m)
                        if res: results.append(res)
                    except Exception as e:
                        log.debug(f"Analyze err {tk.get('symbol')}: {e}")
                    await asyncio.sleep(0.15)

                _watchlist.clear()
                for r in sorted(results,key=lambda x:x["composite"],reverse=True)[:8]:
                    _watchlist.append({"symbol":r["symbol"],"composite":r["composite"],
                                       "ofui":r["ofui"],"price":r["price"],
                                       "contributing":r["contributing"]})

                # fire signals
                fired=0
                for res in sorted(results,key=lambda r:r["composite"],reverse=True):
                    cs,ct,of=res["composite"],res["contributing"],res["ofui"]
                    log.info(f"{res['symbol']:>14} | {cs:5.1f}/100 | {ct:2}/15 | OFUI {of:.2f}x | {res['urgency']}")
                    if cs>=Config.COMPOSITE_THRESHOLD and ct>=Config.MIN_CONTRIBUTING:
                        _status["signals_fired"]+=1; fired+=1
                        ok=await tele_send(build_alert(res))
                        log.info(f"  SIGNAL {'SENT' if ok else 'FAIL'} -> {res['symbol']}")
                    await asyncio.sleep(0.15)

                # watchlist digest — SELALU kirim tiap cycle
                if results:
                    top=sorted(results,key=lambda r:r["composite"],reverse=True)[:5]
                    rows=[f"SCAN #{_loop_counter} | {ts} UTC",
                          f"Kandidat:{len(results)} | Gate:{Config.COMPOSITE_THRESHOLD} | Total sinyal:{_status['signals_fired']}",""]
                    for i,r in enumerate(top,1):
                        tag="[SINYAL]" if r["composite"]>=Config.COMPOSITE_THRESHOLD and r["contributing"]>=Config.MIN_CONTRIBUTING else "[DEKAT]" if r["composite"]>=Config.COMPOSITE_THRESHOLD-5 else "[WATCH]"
                        rows.append(f"{tag} {i}. {r['symbol']}")
                        rows.append(f"   Score:{r['composite']} OFUI:{r['ofui']}x Data:{r['contributing']}/15")
                        rows.append(f"   ${r['price']} | {r['urgency']}")
                        rows.append("")
                    await tele_send("\n".join(rows))

                elapsed=time.time()-t0
                log.info(f"Cycle #{_loop_counter} done {elapsed:.1f}s | fired:{fired} | total:{_status['signals_fired']}")
                await asyncio.sleep(max(5.0,Config.SCAN_INTERVAL-elapsed))

            except KeyboardInterrupt:
                _status["status"]="STOPPED"; break
            except Exception as e:
                log.error(f"Loop err cycle #{_loop_counter}: {e}")
                await tele_send(f"ERROR CYCLE #{_loop_counter}: {str(e)[:300]}")
                await asyncio.sleep(10)

if __name__=="__main__":
    import platform
    log.info(f"Python {platform.python_version()} | GOD-MODE v7.2")
    IS_HF=os.getenv("SPACE_ID") is not None
    IS_COLAB="google.colab" in sys.modules
    if IS_HF or not IS_COLAB:
        uvicorn.run(app,host=Config.HF_HOST,port=Config.HF_PORT,log_level="warning")
    else:
        asyncio.run(scanner_main_loop())
