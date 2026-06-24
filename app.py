#!/usr/bin/env python3
"""GOD-MODE v7.4 — Deribit IV + DexScreener + CVD divergence + spoof filter + TIERS"""
import subprocess, sys
for pkg in ["fastapi","uvicorn[standard]","aiohttp","nest_asyncio","numpy","requests"]:
    try: __import__(pkg.split("[")[0])
    except: subprocess.check_call([sys.executable,"-m","pip","install","-q",pkg])

import os, gc, time, asyncio, logging, statistics, math
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor
import requests as rq
import aiohttp, numpy as np, nest_asyncio
from fastapi import FastAPI
from fastapi.responses import JSONResponse
import uvicorn

nest_asyncio.apply()
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-7s | %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("v7.4")

TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN","8737317449:AAH7y1Kc7_fn3YxqCK2VfgWAKGjbIScm_js")
TG_CHAT  = os.getenv("TELEGRAM_CHAT_ID",  "8558439530")

class C:
    MIN_VOL=2_000_000; MAX_SPREAD=0.0015
    TIER_S=70; TIER_A=55; TIER_B=42     # konviksi thresholds
    MIN_DATA=6; TOP=18; SCAN=50; PORT=int(os.getenv("PORT",8080))
    F="https://fapi.binance.com"; S="https://api.binance.com"
    DERIBIT="https://www.deribit.com/api/v2"
    DEX="https://api.dexscreener.com/latest/dex"
    SL_PCT=0.015; TP_MULT=2.8
    SPOOF_WALL_RATIO=8.0   # wall vs flow > ini = curiga spoof/inaktif

_st={"status":"INIT","fired":0,"cycle":0,"last":"N/A","dvol_btc":0,"dvol_eth":0}
_tiers={"S":[],"A":[],"B":[],"SHORT":[]}
_exec=ThreadPoolExecutor(max_workers=3)

# ══ TELEGRAM ══════════════════════════════════════════════════════════════════
def _tg_sync(text):
    url=f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    for ch in ["*","_","`","[","]","~"]: text=text.replace(ch,"")
    for a in range(4):
        try:
            r=rq.post(url,json={"chat_id":TG_CHAT,"text":text[:4000],"disable_web_page_preview":True},timeout=20)
            if r.json().get("ok"): return True
            if r.status_code==429: time.sleep(r.json().get("parameters",{}).get("retry_after",8)); continue
            return False
        except Exception as e: log.warning(f"TG {a+1}: {e}"); time.sleep(3*(a+1))
    return False
async def tg(t): return await asyncio.get_event_loop().run_in_executor(_exec,_tg_sync,t)

# ══ FASTAPI ═══════════════════════════════════════════════════════════════════
app=FastAPI()
@app.get("/")
async def root(): return JSONResponse({"status":"GOD-MODE v7.4 ACTIVE","cycle":_st["cycle"],"fired":_st["fired"],"dvol_btc":_st["dvol_btc"]})
@app.get("/health")
async def health(): return JSONResponse({"alive":True,"cycle":_st["cycle"]})
@app.get("/tiers")
async def tiers(): return JSONResponse(_tiers)
@app.on_event("startup")
async def _s(): asyncio.create_task(main_loop())

async def fetch(s,url,p=None,retries=3,total=12):
    for a in range(retries):
        try:
            async with s.get(url,params=p,timeout=aiohttp.ClientTimeout(total=total)) as r:
                if r.status==429: await asyncio.sleep(2*(2**a)); continue
                if r.status!=200: return None
                return await r.json()
        except Exception: await asyncio.sleep(1.5*(2**a))
    return None

# ══ BINANCE ═══════════════════════════════════════════════════════════════════
class B:
    @staticmethod
    async def tickers(s): d=await fetch(s,f"{C.F}/fapi/v1/ticker/24hr"); return d if isinstance(d,list) else []
    @staticmethod
    async def spot(s,sym): return await fetch(s,f"{C.S}/api/v3/ticker/24hr",{"symbol":sym})
    @staticmethod
    async def ob(s,sym,l=50): return await fetch(s,f"{C.F}/fapi/v1/depth",{"symbol":sym,"limit":l})
    @staticmethod
    async def kl(s,sym,iv="1m",l=100): return await fetch(s,f"{C.F}/fapi/v1/klines",{"symbol":sym,"interval":iv,"limit":l})
    @staticmethod
    async def oi(s,sym): return await fetch(s,f"{C.F}/futures/data/openInterestHist",{"symbol":sym,"period":"5m","limit":12})
    @staticmethod
    async def fund(s,sym): return await fetch(s,f"{C.F}/fapi/v1/fundingRate",{"symbol":sym,"limit":3})
    @staticmethod
    async def trades(s,sym): return await fetch(s,f"{C.F}/fapi/v1/aggTrades",{"symbol":sym,"limit":300})
    @staticmethod
    async def crowd(s,sym): return await fetch(s,f"{C.F}/futures/data/globalLongShortAccountRatio",{"symbol":sym,"period":"5m","limit":2})
    @staticmethod
    async def toptrader(s,sym): return await fetch(s,f"{C.F}/futures/data/topLongShortPositionRatio",{"symbol":sym,"period":"5m","limit":2})

# ══ DERIBIT (IV regime, keyless) ══════════════════════════════════════════════
class DBT:
    @staticmethod
    async def dvol(s,ccy):
        now=int(time.time()*1000); start=now-3*3600*1000
        d=await fetch(s,f"{C.DERIBIT}/public/get_volatility_index_data",
                      {"currency":ccy,"start_timestamp":start,"end_timestamp":now,"resolution":3600})
        try: return round(float(d["result"]["data"][-1][4]),1)  # last close = DVOL %
        except Exception: return 0.0

# ══ DEXSCREENER (on-chain momentum, keyless) ══════════════════════════════════
class DEX:
    @staticmethod
    async def momentum(s,base):
        d=await fetch(s,f"{C.DEX}/search",{"q":base},total=10)
        try:
            pairs=d.get("pairs",[])
            if not pairs: return None
            # ambil pair likuiditas tertinggi (paling kredibel, hindari token sampah)
            best=max(pairs,key=lambda p:float((p.get("liquidity") or {}).get("usd",0) or 0))
            liq=float((best.get("liquidity") or {}).get("usd",0) or 0)
            if liq<50000: return None  # likuiditas terlalu kecil = abaikan
            ch=float((best.get("priceChange") or {}).get("h24",0) or 0)
            vol=float((best.get("volume") or {}).get("h24",0) or 0)
            return {"chg_h24":ch,"vol_h24":vol,"liq":liq}
        except Exception: return None

# ══ UTILS ═════════════════════════════════════════════════════════════════════
def clamp(x,lo=-1.0,hi=1.0): return max(lo,min(hi,x))
def cvd_tr(tr):
    bv=sv=0.0
    for t in tr:
        q=float(t.get("q",0))
        if t.get("m",False): sv+=q
        else: bv+=q
    return bv,sv
def cvd_series(kl):
    c,cum=[],0.0
    for k in kl:
        o,h,l,cl,v=float(k[1]),float(k[2]),float(k[3]),float(k[4]),float(k[5])
        d=v*(cl-o)/(h-l+1e-9) if cl>o else (-v*(o-cl)/(h-l+1e-9) if cl<o else 0.0)
        cum+=d; c.append(cum)
    return c
def ema(p,n):
    if not p: return 0.0
    if len(p)<n: return p[-1]
    k=2.0/(n+1); v=statistics.mean(p[:n])
    for x in p[n:]: v=x*k+v*(1-k)
    return v
def zsc(s,w=20):
    if len(s)<w: return 0.0
    sub=s[-w:]
    try: return (sub[-1]-statistics.mean(sub))/(statistics.stdev(sub)+1e-9)
    except: return 0.0

# ── CVD DIVERGENCE: harga vs CVD beda arah = akumulasi/distribusi tersembunyi ──
def cvd_divergence(k1):
    if len(k1)<20: return 0.0,"-"
    closes=[float(x[4]) for x in k1]; cvd=cvd_series(k1)
    n=12
    p_chg=(closes[-1]-closes[-n])/(closes[-n]+1e-9)
    c_ref=abs(cvd[-n])+1e-9
    c_chg=(cvd[-1]-cvd[-n])/c_ref
    # bullish div: harga turun/flat TAPI CVD naik (dikumpulin diam-diam)
    if p_chg<0.002 and c_chg>0.05: return clamp(c_chg),"BULLISH DIV (akumulasi diam)"
    # bearish div: harga naik TAPI CVD turun (didistribusi ke ritel)
    if p_chg>0.005 and c_chg<-0.05: return -clamp(abs(c_chg)),"BEARISH DIV (distribusi)"
    return clamp(c_chg*0.3),"-"

# ── ABSORPSI vs SPOOF: tembok beneran diserap atau cuma pajangan lalu dicabut ──
def absorption(ob,tr,cvd_sign):
    try:
        bid=sum(float(q) for _,q in ob.get("bids",[])[:10])
        ask=sum(float(q) for _,q in ob.get("asks",[])[:10])
        last_px=float(ob.get("bids",[["0"]])[0][0])
        flow=sum(float(t.get("q",0)) for t in tr[-100:]) if tr else 0.0
        wall=max(bid,ask)
        ratio=wall/(flow+1e-9)
        # tembok raksasa tapi nyaris gak ada transaksi = kemungkinan spoof/inaktif
        if ratio>C.SPOOF_WALL_RATIO and abs(cvd_sign)<0.2:
            return False,"SPOOF? tembok besar tanpa flow"
        # bid dominan + CVD positif + transaksi aktif = absorpsi beli nyata
        if bid>ask*1.2 and cvd_sign>0.1 and ratio<C.SPOOF_WALL_RATIO:
            return True,"ABSORPSI BELI NYATA"
        if ask>bid*1.2 and cvd_sign<-0.1:
            return False,"tekanan jual nyata"
        return None,"netral"
    except Exception:
        return None,"-"

# ── EXPECTED MOVE (volatilitas) — RANGE statistik, BUKAN target prediksi ──────
def atr_pct(k1h,px):
    if len(k1h)<15 or px<=0: return 0.0
    trs=[]
    for i in range(1,len(k1h)):
        h=float(k1h[i][2]); l=float(k1h[i][3]); pc=float(k1h[i-1][4])
        trs.append(max(h-l,abs(h-pc),abs(l-pc)))
    atr=statistics.mean(trs[-14:])
    return round(atr/px*100,2)  # ATR per jam dalam %

def expected_daily_pct(atr_h,dvol):
    # gabung ATR realized (per jam → harian) + IV regime Deribit kalau ada
    daily_realized=atr_h*math.sqrt(24) if atr_h else 0.0
    if dvol>0:
        daily_iv=dvol/math.sqrt(365)  # DVOL annualized % → daily %
        return round((daily_realized*0.6+daily_iv*0.4),1)
    return round(daily_realized,1)

# ══ SIGNED SIGNALS ════════════════════════════════════════════════════════════
def s_cvd(k1,tr):
    bv,sv=cvd_tr(tr); micro=2*(bv/(bv+sv+1e-9))-1
    if k1 and len(k1)>=6:
        c=[float(x[4]) for x in k1]; sl=(c[-1]-c[-6])/(c[-6]+1e-9)
        return clamp(micro*0.6+clamp(sl*40)*0.4)
    return clamp(micro)
def s_ob(ob):
    try:
        b=sum(float(q) for _,q in ob.get("bids",[])[:10]); a=sum(float(q) for _,q in ob.get("asks",[])[:10])
        return clamp((b-a)/(b+a+1e-9))
    except: return 0.0
def s_block(tr):
    if len(tr)<20: return 0.0
    sz=[float(t.get("q",0)) for t in tr]; thr=statistics.median(sz)*5
    bb=sum(float(t.get("q",0)) for t in tr if float(t.get("q",0))>=thr and not t.get("m",True))
    bs=sum(float(t.get("q",0)) for t in tr if float(t.get("q",0))>=thr and t.get("m",True))
    tot=bb+bs; return clamp((bb-bs)/(tot+1e-9)) if tot>1e-9 else 0.0
def s_oi(oi,chg):
    if not oi or len(oi)<4: return 0.0
    v=[float(x["sumOpenInterest"]) for x in oi]; roc=(v[-1]-v[0])/(v[0]+1e-9)
    return clamp(roc*15)*(1 if chg>=0 else -1)
def s_trend(k1,k5,k1h):
    def tf(kl):
        if not kl or len(kl)<22: return 0.0
        c=[float(x[4]) for x in kl]; e9=ema(c,9); e21=ema(c,21); p=c[-1]
        if p>e9>e21: return 1.0
        if p<e9<e21: return -1.0
        return clamp((p-e21)/(e21+1e-9)*50)
    return clamp(tf(k1)*0.2+tf(k5)*0.3+tf(k1h)*0.5)
def s_fund(fd):
    if not fd: return 0.0
    lt=float(fd[-1]["fundingRate"])
    if lt<-0.0005: return 0.6
    if lt>0.0008: return -0.5
    return clamp(-lt*400)
def s_crowd(cr):
    if not cr: return 0.0
    try: return clamp(-(float(cr[-1]["longShortRatio"])-1.0))
    except: return 0.0
def s_top(tt):
    if not tt: return 0.0
    try: return clamp(float(tt[-1]["longShortRatio"])-1.0)
    except: return 0.0
def s_dex(dx):
    if not dx: return 0.0
    # volume on-chain gede + harga belum gerak banyak = minat awal (bullish lead)
    ch=dx["chg_h24"]
    return clamp(ch/30.0)

# ══ ANALYZE ═══════════════════════════════════════════════════════════════════
async def analyze(s,tk,dvol):
    sym=tk["symbol"]; px=float(tk.get("lastPrice",0)); ob=tk.get("_ob",{})
    chg=float(tk.get("priceChangePercent",0)); base=sym.replace("USDT","")
    if px<=0: return None
    k1,k5,k1h,oi,fd,tr,cr,tt,dx=await asyncio.gather(
        B.kl(s,sym,"1m",80),B.kl(s,sym,"5m",60),B.kl(s,sym,"1h",40),
        B.oi(s,sym),B.fund(s,sym),B.trades(s,sym),B.crowd(s,sym),B.toptrader(s,sym),
        DEX.momentum(s,base))
    k1=k1 or[]; k5=k5 or[]; k1h=k1h or[]; tr=tr or[]

    v_cvd=s_cvd(k1,tr); v_ob=s_ob(ob); v_blk=s_block(tr); v_oi=s_oi(oi,chg)
    v_tr=s_trend(k1,k5,k1h); v_fd=s_fund(fd); v_cr=s_crowd(cr); v_tt=s_top(tt); v_dx=s_dex(dx)
    div_val,div_lbl=cvd_divergence(k1)
    absb,abs_lbl=absorption(ob,tr,v_cvd)
    z=zsc([float(x[4]) for x in k1],20) if k1 else 0.0

    parts=[(v_cvd,0.18,"CVD"),(v_tr,0.14,"Trend"),(div_val,0.12,"Divergence"),
           (v_oi,0.11,"OI"),(v_blk,0.10,"Block"),(v_tt,0.09,"TopTrader"),
           (v_ob,0.08,"OBook"),(v_dx,0.06,"DEX"),(v_fd,0.06,"Funding"),(v_cr,0.06,"CrowdContra")]
    raw=sum(v*w for v,w,_ in parts); ds=round(clamp(raw)*100)
    ranked=sorted(parts,key=lambda c:abs(c[0]*c[1]),reverse=True)
    drivers=[f"{n}{'↑' if v>0 else '↓'}" for v,w,n in ranked[:4] if abs(v)>0.15]

    cov=sum([bool(k1),bool(k5),bool(k1h),bool(oi),bool(fd),len(tr)>=50,bool(cr),bool(tt),bool(dx),bool(ob.get("bids"))])
    cov15=round(cov*1.5)
    conv=min(100,round(abs(ds)*(0.5+0.5*cov/10)))
    side="LONG" if ds>=0 else "SHORT"

    ib=0.0
    try: ib=max([(float(p),float(q)) for p,q in ob.get("bids",[])[:30]],key=lambda x:x[1])[0]
    except: ib=px*0.995
    if side=="LONG":
        sl=min(ib,px)*(1-C.SL_PCT); risk=px-sl; tp=px+risk*C.TP_MULT
    else:
        sl=px*(1+C.SL_PCT); risk=sl-px; tp=px-risk*C.TP_MULT
    rr=round(abs(tp-px)/(abs(px-sl)+1e-9),2)

    atrh=atr_pct(k1h,px); exp_d=expected_daily_pct(atrh,dvol)

    return {"sym":sym,"px":px,"chg":chg,"dir":ds,"side":side,"conv":conv,"drivers":drivers,
            "data":cov15,"z":round(z,2),"div":div_val,"div_lbl":div_lbl,
            "absorb":absb,"abs_lbl":abs_lbl,"dex":dx,"exp_daily":exp_d,
            "sl":round(sl,6),"tp":round(tp,6),"rr":rr,"e1":round(px,6),"e2":round(px*1.0007,6)}

# ══ FORMAT ════════════════════════════════════════════════════════════════════
def fmt(r):
    tags=[]
    if r.get("_laggard"): tags.append("LAGGARD")
    if r.get("absorb") is True: tags.append("ABSORPSI-NYATA")
    if r.get("absorb") is False and "SPOOF" in (r.get("abs_lbl") or ""): tags.append("SPOOF?")
    tagstr=(" ["+", ".join(tags)+"]") if tags else ""
    dexstr=f" | DEX24h:{r['dex']['chg_h24']:+.0f}%" if r.get("dex") else ""
    return (f"{r['sym']} {('↑'+r['side']) if r['side']=='LONG' else '↓'+r['side']} konv {r['conv']}{tagstr}\n"
            f"  Bias:{r['dir']:+d} | 24h:{r['chg']:+.1f}%{dexstr} | exp/hari ±{r['exp_daily']}%\n"
            f"  {r['div_lbl']} | {r['abs_lbl']}\n"
            f"  Entry ${r['e1']}-${r['e2']} | SL ${r['sl']} | TP ${r['tp']} | RR 1:{r['rr']}\n"
            f"  Driver: {', '.join(r['drivers']) if r['drivers'] else 'lemah'} | Data {r['data']}/15")

# ══ MAIN LOOP ═════════════════════════════════════════════════════════════════
async def main_loop():
    _st["status"]="SCANNING"
    ok=await tg(f"GOD-MODE v7.4 ONLINE\n{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')} UTC\n"
                f"+Deribit IV +DexScreener +CVD divergence +filter spoof +TIER S/A/B\n"
                f"Catatan: tier=kualitas setup, BUKAN prediksi %. Magnitudo=expected move dari volatilitas.\n"
                f"Scan pertama ~50 detik.")
    log.info(f"startup TG: {'OK' if ok else 'FAIL'}")
    conn=aiohttp.TCPConnector(limit=30,ttl_dns_cache=300)
    async with aiohttp.ClientSession(connector=conn) as s:
        while True:
            try:
                _st["cycle"]+=1; t0=time.time(); ts=datetime.utcnow().strftime("%H:%M:%S"); _st["last"]=ts
                log.info(f"{'='*40} SCAN #{_st['cycle']} {ts}")
                if _st["cycle"]%5==0: gc.collect()

                # IV regime Deribit (sekali per cycle)
                _st["dvol_btc"]=await DBT.dvol(s,"BTC"); _st["dvol_eth"]=await DBT.dvol(s,"ETH")
                dvol=_st["dvol_btc"]

                tks=await B.tickers(s)
                if not tks: await tg(f"SCAN #{_st['cycle']} Binance gagal"); await asyncio.sleep(C.SCAN); continue
                usdt=[t for t in tks if t.get("symbol","").endswith("USDT") and float(t.get("quoteVolume",0))>=C.MIN_VOL]
                usdt.sort(key=lambda x:float(x.get("quoteVolume",0)),reverse=True)

                cands=[]
                for t in usdt[:C.TOP]:
                    ob=await B.ob(s,t["symbol"],20)
                    if not ob: continue
                    try:
                        bb=float(ob["bids"][0][0]); ba=float(ob["asks"][0][0])
                        if (ba-bb)/(bb+1e-9)<=C.MAX_SPREAD: t["_ob"]=ob; cands.append(t)
                    except: pass
                    await asyncio.sleep(0.03)

                res=[]
                for t in cands:
                    try:
                        r=await analyze(s,t,dvol)
                        if r: res.append(r)
                    except Exception as e: log.debug(f"err {t.get('symbol')}: {e}")
                    await asyncio.sleep(0.12)
                if not res: await tg(f"SCAN #{_st['cycle']} 0 hasil"); await asyncio.sleep(C.SCAN); continue

                med=statistics.median([r["chg"] for r in res])
                for r in res:
                    r["_laggard"]=(r["chg"]<med and r["dir"]>15)

                longs=[r for r in res if r["side"]=="LONG"]
                shorts=sorted([r for r in res if r["side"]=="SHORT"],key=lambda x:x["conv"],reverse=True)

                # TIERING — berdasarkan kualitas setup, bukan prediksi %
                tS=[r for r in longs if r["conv"]>=C.TIER_S and r["absorb"] is True and r["_laggard"]]
                used={r["sym"] for r in tS}
                tA=[r for r in longs if r["conv"]>=C.TIER_A and r["sym"] not in used]
                usedA=used|{r["sym"] for r in tA}
                tB=[r for r in longs if r["conv"]>=C.TIER_B and r["sym"] not in usedA]
                for L in (tS,tA,tB,shorts): L.sort(key=lambda x:x["conv"],reverse=True)
                _tiers["S"]=[r["sym"] for r in tS]; _tiers["A"]=[r["sym"] for r in tA]
                _tiers["B"]=[r["sym"] for r in tB]; _tiers["SHORT"]=[r["sym"] for r in shorts[:5]]

                # fire SINYAL utk Tier S (kualitas tertinggi + absorpsi nyata + laggard)
                fired=0
                for r in tS[:3]:
                    _st["fired"]+=1; fired+=1
                    await tg(f"SINYAL TIER-S (akumulasi nyata, belum naik) - {r['sym']}\n{'='*30}\n"
                             f"Harga ${r['px']} | konviksi {r['conv']}/100 | bias {r['dir']:+d}\n"
                             f"{r['div_lbl']} | {r['abs_lbl']}\n"
                             f"Expected move ~±{r['exp_daily']}%/hari (volatilitas, bukan target)\n\n"
                             f"ENTRY ${r['e1']}-${r['e2']} | SL ${r['sl']} | TP ${r['tp']} | RR 1:{r['rr']}\n"
                             f"Driver: {', '.join(r['drivers'])} | Data {r['data']}/15\n\n[PAPER - verifikasi]")

                # DIGEST bertingkat
                rows=[f"SCAN #{_st['cycle']} | {ts} UTC",
                      f"IV regime (DVOL BTC): {_st['dvol_btc']} | ETH: {_st['dvol_eth']}",
                      f"Median 24h: {med:+.1f}% | Sinyal: {_st['fired']}",""]
                rows.append("===== TIER S: akumulasi NYATA, belum naik =====")
                rows+= [fmt(r) for r in tS[:3]] or ["(kosong - belum ada yg lolos absorpsi+laggard)"]
                rows.append("")
                rows.append("===== TIER A: setup solid =====")
                rows+= [fmt(r) for r in tA[:3]] or ["(kosong)"]
                rows.append("")
                rows.append("===== TIER B: watch / awal =====")
                rows+= [fmt(r) for r in tB[:2]] or ["(kosong)"]
                rows.append("")
                rows.append("===== SHORT: distribusi / overextended =====")
                rows+= [fmt(r) for r in shorts[:2]] or ["(kosong)"]
                rows.append("")
                rows.append("Akurasi terukur: /tiers + nanti /calibration. Tier = kualitas, exp% = volatilitas.")
                await tg("\n".join(rows))

                log.info(f"#{_st['cycle']} {time.time()-t0:.1f}s S:{len(tS)} A:{len(tA)} B:{len(tB)} short:{len(shorts)} fired:{fired}")
                await asyncio.sleep(max(5,C.SCAN-(time.time()-t0)))
            except Exception as e:
                log.error(f"loop err: {e}"); await tg(f"ERROR #{_st['cycle']}: {str(e)[:200]}"); await asyncio.sleep(10)

if __name__=="__main__":
    IS_COLAB = "google.colab" in sys.modules
    if IS_COLAB:
        # Colab/Jupyter sudah punya event loop — jalankan scanner langsung (tanpa uvicorn)
        log.info("Mode: COLAB direct-run (tanpa FastAPI)")
        try:
            loop = asyncio.get_event_loop()
            loop.run_until_complete(main_loop())
        except RuntimeError:
            # fallback kalau loop sudah jalan (nest_asyncio sudah di-apply di atas)
            asyncio.run(main_loop())
    else:
        # Railway/HuggingFace/VPS — pakai FastAPI anti-sleep
        log.info(f"Mode: SERVER FastAPI port {C.PORT}")
        uvicorn.run(app, host="0.0.0.0", port=C.PORT, log_level="warning")
