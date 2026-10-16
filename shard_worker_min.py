#!/usr/bin/env python3
"""shard_worker_min.py — A股 5 分钟线全史回补 worker (GitHub Actions 用)
用法: shard_worker_min.py <shard_id> <total_shards> [batch]
断点: shard_<id>_done.txt  (每只股票完成后追加一行)
输出: wh/minute5/{YYYYMM}.parquet (zstd), 列: symbol,datetime,open,close,high,low,volume, 按 (symbol,datetime) 去重

数据源分层 (实测 2026-10):
  1. 新浪 5 分钟 (quotes.sina.cn CN_MarketDataService.getKLineData):
     单请求 datalen 上限 ~1970 根 (约 2 个月), 无翻页参数 → 只作近期增量
  2. 腾讯 mkline m5 分段翻页 (ifzq.gtimg.cn, param=code,m5,{end},800):
     实测历史深度只到 ~6 个月, 每页 ≤800 根 → 补 2~6 个月中段
  3. Baostock frequency=5: 全史 2011 起, 单请求可拿任意区间 (sz000001 2022 起一次 55488 根)
     → 主力全史源, 每次请求分段 1~2 年拉到 2022 之前
注意: Actions 内跑 baostock 需要其登录服务器可达 (bs.login()), 失败时跳过并保留前两层。
"""
import json, os, sys, time, datetime as dt

WH = "wh/minute5"
DONE = f"shard_{sys.argv[1]}_done.txt"
BATCH = int(sys.argv[3]) if len(sys.argv) > 3 else 100
UA_S = {"User-Agent": "Mozilla/5.0", "Referer": "https://finance.sina.com.cn"}
UA_T = {"User-Agent": "Mozilla/5.0", "Referer": "https://gu.qq.com"}
SINA_DALEN = 1970  # 实测 >1970 返回 null
TENCENT_PAGE = 800
TENCENT_FLOOR = "202604160000"  # 实测腾讯 m5 深度 ~6 个月

import urllib.request
def get(url, h, timeout=20, tries=3):
    for a in range(tries):
        try:
            req = urllib.request.Request(url, headers=h)
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.read().decode("utf-8", "ignore")
        except Exception:
            time.sleep(2 * (a + 1))
    return ""

def rows_of(d):
    return [r["day"], r["open"], r["close"], r["high"], r["low"], r["volume"]]

def sina_m5(code):
    """新浪 5 分钟: 最近 ~1970 根 (~2 个月)。return dict datetime->row"""
    url = ("https://quotes.sina.cn/cn/api/json_v2.php/CN_MarketDataService.getKLineData"
           f"?symbol={code}&scale=5&ma=no&datalen={SINA_DALEN}")
    raw = get(url, UA_S)
    try:
        d = json.loads(raw)
    except Exception:
        return {}
    if not isinstance(d, list):
        return {}
    out = {}
    for r in d:
        ts = r["day"].replace("-", "").replace(":", "").replace(" ", "")[:12]
        out[ts] = [r["day"], r["open"], r["close"], r["high"], r["low"], r["volume"]]
    return out

def tencent_m5(code, floor=TENCENT_FLOOR):
    """腾讯 m5 分段翻页, 补 2~6 个月中段。"""
    out = {}
    # 先拿最新页确定起点
    raw = get(f"https://ifzq.gtimg.cn/appstock/app/kline/mkline?param={code},m5,,{TENCENT_PAGE}", UA_T)
    try:
        k = json.loads(raw)["data"][code]["m5"]
    except Exception:
        return out
    if not k:
        return out
    end = k[0][0]
    pages = 0
    while end > floor and pages < 60:
        raw = get(f"https://ifzq.gtimg.cn/appstock/app/kline/mkline?param={code},m5,{end},{TENCENT_PAGE}", UA_T)
        try:
            kk = json.loads(raw)["data"][code]["m5"]
        except Exception:
            kk = []
        if not kk:
            # end 恰为一根K线时偶发空页: end +5min 重试一次
            e = dt.datetime.strptime(end, "%Y%m%d%H%M") + dt.timedelta(minutes=5)
            end2 = e.strftime("%Y%m%d%H%M")
            raw = get(f"https://ifzq.gtimg.cn/appstock/app/kline/mkline?param={code},m5,{end2},{TENCENT_PAGE}", UA_T)
            try:
                kk = json.loads(raw)["data"][code]["m5"]
            except Exception:
                kk = []
            if not kk:
                break
        for r in kk:
            out[r[0]] = [r[1], r[2], r[3], r[4], r[5] or 0]
        if kk[0][0] == end:
            break
        end = kk[0][0]
        pages += 1
        time.sleep(0.35)
    return out

def _bs_worker(sym, q):
    """子进程内跑 baostock (库内部无超时, 断线会永久阻塞 -> 必须进程级 kill)"""
    import baostock as bs
    out = {}
    lg = bs.login()
    if lg.error_code != "0":
        q.put(out); return
    try:
        s = dt.date(2020, 1, 2); e = dt.date.today()
        while s < e:
            seg_end = min(s + dt.timedelta(days=730), e)
            rs = bs.query_history_k_data_plus(
                sym, "date,time,open,close,high,low,volume",
                start_date=s.isoformat(), end_date=seg_end.isoformat(),
                frequency="5", adjustflag="2")
            while rs.error_code == "0" and rs.next():
                d, t, o, c, h, l, v = rs.get_row_data()
                if not o:
                    continue
                ts = d.replace("-", "") + t[8:12]
                out[ts] = [o, c, h, l, v or 0]
            s = seg_end + dt.timedelta(days=1)
    finally:
        try:
            bs.logout()
        except Exception:
            pass
    q.put(out)


def baostock_m5(code, start="2020-01-02", end=None, hard_timeout=120):
    """Baostock 5 分钟全史, 子进程隔离 + 硬超时 (防库级挂死)。code: sz000001/sh600519"""
    import multiprocessing as mp
    if code.startswith("bj"):
        return {}
    sym = code[:2] + "." + code[2:]
    q = mp.Queue()
    p = mp.Process(target=_bs_worker, args=(sym, q))
    p.start()
    p.join(hard_timeout)
    if p.is_alive():
        p.terminate(); p.join(5)
        print(f"  {code} baostock HARD-TIMEOUT {hard_timeout}s", flush=True)
        return {}
    if p.exitcode != 0:
        return {}
    try:
        return q.get_nowait()
    except Exception:
        return {}

def write_partitions(rows_by_month):
    """rows_by_month: {YYYYMM: [(symbol,datetime,o,c,h,l,v), ...]} — flock + tmp + os.replace 原子合并"""
    import pyarrow as pa, pyarrow.parquet as pq, pathlib, fcntl
    cols = ["symbol", "datetime", "open", "close", "high", "low", "volume"]
    pathlib.Path(WH).mkdir(parents=True, exist_ok=True)
    lock = open(os.path.join(WH, ".lock"), "w")
    try:
        fcntl.flock(lock, fcntl.LOCK_EX)
        for ym, rows in sorted(rows_by_month.items()):
            path = f"{WH}/{ym}.parquet"
            old = []
            old_keys = set()
            if os.path.exists(path):
                t = pq.read_table(path)
                for row in zip(*[t.column(c).to_pylist() for c in cols]):
                    old.append(row)
                    old_keys.add((row[0], row[1]))
            merged = old + [r for r in rows if (r[0], r[1]) not in old_keys]
            merged.sort(key=lambda r: (r[0], r[1]))
            tmp = path + ".tmp"
            pq.write_table(pa.table({c: [r[i] for r in merged] for i, c in enumerate(cols)}),
                           tmp, compression="zstd")
            os.replace(tmp, path)
    finally:
        fcntl.flock(lock, fcntl.LOCK_UN)
        lock.close()

def main():
    shard, total = int(sys.argv[1]), int(sys.argv[2])
    syms = json.load(open("symbols.json"))
    codes = syms["symbols"] if isinstance(syms, dict) else [r["symbol"] for r in syms]
    codes = [c for c in sorted(codes) if not c.startswith("bj")][shard::total]
    done = set(open(DONE).read().split()) if os.path.exists(DONE) else set()
    todo = [c for c in codes if c not in done][:BATCH]
    print(f"shard {shard}/{total}: {len(todo)}/{len(codes)} 本轮待补", flush=True)
    t0 = time.time()
    for i, code in enumerate(todo):
        all_rows = {}  # datetime -> [o,c,h,l,v]
        # 1) 新浪近期
        for ts, r in sina_m5(code).items():
            all_rows[ts] = r
        # 2) 腾讯中段
        for ts, r in tencent_m5(code).items():
            all_rows.setdefault(ts, r)
        # 3) Baostock 全史 2020-01-02 起 (实测地板; sh/sz 主力, bj 无数据)
        if not code.startswith("bj"):
            bs_rows = baostock_m5(code)
            if bs_rows:
                all_rows.update(bs_rows)
        if all_rows:
            by_month = {}
            for ts, r in all_rows.items():
                ym = ts[:6]
                try:
                    by_month.setdefault(ym, []).append(
                        [code, ts, float(r[0]), float(r[1]), float(r[2]), float(r[3]), float(r[4] or 0)])
                except (ValueError, TypeError):
                    continue
            try:
                write_partitions(by_month)
            except Exception as e:
                print(f"  {code} partition FAIL {e}", flush=True)
        open(DONE, "a").write(code + "\n")
        if (i + 1) % 10 == 0:
            rate = (i + 1) / max(time.time() - t0, 1) * 60
            print(f"  {i+1}/{len(todo)} {code} {len(all_rows)}根 {rate:.0f}只/min", flush=True)
        time.sleep(0.4)
    print("batch done", flush=True)

if __name__ == "__main__":
    main()
