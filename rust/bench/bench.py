"""Matched pgwire micro-benchmark: same workload, same container limits, against one server at a time."""
import argparse, json, random, statistics, subprocess, threading, time
import psycopg

def connect(port):
    c = psycopg.connect(host="127.0.0.1", port=port, user="xtdb", dbname="xtdb", autocommit=True)
    c.adapters.register_dumper(str, psycopg.types.string.StrDumper)
    return c

def wait_ready(port, timeout=300):
    start = time.monotonic()
    while time.monotonic() - start < timeout:
        try:
            with connect(port) as c:
                c.execute("SELECT 1").fetchall()
            return time.monotonic() - start
        except Exception:
            time.sleep(0.05)
    raise TimeoutError("server not ready")

def pct(xs, p):
    s = sorted(xs)
    return s[int(p * (len(s) - 1))]

def timed(fn, n):
    lat = []
    for i in range(n):
        t = time.perf_counter(); fn(i); lat.append(time.perf_counter() - t)
    return {"n": n, "p50_ms": pct(lat, .5) * 1e3, "p95_ms": pct(lat, .95) * 1e3, "p99_ms": pct(lat, .99) * 1e3, "mean_ms": statistics.mean(lat) * 1e3}

class MemorySampler(threading.Thread):
    def __init__(self, container):
        super().__init__(daemon=True); self.container = container; self.peak = 0; self.stop = False
    def run(self):
        while not self.stop:
            out = subprocess.run(["docker", "stats", "--no-stream", "--format", "{{.MemUsage}}", self.container], capture_output=True, text=True).stdout.strip()
            if out:
                used = out.split("/")[0].strip()
                units = {"KiB": 1 << 10, "MiB": 1 << 20, "GiB": 1 << 30, "B": 1}
                for u, m in units.items():
                    if used.endswith(u):
                        self.peak = max(self.peak, float(used[:-len(u)]) * m); break
            time.sleep(0.5)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--container", required=True)
    ap.add_argument("--rows", type=int, default=100_000)
    ap.add_argument("--batch", type=int, default=1000)
    ap.add_argument("--output", required=True)
    a = ap.parse_args()
    rnd = random.Random(42)
    result = {"startup_s": wait_ready(a.port)}
    sampler = MemorySampler(a.container); sampler.start()
    with connect(a.port) as c:
        cur = c.cursor()
        # ingest: customers then orders, one transaction per batch
        t = time.perf_counter()
        for start in range(0, 1000, a.batch):
            with c.transaction():
                cur.executemany("INSERT INTO customers (_id, name, region) VALUES (%s, %s, %s)",
                                [(i, f"customer-{i}", ["eu", "us", "apac"][i % 3]) for i in range(start, min(start + a.batch, 1000))])
        batches = 0
        for start in range(0, a.rows, a.batch):
            rows = [(i, i % 1000, rnd.randint(1, 10_000) / 100, f"sku-{i % 500}") for i in range(start, min(start + a.batch, a.rows))]
            with c.transaction():
                cur.executemany("INSERT INTO orders (_id, customer_id, amount, sku) VALUES (%s, %s, %s, %s)", rows)
            batches += 1
        elapsed = time.perf_counter() - t
        result["ingest"] = {"rows": a.rows + 1000, "seconds": elapsed, "rows_per_s": (a.rows + 1000) / elapsed, "transactions": batches + 1}
        assert cur.execute("SELECT COUNT(*) FROM orders").fetchone()[0] == a.rows
        ids = [rnd.randrange(a.rows) for _ in range(2000)]
        cur.execute("SELECT amount FROM orders WHERE _id = %s", (0,)).fetchall()
        result["point_lookup"] = timed(lambda i: cur.execute("SELECT _id, amount, sku FROM orders WHERE _id = %s", (ids[i],)).fetchall(), 2000)
        result["range_aggregate"] = timed(lambda i: cur.execute("SELECT COUNT(*), SUM(amount) FROM orders WHERE amount BETWEEN %s AND %s", (i % 50, i % 50 + 10)).fetchall(), 50)
        result["join_group_by"] = timed(lambda i: cur.execute("SELECT c.region, COUNT(*), SUM(o.amount) FROM orders o JOIN customers c ON o.customer_id = c._id GROUP BY c.region ORDER BY c.region").fetchall(), 20)
        result["order_by_limit"] = timed(lambda i: cur.execute("SELECT _id, amount FROM orders ORDER BY amount DESC, _id LIMIT 100").fetchall(), 20)
        # bitemporal: update 1000 orders with explicit valid time, then query as of the past and history
        t = time.perf_counter()
        with c.transaction():
            cur.executemany("UPDATE orders FOR PORTION OF VALID_TIME FROM TIMESTAMP '2030-01-01Z' TO NULL SET amount = amount * 2 WHERE _id = %s", [(i,) for i in range(1000)])
        result["bitemporal_update_1000_s"] = time.perf_counter() - t
        result["valid_time_as_of"] = timed(lambda i: cur.execute("SELECT SUM(amount) FROM orders FOR VALID_TIME AS OF TIMESTAMP '2031-01-01Z' WHERE _id < 1000").fetchall(), 20)
        result["history_scan"] = timed(lambda i: cur.execute("SELECT _id, amount, _valid_from FROM orders FOR ALL VALID_TIME WHERE _id = %s", (i,)).fetchall(), 200)
        # writes: one statement per transaction, as an OLTP client issues them
        base = a.rows + 10_000
        result["single_insert"] = timed(lambda i: cur.execute("INSERT INTO orders (_id, customer_id, amount, sku) VALUES (%s, %s, %s, %s)", (base + i, i % 1000, 1.5, "sku-new")), 500)
        upd = [rnd.randrange(a.rows) for _ in range(500)]
        result["single_update_by_id"] = timed(lambda i: cur.execute("UPDATE orders SET amount = amount + 1 WHERE _id = %s", (upd[i],)), 500)
        result["single_delete_by_id"] = timed(lambda i: cur.execute("DELETE FROM orders WHERE _id = %s", (base + i,)), 200)
        result["set_update_10pct"] = timed(lambda i: cur.execute("UPDATE orders SET amount = amount * 1.01 WHERE customer_id < 100"), 3)
        def read_your_write(i):
            cur.execute("INSERT INTO orders (_id, customer_id, amount, sku) VALUES (%s, 1, 2.5, 'ryw')", (base + 1000 + i,))
            assert cur.execute("SELECT amount FROM orders WHERE _id = %s", (base + 1000 + i,)).fetchall()
        result["read_your_write"] = timed(read_your_write, 200)
    def writer(n, out):
        with connect(a.port) as c:
            cur = c.cursor()
            for k in range(200):
                cur.execute("INSERT INTO orders (_id, customer_id, amount, sku) VALUES (%s, %s, 1.0, 'conc')", (a.rows + 100_000 + n * 1000 + k, k))
            out.append(200)
    done = []; ts = [threading.Thread(target=writer, args=(n, done)) for n in range(8)]
    t = time.perf_counter(); [x.start() for x in ts]; [x.join() for x in ts]
    result["concurrent_insert_8x200_tps"] = sum(done) / (time.perf_counter() - t)
    def mixed(n, out):
        with connect(a.port) as c:
            cur = c.cursor(); r = random.Random(100 + n)
            for k in range(500):
                if r.random() < 0.2: cur.execute("UPDATE orders SET amount = amount + 1 WHERE _id = %s", (r.randrange(a.rows),))
                else: cur.execute("SELECT amount FROM orders WHERE _id = %s", (r.randrange(a.rows),)).fetchall()
            out.append(500)
    done = []; ts = [threading.Thread(target=mixed, args=(n, done)) for n in range(8)]
    t = time.perf_counter(); [x.start() for x in ts]; [x.join() for x in ts]
    result["mixed_80r20w_8x500_ops"] = sum(done) / (time.perf_counter() - t)
    # concurrent lookups: 8 connections
    def worker(n, out):
        with connect(a.port) as c:
            cur = c.cursor(); r = random.Random(n)
            for _ in range(500):
                cur.execute("SELECT amount FROM orders WHERE _id = %s", (r.randrange(a.rows),)).fetchall()
            out.append(500)
    done = []; ts = [threading.Thread(target=worker, args=(n, done)) for n in range(8)]
    t = time.perf_counter(); [x.start() for x in ts]; [x.join() for x in ts]
    result["concurrent_lookup_8x500_qps"] = sum(done) / (time.perf_counter() - t)
    sampler.stop = True; sampler.join()
    result["peak_memory_bytes"] = sampler.peak
    json.dump(result, open(a.output, "w"), indent=2)
    print(json.dumps(result, indent=2))

main()
