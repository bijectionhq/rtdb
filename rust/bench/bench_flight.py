"""The pgwire benchmark's workload over ADBC / Flight SQL, against its own tables."""
import argparse, json, random, statistics, threading, time
import adbc_driver_flightsql.dbapi as flight

def connect(port):
    return flight.connect(f"grpc://127.0.0.1:{port}", autocommit=True)

def wait_ready(port, timeout=300):
    start = time.monotonic()
    while time.monotonic() - start < timeout:
        try:
            with connect(port) as c:
                cur = c.cursor(); cur.execute("SELECT 1"); cur.fetchall()
            return time.monotonic() - start
        except Exception:
            time.sleep(0.05)
    raise TimeoutError("server not ready")

def pct(xs, p):
    s = sorted(xs); return s[int(p * (len(s) - 1))]

def timed(fn, n):
    lat = []
    for i in range(n):
        t = time.perf_counter(); fn(i); lat.append(time.perf_counter() - t)
    return {"n": n, "p50_ms": pct(lat, .5) * 1e3, "p95_ms": pct(lat, .95) * 1e3, "p99_ms": pct(lat, .99) * 1e3, "mean_ms": statistics.mean(lat) * 1e3}

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--rows", type=int, default=100_000)
    ap.add_argument("--batch", type=int, default=1000)
    ap.add_argument("--output", required=True)
    a = ap.parse_args()
    rnd = random.Random(42)
    result = {"ready_s": wait_ready(a.port)}
    with connect(a.port) as c:
        cur = c.cursor()
        def run(sql, params=None):
            cur.execute(sql, parameters=params)
            try: return cur.fetchall()
            except Exception: return None
        t = time.perf_counter()
        cur.executemany("INSERT INTO f_customers (_id, name, region) VALUES ($1, $2, $3)", [(i, f"customer-{i}", ["eu", "us", "apac"][i % 3]) for i in range(1000)])
        for start in range(0, a.rows, a.batch):
            cur.executemany("INSERT INTO f_orders (_id, customer_id, amount, sku) VALUES ($1, $2, $3, $4)",
                            [(i, i % 1000, rnd.randint(1, 10_000) / 100, f"sku-{i % 500}") for i in range(start, min(start + a.batch, a.rows))])
        elapsed = time.perf_counter() - t
        result["ingest"] = {"rows": a.rows + 1000, "seconds": elapsed, "rows_per_s": (a.rows + 1000) / elapsed}
        assert run("SELECT COUNT(*) FROM f_orders")[0][0] == a.rows
        ids = [rnd.randrange(a.rows) for _ in range(2000)]
        run("SELECT amount FROM f_orders WHERE _id = $1", (0,))
        result["point_lookup"] = timed(lambda i: run("SELECT _id, amount, sku FROM f_orders WHERE _id = $1", (ids[i],)), 2000)
        result["range_aggregate"] = timed(lambda i: run("SELECT COUNT(*), SUM(amount) FROM f_orders WHERE amount BETWEEN $1 AND $2", (i % 50, i % 50 + 10)), 50)
        result["join_group_by"] = timed(lambda i: run("SELECT c.region, COUNT(*), SUM(o.amount) FROM f_orders o JOIN f_customers c ON o.customer_id = c._id GROUP BY c.region ORDER BY c.region"), 20)
        result["order_by_limit"] = timed(lambda i: run("SELECT _id, amount FROM f_orders ORDER BY amount DESC, _id LIMIT 100"), 20)
        result["full_scan_fetch_100k"] = timed(lambda i: run("SELECT _id, customer_id, amount, sku FROM f_orders"), 5)
        result["history_scan"] = timed(lambda i: run("SELECT _id, amount, _valid_from FROM f_orders FOR ALL VALID_TIME WHERE _id = $1", (i,)), 200)
        base = a.rows + 10_000
        result["single_insert"] = timed(lambda i: run("INSERT INTO f_orders (_id, customer_id, amount, sku) VALUES ($1, $2, $3, $4)", (base + i, i % 1000, 1.5, "sku-new")), 500)
        upd = [rnd.randrange(a.rows) for _ in range(500)]
        result["single_update_by_id"] = timed(lambda i: run("UPDATE f_orders SET amount = amount + 1 WHERE _id = $1", (upd[i],)), 500)
        result["single_delete_by_id"] = timed(lambda i: run("DELETE FROM f_orders WHERE _id = $1", (base + i,)), 200)
        def read_your_write(i):
            run("INSERT INTO f_orders (_id, customer_id, amount, sku) VALUES ($1, 1, 2.5, 'ryw')", (base + 1000 + i,))
            assert run("SELECT amount FROM f_orders WHERE _id = $1", (base + 1000 + i,))
        result["read_your_write"] = timed(read_your_write, 200)
    def worker(n, out, write):
        with connect(a.port) as c:
            cur = c.cursor(); r = random.Random(n)
            for k in range(500 if not write else 200):
                if write:
                    cur.execute("INSERT INTO f_orders (_id, customer_id, amount, sku) VALUES ($1, $2, 1.0, 'conc')", parameters=(a.rows + 100_000 + n * 1000 + k, k))
                else:
                    cur.execute("SELECT amount FROM f_orders WHERE _id = $1", parameters=(r.randrange(a.rows),)); cur.fetchall()
            out.append(200 if write else 500)
    for name, write in [("concurrent_insert_8x200_tps", True), ("concurrent_lookup_8x500_qps", False)]:
        done = []; ts = [threading.Thread(target=worker, args=(n, done, write)) for n in range(8)]
        t = time.perf_counter(); [x.start() for x in ts]; [x.join() for x in ts]
        result[name] = sum(done) / (time.perf_counter() - t)
    json.dump(result, open(a.output, "w"), indent=2)
    print(json.dumps(result, indent=2))

main()
