import json, sys
def load(pattern, reps): return [json.load(open(pattern % r)) for r in reps]
def val(x):
    if isinstance(x, dict): return x.get('p50_ms', x.get('rows_per_s'))
    return x
higher = lambda k: any(s in k for s in ['qps','tps','ops','ingest'])
for proto, pat in [("pgwire", "result-%s-%d.json"), ("flight", "result-flight-%s-%d.json")]:
    j = load(pat.replace('%s','jvm'), [1,2]); r = load(pat.replace('%s','rust'), [1,2])
    print(f"\n== {proto}: metric | xtdb (best of 2) | rtdb (best of 2) | rtdb/xtdb speedup")
    for k in j[0]:
        if k not in r[0]: continue
        jv=[val(x[k]) for x in j]; rv=[val(x[k]) for x in r]
        if higher(k): jb, rb = max(jv), max(rv); sp = rb/jb if jb else float('nan')
        else: jb, rb = min(jv), min(rv); sp = jb/rb if rb else float('nan')
        flag = "WIN " if sp >= 1 else "LOSS"
        print(f"{flag} {k:30} {jb:12.2f} {rb:12.2f}   x{sp:6.2f}")
