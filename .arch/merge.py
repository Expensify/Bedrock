#!/usr/bin/env python3
"""Merge per-batch fragments into .arch/index.json and populate the cache.

Idempotent: safe to run repeatedly while batches are still landing. Reports
coverage against the manifest so a partial run is obvious rather than silent.
"""
import json, os, sys, collections

ARCH = os.path.dirname(os.path.abspath(__file__))
os.chdir(os.path.dirname(ARCH))

manifest = json.load(open('.arch/manifest.json'))
units = {u['key']: u for u in manifest['units']}
def keyof(r):
    d = r.get('dir', '')
    return f"{d}/{r['unit']}" if d else r['unit']

records, dupes, malformed = {}, [], []

# Cache first (survives across sessions), then fragments override with fresher data.
for fn in sorted(os.listdir('.arch/cache')):
    if not fn.endswith('.json'):
        continue
    try:
        r = json.load(open(f'.arch/cache/{fn}'))
        records[keyof(r)] = r
    except Exception as e:
        malformed.append((fn, str(e)))

for fn in sorted(os.listdir('.arch/fragments')):
    if not fn.endswith('.json'):
        continue
    try:
        frag = json.load(open(f'.arch/fragments/{fn}'))
    except Exception as e:
        malformed.append((fn, str(e)))
        continue
    if isinstance(frag, dict):
        frag = [frag]
    for r in frag:
        name = keyof(r) if r.get('unit') else None
        if not name:
            malformed.append((fn, 'record with no "unit" key'))
            continue
        if name in records and records[name].get('_src') not in (None, fn):
            dupes.append(name)
        r['_src'] = fn
        records[name] = r

# Write cache entries keyed by content hash so unchanged units skip next run.
written = 0
for name, r in records.items():
    u = units.get(name)
    if not u:
        continue
    r['hash'] = u['hash']
    r.setdefault('dir', u['dir'])
    r.setdefault('lines', u['lines'])
    r.setdefault('test', u['test'])
    r.setdefault('files', u['files'])
    path = f".arch/cache/{u['hash']}.json"
    if not os.path.exists(path):
        json.dump(r, open(path, 'w'), indent=1)
        written += 1

covered = [n for n in units if n in records]
missing = sorted(set(units) - set(records))
orphan = sorted(set(records) - set(units))

index = {
    'units': [records[n] for n in sorted(covered)],
    'rollups': {},          # filled by Phase 3
    'coverage': {'total': len(units), 'annotated': len(covered),
                 'missing': missing, 'orphan': orphan},
}
json.dump(index, open('.arch/index.json', 'w'), indent=1)

mf = sum(len(r.get('misfits', [])) for r in index['units'])
sev = collections.Counter(m.get('severity', '?')
                          for r in index['units'] for m in r.get('misfits', []))
print(f"coverage : {len(covered)}/{len(units)} units")
print(f"misfits  : {mf}  ({dict(sev)})")
print(f"cache    : +{written} entries")
if malformed:
    print(f"MALFORMED fragments ({len(malformed)}):")
    for fn, e in malformed:
        print(f"   {fn}: {e[:90]}")
if dupes:
    print(f"DUPLICATE unit records: {sorted(set(dupes))}")
if orphan:
    print(f"ORPHAN records (unit not in manifest): {orphan}")
if missing:
    print(f"MISSING ({len(missing)}): {', '.join(missing[:14])}"
          + (' ...' if len(missing) > 14 else ''))
sys.exit(0)
