#!/usr/bin/env python3
"""Build bounded adjudication packets for Phase 4.

One packet per misfit-bearing unit: the findings, the unit's own summary, and a
SHORT retrieved candidate list of possible homes. Everything the adjudicator
needs and nothing else — context per call stays constant as the repo grows,
which is the property that makes Phase 4 survive a repo 50x this size.

Escalation filtering: an item a directory already resolved locally does not
need a global verdict. Only what actually propagated gets an expensive call.
"""
import json, os, re, sys, collections

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from retrieve import Retriever, unitkey, priority          # noqa: E402

os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
ROLLUP = re.compile(r'<!--\s*ROLLUP(.*?)-->', re.S)

index = json.load(open('.arch/index.json'))
graph = json.load(open('.arch/graph.json'))
r = Retriever()

# Collect every ROLLUP block written so far, so the adjudicator can see what
# each directory claims for itself when judging where something belongs.
rollups = {}
for root, _, files in os.walk('.'):
    if any(p in root for p in ('.git', '.arch', 'externalLib', 'mbedtls')):
        continue
    for f in files:
        if f.startswith('SUMMARY') and f.endswith('.md'):
            m = ROLLUP.search(open(os.path.join(root, f), encoding='utf8').read())
            if m:
                rollups.setdefault(root.lstrip('./'), []).append(m.group(1).strip())

os.makedirs('.arch/adjudicate_in', exist_ok=True)
packets, nmf = [], 0
for u in index['units']:
    mfs = u.get('misfits') or []
    if not mfs:
        continue
    k = unitkey(u)
    items = []
    for mf in mfs:
        cands = r.candidates(mf, k, k=8)
        items.append({
            'item': mf.get('item'), 'why': mf.get('why'),
            'severity': mf.get('severity'), 'agent_suggested_home': mf.get('suggested_home'),
            'blast_radius': graph['blast_radius'].get(k, 0),
            'priority_unadjudicated': priority(mf, u, graph),
            'candidate_homes': [
                {'dir': d or '(root)', 'score': round(s, 3), 'signals': why,
                 'dir_claims': rollups.get(d, [])[:1]}
                for d, s, why in cands],
        })
        nmf += 1
    p = {'unit': k, 'dir': u['dir'], 'lines': u.get('lines'), 'test': u.get('test'),
         'intent': u.get('intent'), 'objects': u.get('objects', []),
         'depends_on_units': graph['depends'].get(k, []),
         'depended_on_by_units': graph['dependents'].get(k, [])[:20],
         'findings': items}
    fn = f".arch/adjudicate_in/{k.replace('/', '~')}.json"
    json.dump(p, open(fn, 'w'), indent=1)
    packets.append((fn, len(items), os.path.getsize(fn)))

packets.sort(key=lambda x: -x[2])
print(f"{len(packets)} packets, {nmf} findings, "
      f"{sum(s for _, _, s in packets)//1024}KB total, "
      f"largest {packets[0][2]//1024}KB")
for fn, n, s in packets[:6]:
    print(f"  {n:2d} findings {s//1024:3d}KB  {fn}")
