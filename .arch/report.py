#!/usr/bin/env python3
"""Rank adjudicated findings and emit the skeleton of REFACTORING.md.

Ranking is arithmetic (see SCALING.md): an agent writing prose for the top N is
fine, an agent ranking N findings is not — it does not scale, is not
reproducible, and gets worse as N grows.

    priority = severity x confidence x (1 + blast_radius) / log2(lines)

blast_radius is reverse include-graph fan-in, so a stranded helper that 68
units depend on outranks an identical one nobody imports. That is the whole
reason the graph is built from source rather than inferred.
"""
import json, os, sys, collections

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from retrieve import priority, unitkey                      # noqa: E402

os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
index = json.load(open('.arch/index.json'))
graph = json.load(open('.arch/graph.json'))
adj = {}
if os.path.exists('.arch/adjudicated.json'):
    adj = {(a['unit'], a['item']): a for a in json.load(open('.arch/adjudicated.json'))}

rows = []
for u in index['units']:
    k = unitkey(u)
    for mf in u.get('misfits', []):
        a = adj.get((k, mf.get('item')), {})
        if a.get('verdict') == 'rejected':
            continue
        m = dict(mf)
        m['confidence'] = a.get('confidence', mf.get('confidence', 0.5))
        rows.append({
            'unit': k, 'dir': u['dir'], 'test': u.get('test', False),
            'lines': u.get('lines', 0),
            'item': mf.get('item', ''), 'why': mf.get('why', ''),
            'severity': mf.get('severity', 'low'),
            'confidence': m['confidence'],
            'blast': graph['blast_radius'].get(k, 0),
            'home': a.get('home') or mf.get('suggested_home'),
            'rationale': a.get('rationale', ''),
            'verdict': a.get('verdict', 'unadjudicated'),
            'priority': priority(m, u, graph),
        })

rows.sort(key=lambda r: -r['priority'])
src = [r for r in rows if not r['test']]
tst = [r for r in rows if r['test']]

json.dump(rows, open('.arch/ranked.json', 'w'), indent=1)

def group(rs):
    """Collapse findings that share a source unit into one theme.

    Seven separate rows saying "this symbol is stranded in libstuff.h" are one
    refactor, not seven. Reporting them individually both buries the lede and
    overstates the finding count. A unit with 3+ findings is a decomposition
    job; report it as such, with the parts nested underneath.
    """
    by = collections.defaultdict(list)
    for r in rs:
        by[r['unit']].append(r)
    themes = []
    for unit, items in by.items():
        items.sort(key=lambda r: -r['priority'])
        themes.append({
            'unit': unit, 'dir': items[0]['dir'], 'items': items,
            'n': len(items),
            # A theme is as urgent as its worst part, with a modest bonus for
            # breadth: many strandings in one file is itself the finding.
            'priority': round(items[0]['priority'] * (1 + 0.12 * (len(items) - 1)), 1),
            'blast': items[0]['blast'],
            'severity': max((r['severity'] for r in items),
                            key=lambda s: {'low': 0, 'med': 1, 'high': 2}[s]),
        })
    themes.sort(key=lambda t: -t['priority'])
    return themes


def table(rs, n=None):
    out, i = [], 0
    for t in group(rs)[:n] if n else group(rs):
        i += 1
        head = (f"**{i}. `{t['unit']}`** — priority {t['priority']}, "
                f"{t['severity']}, blast {t['blast']}"
                + (f", **{t['n']} findings**" if t['n'] > 2 else ''))
        out.append(head + '\n')
        for r in t['items']:
            item = r['item'][:80]
            home = f" → `{r['home']}`" if r['home'] else ''
            out.append(f"   - {item}{home}")
            if r['why']:
                out.append(f"     <br/>{r['why']}")
        out.append('')
    return '\n'.join(out)

print(f"findings: {len(rows)}  (source {len(src)}, test {len(tst)})")
print(f"adjudicated: {sum(1 for r in rows if r['verdict'] != 'unadjudicated')}")
print()
print(table(src, 15))
open('.arch/ranked_tables.md', 'w').write(
    '## Source findings\n\n' + table(src) + '\n\n## Test findings\n\n' + table(tst) + '\n')
