#!/usr/bin/env python3
"""Plan the Phase 3 directory rollup: wave order + sibling batching.

Two properties this must guarantee, both of which are what make the approach
survive a repo 50x larger:

  1. BOUNDED FAN-IN. A directory agent reads only its immediate children. For
     unusually wide directories (App has src/components with 201 children) even
     that is too much, so those are split into thematic CLUSTERS handled in an
     extra hop. Fan-in is then bounded by CLUSTER_MAX everywhere, independent of
     both repo size and directory width.

  2. CORRECT WAVE ORDER. Pass A runs deepest-first, so every child SUMMARY.md
     exists before its parent reads it. Pass B runs shallowest-first, so every
     parent's rollup exists before its children situate against it.

Sibling directories under one parent are grouped into a single agent call —
at App scale this is the difference between 2,762 calls and ~880.
"""
import json, os, sys, collections

CLUSTER_MAX = 16     # max children one agent reads before we insert a cluster hop
GROUP = 6            # sibling dirs handled per agent call

os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
manifest = json.load(open('.arch/manifest.json'))
units = manifest['units']

dirs = set()
for u in units:
    d = u['dir']
    while True:
        dirs.add(d)
        if not d:
            break
        d = os.path.dirname(d)

children = collections.defaultdict(list)
for d in dirs:
    if d:
        children[os.path.dirname(d)].append(d)
units_in = collections.defaultdict(list)
for u in units:
    units_in[u['dir']].append(u['unit'])

def fanin(d):
    return len(children[d]) + len(units_in[d])

wide = {d: fanin(d) for d in dirs if fanin(d) > CLUSTER_MAX}

depth = {d: (d.count('/') + 1 if d else 0) for d in dirs}
maxd = max(depth.values())

passA, passB = [], []
for lvl in range(maxd, -1, -1):
    lvl_dirs = sorted([d for d in dirs if depth[d] == lvl])
    if lvl_dirs:
        passA.append((lvl, lvl_dirs))
for lvl in range(0, maxd + 1):
    lvl_dirs = sorted([d for d in dirs if depth[d] == lvl])
    if lvl_dirs:
        passB.append((lvl, lvl_dirs))

def group(ds):
    """Batch siblings under a shared parent into single agent calls."""
    byparent = collections.defaultdict(list)
    for d in ds:
        byparent[os.path.dirname(d)].append(d)
    out = []
    for p, v in sorted(byparent.items()):
        for i in range(0, len(v), GROUP):
            out.append(v[i:i + GROUP])
    return out

plan = {'cluster_max': CLUSTER_MAX, 'group': GROUP,
        'wide': wide,
        'passA': [{'level': l, 'calls': [b for b in group(ds)]} for l, ds in passA],
        'passB': [{'level': l, 'calls': [b for b in group(ds)]} for l, ds in passB]}
json.dump(plan, open('.arch/rollup_plan.json', 'w'), indent=1)

ca = sum(len(w['calls']) for w in plan['passA'])
cb = sum(len(w['calls']) for w in plan['passB'])
print(f"directories        : {len(dirs)}   max depth {maxd}")
print(f"pass A waves       : {len(passA)}  ({ca} agent calls, deepest first)")
print(f"pass B waves       : {len(passB)}  ({cb} agent calls, shallowest first)")
print(f"wide dirs (>{CLUSTER_MAX} children, need cluster hop): "
      f"{wide if wide else 'none'}")
print()
for w in plan['passA']:
    ds = [d for c in w['calls'] for d in c]
    print(f"  A/L{w['level']}  {len(w['calls'])} call(s): "
          + ', '.join((d or '(root)') for d in ds[:6])
          + (' ...' if len(ds) > 6 else ''))
