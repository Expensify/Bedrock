#!/usr/bin/env python3
"""Materialize the input file for one directory's rollup agent.

Enforcing bounded fan-in mechanically rather than by asking politely. If the
agent had to filter index.json itself it would read the whole index, which is
exactly what must not happen on a large repo. So each agent gets a file
containing only what it is allowed to see:

  - unit records for units DIRECTLY in this directory
    (or, for a wide directory, the names of its cluster summaries instead)
  - the path of each direct subdirectory's SUMMARY.md
  - for pass B, the parent's ROLLUP block

Usage:  make_rollup_input.py <dir|--root> <passA|passB>
"""
import json, os, re, sys

os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
ROLLUP = re.compile(r'<!--\s*ROLLUP(.*?)-->', re.S)


def rollup_of(path):
    if not os.path.exists(path):
        return None
    m = ROLLUP.search(open(path, encoding='utf8').read())
    return m.group(1).strip() if m else None


def build(d, phase):
    index = json.load(open('.arch/index.json'))
    clusters = json.load(open('.arch/clusters.json')) if os.path.exists('.arch/clusters.json') else {}

    dirs = set()
    for u in index['units']:
        x = u['dir']
        while True:
            dirs.add(x)
            if not x:
                break
            x = os.path.dirname(x)

    subdirs = sorted(x for x in dirs if x and os.path.dirname(x) == d and x != d)
    out = {'dir': d, 'pass': phase, 'subdirectories': [], 'units': [], 'clusters': []}

    if d in clusters:
        # Wide directory: read the cluster summaries, never the units directly.
        for c in clusters[d]:
            p = f"{d}/SUMMARY.{c['label']}.md" if d else f"SUMMARY.{c['label']}.md"
            out['clusters'].append({'label': c['label'], 'summary_file': p,
                                    'unit_count': len(c['units']),
                                    'rollup': rollup_of(p)})
        out['note'] = ('This directory is WIDE. Its units are pre-grouped into the '
                       'clusters above. Read the cluster summary files, NOT the '
                       'individual units.')
    else:
        out['units'] = [u for u in index['units'] if u['dir'] == d]

    for s in subdirs:
        p = f'{s}/SUMMARY.md'
        out['subdirectories'].append({'dir': s, 'summary_file': p,
                                      'exists': os.path.exists(p),
                                      'rollup': rollup_of(p)})

    if phase == 'passB':
        parent = os.path.dirname(d) if d else None
        pp = (f'{parent}/SUMMARY.md' if parent else 'SUMMARY.md') if d else None
        out['parent'] = {'dir': parent, 'summary_file': pp,
                         'rollup': rollup_of(pp) if pp else None} if d else None
        # Siblings, so this directory can situate itself against them.
        pd = os.path.dirname(d) if d else None
        sibs = sorted(x for x in dirs if x and x != d and os.path.dirname(x) == pd)
        out['siblings'] = [{'dir': s, 'rollup': rollup_of(f'{s}/SUMMARY.md')} for s in sibs]

        # When the parent is WIDE, its units live in clusters rather than in
        # sibling directories — so those cluster summaries ARE this directory's
        # real siblings. Without them a package like libstuff/JSON sees an empty
        # siblings list and cannot tell what neighbouring territory its parent
        # already covers, which is exactly the question Pass B exists to answer.
        if parent in clusters:
            out['sibling_clusters'] = [
                {'label': c['label'],
                 'summary_file': f"{parent}/SUMMARY.{c['label']}.md",
                 'rollup': rollup_of(f"{parent}/SUMMARY.{c['label']}.md")}
                for c in clusters[parent]]

    safe = (d or 'ROOT').replace('/', '~')
    path = f'.arch/rollup_in/{safe}.{phase}.json'
    json.dump(out, open(path, 'w'), indent=1)
    return path, out


if __name__ == '__main__':
    d = '' if sys.argv[1] == '--root' else sys.argv[1].rstrip('/')
    path, out = build(d, sys.argv[2])
    print(f"{path}  units={len(out['units'])} clusters={len(out['clusters'])} "
          f"subdirs={len(out['subdirectories'])} "
          f"({sum(1 for s in out['subdirectories'] if not s['exists'])} missing) "
          f"{os.path.getsize(path)//1024}KB")
