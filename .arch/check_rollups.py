#!/usr/bin/env python3
"""Verify every SUMMARY's ROLLUP block balances its books.

A rollup agent found a child claiming `misfit_count: {low: 3}` alongside
`resolved_locally: 0` and `escalate: []` — three findings declared and none
dispositioned. Silent leaks like that are exactly how the resolve-or-escalate
accounting stops being trustworthy, and the invariant is arithmetic, so check
it rather than trusting each agent to self-audit:

    sum(misfit_count.values()) == resolved_locally + len(escalate)

Cheap, deterministic, and it scales — at App scale nobody will read 1,381
rollup blocks by hand.
"""
import os, re, sys, json

os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
ROLLUP = re.compile(r'<!--\s*ROLLUP(.*?)-->', re.S)
COUNTS = re.compile(r'misfit_count:\s*\{([^}]*)\}')
RESOLVED = re.compile(r'resolved_locally:\s*(\d+)')
NUM = re.compile(r'(\w+)\s*:\s*(\d+)')


def escalate_items(body):
    m = re.search(r'^escalate:\s*(.*)$', body, re.M | re.S)
    if not m:
        return None
    tail = m.group(1)
    if tail.strip().startswith('[]'):
        return 0
    return len(re.findall(r'^\s*-\s+item:', tail, re.M))


bad, ok, missing = [], 0, []
for root, dirs, files in os.walk('.'):
    dirs[:] = [d for d in dirs
               if d not in ('.git', '.arch', 'externalLib', 'mbedtls', 'docs')]
    for f in sorted(files):
        if not (f.startswith('SUMMARY') and f.endswith('.md')):
            continue
        p = os.path.join(root, f).lstrip('./')
        text = open(p, encoding='utf8').read()
        m = ROLLUP.search(text)
        if not m:
            missing.append(p)
            continue
        body = m.group(1)
        cm = COUNTS.search(body)
        rm = RESOLVED.search(body)
        esc = escalate_items(body)
        if not cm or not rm or esc is None:
            bad.append((p, 'ROLLUP missing misfit_count / resolved_locally / escalate'))
            continue
        total = sum(int(v) for _, v in NUM.findall(cm.group(1)))
        acct = int(rm.group(1)) + esc
        if total != acct:
            bad.append((p, f'{total} misfits declared but {acct} dispositioned '
                           f'(resolved {rm.group(1)} + escalated {esc})'))
        else:
            ok += 1

print(f'{ok} rollups balance; {len(bad)} do not; {len(missing)} missing a ROLLUP block')
for p, why in bad:
    print(f'  UNBALANCED  {p}: {why}')
for p in missing:
    print(f'  NO ROLLUP   {p}')
sys.exit(0)
