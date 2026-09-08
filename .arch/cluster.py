#!/usr/bin/env python3
"""Cluster the children of a wide directory so fan-in stays bounded.

A directory agent reads its immediate children — but "immediate children" is
not a small number everywhere. Bedrock's test/clustertest/tests has 36; App's
src/components has 201. Bounded-by-width is not the same as bounded.

So: partition the children of any over-wide directory into thematic clusters of
at most CLUSTER_MAX, summarize each cluster, and let the directory agent read
the cluster summaries. Fan-in is then bounded by CLUSTER_MAX EVERYWHERE, at the
cost of one extra hop for wide directories only.

The clustering itself is algorithmic — token overlap on unit names and intents,
average-linkage agglomerative. Deliberately not an agent call: at App scale this
runs on ~1,400 directories and must be free. It only has to be good enough to
group related siblings; the agent supplies the actual understanding.
"""
import json, os, re, collections

CLUSTER_MAX = 16

STOP = set('the a an and or of to for in is are be this that it its with as by '
           'from on at test tests get set new all any'.split())


def toks(s):
    s = re.sub(r'([a-z0-9])([A-Z])', r'\1 \2', s)
    return {t for t in re.findall(r'[a-z0-9]+', s.lower())
            if t not in STOP and len(t) > 2}


def jaccard(a, b):
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


def cluster(items, maxsize=CLUSTER_MAX):
    """items: {name: text}. Returns [[name, ...], ...], each <= maxsize.

    Average-linkage agglomerative with a size cap. O(n^3) worst case, which is
    fine: n is one directory's child count, never the repo.
    """
    names = list(items)
    if len(names) <= maxsize:
        return [names]

    sig = {n: toks(items[n]) for n in names}
    groups = [[n] for n in names]

    while True:
        best, bi, bj = 0.0, -1, -1
        for i in range(len(groups)):
            for j in range(i + 1, len(groups)):
                if len(groups[i]) + len(groups[j]) > maxsize:
                    continue
                pairs = [(a, b) for a in groups[i] for b in groups[j]]
                s = sum(jaccard(sig[a], sig[b]) for a, b in pairs) / len(pairs)
                if s > best:
                    best, bi, bj = s, i, j
        if bi < 0 or best <= 0.02:
            break
        groups[bi] = groups[bi] + groups[bj]
        del groups[bj]

    # Anything still unmerged and lonely: pack leftovers together by size so we
    # do not emit a long tail of singleton "clusters".
    small = [g for g in groups if len(g) == 1]
    big = [g for g in groups if len(g) > 1]
    if len(small) > 1:
        packed, cur = [], []
        for g in small:
            if len(cur) + 1 > maxsize:
                packed.append(cur); cur = []
            cur += g
        if cur:
            packed.append(cur)
        groups = big + packed
    return groups


def label(names, items):
    """Cheap human-readable cluster label from the most shared tokens."""
    c = collections.Counter()
    for n in names:
        c.update(toks(items[n]))
    common = [w for w, k in c.most_common(3) if k > 1]
    return '-'.join(common) if common else 'misc'


if __name__ == '__main__':
    os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    index = json.load(open('.arch/index.json'))
    bydir = collections.defaultdict(dict)
    for u in index['units']:
        bydir[u['dir']][u['unit']] = u['unit'] + ' ' + u.get('intent', '')

    out = {}
    for d, items in sorted(bydir.items()):
        if len(items) <= CLUSTER_MAX:
            continue
        gs = cluster(items)
        out[d] = [{'label': label(g, items), 'units': sorted(g)} for g in gs]
        print(f"\n{d or '(root)'}  {len(items)} units -> {len(gs)} clusters")
        for c in out[d]:
            print(f"   [{c['label']:<26}] {len(c['units']):2d}  "
                  + ', '.join(c['units'][:5])
                  + (' ...' if len(c['units']) > 5 else ''))
    json.dump(out, open('.arch/clusters.json', 'w'), indent=1)
    print(f"\nwrote .arch/clusters.json ({len(out)} wide directories)")
