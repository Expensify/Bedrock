#!/usr/bin/env python3
"""Candidate retrieval for misfit adjudication (Phase 4).

Answers: "this symbol looks stranded — which directories could plausibly host
it?" and returns a SMALL ranked list. Recall matters, precision does not: an
LLM adjudicates the shortlist and supplies the semantics. That division is what
lets this stay cheap enough to run per-misfit on a repo of any size.

Two signals, both free:

  1. COUPLING (include graph). If the stranded symbol's unit depends on things
     that live in directory X, X is a candidate. In C++ this is the single
     strongest signal available and it is exact, not probabilistic.
  2. LEXICAL (BM25 over directory rollup blocks). Catches the case where the
     right home has no include relationship yet — which is common, because a
     symbol in the wrong place often has no edge to where it belongs.

Deliberately NOT embeddings. See .arch/SCALING.md; the short version is that
embeddings buy recall only on vocabulary mismatch and cost an O(repo) serial
CPU pass. The `candidates()` signature is the seam to swap them in if a repo
ever needs it.
"""
import json, math, os, re, collections

STOP = set('the a an and or of to for in is are be this that it its with as by '
           'from on at we you not no if then else all any can may use used uses '
           'one two class struct file files code line lines'.split())


def tokenize(text):
    return [t for t in re.findall(r'[a-z0-9]+', _split_camel(text).lower())
            if t not in STOP and len(t) > 2]


def _split_camel(s):
    s = re.sub(r'([a-z0-9])([A-Z])', r'\1 \2', s)
    return re.sub(r'([A-Z]+)([A-Z][a-z])', r'\1 \2', s)


def unitkey(u):
    """Path-qualified unit key. NEVER key on basename alone: Bedrock has four
    distinct `main` units and two `StatusTest`s, and App has `index.tsx` in a
    thousand directories. Basename keying silently merges them."""
    return u.get('key') or (f"{u['dir']}/{u['unit']}" if u.get('dir') else u['unit'])


class BM25:
    """Standard Okapi BM25. ~40 lines, no dependency, scales linearly."""

    def __init__(self, docs, k1=1.5, b=0.75):
        self.ids = list(docs)
        self.toks = {d: tokenize(docs[d]) for d in docs}
        self.len = {d: len(self.toks[d]) for d in docs}
        self.avg = (sum(self.len.values()) / len(self.len)) if self.len else 0
        self.k1, self.b = k1, b
        self.tf = {d: collections.Counter(t) for d, t in self.toks.items()}
        df = collections.Counter()
        for d in docs:
            for t in set(self.toks[d]):
                df[t] += 1
        N = max(1, len(docs))
        self.idf = {t: math.log(1 + (N - n + 0.5) / (n + 0.5)) for t, n in df.items()}

    def score(self, query):
        q = tokenize(query)
        out = {}
        for d in self.ids:
            s, tf, dl = 0.0, self.tf[d], self.len[d]
            for t in q:
                if t not in tf:
                    continue
                f = tf[t]
                s += self.idf.get(t, 0) * f * (self.k1 + 1) / (
                    f + self.k1 * (1 - self.b + self.b * dl / max(1, self.avg)))
            if s:
                out[d] = s
        return out


class Retriever:
    def __init__(self, index_path='.arch/index.json', graph_path='.arch/graph.json'):
        self.index = json.load(open(index_path))
        self.graph = json.load(open(graph_path))
        self.units = {unitkey(u): u for u in self.index['units']}

        # A directory's document = its theme + exports + the intents of the
        # units directly inside it. Directory-level, not file-level: "where
        # should this live" is a directory question, and there are far fewer
        # directories than files, which is what keeps this tractable at scale.
        docs = collections.defaultdict(list)
        for u in self.index['units']:
            docs[u['dir']].append(u['unit'] + ' ' + u.get('intent', ''))
            for o in u.get('objects', []):
                docs[u['dir']].append(o.get('name', '') + ' ' + o.get('role', ''))
        for d, roll in self.index.get('rollups', {}).items():
            docs[d].append(roll.get('theme', ''))
            docs[d].extend(roll.get('exports', []))
        self.docs = {d: ' '.join(v) for d, v in docs.items()}
        self.bm25 = BM25(self.docs)

        # Hub penalty. "This depends on libstuff" is true of nearly every unit
        # in the repo, so a raw coupling count just elects the hub directory
        # every time and tells us nothing. Down-weight each directory by how
        # many distinct units depend on it — the same intuition as IDF. Without
        # this, src/libs would win every candidate list on App.
        indeg = collections.Counter()
        for src, deps in self.graph['depends'].items():
            for d in set(self.units[x]['dir'] for x in deps if x in self.units):
                indeg[d] += 1
        n = max(1, len(self.units))
        self.hub = {d: math.log(1 + n / (1 + c)) for d, c in indeg.items()}
        self._hubmax = max(self.hub.values()) if self.hub else 1.0

    def _hubw(self, d):
        """0..1 weight; ~0 for a directory everything depends on."""
        return self.hub.get(d, self._hubmax) / self._hubmax

    def candidates(self, misfit, home_unit, k=10):
        """Return [(dir, score, why)] — the shortlist handed to the adjudicator."""
        q = ' '.join(filter(None, [misfit.get('item', ''), misfit.get('why', ''),
                                   misfit.get('suggested_home') or '']))
        lex = self.bm25.score(q)
        if lex:
            m = max(lex.values())
            lex = {d: v / m for d, v in lex.items()}

        # Coupling: where do this unit's dependencies actually live?
        coup = collections.Counter()
        for dep in self.graph['depends'].get(home_unit, []):
            u = self.units.get(dep)
            if u:
                coup[u['dir']] += 1 * self._hubw(u['dir'])
        # and where do its dependents live? a symbol often belongs near its callers
        for dep in self.graph['dependents'].get(home_unit, []):
            u = self.units.get(dep)
            if u:
                coup[u['dir']] += 0.5 * self._hubw(u['dir'])
        if coup:
            m = max(coup.values())
            coup = {d: v / m for d, v in coup.items()}

        cur = self.units.get(home_unit, {}).get('dir')
        out = []
        for d in set(lex) | set(coup):
            if d == cur:
                continue                      # its current home is not a candidate
            l, c = lex.get(d, 0), coup.get(d, 0)
            why = []
            if c: why.append(f'coupling {c:.2f}')
            if l: why.append(f'lexical {l:.2f}')
            out.append((d, 0.6 * c + 0.4 * l, ', '.join(why)))
        out.sort(key=lambda x: -x[1])
        return out[:k]


def priority(misfit, unit, graph):
    """severity x confidence x blast_radius / effort — see SCALING.md.

    Arithmetic, not agent judgment, so it is reproducible and so ranking cost
    does not grow with the number of findings.
    """
    sev = {'low': 1, 'med': 3, 'high': 9}.get(misfit.get('severity', 'low'), 1)
    conf = float(misfit.get('confidence', 0.5))
    blast = graph['blast_radius'].get(unitkey(unit), 0)
    effort = math.log2(max(2, unit.get('lines', 2)))
    return round(sev * conf * (1 + blast) / effort, 3)


if __name__ == '__main__':
    r = Retriever()
    n = 0
    for u in r.index['units']:
        for mf in u.get('misfits', []):
            print(f"\n{unitkey(u)}: {mf['item']}")
            for d, s, why in r.candidates(mf, unitkey(u), k=5):
                print(f"    {s:.3f}  {d or '(root)':<28} {why}")
            n += 1
            if n >= 8:
                raise SystemExit
