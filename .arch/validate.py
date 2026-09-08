#!/usr/bin/env python3
"""Prove that annotation changed nothing but prepended comments.

Stronger than compiling, and unlike a build it scales to any repo and needs no
toolchain. For each modified file we assert:

  1. the file now begins with exactly one SUMMARY block comment,
  2. that block contains no premature `*/` (which would end it early and dump
     prose into the token stream),
  3. and the entire remainder of the file is BYTE-IDENTICAL to the committed
     version.

If (3) holds, the change cannot alter compilation, because the only difference
is a leading comment. No build required to know that.

Usage:  python3 .arch/validate.py [--ref HEAD]
"""
import subprocess, sys, re

REF = 'HEAD'
if '--ref' in sys.argv:
    REF = sys.argv[sys.argv.index('--ref') + 1]

OPEN = '/* SUMMARY'
CLOSE_RE = re.compile(r'^\s*\*[─-]*\*/\s*$')   # the ─── */ terminator line

def committed(path):
    try:
        return subprocess.check_output(['git', 'show', f'{REF}:{path}'])
    except subprocess.CalledProcessError:
        return None

def modified():
    out = subprocess.check_output(['git', 'diff', '--name-only', REF], text=True)
    return [p for p in out.splitlines()
            if p.endswith(('.h', '.cpp', '.c', '.hpp'))]

def check(new, old):
    """Return None if `new` is `old` with a single well-formed SUMMARY comment
    inserted, else a precise reason string.

    Excise-and-compare rather than prefix-match: the spec permits inserting
    after an existing licence header, so the block is not always at offset 0.
    Removing the block and demanding byte-equality with the original is just as
    rigorous and actually matches the instruction agents were given.
    """
    n = new.count(OPEN)
    if n == 0:
        return 'file changed but no SUMMARY block was added'
    if n > 1:
        return f'{n} SUMMARY blocks found, expected exactly 1'

    start = new.index(OPEN)
    end = new.find('*/', start)
    if end == -1:
        return 'SUMMARY block is never closed'
    end += 2

    block = new[start:end]
    # The block owns everything between /* SUMMARY and its first */. If the
    # author wrote a bare */ in prose (e.g. "const char*/string") the comment
    # ends early and the rest spills into the token stream — the excision below
    # then fails byte-equality, but say so precisely.
    if block.count('/*') != 1:
        return (f"malformed block: {block.count('/*')} openers — nested /* is "
                f"not legal in a C comment")

    # Excise the block, then absorb whitespace at the seam. A blank line after
    # the block is natural to write and harmless, so tolerate it rather than
    # forcing agents to fight the formatter — but only whitespace is forgiven.
    head, tail_ = new[:start], new[end:]
    for lead in range(0, 3):
        for trail in range(0, 3):
            h = head[:len(head) - lead] if lead and head[len(head) - lead:].strip() == '' else head
            t = tail_[trail:] if trail and tail_[:trail].strip() == '' else tail_
            if h + t == old:
                return None
    excised = head + tail_

    if old.replace('\n', '') == excised.replace('\n', ''):
        return 'only whitespace/newlines differ — likely a stray blank line, not code'

    i = next((i for i, (a, b) in enumerate(zip(excised, old)) if a != b),
             min(len(excised), len(old)))
    line = old[:i].count('\n') + 1
    tail = new[end:end + 60].strip().replace('\n', '\\n')[:50]
    hint = ''
    if '*/' in block[len(OPEN):-2]:
        hint = ' — a premature */ inside the block ended the comment early'
    return (f'code differs from {REF} at line ~{line}{hint}; '
            f'after block: {tail!r}')

def main():
    files = modified()
    if not files:
        print('no modified source files'); return 0

    bad = []
    for path in files:
        old_b = committed(path)
        if old_b is None:
            bad.append((path, 'new file, not in ref')); continue
        try:
            new = open(path, 'rb').read().decode('utf8')
            old = old_b.decode('utf8')
        except UnicodeDecodeError:
            bad.append((path, 'not utf8')); continue

        why = check(new, old)
        if why:
            bad.append((path, why))

    ok = len(files) - len(bad)
    print(f'{ok}/{len(files)} files verified comment-only against {REF}')
    for path, why in bad:
        print(f'  FAIL  {path}: {why}')
    return 1 if bad else 0

if __name__ == '__main__':
    sys.exit(main())
