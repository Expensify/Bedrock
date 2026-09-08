# Annotation Spec (Phase 2)

You are annotating **units** of the Bedrock C++ codebase. A *unit* is one class:
`Foo.h` + `Foo.cpp` together, or a lone file when there is no pair.

You will be given a list of units. For each one you must do TWO things:

1. Insert a `SUMMARY` block comment at the top of **each file** in the unit.
2. Append a JSON record describing the unit to your batch output file.

---

## 1. The in-file SUMMARY block

### Placement rules — these matter, get them exactly right

- Insert at the **very top of the file**, before everything else — before
  `#pragma once`, before include guards, before any existing comment.
- If the file already opens with a licence header, insert **after** the licence
  and before the code.
- Do **not** modify any other line of the file. No reformatting, no reordering
  includes, no fixing what you find. You are annotating, not refactoring.
- Never break compilation. The block is a `/* ... */` comment.

### The `*/` hazard — this WILL bite you, verify explicitly

Writing about C++ makes it very easy to type `*/` by accident. `const char*/string`,
`T*/U`, `int*/*count*/` — each of these silently **terminates your comment early**
and dumps the remaining prose into the token stream as code. The file then fails
to compile, and the cause is not obvious from the error.

The calibration run hit this exactly once, on the phrase `const char*/string`.

So: **after writing each block, re-read it and search for `*/`.** There must be
exactly one, and it must be the final line. If prose needs a pointer type, write
`const char *` with a space, or say "char pointer". Do not rely on remembering
this while composing — check afterwards, every time.

A validator (`.arch/validate.py`) runs after your batch and will catch this, but
finding it yourself is cheaper than a repair pass.

### Format

```
/* SUMMARY ─────────────────────────────────────────────────────────────
 * File:    <basename>
 * Path:    <full repo-relative path>
 * Pair:    <basename of the other file in the unit, or omit this line>
 *
 * INTENT
 *   What this file is for, at the level a new engineer needs. 1-3 sentences.
 *   Say what it *does*, not what it *contains*.
 *
 * OBJECTS
 *   <Name>  - <role in one line>
 *   ...
 *   List every class, struct, enum, free function group, and significant
 *   macro. Nested types written as Outer::Inner. For a .cpp, list what it
 *   implements plus anything file-local (anonymous-namespace helpers,
 *   statics) that does not appear in the header.
 *
 * OUT OF PLACE
 *   Anything here that does not serve this file's stated intent. Be concrete
 *   and name the symbol. If nothing, write exactly: Nothing.
 *   Mark every entry [CANDIDATE] — you are reading one unit and cannot see
 *   the alternatives, so you are raising leads, not delivering verdicts.
 *
 * NAME/LOCATION FIT
 *   Does the filename describe the contents? Does the directory make sense?
 *   State the mismatch, or say it fits.
 *
 * NAMING QUALITY
 *   Do the type/member/variable names match repo convention and each other?
 *   Flag misleading names, inconsistent prefixes, and types that are
 *   trivially confusable (e.g. two distinct concepts both typedef'd to
 *   uint64_t and mutually assignable).
 * ─────────────────────────────────────────────────────────────────────*/
```

### Length — scale it to the file

Do not pad. A 40-line helper deserves ~8 lines of summary; `SQLiteNode.cpp`
deserves the full treatment. `OBJECTS` and `OUT OF PLACE` carry the value —
the other three sections are frequently one line each and should be.

For the `.cpp` of a pair, keep `INTENT` to one line pointing at the header and
spend the space on implementation-only detail. Do not duplicate the header's
block.

### Units with no header of their own

Some `.cpp` files are single-file units because their declarations live in a
shared header elsewhere (e.g. `SSignal.cpp`, declared in `libstuff.h`). For
these:

- Treat the shared header's declarations as "the header" for the purpose of
  deciding what is public vs file-local.
- Name that header explicitly in the `Pair:` line, like
  `Pair: (declarations in libstuff/libstuff.h)`.
- Being declared in a catch-all shared header is itself worth noting under
  `OUT OF PLACE` if the symbol has no business being in a catch-all.

If a unit's obvious counterpart file exists but is not in your batch, do not go
annotate it — it is either in another batch or deliberately excluded as
third-party. Note its existence in `Pair:` and move on.

### Judgement

Be a critical reader, not a flatterer. "Nothing." in OUT OF PLACE is a perfectly
good and common answer — do not invent problems to look thorough. But when
something genuinely does not belong, say so plainly and name it. A junk-drawer
file with fifteen unrelated helpers should be described as exactly that.

---

## 2. The JSON record

Append one object per unit to your assigned output file
(`.arch/fragments/<batch-id>.json`), as a JSON array. Schema:

```json
{
  "unit": "SData",
  "dir": "libstuff",
  "files": ["libstuff/SData.h", "libstuff/SData.cpp"],
  "lines": 283,
  "test": false,
  "intent": "One sentence. Same substance as the INTENT block, compressed.",
  "objects": [
    {"name": "SData", "kind": "struct", "role": "one line"}
  ],
  "misfits": [
    {
      "item": "SData::deserialize",
      "why": "HTTP wire-format parsing inside a generic string-map container",
      "severity": "med",
      "suggested_home": "libstuff/SHTTPSManager or a new SHTTPMessage unit, or null"
    }
  ],
  "name_fit":       {"score": 4, "note": "brief"},
  "location_fit":   {"score": 3, "note": "brief"},
  "naming_quality": {"score": 4, "note": "brief"},
  "depends_on": ["libstuff/libstuff.h", "BedrockCommand.h"]
}
```

- `severity`: `low` | `med` | `high`. Reserve `high` for something that is
  clearly in the wrong file or wrong directory, not mere untidiness.
- `suggested_home`: your best guess or `null`. A guess is genuinely useful even
  when wrong — Phase 4 has the full map and will adjudicate.
- Scores are 1-5, 5 = excellent. Be willing to use the low end.
- `depends_on`: repo-relative paths from this unit's `#include` lines. Skip
  system/std headers. This feeds coupling analysis in Phase 4.

Write valid JSON. A trailing comma or an unescaped quote costs a repair pass.

---

## Reporting back

When done, reply with only:
- units annotated (count), files touched (count)
- the 3 most interesting misfits you found, one line each
- anything that blocked you

Do not paste the summaries back — they are on disk.
