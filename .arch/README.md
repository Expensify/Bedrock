# .arch — recursive codebase summarization machinery

Reusable across repos. Specs are repo-agnostic; the JSON is generated.

| File | What it is |
|---|---|
| `SCALING.md` | Architecture + why each technique was chosen to scale |
| `ANNOTATION_SPEC.md` | Phase 2 spec handed verbatim to every leaf agent |
| `ROLLUP_SPEC.md` | Phase 3 spec: bounded fan-in, resolve-or-escalate, rollup block |
| `manifest.json` | 152 units (`.h`+`.cpp` paired), content-hashed |
| `graph.json` | Include graph: depends / dependents / blast_radius |
| `batches_*.json` | Line-budgeted work assignments |
| `state.json` | Per-batch pending/running/done — resume point |
| `fragments/` | Per-batch JSON output from leaf agents |
| `cache/` | Content-hash keyed records; unchanged units skip on re-run |

Committed deliberately, including `cache/`: this container is ephemeral, so
git is the only place run state survives to the next session.
