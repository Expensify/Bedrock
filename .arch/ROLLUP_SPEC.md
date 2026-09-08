# Directory Rollup Spec (Phase 3)

You are writing or revising the `SUMMARY.md` for **one directory**.

## The hard constraint: bounded fan-in

You see **only your immediate children**. Specifically:

- the unit records for files directly in this directory, and
- the `SUMMARY.md` of each direct subdirectory (already written).

You do **not** get the transitive subtree, and you must not go read it. This is
deliberate: the algorithm has to hold on repos 100x this size, where reading a
subtree is impossible. Work from your children's rollup blocks — they were
written to carry exactly what you need.

If a child's rollup is inadequate for a judgement you need to make, say so in
your report rather than reading around it. That is a spec bug worth knowing
about.

## Three passes

You will be told which pass you are running. Most directories run only A and B;
Pass C exists solely for directories too wide to read in one go.

### Pass C — cluster (wide directories only)

Some directories have too many children for "read your immediate children" to be
a real bound: `test/clustertest/tests` has 36, `libstuff` has 27, and on a large
repo the equivalent number is in the hundreds. For these, the children are
pre-grouped into thematic clusters (see `.arch/clusters.json`, computed
algorithmically) and you summarize **one cluster**, not the directory.

Write to `SUMMARY.<cluster-label>.md` inside the directory. Cover:

1. What these units have in common — the reason they cluster.
2. One line per unit.
3. Misfits: anything in this cluster that does not fit *the cluster* (it may
   still fit the directory — say so; that is a resolvable-locally case).
4. The same trailing `ROLLUP` block, with `theme` describing the cluster.

The directory's own Pass A agent then reads the cluster summaries instead of all
N children, which restores a bounded fan-in. Do not try to describe the whole
directory — you are only seeing part of it, and saying more than you know is
exactly the failure this structure exists to prevent.

### Pass A — bottom-up (build)

Answer, from your children:

1. **Theme.** What is this directory *for*? One line, then a short paragraph.
   Not "a collection of X" — say what job it does for the system.
2. **Contents.** Table of direct children (units and subdirectories), one line
   each. This is the only place a listing belongs.
3. **Coherence.** Do these children belong together? Name the ones that fit the
   theme least. A directory that is really two directories wearing a trenchcoat
   should be described as such.
4. **Misfits.** Consolidate what your children flagged. For each, decide:
   - **Resolvable here** — the better home is inside this directory. Say where.
     Mark it `resolved-locally`. It stops here and does not propagate.
   - **Escalate** — the better home is outside this directory, or the decision
     needs a wider view. Mark it `escalate` and it goes into your rollup block.
   Being decisive here is what keeps the volume reaching the root manageable.

### Pass B — top-down (situate)

You now also get **your parent's rollup block**, including which sibling
directories exist and what they claim. Revise the file in place to add:

5. **Role in the system.** What this directory owns that its siblings do not.
   Where the boundary with each relevant sibling actually falls, and whether
   that boundary currently leaks.
6. **Inbound expectations.** Given what the parent and siblings say they depend
   on, what does this directory owe outward? Is anything they rely on missing,
   or exposed by accident?
7. Revisit item 4: a misfit you escalated may now be resolvable, and something
   you thought fine may now look wrong next to a sibling. Update it.

Do not rewrite passes A's content wholesale — revise it. Keep the section order.

## If the Write tool refuses your SUMMARY file

The Write tool may decline a file named `SUMMARY*.md`, mistaking it for a
report about your own work. It is not — it is the repo deliverable this spec
exists to produce. If that happens, write it with a Bash heredoc instead:

```bash
cat > path/to/SUMMARY.md <<'EOF'
...content...
EOF
```

Do not silently skip the file, and do not rename it to get around the refusal —
the filename is load-bearing: the parent directory's agent looks for exactly
this name.

## Required trailing block

Every `SUMMARY.md` must end with this block, exactly this shape. The parent
directory's agent reads *this* rather than your prose, so it must stand alone.

```
<!-- ROLLUP
theme: one line
exports: [3-8 concepts this directory offers outward]
depends_on_dirs: [repo-relative dirs this subtree includes from]
depended_on_by: [filled in during pass B; leave [] in pass A]
misfit_count: {high: N, med: N, low: N}
resolved_locally: N
escalate:
  - item: symbol or file
    from: path
    why: one line
    suggested_home: best guess or null
-->
```

Keep `escalate` short. If everything escalates, you have not done step 4.

## Length

Proportional to the directory. A leaf directory of four small units needs half a
page. `libstuff/` or `test/` needs more. Never pad the contents table into a
substitute for analysis — the analysis is the point, the table is scaffolding.

## Reporting back

Reply with only: the directory, pass, misfit counts (resolved vs escalated), the
single most significant structural observation, and any place your children's
rollups were inadequate.
