---
name: sync-downstream-branches
description: Synchronize upstream LORIS-MRI Python releases, post-release main commits, and the provisional MEG feature stack into the version-26-database C-BIG fork. Use when maintaining C-BIG major-version branches, cbig-main, or cbig-meg; resolving C-BIG override annotations; deciding which Python, Perl, test, CI, or ORM changes to port; or safely publishing rewritten LORIS-MRI branch stacks.
---

# Synchronize LORIS-MRI with C-BIG

Port modern upstream Python code onto the C-BIG fork while retaining the fork's version-26 database and behavior. Never make a `cbig-*` branch descend from `upstream/main` or an upstream release branch.

Read [references/cbig-invariants.md](references/cbig-invariants.md) before resolving conflicts. Select the applicable procedure from [references/workflow.md](references/workflow.md). Read [references/validation-checklist.md](references/validation-checklist.md) before declaring completion or pushing.

## Synchronize the intended content

Treat these as the primary synchronization scope:

- `pyproject.toml`;
- `python/`, including `python/tests/`;
- `.github/`;
- `test/`.

Do not blindly discard out-of-scope changes. Search the full repository for consumers whenever synchronized Python code renames or removes a command, module, package, configuration key, path, or API.

Handle Perl conservatively:

- Do not copy general upstream Perl implementation changes into C-BIG.
- Port the smallest required `*.pl` or `*.pm` caller changes when Python entry points or interfaces move.
- Check root pipeline scripts, `uploadNeuroDB/`, and `tools/` for callers.
- Update directly corresponding generated/user documentation when a Perl command reference changes.
- Preserve unrelated C-BIG Perl behavior.

## Respect the database boundary

Treat C-BIG as a version-26 database even when porting version-29-or-later Python. Keep `VERSION` and existing-table ORM contracts compatible with that database.

Classify every upstream database-related change:

1. **Existing version-26 table or column:** preserve the C-BIG model/query shape. Do not add an upstream column merely because a main commit does so. For example, omit `candidate.DoD_precision` and `sex.Colour` when absent from the C-BIG schema.
2. **Existing C-BIG divergence:** preserve and, when code moves, migrate the annotated override.
3. **New feature-required schema:** port the Python contract only when the feature needs it, document the required schema work, and do not claim integration readiness until the C-BIG migration exists.

Inspect `# C-BIG OVERRIDE START` / `# C-BIG OVERRIDE END` regions before and after every port. Never erase an override merely to match upstream. Move or rewrite the marked region when upstream relocates its surrounding code.

## Distinguish synchronization types

### Major release

Build or update `cbig-<major>` on the existing C-BIG/version-26 ancestry. Consolidate the release's primary-scope Python changes into the agreed major-version backport commit, with C-BIG adaptations. Do not cherry-pick the upstream release branch as ancestry.

### Post-release main

Build `cbig-main` on `cbig-<major>`. Replay each commit from the upstream release integration point through `upstream/main` separately when feasible:

- retain scoped Python/CI/test content;
- add only required Perl glue;
- omit commits whose only effect violates the version-26 schema;
- for a release-integration commit already represented by `cbig-<major>`, add any genuinely missing scoped changes, then skip it or retain an empty bookkeeping commit as appropriate.

### MEG feature — provisional

Do not generalize this procedure to other features yet.

1. Verify every branch feeding `meg`, including every secondary merge parent, is based on current `upstream/main`.
2. Rebase leaf dependencies and reconstruct the upstream merge without changing the final feature tree unless required.
3. Build `cbig-meg` on `cbig-main` without upstream ancestry.
4. Make the first commit one consolidated port of the latest `upstream/main..meg` implementation.
5. Preserve later C-BIG-specific commits separately, including an empty commit when its logical role remains useful.
6. Read `CBIG_MEG_PORT_NOTES.md` from the relevant branch.
7. Compare the latest MEG implementation with the source version used by any older C-BIG port. Audit file contents and class/function inventories; file presence is insufficient.

Ask the user how to model a non-MEG feature and update this skill after a second proven workflow exists.

## Operate safely

Fetch first, require a clean worktree, run `scripts/audit_loris_sync.sh`, and present the interpreted branch/commit mapping before rewriting when it is ambiguous. Create local dated recovery refs for every moved branch.

Push only with explicit authorization. Fetch again, use `--force-with-lease` for rewritten refs, prefer an atomic stack push, and verify local/remote object IDs. Do not push recovery or temporary branches.
