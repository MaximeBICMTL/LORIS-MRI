# Validation checklist

## C-BIG history

- `cbig-<major>`, `cbig-main`, and `cbig-meg` remain on C-BIG ancestry.
- Neither `upstream/main` nor the release branch is an ancestor of a `cbig-*` branch.
- Major-release content is consolidated as agreed.
- Post-release main commits remain separately mapped; incompatible database-only commits are absent.
- MEG retains its requested consolidated port plus separate C-BIG patches.
- Recovery refs retain every original rewritten tip.

## Scope and callers

- Primary synchronization paths are `pyproject.toml`, `python/`, `.github/`, and `test/`.
- Every out-of-scope change has a stated compatibility reason.
- Deleted/renamed Python commands, packages, paths, and configuration keys have been searched repository-wide.
- Required Perl callers in root scripts, `uploadNeuroDB/`, and `tools/` use the new interface.
- Unrelated Perl behavior remains unchanged.
- C-BIG wrappers under `tools/cbig/` remain valid.

## Database and overrides

- `VERSION` and existing ORM/query contracts still match the version-26 C-BIG database.
- Incompatible upstream columns such as `DoD_precision` and `Colour` were not imported.
- Required new feature schema is explicitly documented and not confused with deployed schema.
- `C-BIG OVERRIDE START/END` markers remain balanced.
- Each pre-port override remains present or has a documented relocation/removal.
- Known acquisition-protocol, MRI scan-type, event/HED, candidate, project, session, and validation behavior remains intact.

## Semantic completeness

- Latest upstream feature content was used, not merely the source of an older port.
- Old/new upstream feature trees were compared after rebases.
- Important files were compared by content.
- Model, class, field, function, endpoint, command, and test inventories were compared where relevant.
- `git diff --check` passes.

## Quality and publication

- Ruff, Pyright, and unit tests pass.
- Relevant integration tests pass or their missing database/environment dependency is reported.
- The working tree is clean after generated artifacts are removed.
- Push and remote deletion were explicitly authorized.
- Remotes were fetched immediately before an atomic `--force-with-lease` push.
- Local and remote object IDs match; no backup/temp refs were pushed.
