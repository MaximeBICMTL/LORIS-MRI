# LORIS-MRI/C-BIG workflows

## Major-release backport

1. Fetch `upstream` and the C-BIG remote.
2. Identify the existing C-BIG base and its actual database version from `VERSION`, schema knowledge, and override annotations.
3. Inventory upstream release changes under `pyproject.toml`, `python/`, `.github/`, and `test/`.
4. Compare them with the preceding C-BIG major backport; do not assume the commit subject proves completeness.
5. Create `cbig-<major>` on C-BIG ancestry and consolidate the scoped release delta into the agreed backport commit.
6. Resolve ORM/query changes using `cbig-invariants.md`.
7. Search for Perl callers affected by Python moves; port only required glue.
8. Preserve and migrate all applicable `C-BIG OVERRIDE` regions.

## Post-release main backport

1. List commits from the release integration point to `upstream/main` in oldest-first topological order.
2. Create `cbig-main` from the current `cbig-<major>`.
3. Map each upstream commit to one downstream commit unless it is intentionally omitted for database incompatibility.
4. Treat the upstream release-integration commit specially: verify whether its primary-scope content already exists in `cbig-<major>`. Add missing content only.
5. For each later commit:
   - port primary-scope content;
   - retain C-BIG overrides;
   - omit incompatible existing-schema changes;
   - trace deleted or renamed Python entry points into Perl and documentation callers.
6. Audit the accumulated diff against the primary scope and explain every necessary exception.

## MEG feature backport — provisional

1. Enumerate all refs and merge parents reachable from `meg` but not from `upstream/main`.
2. Test `upstream/main` ancestry for each dependency tip independently.
3. Create recovery refs, rebase outdated leaf branches, reconstruct the merge with the intended parent order, and replay post-merge commits.
4. Confirm an ancestry-only upstream rewrite preserves the old feature tree.
5. Identify which upstream MEG tree/version produced the existing C-BIG port. Diff that version against current `meg`; include changes folded into rebase conflict resolutions.
6. Create `cbig-meg` from `cbig-main` and produce one first commit representing the complete latest MEG feature delta with C-BIG adaptations.
7. Preserve later C-BIG patches separately.
8. Compare important latest-MEG files byte-for-byte where no C-BIG divergence is needed. Else compare models, classes, fields, functions, endpoints, and behavior explicitly.
9. Follow `CBIG_MEG_PORT_NOTES.md`, especially its schema requirements and exclusions.

## Conflict rules

- Never resolve wholesale with “ours” or “theirs” without checking the affected contract.
- Prefer C-BIG behavior inside an existing override region.
- Prefer the upstream implementation outside overrides unless it requires unavailable schema.
- When code moves, carry the override to the new path and keep the markers balanced.
- If a resolution changes a database assumption not covered by this skill, stop and ask.
- If a Python command disappears, do not complete the commit until repository-wide caller searches are clean.

## Publication

1. Keep recovery refs local.
2. Fetch the push remote immediately before publication.
3. Push related rewrites atomically with `--force-with-lease` when supported.
4. For a requested rename, create the new remote ref and delete the old one in the same atomic push.
5. Compare every local and remote-tracking object ID after the push.
