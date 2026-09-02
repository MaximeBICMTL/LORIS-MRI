# C-BIG invariants

## Repository and branch model

- `upstream` is the canonical LORIS-MRI history.
- `cbig-<major>` contains an adapted major-version Python stack on C-BIG ancestry.
- `cbig-main` adds post-release upstream-main commits individually.
- `cbig-meg` adds the provisional MEG feature port on `cbig-main`.
- No `cbig-*` branch may acquire `upstream/main` or an upstream release as an ancestor.

## Content boundary

Synchronize `pyproject.toml`, `python/`, `.github/`, and `test/` by default. `python/tests/` is part of `python/`.

Perl is compatibility glue, not a general synchronization target. Port a Perl change when required to invoke or configure synchronized Python correctly. Common inspection locations are root `*.pl`, `uploadNeuroDB/`, and `tools/`.

## Database boundary

C-BIG currently runs a version-26 database. Python package/release code may be newer, but existing ORM models and queries must match the deployed C-BIG schema.

Known examples include:

- C-BIG-specific candidate, project, session, and validation fields;
- `files.AcquisitionProtocolID` rather than upstream `files.MriScanTypeID` where documented;
- C-BIG MRI scan-type behavior in the NIfTI insertion pipeline;
- C-BIG event/HED mappings;
- omission of upstream columns unavailable in version 26, including `candidate.DoD_precision` and `sex.Colour`.

Feature-required new tables are a separate migration concern. Preserve their Python model contract only with explicit notes describing the required C-BIG schema work.

## Override annotations

The exact convention is:

```python
# C-BIG OVERRIDE START
# downstream implementation
# C-BIG OVERRIDE END
```

Annotations occur in ORM models, legacy database helpers, imaging code, importers, configuration, and tests. Inventory them before a rewrite. Preserve both semantics and markers. If upstream moves the containing function or file, move the override rather than silently dropping it.

Also inspect C-BIG wrappers under `tools/cbig/`; upstream command changes may require wrapper updates.
