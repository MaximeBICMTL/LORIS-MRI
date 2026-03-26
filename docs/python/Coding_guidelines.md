# LORIS Python coding guidelines

This file presents the LORIS Python coding guidelines.

## Modules

- Divide the code in modules and submodules divided by theme.
- If a module grows too much, divide it in smaller modules.
- Do not naively follow the "one file = one class" architecture, this is Python, not Java!

## Data structures

### Static-structure dictionaries

Avoid using dictionaries to represent objects with a static structure (fixed set of keys). Instead, use typed alternatives like `dataclass` or `NamedTyple` that are more easily typed and documented.

Do not use:
```py
file = {
    'name': 'abcd.nii.gz',
    'size': 1234,
}
```

Use instead:
```py
@dataclass
class File:
    name: str
    size: int

file = File(
    name = 'abcd.nii.gz',
    size = 1234,
)
```

### Stringly-typed code

## Variables

- Variables names should follow the snake_case convention, with underscores to separate the different words of the name.
- For instance, do not use the names `candid`, `pscid`, and `filename`. Prefer the names `cand_id`, `psc_id`, and `file_name`.

## Paths

- Clearly distinguish file (and directory) _paths_ and _names_.
- A file _path_ variable should be of type `pathlib.Path` and end in `_path`.
- A file _name_ variable should be of type `str` and end in `_name`.
- When appropriate, document whether a path is absolute or relative, and to what it may be relative.
