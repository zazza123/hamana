# copilot-instructions.md

## Overview

A python library for seamless data extraction, storage, and SQL-based analysis using pandas and SQLite.

## Folder Architecture

```
hamana/
    - docs/                 # documentation folder
    - src/                  # library folder
        - hamana/           # main library code
        requirements.txt    # library dependencies
    - tests/                # tests folder
    LICENSE                 # project license
    mkdocs.yml              # configuration file for mkdocs-material library to manage documentation
    pyproject.toml          # configuration file for building library
    README.md               # general introduction of the project
```

## Code Style

- Use typing annotations (python > 3.12 version)

## Testing

- Use `ruff check src/hamana tests` to ensure no minor errors appear, and eventually fix them.
- Use `coverage run -m pytest -v -s tests` to run tests and check coverage.
- Fix any test or type errors until the whole suite is green.
- Add or update tests for the code you change, even if nobody asked.

## Documentation

- The project uses `mkdocs-material` python library to manage the documentation.
- The documentation is created dynamically by combining Markdown files with the actual codebase comments imported thanks to `mkdocstrings-python` library.
- The documentation is built using the `mkdocs serve` command and can be previewed locally.