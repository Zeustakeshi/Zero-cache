# Contributing to ZeroCache

Thank you for your interest in contributing! Here's how to get started.

---

## Getting Started

### 1. Fork and Clone

```bash
git clone https://github.com/Zeustakeshi/zerocache
cd zerocache
```

### 2. Set Up Dev Environment

```bash
python -m venv .venv
# Windows
.venv\Scripts\activate
# macOS / Linux
source .venv/bin/activate

pip install -e ".[dev]"
```

### 3. Create a Branch

```bash
git checkout -b feat/my-feature
```

---

## Development Workflow

### Run Tests

```bash
pytest tests/ -v
pytest tests/ --cov=zerocache --cov-report=term-missing
```

### Code Style

```bash
ruff check .
ruff format .
```

### Type Checking

```bash
mypy src/
```

---

## Commit Message Format

We use [Conventional Commits](https://www.conventionalcommits.org/):

```
feat: add GETDEL command
fix: LRUStore.copy() TypeError on snapshot
docs: add FastAPI integration example
perf: replace list scan with bisect in zrangebyscore
test: add coverage for scan_iter pattern matching
refactor: extract Pipeline to its own module
```

---

## Pull Request Guidelines

1. **Add tests** for any new features or bug fixes
2. **Update CHANGELOG.md** under `[Unreleased]`
3. **Pass all CI checks** — tests, ruff, mypy
4. Describe clearly what the PR does and why

---

## Reporting Bugs

Use the [bug report template](.github/ISSUE_TEMPLATE/bug_report.md).

For security vulnerabilities, see [SECURITY.md](SECURITY.md).

---

## Code of Conduct

Be respectful and constructive. We're building something together.
