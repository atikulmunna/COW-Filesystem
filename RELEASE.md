# COWFS Release Process

This project publishes to PyPI from GitHub Actions when a tag matching `v*` is pushed.

Workflow file:
- `.github/workflows/publish.yml`

## Prerequisites

1. PyPI project exists (`cowfs`).
2. GitHub Actions environment `pypi` is configured.
3. Trusted Publishing is configured on PyPI for this repository (OIDC).
4. `master` is green in CI.

## Versioning Policy

- Follow SemVer: `MAJOR.MINOR.PATCH`.
- Tag format must be `vX.Y.Z` (example: `v0.2.0`).
- `pyproject.toml` `project.version` must match the tag without `v`.

## Release Steps

1. Ensure working tree is clean:
   ```bash
   git status
   ```
2. Update version in `pyproject.toml`:
   - Example: `version = "0.2.0"`
3. Commit version bump:
   ```bash
   git add pyproject.toml
   git commit -m "Bump version to 0.2.0"
   git push origin master
   ```
4. Create and push release tag:
   ```bash
   git tag v0.2.0
   git push origin v0.2.0
   ```
5. Verify GitHub Actions:
   - `Publish` workflow runs.
   - `build` job succeeds.
   - `publish` job succeeds.
6. Verify package on PyPI:
   ```bash
   pip install -U cowfs==0.2.0
   ```

## Optional: TestPyPI Dry Run

Before first real publish, validate packaging with TestPyPI.

1. Create a TestPyPI workflow variant (or temporary branch workflow) targeting TestPyPI.
2. Publish a pre-release version like `0.2.0rc1`.
3. Install from TestPyPI and sanity check:
   ```bash
   pip install -i https://test.pypi.org/simple/ cowfs==0.2.0rc1
   ```

## Rollback Notes

- If a tag is wrong before publish starts:
  ```bash
  git tag -d v0.2.0
  git push origin :refs/tags/v0.2.0
  ```
- If already published to PyPI:
  - Do not overwrite.
  - Release a new patch version.
