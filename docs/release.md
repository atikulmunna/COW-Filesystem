# Release Guide

## Version and Tag Parity

Always keep `pyproject.toml` version aligned with the Git tag:

- package version: `X.Y.Z`
- git tag: `vX.Y.Z`

The publish workflow enforces this parity and fails on mismatch.

## Release Steps

```bash
# 1) bump version in pyproject.toml
git add pyproject.toml
git commit -m "Release X.Y.Z"
git push origin master

# 2) tag and publish
git tag vX.Y.Z
git push origin vX.Y.Z
```

## Verify Published Package

```bash
python -m venv /tmp/cowfs-verify
source /tmp/cowfs-verify/bin/activate
pip install -U cowfs==X.Y.Z
cowfs --help
```

## TestPyPI Rehearsal

When needed, publish a pre-release (for example `0.3.0rc1`) to TestPyPI before final release.
