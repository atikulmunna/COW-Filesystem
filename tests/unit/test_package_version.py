"""Package metadata/version consistency tests."""

from importlib.metadata import PackageNotFoundError, version

import cowfs


def test_dunder_version_matches_installed_distribution() -> None:
    """`cowfs.__version__` should reflect installed distribution version."""
    try:
        dist_version = version("cowfs")
    except PackageNotFoundError:
        assert cowfs.__version__ == "0.0.0+unknown"
    else:
        assert cowfs.__version__ == dist_version
