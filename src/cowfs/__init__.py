"""COWFS — Copy-on-Write Filesystem."""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("cowfs")
except PackageNotFoundError:
    # Source tree fallback when distribution metadata is unavailable.
    __version__ = "0.0.0+unknown"
