"""Project package.

Importing ``src`` prefers ``.venv`` site-packages when present so notebook kernels
that are not the project venv can still resolve pyspark/delta before ``src.lake``
imports them at module load time.
"""

from __future__ import annotations


def _early_venv_bootstrap() -> None:
    from .spark import ensure_runtime

    ensure_runtime()


_early_venv_bootstrap()
