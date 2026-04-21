"""Console-script entry point that launches the Streamlit dashboard.

``exoplanet-dashboard`` (defined in ``pyproject.toml``) resolves to
:func:`run` below, which shells out to ``python -m streamlit run app.py``.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


def run() -> None:
    """Exec ``streamlit run dashboard/app.py`` using the current interpreter.

    Reads ``EXOPLANET_DASHBOARD_PORT`` from the environment (defaulting to
    8501) so the launcher can be reconfigured without code changes.
    """
    app_path = Path(__file__).resolve().parent / "app.py"
    if not app_path.exists():  # pragma: no cover - guard for broken installs
        raise SystemExit(f"Dashboard app.py not found at {app_path}")

    port = os.environ.get("EXOPLANET_DASHBOARD_PORT", "8501")
    cmd = [
        sys.executable,
        "-m",
        "streamlit",
        "run",
        str(app_path),
        "--server.port",
        str(port),
    ]
    try:
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as e:  # pragma: no cover
        raise SystemExit(e.returncode) from e
    except FileNotFoundError as e:  # pragma: no cover
        raise SystemExit(
            "Streamlit is not installed in this environment. "
            "Install with `pip install streamlit`."
        ) from e


if __name__ == "__main__":  # pragma: no cover
    run()
