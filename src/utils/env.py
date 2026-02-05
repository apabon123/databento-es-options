"""Load .env from project root. Call load_env() at application startup."""

from pathlib import Path

from dotenv import load_dotenv


def _find_project_root() -> Path:
    """Find project root by walking up from this file until pyproject.toml or .env exists."""
    path = Path(__file__).resolve().parent
    for _ in range(5):
        if (path / "pyproject.toml").exists() or (path / ".env").exists():
            return path
        parent = path.parent
        if parent == path:
            break
        path = parent
    return Path.cwd()


def load_env() -> bool:
    """Load .env from project root. Idempotent. Returns True if .env was found and loaded."""
    root = _find_project_root()
    env_path = root / ".env"
    return load_dotenv(dotenv_path=env_path)
