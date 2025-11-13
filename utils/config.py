import yaml
from pathlib import Path


def get_project_root() -> Path:
    """
    Return the project root directory (where the repo lives).
    Works no matter where the script is run from.
    """
    current = Path(__file__).resolve()

    for parent in [current.parent, current.parent.parent, current.parent.parent.parent]:
        if (parent / ".git").exists():
            return parent
        if (parent / "docker").exists() and (parent / "scripts").exists():
            return parent
        if (parent / "configs").exists() and (parent / "utils").exists():
            return parent

    return current.parent.parent

def load_config(config_path: str):
    project_root = get_project_root()
    path = Path(config_path)
    full_path = (project_root / path).resolve()

    if not full_path.exists():
        raise FileNotFoundError(f"Config not found: {config_path}")
    with open(full_path) as f:
        return yaml.safe_load(f)