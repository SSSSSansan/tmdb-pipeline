"""Мини-обёртка: берёт пути из src/config.py (часть 1), а если там нет
нужной константы - использует значение по умолчанию. Так код роли B
работает независимо от точных имён в config.py и stats.py."""
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

try:
    from src import config as _config
except Exception:  # config.py ещё нет или он не импортируется
    _config = None


def cfg(name: str, default: str) -> Path:
    """Путь из config.<name> либо ROOT/default."""
    value = getattr(_config, name, None) if _config else None
    path = Path(value) if value is not None else ROOT / default
    return path if path.is_absolute() else ROOT / path


try:
    from src.stats import update_counts  # пишет data/pipeline_counts.json
except Exception:
    def update_counts(stage: str, value: int) -> None:
        """Запасной вариант, пока нет stats.py (роль C)."""
        path = ROOT / "data" / "pipeline_counts.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        counts = json.loads(path.read_text("utf-8")) if path.exists() else {}
        counts[stage] = value
        path.write_text(json.dumps(counts, indent=2, ensure_ascii=False), "utf-8")
