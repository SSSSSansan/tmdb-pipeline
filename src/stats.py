"""Счётчики стадий пайплайна: пишем в data/pipeline_counts.json."""
import json

from src import config


def update_counts(stage, value):
    """Записывает число записей для стадии (collected, produced, consumed, ...)."""
    config.DATA_DIR.mkdir(parents=True, exist_ok=True)
    counts = {}
    if config.COUNTS_JSON.exists():
        counts = json.loads(config.COUNTS_JSON.read_text(encoding="utf-8"))
    counts[stage] = value
    config.COUNTS_JSON.write_text(
        json.dumps(counts, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    return counts
