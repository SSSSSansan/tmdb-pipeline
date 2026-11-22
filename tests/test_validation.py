"""Тесты Pandera-валидации. Запуск: python -m pytest -v"""
import pandas as pd
import pytest

from src.validator import make_test_records, validate_dataframe


def good(**over):
    row = dict(
        id=1, title="Film", original_title="Film", original_language="en",
        release_date="2020-05-01", popularity=10.0, vote_average=7.5,
        vote_count=100, adult=False, overview="text", runtime=120,
        budget=1_000_000, revenue=5_000_000, status="Released", genres="Drama",
    )
    row.update(over)
    return row


def run(*rows):
    return validate_dataframe(pd.DataFrame(rows))


def test_good_record_passes():
    valid, invalid = run(good())
    assert len(valid) == 1 and len(invalid) == 0


@pytest.mark.parametrize("override, column", [
    (dict(id=-5), "id"),
    (dict(vote_average=15), "vote_average"),
    (dict(release_date="2099-01-01"), "release_date"),
    (dict(title="   "), "title"),
    (dict(runtime=9999), "runtime"),
    (dict(budget=-1), "budget"),
    (dict(status="Unknown"), "status"),
    (dict(original_language="english"), "original_language"),
])
def test_bad_record_rejected(override, column):
    valid, invalid = run(good(), good(**{"id": 2, **override}))
    assert list(valid["id"]) == [1]          # хорошая запись осталась
    assert len(invalid) == 1
    assert column in invalid.iloc[0]["error_reason"]


def test_duplicate_id_keeps_first():
    valid, invalid = run(good(), good())
    assert len(valid) == 1 and len(invalid) == 1


def test_builtin_test_records_all_rejected():
    tests = make_test_records()
    valid, invalid = validate_dataframe(tests)
    assert len(valid) == 0
    assert len(invalid) == len(tests) >= 3
    assert invalid["is_test_record"].all()
