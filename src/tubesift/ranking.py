from __future__ import annotations

import datetime as dt
import math


MATCH_WEIGHTS = {
    "title": 4.0,
    "description": 2.0,
    "subtitle": 1.5,
    "metadata": 1.0,
}


def compute_score(
    *,
    match_type: str,
    fts_rank: float,
    published_at: str,
    view_count: int,
    phrase_hit: bool = False,
) -> float:
    relevance = 1.0 / (1.0 + abs(float(fts_rank)))
    weight = MATCH_WEIGHTS.get(match_type, MATCH_WEIGHTS["metadata"])

    recency_bonus = _recency_bonus(published_at)
    views_bonus = math.log10(max(view_count, 0) + 1) * 0.08
    phrase_bonus = 0.5 if phrase_hit else 0.0
    return (relevance * weight) + recency_bonus + views_bonus + phrase_bonus


def _recency_bonus(published_at: str) -> float:
    try:
        video_date = dt.date.fromisoformat(published_at[:10])
    except ValueError:
        return 0.0

    age_days = (dt.date.today() - video_date).days
    if age_days <= 30:
        return 0.7
    if age_days <= 180:
        return 0.4
    if age_days <= 365:
        return 0.2
    if age_days <= 365 * 2:
        return 0.1
    return 0.0
