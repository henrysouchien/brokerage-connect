"""Vendor helpers used by the standalone brokerage package."""

from __future__ import annotations

import math
from datetime import datetime
from typing import Any

try:
    import numpy as np
except Exception:  # pragma: no cover - optional dependency
    np = None

try:
    import pandas as pd
except Exception:  # pragma: no cover - optional dependency
    pd = None


def make_json_safe(obj: Any) -> Any:
    """Recursively convert values into JSON-serializable forms."""
    if isinstance(obj, dict):
        safe_dict = {}
        for key, value in obj.items():
            if pd is not None and isinstance(key, (pd.Timestamp, datetime)):
                safe_key = key.strftime("%Y-%m-%d %H:%M:%S")
            elif isinstance(key, (int, float, str, bool, type(None))):
                safe_key = key
            else:
                safe_key = str(key)
            safe_dict[safe_key] = make_json_safe(value)
        return safe_dict

    if isinstance(obj, list):
        return [make_json_safe(item) for item in obj]

    if pd is not None and isinstance(obj, pd.DataFrame):
        return obj.to_dict("records")

    if pd is not None and isinstance(obj, pd.Series):
        return {str(k): make_json_safe(v) for k, v in obj.to_dict().items()}

    if np is not None and isinstance(obj, np.ndarray):
        return obj.tolist()

    if np is not None and isinstance(obj, (np.int64, np.int32)):
        return int(obj)

    if np is not None and isinstance(obj, (np.float64, np.float32)):
        f = float(obj)
        return None if (math.isnan(f) or math.isinf(f)) else f

    if np is not None and isinstance(obj, np.bool_):
        return bool(obj)

    if pd is not None and isinstance(obj, (pd.Timestamp, datetime)):
        return obj.strftime("%Y-%m-%d %H:%M:%S")

    if pd is not None:
        try:
            if pd.isna(obj):
                return None
        except Exception:
            pass

    if isinstance(obj, (int, float, str, bool, type(None))):
        if isinstance(obj, float) and (obj != obj or math.isinf(obj)):
            return None
        return obj

    return str(obj)


def _to_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        result = float(value)
        if math.isinf(result):
            return None
        return result
    except (TypeError, ValueError):
        return None
