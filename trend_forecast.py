#!/usr/bin/env python3
"""Predict trend persistence using NeuralProphet."""
import json
import warnings
from pathlib import Path
from datetime import datetime
warnings.filterwarnings("ignore")

HISTORY_FILE = Path.home() / ".openclaw" / "workspace" / "trends_all_history.jsonl"

def _load_series(keyword: str, country: str) -> list:
    """Load time-series data for a keyword-country pair."""
    series = []
    if not HISTORY_FILE.exists():
        return series
    with open(HISTORY_FILE) as f:
        for line in f:
            try:
                rec = json.loads(line.strip())
                kw = rec.get("keyword") or rec.get("term") or rec.get("title", "")
                ct = rec.get("country") or rec.get("geo", "unknown")
                if kw.lower() == keyword.lower() and ct == country:
                    date = rec.get("date") or rec.get("fetched_at") or rec.get("pubDate", "")
                    traffic = rec.get("traffic", 0)
                    if date and traffic:
                        series.append({"ds": str(date)[:10], "y": float(str(traffic).replace("K+", "000").replace("M+", "000000").replace("+", "").replace(",", "") or 0)})
            except Exception:
                continue
    return sorted(series, key=lambda x: x["ds"])

def predict_persistence(keyword: str, country: str, horizon_days: int = 7) -> dict:
    """
    Returns:
        persistence_probability: float 0-1, probability trend stays above 20k
        predicted_halflife_days: int, estimated days until trend drops by half
    """
    series = _load_series(keyword, country)

    # Cold start — not enough history
    if len(series) < 5:
        return {"persistence_probability": 0.5, "predicted_halflife_days": 3,
                "confidence": "low", "data_points": len(series)}

    try:
        import pandas as pd
        from neuralprophet import NeuralProphet

        df = pd.DataFrame(series).drop_duplicates("ds").sort_values("ds")
        df["ds"] = pd.to_datetime(df["ds"])

        m = NeuralProphet(epochs=50, batch_size=16, learning_rate=0.01,
                          seasonality_mode="multiplicative", verbose=False)
        m.fit(df, freq="D", progress=None)

        future = m.make_future_dataframe(df, periods=horizon_days)
        forecast = m.predict(future)

        predicted_values = forecast["yhat1"].tail(horizon_days).values
        threshold = 20000
        days_above = sum(1 for v in predicted_values if v >= threshold)
        persistence_prob = days_above / horizon_days

        # Estimate half-life from decay rate
        current = float(df["y"].iloc[-1]) if len(df) > 0 else threshold
        half_life = horizon_days
        for i, v in enumerate(predicted_values):
            if v <= current / 2:
                half_life = i + 1
                break

        return {"persistence_probability": round(persistence_prob, 3),
                "predicted_halflife_days": half_life,
                "confidence": "medium" if len(series) >= 10 else "low",
                "data_points": len(series)}

    except Exception as e:
        return {"persistence_probability": 0.5, "predicted_halflife_days": 3,
                "confidence": "error", "error": str(e), "data_points": len(series)}
