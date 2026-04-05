# Dollar-based budget tracking functions for cpc_cache.py
# These will be integrated into cpc_cache.py

import json
import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent.parent / "oracle.db"

def _conn():
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    return con


def get_today_usd_spent() -> float:
    """Returns total USD spent today across all DataForSEO endpoints."""
    with _conn() as con:
        row = con.execute(
            "SELECT usd_spent_today FROM api_usage WHERE date = date('now')"
        ).fetchone()
    return float(row["usd_spent_today"]) if row else 0.0


def increment_usd_spent(amount: float, endpoint: str):
    """
    Records USD spend immediately after each API call.
    Called per-task, not per-batch-completion.
    
    Args:
        amount: USD cost of the API call
        endpoint: Name of the endpoint (e.g., 'keyword_ideas', 'keyword_overview', 'bulk_kd')
    """
    with _conn() as con:
        # Get current breakdown
        row = con.execute(
            "SELECT endpoint_breakdown FROM api_usage WHERE date = date('now')"
        ).fetchone()
        
        breakdown = {}
        if row and row["endpoint_breakdown"]:
            try:
                breakdown = json.loads(row["endpoint_breakdown"])
            except (json.JSONDecodeError, ValueError, TypeError):
                breakdown = {}
        
        # Update breakdown
        breakdown[endpoint] = breakdown.get(endpoint, 0.0) + amount
        
        # Update total and breakdown
        con.execute(
            """
            INSERT INTO api_usage (date, usd_spent_today, endpoint_breakdown)
            VALUES (date('now'), ?, ?)
            ON CONFLICT(date) DO UPDATE SET
                usd_spent_today = usd_spent_today + excluded.usd_spent_today,
                endpoint_breakdown = excluded.endpoint_breakdown
            """,
            (amount, json.dumps(breakdown)),
        )


def budget_remaining(daily_budget_usd: float = 2.00) -> float:
    """
    Returns remaining budget in USD for today.
    
    Args:
        daily_budget_usd: Daily budget cap in USD (default $2.00)
    
    Returns:
        Remaining budget in USD
    """
    spent = get_today_usd_spent()
    return max(0.0, daily_budget_usd - spent)


def pre_flight_budget_check(estimated_cost: float, daily_budget_usd: float = 2.00) -> bool:
    """
    Returns False if this call would exceed the daily budget.
    
    Args:
        estimated_cost: Estimated cost of the upcoming API call in USD
        daily_budget_usd: Daily budget cap in USD (default $2.00)
    
    Returns:
        True if budget allows the call, False otherwise
    """
    remaining = budget_remaining(daily_budget_usd)
    return remaining >= estimated_cost
