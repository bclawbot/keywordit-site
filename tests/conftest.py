"""
Shared pytest fixtures for OpenClaw/Dwight test suite.

Usage:
    pytest                          # run all tests
    pytest -m "not integration"     # skip tests requiring live APIs
    pytest -m "not slow"            # skip slow tests
    pytest tests/                   # run only tests in tests/ dir
"""

import json
import os
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# Ensure workspace root is importable
WORKSPACE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(WORKSPACE))


# ── Fixtures: paths ──────────────────────────────────────────────────────────

@pytest.fixture
def workspace():
    """Return the workspace root Path."""
    return WORKSPACE


@pytest.fixture
def tmp_data_dir(tmp_path):
    """Create a temporary data directory with sample files."""
    data = tmp_path / "data"
    data.mkdir()
    return data


# ── Fixtures: mock LLM ──────────────────────────────────────────────────────

@pytest.fixture
def mock_llm_response():
    """Return a factory for mocking LLM responses."""
    def _make(content: str = "test response"):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json.return_value = {
            "choices": [{"message": {"content": content}}]
        }
        return mock_resp
    return _make


@pytest.fixture
def mock_ollama_response():
    """Return a factory for mocking Ollama /api/chat responses."""
    def _make(content: str = "test response"):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json.return_value = {
            "message": {"content": content}
        }
        return mock_resp
    return _make


@pytest.fixture
def no_llm_calls(monkeypatch):
    """Prevent any real LLM calls during tests."""
    monkeypatch.setattr("llm_client._litellm_ok", lambda: False)
    monkeypatch.setattr("llm_client.OPENROUTER_KEY", "")
    # Ollama calls will still fail with connection refused — that's fine


# ── Fixtures: mock HTTP ──────────────────────────────────────────────────────

@pytest.fixture
def mock_requests(monkeypatch):
    """Replace requests.post/get with a configurable mock."""
    mock = MagicMock()
    monkeypatch.setattr("requests.post", mock.post)
    monkeypatch.setattr("requests.get", mock.get)
    return mock


# ── Fixtures: sample data ────────────────────────────────────────────────────

@pytest.fixture
def sample_trend():
    """A single trend record matching the Stage 1 schema."""
    return {
        "run_id": "2026-04-05T10:00:00",
        "term": "best vpn 2026",
        "traffic": "50K+",
        "region": "United States",
        "geo": "US",
        "source": "google_trends",
        "sources": ["google_trends"],
        "fetched_at": "2026-04-05T10:00:00",
    }


@pytest.fixture
def sample_opportunity():
    """A validated opportunity matching the Stage 4 schema."""
    return {
        "keyword": "best vpn for streaming",
        "country": "US",
        "vertical": "tech",
        "cpc_usd": 4.50,
        "search_volume": 12000,
        "competition": 0.65,
        "arbitrage_index": 54000,
        "rsoc_score": 72.5,
        "tag": "GOLDEN_OPPORTUNITY",
        "rpc_expected": 0.85,
        "source_trend": "best vpn 2026",
        "validated_at": "2026-04-05T12:00:00",
    }


@pytest.fixture
def sample_opportunities_file(tmp_path, sample_opportunity):
    """Create a temporary validated_opportunities.json with sample data."""
    opp_file = tmp_path / "validated_opportunities.json"
    opp_file.write_text(json.dumps([sample_opportunity, {
        **sample_opportunity,
        "keyword": "cheap car insurance",
        "vertical": "finance",
        "cpc_usd": 8.20,
        "search_volume": 45000,
        "arbitrage_index": 369000,
        "rsoc_score": 85.0,
    }]))
    return opp_file


# ── Fixtures: environment ────────────────────────────────────────────────────

@pytest.fixture
def clean_env(monkeypatch):
    """Remove API credentials from environment to test credential-missing paths."""
    for key in [
        "GOOGLE_ADS_CLIENT_ID", "GOOGLE_ADS_CLIENT_SECRET",
        "GOOGLE_ADS_REFRESH_TOKEN", "GOOGLE_ADS_DEVELOPER_TOKEN",
        "GOOGLE_ADS_CUSTOMER_ID", "GOOGLE_ADS_LOGIN_CUSTOMER_ID",
        "DATAFORSEO_LOGIN", "DATAFORSEO_PASSWORD",
        "OPENROUTER_API_KEY", "TELEGRAM_TOKEN",
    ]:
        monkeypatch.delenv(key, raising=False)


# ── Markers ──────────────────────────────────────────────────────────────────

def pytest_configure(config):
    config.addinivalue_line("markers", "integration: requires live API credentials")
    config.addinivalue_line("markers", "slow: takes more than 30 seconds")
