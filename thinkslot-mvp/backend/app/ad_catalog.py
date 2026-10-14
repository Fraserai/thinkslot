from __future__ import annotations

from copy import deepcopy
from typing import Any
from uuid import uuid4

from .models import AdPayload

_AD_TEMPLATES: list[dict[str, Any]] = [
    {
        "type": "sponsored_tool",
        "title": "Try VectorPilot Search",
        "body": "Speed up retrieval-heavy chains with sub-50ms semantic lookups.",
        "reward": {"type": "token_credit", "amount": 850, "currency": "tokens"},
        "action": {
            "type": "tool_call",
            "tool_name": "vectorpilot.search",
            "parameters": {"plan": "starter", "region": "global"},
        },
    },
    {
        "type": "token_credit",
        "title": "Claim Bonus Inference Tokens",
        "body": "Opt into sponsor tasks and receive bonus context-window credits.",
        "reward": {"type": "token_credit", "amount": 1200, "currency": "tokens"},
        "action": {
            "type": "tool_call",
            "tool_name": "thinkslot.claim_tokens",
            "parameters": {"campaign": "waitroom-q1"},
        },
    },
    {
        "type": "discount",
        "title": "40% Off Tool Sandbox",
        "body": "Get discounted isolated runtimes for long-running workflows.",
        "reward": {"type": "token_credit", "amount": 600, "currency": "tokens"},
        "action": {
            "type": "tool_call",
            "tool_name": "sandbox.activate_discount",
            "parameters": {"code": "THINK40"},
        },
    },
    {
        "type": "message",
        "title": "Monetize Idle Compute",
        "body": "Enable sponsor routing to earn while your agent reasons.",
        "reward": {"type": "token_credit", "amount": 300, "currency": "tokens"},
        "action": {
            "type": "tool_call",
            "tool_name": "thinkslot.enable_sponsors",
            "parameters": {"mode": "balanced"},
        },
    },
    {
        "type": "sponsored_tool",
        "title": "Framework Builder Pack: AgentOps Runtime",
        "body": "Ship observability, eval tracing, and memory controls for LangGraph/CrewAI in one SDK.",
        "reward": {"type": "token_credit", "amount": 1450, "currency": "tokens"},
        "action": {
            "type": "tool_call",
            "tool_name": "agentops.builder_pack",
            "parameters": {"frameworks": ["langgraph", "crewai"], "plan": "growth"},
        },
    },
]


def build_ad(sequence_number: int) -> AdPayload:
    template = deepcopy(_AD_TEMPLATES[sequence_number % len(_AD_TEMPLATES)])
    template["ad_id"] = f"ad_{uuid4().hex}"
    return AdPayload(**template)
