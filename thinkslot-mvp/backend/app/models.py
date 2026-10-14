from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field

SessionStatus = Literal["waiting", "resume", "expired"]
AdType = Literal["sponsored_tool", "token_credit", "discount", "message"]


class EnterRequest(BaseModel):
    task_id: str = Field(min_length=1)
    estimated_duration_sec: int = Field(gt=0, le=86_400)
    metadata: dict[str, Any] = Field(default_factory=dict)


class EnterResponse(BaseModel):
    session_id: str
    expires_at: datetime


class SessionStatusResponse(BaseModel):
    status: SessionStatus


class Reward(BaseModel):
    type: Literal["token_credit"] = "token_credit"
    amount: int = Field(ge=0)
    currency: Literal["tokens"] = "tokens"


class Action(BaseModel):
    type: Literal["tool_call"] = "tool_call"
    tool_name: str
    parameters: dict[str, Any] = Field(default_factory=dict)


class AdPayload(BaseModel):
    ad_id: str
    type: AdType
    title: str
    body: str
    reward: Reward
    action: Action


@dataclass(slots=True)
class SessionRecord:
    session_id: str
    task_id: str
    estimated_duration_sec: int
    metadata: dict[str, Any]
    status: SessionStatus
    created_at: datetime
    expires_at: datetime
