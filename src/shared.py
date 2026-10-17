"""Code shared by every Lambda function in this stack.

Single-table design (one DynamoDB table for the whole app):

    Entity     PK              SK                 GSI1PK          GSI1SK
    --------   -------------   ----------------   -------------   ---------------------------------
    Task       USER#<sub>      TASK#<taskId>      USER#<sub>      STATUS#<Status>#<timestamp>
    Profile    USER#<sub>      PROFILE            -               -

The user id always comes from the Cognito token (or the Cognito trigger
event), never from the request body, so a user can only reach their own items.
"""
import os
from datetime import datetime, timezone
from decimal import Decimal

import boto3

TABLE_NAME = os.environ["TABLE_NAME"]
table = boto3.resource("dynamodb").Table(TABLE_NAME)

PENDING = "Pending"
COMPLETED = "Completed"
EXPIRED = "Expired"
STATUSES = (PENDING, COMPLETED, EXPIRED)


# ---------------------------------------------------------------------------
# Keys
# ---------------------------------------------------------------------------
def user_pk(user_id: str) -> str:
    return f"USER#{user_id}"


def task_key(user_id: str, task_id: str) -> dict:
    return {"PK": user_pk(user_id), "SK": f"TASK#{task_id}"}


def profile_key(user_id: str) -> dict:
    return {"PK": user_pk(user_id), "SK": "PROFILE"}


def status_sort_key(status: str, timestamp: str) -> str:
    """GSI1SK: lets one Query return a user's tasks of a single status."""
    return f"STATUS#{status}#{timestamp}"


def schedule_name(task_id: str) -> str:
    """Name of the one-time EventBridge Scheduler schedule that expires a task."""
    return f"task-{task_id}"


# ---------------------------------------------------------------------------
# Time
# ---------------------------------------------------------------------------
def now_utc() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


# ---------------------------------------------------------------------------
# Mapping
# ---------------------------------------------------------------------------
def _plain_number(value):
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    return value


def to_api(item: dict) -> dict:
    """DynamoDB task item -> API representation (key attributes are not exposed)."""
    return {
        "taskId": item["TaskId"],
        "userId": item["UserId"],
        "description": item["Description"],
        "date": item.get("Date"),
        "status": item["Status"],
        "deadline": item.get("DeadlineISO"),
        "deadlineEpoch": _plain_number(item.get("Deadline")),
        "createdAt": item.get("CreatedAt"),
        "updatedAt": item.get("UpdatedAt"),
        "completedAt": item.get("CompletedAt"),
        "expiredAt": item.get("ExpiredAt"),
        "notifiedAt": item.get("NotifiedAt"),
    }
