"""TasksApiFunction - one Lambda function for every task CRUD operation.

API Gateway sends all five routes to this function and the Powertools
``APIGatewayRestResolver`` dispatches each request to the matching handler
below. Adding or changing an operation means adding or changing one decorated
function here; the template only lists the route as another Api event.

    POST    /tasks              create a Pending task + its expiry schedule
    GET     /tasks              list the caller's tasks (?status=Pending|Completed|Expired)
    GET     /tasks/{taskId}     read one task
    PUT     /tasks/{taskId}     edit a Pending task, or mark it Completed
    DELETE  /tasks/{taskId}     delete a task

Every route sits behind the Cognito authorizer; the user id is the token's
``sub`` claim.
"""
import json
import os
import uuid
from datetime import date, timedelta

import boto3
from aws_lambda_powertools import Logger, Metrics
from aws_lambda_powertools.event_handler import APIGatewayRestResolver, CORSConfig, Response, content_types
from aws_lambda_powertools.event_handler.exceptions import (
    BadRequestError,
    NotFoundError,
    ServiceError,
    UnauthorizedError,
)
from aws_lambda_powertools.logging import correlation_paths
from aws_lambda_powertools.metrics import MetricUnit
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

import shared

logger = Logger()
metrics = Metrics()
app = APIGatewayRestResolver(
    cors=CORSConfig(allow_origin="*", allow_headers=["Authorization", "Content-Type"], max_age=300)
)
scheduler = boto3.client("scheduler")

DEFAULT_DEADLINE_MINUTES = int(os.environ.get("DEFAULT_DEADLINE_MINUTES", "5"))
SCHEDULE_GROUP = os.environ.get("SCHEDULE_GROUP", "")
EXPIRE_FUNCTION_ARN = os.environ.get("EXPIRE_FUNCTION_ARN", "")
SCHEDULER_ROLE_ARN = os.environ.get("SCHEDULER_ROLE_ARN", "")

MAX_DESCRIPTION = 500
MAX_DEADLINE_MINUTES = 1440
UPDATABLE_FIELDS = {"description", "date", "status"}


# ---------------------------------------------------------------------------
# Request helpers
# ---------------------------------------------------------------------------
def caller() -> tuple[str, str | None]:
    """(user id, email) from the Cognito authorizer claims."""
    claims = app.current_event.request_context.authorizer.claims or {}
    if not claims.get("sub"):
        raise UnauthorizedError("Unauthorized")
    return claims["sub"], claims.get("email")


def body() -> dict:
    try:
        data = json.loads(app.current_event.body or "{}")
    except json.JSONDecodeError:
        raise BadRequestError("Request body must be valid JSON")
    if not isinstance(data, dict):
        raise BadRequestError("Request body must be a JSON object")
    return data


def json_response(status: int, payload=None) -> Response:
    return Response(
        status_code=status,
        content_type=content_types.APPLICATION_JSON,
        body=None if payload is None else json.dumps(payload),
    )


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------
def valid_description(value) -> str:
    if not isinstance(value, str) or not value.strip():
        raise BadRequestError("description is required")
    if len(value.strip()) > MAX_DESCRIPTION:
        raise BadRequestError(f"description must be at most {MAX_DESCRIPTION} characters")
    return value.strip()


def valid_date(value) -> str:
    try:
        return date.fromisoformat(value).isoformat()
    except (TypeError, ValueError):
        raise BadRequestError("date must be in YYYY-MM-DD format")


def valid_deadline_minutes(value) -> int:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or int(value) != value:
        raise BadRequestError("deadlineMinutes must be a whole number")
    if not 1 <= int(value) <= MAX_DEADLINE_MINUTES:
        raise BadRequestError(f"deadlineMinutes must be between 1 and {MAX_DEADLINE_MINUTES}")
    return int(value)


# ---------------------------------------------------------------------------
# Expiry schedule (EventBridge Scheduler)
# ---------------------------------------------------------------------------
def create_expiry_schedule(user_id: str, task_id: str, deadline) -> str:
    """One-time schedule that invokes ExpireTaskFunction at the deadline, then deletes itself."""
    name = shared.schedule_name(task_id)
    scheduler.create_schedule(
        Name=name,
        GroupName=SCHEDULE_GROUP,
        Description=f"Expire task {task_id} at its deadline",
        ScheduleExpression=f"at({deadline:%Y-%m-%dT%H:%M:%S})",
        ScheduleExpressionTimezone="UTC",
        FlexibleTimeWindow={"Mode": "OFF"},
        ActionAfterCompletion="DELETE",
        Target={
            "Arn": EXPIRE_FUNCTION_ARN,
            "RoleArn": SCHEDULER_ROLE_ARN,
            "Input": json.dumps({"userId": user_id, "taskId": task_id}),
            "RetryPolicy": {"MaximumRetryAttempts": 3, "MaximumEventAgeInSeconds": 3600},
        },
    )
    return name


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------
@app.post("/tasks")
def create_task():
    user_id, email = caller()
    data = body()
    now = shared.now_utc()
    deadline = now + timedelta(minutes=valid_deadline_minutes(data.get("deadlineMinutes", DEFAULT_DEADLINE_MINUTES)))
    task_id = str(uuid.uuid4())

    item = {
        **shared.task_key(user_id, task_id),
        "GSI1PK": shared.user_pk(user_id),
        "GSI1SK": shared.status_sort_key(shared.PENDING, shared.iso(deadline)),
        "EntityType": "TASK",
        "TaskId": task_id,
        "UserId": user_id,
        "Email": email,
        "Description": valid_description(data.get("description")),
        "Date": valid_date(data["date"]) if data.get("date") else now.date().isoformat(),
        "Status": shared.PENDING,
        "Deadline": int(deadline.timestamp()),
        "DeadlineISO": shared.iso(deadline),
        "CreatedAt": shared.iso(now),
        "UpdatedAt": shared.iso(now),
        "ScheduleGroup": SCHEDULE_GROUP,
    }

    # Schedule first; if the write then fails, remove the schedule again. A schedule
    # left behind would only fire for a missing task, which ExpireTask ignores.
    item["ScheduleName"] = create_expiry_schedule(user_id, task_id, deadline)
    try:
        shared.table.put_item(Item=item, ConditionExpression="attribute_not_exists(PK)")
    except Exception:
        logger.exception("Task write failed; removing its expiry schedule", extra={"task_id": task_id})
        try:
            scheduler.delete_schedule(Name=item["ScheduleName"], GroupName=SCHEDULE_GROUP)
        except ClientError:
            logger.exception("Could not remove the orphan schedule (ExpireTask will ignore it)")
        raise

    metrics.add_metric(name="TasksCreated", unit=MetricUnit.Count, value=1)
    logger.info("Task created", extra={"task_id": task_id, "deadline": item["DeadlineISO"]})
    return json_response(201, shared.to_api(item))


@app.get("/tasks")
def list_tasks():
    user_id, _ = caller()
    status = app.current_event.get_query_string_value(name="status", default_value=None)

    if status:
        if status not in shared.STATUSES:
            raise BadRequestError(f"status must be one of {', '.join(shared.STATUSES)}")
        query = {
            "IndexName": "GSI1",
            "KeyConditionExpression": Key("GSI1PK").eq(shared.user_pk(user_id))
            & Key("GSI1SK").begins_with(f"STATUS#{status}#"),
        }
    else:
        query = {
            "KeyConditionExpression": Key("PK").eq(shared.user_pk(user_id)) & Key("SK").begins_with("TASK#"),
        }

    items = []
    while True:
        page = shared.table.query(**query)
        items.extend(page.get("Items", []))
        if "LastEvaluatedKey" not in page:
            break
        query["ExclusiveStartKey"] = page["LastEvaluatedKey"]

    tasks = sorted((shared.to_api(i) for i in items), key=lambda t: t["createdAt"] or "", reverse=True)
    logger.info("Tasks listed", extra={"status_filter": status, "count": len(tasks)})
    return json_response(200, {"items": tasks, "count": len(tasks)})


@app.get("/tasks/<task_id>")
def get_task(task_id: str):
    user_id, _ = caller()
    item = shared.table.get_item(Key=shared.task_key(user_id, task_id)).get("Item")
    if not item:
        raise NotFoundError("Task not found")
    return json_response(200, shared.to_api(item))


@app.put("/tasks/<task_id>")
def update_task(task_id: str):
    """Only Pending tasks change. The status a user may set is Completed (Expired is set by the system).

    The status check and the write are one conditional UpdateItem, so a task that
    expires at the same moment cannot be completed afterwards, and vice versa.
    """
    user_id, _ = caller()
    data = body()
    unknown = set(data) - UPDATABLE_FIELDS
    if unknown:
        raise BadRequestError(f"Unsupported field(s): {', '.join(sorted(unknown))}")
    if not data:
        raise BadRequestError("Nothing to update: send description, date and/or status")

    now = shared.iso(shared.now_utc())
    sets = ["UpdatedAt = :now"]
    names = {"#Status": "Status"}
    values = {":now": now, ":pending": shared.PENDING}

    if "description" in data:
        sets.append("Description = :description")
        values[":description"] = valid_description(data["description"])
    if "date" in data:
        sets.append("#Date = :date")
        names["#Date"] = "Date"
        values[":date"] = valid_date(data["date"])

    completing = data.get("status") == shared.COMPLETED
    if "status" in data and data["status"] not in (shared.PENDING, shared.COMPLETED):
        raise BadRequestError("status can only be set to Completed")
    if completing:
        sets += ["#Status = :completed", "CompletedAt = :now", "GSI1SK = :gsi1sk"]
        values[":completed"] = shared.COMPLETED
        values[":gsi1sk"] = shared.status_sort_key(shared.COMPLETED, now)

    try:
        result = shared.table.update_item(
            Key=shared.task_key(user_id, task_id),
            UpdateExpression="SET " + ", ".join(sets),
            ConditionExpression="attribute_exists(PK) AND #Status = :pending",
            ExpressionAttributeNames=names,
            ExpressionAttributeValues=values,
            ReturnValues="ALL_NEW",
            ReturnValuesOnConditionCheckFailure="ALL_OLD",
        )
    except ClientError as err:
        if err.response["Error"]["Code"] != "ConditionalCheckFailedException":
            raise
        current = err.response.get("Item")
        if not current:
            raise NotFoundError("Task not found")
        raise ServiceError(409, f"Task is {current['Status']['S']}; only Pending tasks can be changed")

    if completing:
        metrics.add_metric(name="TasksCompleted", unit=MetricUnit.Count, value=1)
    logger.info("Task updated", extra={"task_id": task_id, "fields": sorted(data), "completed": completing})
    return json_response(200, shared.to_api(result["Attributes"]))


@app.delete("/tasks/<task_id>")
def delete_task(task_id: str):
    user_id, _ = caller()
    try:
        shared.table.delete_item(
            Key=shared.task_key(user_id, task_id),
            ConditionExpression="attribute_exists(PK)",
        )
    except ClientError as err:
        if err.response["Error"]["Code"] == "ConditionalCheckFailedException":
            raise NotFoundError("Task not found")
        raise

    metrics.add_metric(name="TasksDeleted", unit=MetricUnit.Count, value=1)
    logger.info("Task deleted", extra={"task_id": task_id})
    return json_response(204)


# ---------------------------------------------------------------------------
# Lambda entry point
# ---------------------------------------------------------------------------
@logger.inject_lambda_context(correlation_id_path=correlation_paths.API_GATEWAY_REST)
@metrics.log_metrics
def handler(event, context):
    return app.resolve(event, context)
