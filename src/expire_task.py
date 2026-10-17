"""ExpireTaskFunction - EventBridge Scheduler target, runs at a task's deadline.

Input (set on the schedule by TasksApiFunction): {"userId": "<sub>", "taskId": "<uuid>"}

1. Conditional UpdateItem Pending -> Expired. A task that was Completed or
   Deleted before the deadline is left alone (and nobody is emailed), even if
   the cancellation workflow has not removed the schedule yet.
2. Publish to the SNS topic with message attribute ``userId``. Every user's
   email subscription has a filter policy on userId, so only the owner gets it.
3. Record NotifiedAt on the task.

Idempotent: Scheduler and Lambda may retry. If the status update worked but
the publish failed, the retry finds the task Expired without NotifiedAt and
sends the email then; once NotifiedAt is set, a retry does nothing.
"""
import os

import boto3
from aws_lambda_powertools import Logger, Metrics
from aws_lambda_powertools.metrics import MetricUnit
from boto3.dynamodb.types import TypeDeserializer
from botocore.exceptions import ClientError

import shared

logger = Logger()
metrics = Metrics()
sns = boto3.client("sns")
deserializer = TypeDeserializer()

TOPIC_ARN = os.environ.get("TOPIC_ARN", "")


def mark_expired(user_id: str, task_id: str) -> tuple[dict | None, bool]:
    """Returns (task item, newly_expired). The item is None if the task no longer exists."""
    now = shared.iso(shared.now_utc())
    try:
        result = shared.table.update_item(
            Key=shared.task_key(user_id, task_id),
            UpdateExpression="SET #Status = :expired, ExpiredAt = :now, UpdatedAt = :now, GSI1SK = :gsi1sk",
            ConditionExpression="attribute_exists(PK) AND #Status = :pending",
            ExpressionAttributeNames={"#Status": "Status"},
            ExpressionAttributeValues={
                ":expired": shared.EXPIRED,
                ":pending": shared.PENDING,
                ":now": now,
                ":gsi1sk": shared.status_sort_key(shared.EXPIRED, now),
            },
            ReturnValues="ALL_NEW",
            ReturnValuesOnConditionCheckFailure="ALL_OLD",
        )
        return result["Attributes"], True
    except ClientError as err:
        if err.response["Error"]["Code"] != "ConditionalCheckFailedException":
            raise
        old = err.response.get("Item")
        if not old:
            return None, False
        return {k: deserializer.deserialize(v) for k, v in old.items()}, False


def notify_owner(item: dict) -> None:
    description = item["Description"]
    subject = f"Task expired: {description}"
    if len(subject) > 100:  # SNS subject limit
        subject = subject[:97] + "..."
    message = (
        "Your task has expired without being completed.\n\n"
        f"Task:      {description}\n"
        f"Due date:  {item.get('Date', '-')}\n"
        f"Deadline:  {item.get('DeadlineISO', '-')} (UTC)\n"
        f"Task ID:   {item['TaskId']}\n\n"
        "Open the To-Do app to review your expired tasks."
    )
    sns.publish(
        TopicArn=TOPIC_ARN,
        Subject=subject,
        Message=message,
        MessageAttributes={"userId": {"DataType": "String", "StringValue": item["UserId"]}},
    )
    shared.table.update_item(
        Key=shared.task_key(item["UserId"], item["TaskId"]),
        UpdateExpression="SET NotifiedAt = :now",
        ConditionExpression="attribute_exists(PK)",
        ExpressionAttributeValues={":now": shared.iso(shared.now_utc())},
    )


@logger.inject_lambda_context
@metrics.log_metrics
def handler(event, _context):
    user_id, task_id = event["userId"], event["taskId"]
    logger.append_keys(user_id=user_id, task_id=task_id)

    item, newly_expired = mark_expired(user_id, task_id)

    if item is None:
        logger.info("Task no longer exists (deleted before its deadline); nothing to do")
        return {"result": "skipped", "reason": "deleted"}
    if not newly_expired and item.get("Status") != shared.EXPIRED:
        logger.info("Task is not Pending; expiry skipped", extra={"status": item.get("Status")})
        return {"result": "skipped", "reason": item.get("Status")}
    if newly_expired:
        metrics.add_metric(name="TasksExpired", unit=MetricUnit.Count, value=1)
        logger.info("Task marked Expired")
    if item.get("NotifiedAt"):
        logger.info("Owner already notified; nothing to do")
        return {"result": "already-notified"}

    try:
        notify_owner(item)
    except ClientError:
        metrics.add_metric(name="ExpiryNotificationFailures", unit=MetricUnit.Count, value=1)
        logger.exception("Expiry notification failed; raising so the invocation is retried")
        raise

    metrics.add_metric(name="ExpiryNotificationsSent", unit=MetricUnit.Count, value=1)
    logger.info("Expiry notification published")
    return {"result": "expired", "notified": True}
