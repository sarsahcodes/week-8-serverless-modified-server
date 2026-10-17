"""CancelExpiryFunction - SQS FIFO consumer: deletes the expiry schedule of a task
that was completed or deleted before its deadline.

Idempotent: if the schedule is already gone (deleted earlier, or it fired and
removed itself) Scheduler returns ResourceNotFoundException, which counts as
success - the goal "no schedule for this task" is met.

Failures are reported per message. On a FIFO queue processing stops at the
first failure and that message plus every later one in the batch are returned,
so order is kept. After 3 failed receives SQS moves a message to the DLQ.
"""
import json

import boto3
from aws_lambda_powertools import Logger, Metrics
from aws_lambda_powertools.metrics import MetricUnit
from botocore.exceptions import ClientError

logger = Logger()
metrics = Metrics()
scheduler = boto3.client("scheduler")


def cancel(message: dict) -> str:
    """Delete the schedule. Returns 'deleted' or 'already-gone'."""
    try:
        scheduler.delete_schedule(Name=message["scheduleName"], GroupName=message["scheduleGroup"])
        return "deleted"
    except ClientError as err:
        if err.response["Error"]["Code"] == "ResourceNotFoundException":
            return "already-gone"
        raise


@logger.inject_lambda_context
@metrics.log_metrics
def handler(event, _context):
    records = event.get("Records", [])
    for index, record in enumerate(records):
        try:
            message = json.loads(record["body"])
            outcome = cancel(message)
        except Exception:
            logger.exception("Cancellation failed; message will be retried", extra={"message_id": record["messageId"]})
            metrics.add_metric(name="CancellationFailures", unit=MetricUnit.Count, value=1)
            return {"batchItemFailures": [{"itemIdentifier": r["messageId"]} for r in records[index:]]}

        metrics.add_metric(
            name="ExpiriesCancelled" if outcome == "deleted" else "CancellationsAlreadyDone",
            unit=MetricUnit.Count,
            value=1,
        )
        logger.info(
            "Expiry schedule cancelled",
            extra={"task_id": message.get("taskId"), "reason": message.get("reason"), "outcome": outcome},
        )
    return {"batchItemFailures": []}
