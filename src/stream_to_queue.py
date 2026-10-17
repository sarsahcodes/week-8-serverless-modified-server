"""StreamToQueueFunction - DynamoDB Stream consumer for the cancellation workflow.

The event source mapping in template.yaml filters the stream, so this function
only receives two kinds of record, both for TASK# items:

  * MODIFY  old status Pending, new status Completed   (completed before the deadline)
  * REMOVE  old status Pending                         (deleted before the deadline)

Each record becomes one message on the SQS FIFO cancellation queue:

  MessageGroupId          = taskId    messages for one task are processed in order
  MessageDeduplicationId  = eventID   a redelivered stream record is dropped by SQS

Only failed records are retried (ReportBatchItemFailures).
"""
import json
import os

import boto3
from aws_lambda_powertools import Logger, Metrics
from aws_lambda_powertools.metrics import MetricUnit
from boto3.dynamodb.types import TypeDeserializer

import shared

logger = Logger()
metrics = Metrics()
sqs = boto3.client("sqs")
deserializer = TypeDeserializer()

QUEUE_URL = os.environ.get("QUEUE_URL", "")
SCHEDULE_GROUP = os.environ.get("SCHEDULE_GROUP", "")


def build_message(record: dict) -> dict:
    """The cancellation instruction for one stream record."""
    images = record["dynamodb"]
    image = images.get("OldImage") or images.get("NewImage") or {}
    task = {k: deserializer.deserialize(v) for k, v in image.items()}
    return {
        "taskId": task["TaskId"],
        "userId": task.get("UserId"),
        "scheduleName": task.get("ScheduleName") or shared.schedule_name(task["TaskId"]),
        "scheduleGroup": task.get("ScheduleGroup") or SCHEDULE_GROUP,
        "reason": "completed" if record["eventName"] == "MODIFY" else "deleted",
        "streamEventId": record["eventID"],
    }


@logger.inject_lambda_context
@metrics.log_metrics
def handler(event, _context):
    failures = []
    for record in event.get("Records", []):
        try:
            message = build_message(record)
            sqs.send_message(
                QueueUrl=QUEUE_URL,
                MessageBody=json.dumps(message),
                MessageGroupId=message["taskId"],
                MessageDeduplicationId=record["eventID"][:128],
            )
            metrics.add_metric(name="CancellationsQueued", unit=MetricUnit.Count, value=1)
            logger.info("Cancellation queued", extra={"task_id": message["taskId"], "reason": message["reason"]})
        except Exception:
            logger.exception("Could not queue cancellation", extra={"event_id": record.get("eventID")})
            failures.append({"itemIdentifier": record["dynamodb"]["SequenceNumber"]})
    return {"batchItemFailures": failures}
