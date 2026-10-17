"""PostAuthFunction - Cognito PostAuthentication trigger.

Subscribes the signed-in user's email address to the task notifications SNS
topic. The subscription has a filter policy on ``userId`` so each user only
receives notifications published for their own tasks.

Runs on every sign-in, so it is idempotent:
  * a USER#<sub> / PROFILE item records the subscription;
  * if that subscription is confirmed, nothing happens;
  * if it is still pending (or was removed), Subscribe is called again, which
    re-sends the confirmation email.

Errors are logged, never raised: a failing PostAuthentication trigger would
stop the user from signing in.
"""
import json
import os

import boto3
from aws_lambda_powertools import Logger, Metrics
from aws_lambda_powertools.metrics import MetricUnit
from botocore.exceptions import ClientError

import shared

logger = Logger()
metrics = Metrics()
sns = boto3.client("sns")

TOPIC_ARN = os.environ.get("TOPIC_ARN", "")


def is_confirmed(subscription_arn: str) -> bool:
    """True if the subscription exists and the user clicked the confirmation link."""
    if not subscription_arn or not subscription_arn.startswith("arn:"):
        return False  # "pending confirmation" or missing
    try:
        attributes = sns.get_subscription_attributes(SubscriptionArn=subscription_arn)["Attributes"]
        return attributes.get("PendingConfirmation") == "false"
    except ClientError as err:
        if err.response["Error"]["Code"] == "NotFound":
            return False
        raise


def subscribe(user_id: str, email: str) -> str:
    response = sns.subscribe(
        TopicArn=TOPIC_ARN,
        Protocol="email",
        Endpoint=email,
        Attributes={"FilterPolicy": json.dumps({"userId": [user_id]})},
        ReturnSubscriptionArn=True,
    )
    return response["SubscriptionArn"]


@logger.inject_lambda_context
@metrics.log_metrics
def handler(event, _context):
    attributes = event.get("request", {}).get("userAttributes", {})
    user_id, email = attributes.get("sub"), attributes.get("email")
    logger.append_keys(user_id=user_id)

    if not user_id or not email:
        logger.warning("PostAuthentication event without sub/email; skipping subscription")
        return event

    try:
        profile = shared.table.get_item(Key=shared.profile_key(user_id)).get("Item")
        if profile and profile.get("Email") == email and is_confirmed(profile.get("SubscriptionArn", "")):
            logger.info("User already subscribed and confirmed")
            return event

        now = shared.iso(shared.now_utc())
        shared.table.put_item(
            Item={
                **shared.profile_key(user_id),
                "EntityType": "PROFILE",
                "UserId": user_id,
                "Email": email,
                "SubscriptionArn": subscribe(user_id, email),
                "SubscribedAt": (profile or {}).get("SubscribedAt", now),
                "UpdatedAt": now,
            }
        )
        metrics.add_metric(name="NotificationSubscriptionsRequested", unit=MetricUnit.Count, value=1)
        logger.info("Subscription requested; confirmation email sent")
    except Exception:  # never block sign-in
        logger.exception("Failed to subscribe user to task notifications")

    return event
