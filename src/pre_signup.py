"""PreSignUpFunction - Cognito PreSignUp trigger.

Auto-confirms every new user and marks the email as verified, so users can
sign in straight after sign-up without entering a verification code.
"""
from aws_lambda_powertools import Logger

logger = Logger()


@logger.inject_lambda_context
def handler(event, _context):
    attributes = event.get("request", {}).get("userAttributes", {})
    response = event.setdefault("response", {})
    response["autoConfirmUser"] = True
    if "email" in attributes:
        response["autoVerifyEmail"] = True

    logger.info("New user auto-confirmed", extra={"trigger_source": event.get("triggerSource")})
    return event
