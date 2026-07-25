"""Resolves the Wistia API token without ever hardcoding it in source control.

Order of precedence:
1. WISTIA_API_TOKEN environment variable (local/dev convenience only).
2. AWS Secrets Manager secret (production - the Glue job's IAM role must
   grant secretsmanager:GetSecretValue on the given secret).
"""
import json
import logging
import os

import boto3

logger = logging.getLogger(__name__)

ENV_TOKEN_VAR = "WISTIA_API_TOKEN"
DEFAULT_SECRET_KEY = "api_token"


def get_wistia_api_token(secret_name=None, secret_key=DEFAULT_SECRET_KEY, region_name=None, secrets_client=None):
    env_token = os.environ.get(ENV_TOKEN_VAR)
    if env_token:
        logger.info("Using Wistia API token from %s environment variable", ENV_TOKEN_VAR)
        return env_token

    if not secret_name:
        raise RuntimeError(
            f"No Wistia API token available: set {ENV_TOKEN_VAR} or pass --secret-name "
            "pointing at an AWS Secrets Manager secret"
        )

    client = secrets_client or boto3.client("secretsmanager", region_name=region_name)
    response = client.get_secret_value(SecretId=secret_name)
    secret_string = response["SecretString"]
    try:
        return json.loads(secret_string)[secret_key]
    except (json.JSONDecodeError, KeyError):
        # Secret was stored as a plain string rather than a {"api_token": ...} blob.
        return secret_string
