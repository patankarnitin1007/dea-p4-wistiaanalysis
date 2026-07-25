import json

import boto3
import pytest
from moto import mock_aws

from wistia_pipeline.secrets import get_wistia_api_token


def test_env_var_takes_precedence(monkeypatch):
    monkeypatch.setenv("WISTIA_API_TOKEN", "token-from-env")

    assert get_wistia_api_token(secret_name="unused") == "token-from-env"


def test_raises_when_no_env_and_no_secret_name(monkeypatch):
    monkeypatch.delenv("WISTIA_API_TOKEN", raising=False)

    with pytest.raises(RuntimeError):
        get_wistia_api_token(secret_name=None)


@mock_aws
def test_reads_json_secret_from_secrets_manager(monkeypatch):
    monkeypatch.delenv("WISTIA_API_TOKEN", raising=False)
    client = boto3.client("secretsmanager", region_name="us-east-1")
    client.create_secret(Name="wistia/api-token", SecretString=json.dumps({"api_token": "token-from-secret"}))

    result = get_wistia_api_token(secret_name="wistia/api-token", region_name="us-east-1", secrets_client=client)

    assert result == "token-from-secret"


@mock_aws
def test_reads_plain_string_secret_from_secrets_manager(monkeypatch):
    monkeypatch.delenv("WISTIA_API_TOKEN", raising=False)
    client = boto3.client("secretsmanager", region_name="us-east-1")
    client.create_secret(Name="wistia/api-token", SecretString="token-from-secret")

    result = get_wistia_api_token(secret_name="wistia/api-token", region_name="us-east-1", secrets_client=client)

    assert result == "token-from-secret"
