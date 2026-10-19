"""Every AWS call sparkxray makes. Nothing else imports boto3.

Credentials come from boto3's default chain, narrowed by a profile and a region when given. An
S3-compatible store is reached through boto3's own `AWS_ENDPOINT_URL_S3`. Nothing here creates,
changes or deletes an AWS resource.
"""

from __future__ import annotations

import contextlib
import os
from collections.abc import Iterator
from pathlib import Path

import boto3
import botocore.exceptions
from boto3.s3.transfer import TransferConfig

from sparkxray.cache_directory import PARTIAL_MARK
from sparkxray.errors import AwsDeniedError
from sparkxray.errors import NoEventLogError

# Codes S3 and STS return when the caller may not make the call, or cannot prove who it is.
_DENIED = frozenset(
    {
        '403',
        'AccessDenied',
        'AllAccessDisabled',
        'AccountProblem',
        'ExpiredToken',
        'InvalidAccessKeyId',
        'InvalidToken',
        'SignatureDoesNotMatch',
    }
)
_MISSING = frozenset({'404', 'NoSuchBucket', 'NoSuchKey'})
_TRANSFER = TransferConfig(multipart_threshold=64 * 2**20, multipart_chunksize=64 * 2**20, max_concurrency=8)


class S3:
    """One S3 client for one ingest."""

    def __init__(self, *, profile: str | None = None, region: str | None = None) -> None:
        try:
            self._client = boto3.Session(profile_name=profile, region_name=region).client('s3')
        except botocore.exceptions.ProfileNotFound as error:
            raise AwsDeniedError(str(error), field='profile', hint='aws configure list-profiles') from error

    def list_objects(self, bucket: str, prefix: str) -> Iterator[dict[str, object]]:
        """Every object under `prefix`, as `list_objects_v2` describes it."""
        pages = self._client.get_paginator('list_objects_v2').paginate(Bucket=bucket, Prefix=prefix)
        with _translated('s3:ListBucket', f's3://{bucket}/{prefix}'):
            for page in pages:
                yield from page.get('Contents', [])

    def leading_bytes(self, bucket: str, key: str, size: int) -> bytes:
        """The first `size` bytes of an object, or all of it when shorter."""
        with _translated('s3:GetObject', f's3://{bucket}/{key}'):
            response = self._client.get_object(Bucket=bucket, Key=key, Range=f'bytes=0-{size - 1}')
            body: bytes = response['Body'].read()
            return body

    def download(self, bucket: str, key: str, destination: Path) -> None:
        """Copy an object to `destination`, whole or not at all."""
        destination.parent.mkdir(parents=True, exist_ok=True)
        partial = destination.with_name(f'{destination.name}{PARTIAL_MARK}{os.getpid()}')
        try:
            with _translated('s3:GetObject', f's3://{bucket}/{key}'):
                self._client.download_file(bucket, key, str(partial), Config=_TRANSFER)
            partial.replace(destination)
        finally:
            partial.unlink(missing_ok=True)


@contextlib.contextmanager
def _translated(action: str, uri: str) -> Iterator[None]:
    """Turn an AWS denial into AwsDeniedError, and a missing bucket or key into NoEventLogError."""
    try:
        yield
    except (botocore.exceptions.NoCredentialsError, botocore.exceptions.PartialCredentialsError) as error:
        raise AwsDeniedError(
            f'{action} on {uri} needs AWS credentials, and none were found: {error}', hint='aws sts get-caller-identity'
        ) from error
    except botocore.exceptions.ClientError as error:
        code = str(error.response.get('Error', {}).get('Code', ''))
        if code in _DENIED:
            raise AwsDeniedError(f'AWS denied {action} on {uri}: {code}', hint='aws sts get-caller-identity') from error
        if code in _MISSING:
            raise NoEventLogError(f'{uri} does not exist: {code}', field='location') from error
        raise
