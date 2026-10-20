"""Every AWS call sparkxray makes. Nothing else imports boto3.

Credentials come from boto3's default chain, narrowed by a profile and a region when given. An
S3-compatible store is reached through boto3's own `AWS_ENDPOINT_URL_S3`, and a stand-in for
CloudWatch or CloudWatch Logs through `AWS_ENDPOINT_URL_CLOUDWATCH` or
`AWS_ENDPOINT_URL_CLOUDWATCH_LOGS`. Nothing here creates, changes or deletes an AWS resource.
"""

from __future__ import annotations

import contextlib
import datetime as dt
import os
from collections.abc import Iterator
from collections.abc import Mapping
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import boto3
import botocore.exceptions
from boto3.s3.transfer import TransferConfig

from sparkxray.cache_directory import PARTIAL_MARK
from sparkxray.errors import AwsDeniedError
from sparkxray.errors import EnrichmentDeniedError
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
# Codes CloudWatch and CloudWatch Logs return beside S3's when the caller may not read, or cannot prove who it is.
_READ_DENIED = _DENIED | {
    'AccessDeniedException',
    'ExpiredTokenException',
    'IncompleteSignature',
    'InvalidClientTokenId',
    'InvalidSignatureException',
    'MissingAuthenticationToken',
    'UnrecognizedClientException',
}
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


class CloudWatch:
    """One CloudWatch client for one enrichment, which lists and reads metrics."""

    def __init__(self, *, profile: str | None = None, region: str | None = None) -> None:
        self._client = _read_client('cloudwatch', profile, region)

    def list_metrics(self, namespace: str, dimensions: Mapping[str, str]) -> Iterator[dict[str, Any]]:
        """Every metric in `namespace` that carries each of `dimensions` among its own, as ListMetrics describes it."""
        filters = [{'Name': name, 'Value': value} for name, value in dimensions.items()]
        pages = self._client.get_paginator('list_metrics').paginate(Namespace=namespace, Dimensions=filters)
        shown = ', '.join(f'{name}={value}' for name, value in dimensions.items())
        with _read_translated('cloudwatch:ListMetrics', f'{namespace} metrics with {shown}'):
            for page in pages:
                yield from page.get('Metrics', [])

    def metric_data(
        self, queries: Sequence[Mapping[str, Any]], start: dt.datetime, end: dt.datetime, resource: str
    ) -> Iterator[dict[str, Any]]:
        """Each query's results over `[start, end)`, oldest first, as GetMetricData describes them. One query's results may span pages."""
        pages = self._client.get_paginator('get_metric_data').paginate(
            MetricDataQueries=list(queries), StartTime=start, EndTime=end, ScanBy='TimestampAscending'
        )
        with _read_translated('cloudwatch:GetMetricData', resource):
            for page in pages:
                yield from page.get('MetricDataResults', [])


class CloudWatchLogs:
    """One CloudWatch Logs client for one enrichment, which reads log streams."""

    def __init__(self, *, profile: str | None = None, region: str | None = None) -> None:
        self._client = _read_client('logs', profile, region)

    def log_events(self, group: str, stream: str) -> list[dict[str, Any]] | None:
        """Every event of one stream, oldest first, as GetLogEvents describes them. None where the group or stream does not exist."""
        events: list[dict[str, Any]] = []
        token = None
        with _read_translated('logs:GetLogEvents', f'{group}:{stream}'):
            while True:
                page_from: dict[str, str] = {'nextToken': token} if token else {}
                try:
                    page = self._client.get_log_events(logGroupName=group, logStreamName=stream, startFromHead=True, **page_from)
                except botocore.exceptions.ClientError as error:
                    if error.response.get('Error', {}).get('Code') == 'ResourceNotFoundException':
                        return None
                    raise
                events.extend(page.get('events', []))
                # The last page hands back the token it was asked with.
                if page.get('nextForwardToken') in (None, token):
                    return events
                token = page['nextForwardToken']


def _read_client(service: str, profile: str | None, region: str | None) -> Any:
    try:
        return boto3.Session(profile_name=profile, region_name=region).client(service)
    except botocore.exceptions.ProfileNotFound as error:
        raise EnrichmentDeniedError(str(error), field='profile', hint='aws configure list-profiles') from error
    except botocore.exceptions.NoRegionError as error:
        raise EnrichmentDeniedError(
            f'{service} needs a region, and none was found', field='region', hint='set AWS_DEFAULT_REGION to the region the job ran in'
        ) from error


@contextlib.contextmanager
def _read_translated(action: str, resource: str) -> Iterator[None]:
    """Turn an AWS denial of a CloudWatch or CloudWatch Logs read into EnrichmentDeniedError."""
    try:
        yield
    except (botocore.exceptions.NoCredentialsError, botocore.exceptions.PartialCredentialsError) as error:
        raise EnrichmentDeniedError(
            f'{action} on {resource} needs AWS credentials, and none were found: {error}', hint='aws sts get-caller-identity'
        ) from error
    except botocore.exceptions.ClientError as error:
        code = str(error.response.get('Error', {}).get('Code', ''))
        if code in _READ_DENIED:
            raise EnrichmentDeniedError(f'AWS denied {action} on {resource}: {code}', hint='aws sts get-caller-identity') from error
        raise


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
