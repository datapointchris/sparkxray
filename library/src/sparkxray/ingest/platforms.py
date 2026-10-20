"""Which platform ran an application, read from its app id and its `SparkListenerEnvironmentUpdate`.

Each rule names a mark only that platform leaves. Anything no rule recognizes is `other`, never a
guess.

| Platform | Mark |
| --- | --- |
| glue | an app id `spark-application-<ms>`, or Glue's own jars under `/usr/share/aws/` on the classpath |
| databricks | a `spark.databricks.clusterUsageTags.*` property |
| emr-serverless | a `spark.emr-serverless.*` property |
| emr-ec2 | a YARN app id, with an `-amzn-` Spark build or EMR's jars under `/usr/share/aws/emr/` |
| local | an app id `local-<ms>`, or a `spark.master` of `local` |

Glue is checked first, because its classpath also holds EMR's jars and its image runs as local.
`spark.databricks.` alone is no mark: open-source Delta Lake sets properties under it.
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from collections.abc import Mapping
from typing import Literal

Platform = Literal['glue', 'emr-serverless', 'emr-ec2', 'databricks', 'local', 'other']

_GLUE_APP_ID = re.compile(r'spark-application-\d+')
_GLUE_JARS = ('/usr/share/aws/glue-pds/', '/usr/share/aws/emr-glue-bootstrap/', '/usr/share/aws/aws-glue-shuffle/')
_EMR_JARS = '/usr/share/aws/emr/'
_YARN_APP_ID = re.compile(r'application_\d+_\d+')
_LOCAL_APP_ID = re.compile(r'local-\d+')


def infer_platform(app_id: str, spark_version: str | None, properties: Mapping[str, str], classpath: Iterable[str]) -> Platform:
    """The platform that ran an application. `classpath` is the keys of its `Classpath Entries`."""
    entries = tuple(classpath)
    if _GLUE_APP_ID.fullmatch(app_id) or any(entry.startswith(_GLUE_JARS) for entry in entries):
        return 'glue'
    if any(key.startswith('spark.databricks.clusterUsageTags.') for key in properties):
        return 'databricks'
    if any(key.startswith('spark.emr-serverless.') for key in properties):
        return 'emr-serverless'
    if _YARN_APP_ID.match(app_id) and ('-amzn-' in (spark_version or '') or any(entry.startswith(_EMR_JARS) for entry in entries)):
        return 'emr-ec2'
    if _LOCAL_APP_ID.fullmatch(app_id) or properties.get('spark.master', '').startswith('local'):
        return 'local'
    return 'other'
