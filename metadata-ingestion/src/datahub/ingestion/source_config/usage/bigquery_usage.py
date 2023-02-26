import json
import logging
import os
import tempfile
from datetime import timedelta
from typing import Any, List, Optional

import pydantic

from datahub.configuration.common import AllowDenyPattern, ConfigurationError
from datahub.ingestion.source.gcp.gcp_common import GCPSourceConfig
from datahub.ingestion.source.usage.usage_common import BaseUsageConfig
from datahub.ingestion.source_config.bigquery import BigQueryBaseConfig

logger = logging.getLogger(__name__)


class BigQueryUsageConfig(BigQueryBaseConfig, GCPSourceConfig, BaseUsageConfig):
    projects: Optional[List[str]] = pydantic.Field(
        default=None,
        description="List of project ids to ingest usage from. If not specified, will infer from environment.",
    )
    project_id: Optional[str] = pydantic.Field(
        default=None,
        description="Project ID to ingest usage from. If not specified, will infer from environment. Deprecated in favour of projects ",
    )
    extra_client_options: dict = pydantic.Field(
        default_factory=dict,
        description="Additional options to pass to google.cloud.logging_v2.client.Client.",
    )
    use_v2_audit_metadata: Optional[bool] = pydantic.Field(
        default=False,
        description="Whether to ingest logs using the v2 format. Required if use_exported_bigquery_audit_metadata is set to True.",
    )

    bigquery_audit_metadata_datasets: Optional[List[str]] = pydantic.Field(
        description="A list of datasets that contain a table named cloudaudit_googleapis_com_data_access which contain BigQuery audit logs, specifically, those containing BigQueryAuditMetadata. It is recommended that the project of the dataset is also specified, for example, projectA.datasetB.",
    )
    use_exported_bigquery_audit_metadata: bool = pydantic.Field(
        default=False,
        description="When configured, use BigQueryAuditMetadata in bigquery_audit_metadata_datasets to compute usage information.",
    )

    use_date_sharded_audit_log_tables: bool = pydantic.Field(
        default=False,
        description="Whether to read date sharded tables or time partitioned tables when extracting usage from exported audit logs.",
    )

    table_pattern: AllowDenyPattern = pydantic.Field(
        default=AllowDenyPattern.allow_all(),
        description="List of regex patterns for tables to include/exclude from ingestion.",
    )
    dataset_pattern: AllowDenyPattern = pydantic.Field(
        default=AllowDenyPattern.allow_all(),
        description="List of regex patterns for datasets to include/exclude from ingestion.",
    )
    log_page_size: pydantic.PositiveInt = pydantic.Field(
        default=1000,
        description="",
    )

    query_log_delay: Optional[pydantic.PositiveInt] = pydantic.Field(
        default=None,
        description="To account for the possibility that the query event arrives after the read event in the audit logs, we wait for at least query_log_delay additional events to be processed before attempting to resolve BigQuery job information from the logs. If query_log_delay is None, it gets treated as an unlimited delay, which prioritizes correctness at the expense of memory usage.",
    )

    max_query_duration: timedelta = pydantic.Field(
        default=timedelta(minutes=15),
        description="Correction to pad start_time and end_time with. For handling the case where the read happens within our time range but the query completion event is delayed and happens after the configured end time.",
    )

    temp_table_dataset_prefix: str = pydantic.Field(
        default="_",
        description="If you are creating temp tables in a dataset with a particular prefix you can use this config to set the prefix for the dataset. This is to support workflows from before bigquery's introduction of temp tables. By default we use `_` because of datasets that begin with an underscore are hidden by default https://cloud.google.com/bigquery/docs/datasets#dataset-naming.",
    )

    def __init__(self, **data: Any):
        super().__init__(**data)

    @pydantic.validator("project_id")
    def note_project_id_deprecation(cls, v, values, **kwargs):
        logger.warning(
            "bigquery-usage project_id option is deprecated; use projects instead"
        )
        values["projects"] = [v]
        return None

    @pydantic.validator("platform")
    def platform_is_always_bigquery(cls, v):
        return "bigquery"

    @pydantic.validator("platform_instance")
    def bigquery_platform_instance_is_meaningless(cls, v):
        raise ConfigurationError(
            "BigQuery project-ids are globally unique. You don't need to provide a platform_instance"
        )

    @pydantic.validator("use_exported_bigquery_audit_metadata")
    def use_exported_bigquery_audit_metadata_uses_v2(cls, v, values):
        if v is True and not values["use_v2_audit_metadata"]:
            raise ConfigurationError(
                "To use exported BigQuery audit metadata, you must also use v2 audit metadata"
            )
        return v

    def get_allow_pattern_string(self) -> str:
        return "|".join(self.table_pattern.allow) if self.table_pattern else ""

    def get_deny_pattern_string(self) -> str:
        return "|".join(self.table_pattern.deny) if self.table_pattern else ""
