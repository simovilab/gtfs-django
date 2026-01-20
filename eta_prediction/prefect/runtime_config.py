"""
Runtime configuration helpers and Prefect Blocks for the ETA runtime flow.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from pydantic import Field
from prefect.blocks.core import Block
from prefect.blocks.system import Secret


@dataclass
class RedisPipelineConfig:
    """Settings for the Prefect polling loop."""

    host: str = "localhost"
    port: int = 6379
    db: int = 0
    password: Optional[str] = None
    vehicle_key_pattern: str = "vehicle:*"
    route_stops_key_prefix: str = "route_stops:"
    route_shape_key_prefix: str = "route_shape:"
    predictions_key_prefix: str = "predictions:"
    predictions_ttl_seconds: int = 300
    poll_interval_seconds: float = 1.0
    max_vehicle_batch: Optional[int] = None
    max_stops_per_vehicle: int = 5
    model_key: Optional[str] = None
    zero_prediction_alert_threshold: int = 0
    notification_blocks: List[str] = field(default_factory=list)
    profiling_artifact_key_prefix: str = "eta-runtime"
    model_registry_dir: Optional[str] = None

    def redis_kwargs(self) -> Dict[str, Any]:
        return {
            "host": self.host,
            "port": self.port,
            "db": self.db,
            "password": self.password,
            "decode_responses": True,
        }


class EtaRuntimeSettings(Block):
    """
    Prefect Block that stores the defaults for the ETA runtime deployment.
    """

    _block_type_name = "ETA Runtime Settings"
    _block_type_slug = "eta-runtime-settings"

    redis_host: str = Field(default="localhost", description="Redis hostname/IP.")
    redis_port: int = Field(default=6379, description="Redis port.")
    redis_db: int = Field(default=0, description="Redis database index.")
    redis_password_secret: Optional[str] = Field(
        default=None,
        description="Prefect Secret block name that stores the Redis password.",
    )
    poll_interval_seconds: float = Field(
        default=1.0, description="Seconds to sleep between polling iterations."
    )
    model_key: Optional[str] = Field(
        default=None, description="Model registry key to load for inference."
    )
    predictions_ttl_seconds: int = Field(
        default=300, description="TTL (seconds) applied to `predictions:*` keys."
    )
    max_vehicle_batch: Optional[int] = Field(
        default=None, description="Optional cap on the number of vehicles processed per poll."
    )
    max_stops_per_vehicle: int = Field(
        default=5, description="How many stops are sent to the estimator per vehicle."
    )
    zero_prediction_alert_threshold: int = Field(
        default=0,
        description=(
            "Number of consecutive polling loops that yield zero predictions before "
            "triggering a notification. Set to 0 to disable alerts."
        ),
    )
    notification_blocks: List[str] = Field(
        default_factory=list,
        description="Prefect notification block names (Slack/Webhook/etc.) to notify on failures.",
    )
    profiling_artifact_key_prefix: str = Field(
        default="eta-runtime",
        description="Artifact key prefix used when uploading profiling CSVs.",
    )
    model_registry_dir: Optional[str] = Field(
        default=None,
        description="Optional override for the MODEL_REGISTRY_DIR environment variable.",
    )

    def build_pipeline_config(
        self, overrides: Optional[Dict[str, Any]] = None
    ) -> RedisPipelineConfig:
        """
        Merge block defaults + overrides into a RedisPipelineConfig.
        """

        overrides = overrides or {}
        password = overrides.get("password")
        if not password and self.redis_password_secret:
            secret = Secret.load(self.redis_password_secret)
            password = secret.get()

        notification_blocks = list(self.notification_blocks)
        override_notifications = overrides.get("notification_blocks")
        if override_notifications:
            notification_blocks = override_notifications

        config = RedisPipelineConfig(
            host=overrides.get("host", self.redis_host),
            port=overrides.get("port", self.redis_port),
            db=overrides.get("db", self.redis_db),
            password=password,
            poll_interval_seconds=overrides.get(
                "poll_interval_seconds", self.poll_interval_seconds
            ),
            max_vehicle_batch=overrides.get("max_vehicle_batch", self.max_vehicle_batch),
            max_stops_per_vehicle=overrides.get(
                "max_stops_per_vehicle", self.max_stops_per_vehicle
            ),
            model_key=overrides.get("model_key", self.model_key),
            predictions_ttl_seconds=overrides.get(
                "predictions_ttl_seconds", self.predictions_ttl_seconds
            ),
            zero_prediction_alert_threshold=overrides.get(
                "zero_prediction_alert_threshold", self.zero_prediction_alert_threshold
            ),
            notification_blocks=notification_blocks,
            profiling_artifact_key_prefix=overrides.get(
                "profiling_artifact_key_prefix", self.profiling_artifact_key_prefix
            ),
            model_registry_dir=overrides.get("model_registry_dir", self.model_registry_dir),
        )
        return config


def build_runtime_config(
    *,
    runtime_settings_block: Optional[str] = None,
    overrides: Optional[Dict[str, Any]] = None,
) -> RedisPipelineConfig:
    """
    Resolve the runtime config either from a Prefect Block or from direct overrides.
    """

    overrides = overrides or {}
    if runtime_settings_block:
        block = EtaRuntimeSettings.load(runtime_settings_block)
        return block.build_pipeline_config(overrides)

    config = RedisPipelineConfig(
        host=overrides.get("host", RedisPipelineConfig.host),
        port=overrides.get("port", RedisPipelineConfig.port),
        db=overrides.get("db", RedisPipelineConfig.db),
        password=overrides.get("password"),
        poll_interval_seconds=overrides.get(
            "poll_interval_seconds", RedisPipelineConfig.poll_interval_seconds
        ),
        max_vehicle_batch=overrides.get("max_vehicle_batch"),
        max_stops_per_vehicle=overrides.get(
            "max_stops_per_vehicle", RedisPipelineConfig.max_stops_per_vehicle
        ),
        model_key=overrides.get("model_key"),
        predictions_ttl_seconds=overrides.get(
            "predictions_ttl_seconds", RedisPipelineConfig.predictions_ttl_seconds
        ),
        zero_prediction_alert_threshold=overrides.get(
            "zero_prediction_alert_threshold", 0
        ),
        notification_blocks=overrides.get("notification_blocks", []),
        profiling_artifact_key_prefix=overrides.get(
            "profiling_artifact_key_prefix", "eta-runtime"
        ),
        model_registry_dir=overrides.get("model_registry_dir"),
    )
    return config
