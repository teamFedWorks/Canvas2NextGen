import boto3
import logging
import os
import datetime
from typing import List, Dict, Any

logger = logging.getLogger(__name__)


class CloudWatchMetrics:
    """
    Utility to publish custom metrics to AWS CloudWatch.
    Emits metrics if CLOUDWATCH_METRICS_ENABLED is set to 'true'.
    """
    def __init__(self, namespace: str = "EduvateHub/CourseOnboarding"):
        self.namespace = namespace
        self._enabled = os.getenv("CLOUDWATCH_METRICS_ENABLED", "false").lower() == "true"
        self._region = os.getenv("AWS_REGION", "us-east-2")
        self._client = None

    def _ensure_client(self):
        if self._enabled and not self._client:
            try:
                self._client = boto3.client("cloudwatch", region_name=self._region)
            except Exception as e:
                logger.warning(f"Failed to initialize CloudWatch client: {e}")
                self._enabled = False

    def emit_metric(self, name: str, value: float = 1.0, unit: str = "Count", dimensions: List[Dict[str, str]] = None) -> None:
        """
        Emits a single custom metric to CloudWatch.
        Dimensions can be provided as a list of dicts: [{'Name': 'DimName', 'Value': 'DimVal'}]
        """
        logger.debug(f"[Metric] {name}={value} ({unit})")
        
        # Always log to standard logger as well
        logger.info(f"Custom metric emitted: {name}={value} Unit: {unit} Dimensions: {dimensions}")
        
        self._ensure_client()
        if not self._enabled or not self._client:
            return

        dims = dimensions or []
        env = os.getenv("ENVIRONMENT") or os.getenv("NODE_ENV") or "staging"
        if not any(d["Name"] == "Environment" for d in dims):
            dims.append({"Name": "Environment", "Value": env})

        try:
            self._client.put_metric_data(
                Namespace=self.namespace,
                MetricData=[
                    {
                        "MetricName": name,
                        "Value": value,
                        "Unit": unit,
                        "Dimensions": dims,
                        "Timestamp": datetime.datetime.utcnow()
                    }
                ]
            )
        except Exception as e:
            logger.warning(f"Failed to send custom CloudWatch metric '{name}' to AWS: {e}")


# Singleton instance
metrics = CloudWatchMetrics()
