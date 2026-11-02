"""SparkSession utility module."""

import os
import sys
from typing import Any, Dict, Optional
import yaml
from pyspark.sql import SparkSession
from src.common.logger import get_logger

logger = get_logger("spark_session")


def load_config(config_path: str = "configs/config.example.yaml") -> Dict[str, Any]:
    """Load configuration from a YAML file.

    Parameters
    ----------
    config_path : str
        Path to the YAML configuration file.

    Returns
    -------
    Dict[str, Any]
        Parsed configuration dictionary.
    """
    if os.path.exists(config_path):
        with open(config_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    logger.warning("Config file '%s' not found, using empty config.", config_path)
    return {}


def get_spark(
    app_name: str = "EcommerceMLPipeline",
    cfg: Optional[Dict[str, Any]] = None,
) -> SparkSession:
    """Initialize and return a SparkSession with capstone standard configuration.

    Parameters
    ----------
    app_name : str
        Name of the Spark application.
    cfg : Optional[Dict[str, Any]]
        Configuration dictionary. If None, default settings are applied.

    Returns
    -------
    SparkSession
        Configured SparkSession instance.
    """
    # Enforce Python executable consistency in Windows and virtual environments
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    # Ensure Windows Hadoop binaries are discovered if present
    if "HADOOP_HOME" not in os.environ and os.path.exists(r"C:\hadoop"):
        os.environ["HADOOP_HOME"] = r"C:\hadoop"
    if os.path.exists(r"C:\hadoop\bin") and r"C:\hadoop\bin" not in os.environ.get("PATH", ""):
        os.environ["PATH"] = r"C:\hadoop\bin;" + os.environ.get("PATH", "")

    spark_cfg = (cfg or {}).get("spark", {})
    # Enforce local[2] and 2g as specified for ML environment
    master = (cfg or {}).get("ml", {}).get("spark_master", "local[2]")
    driver_memory = (cfg or {}).get("ml", {}).get("driver_memory", "2g")
    shuffle_partitions = str((cfg or {}).get("ml", {}).get("shuffle_partitions", spark_cfg.get("shuffle_partitions", 8)))
    log_level = spark_cfg.get("log_level", "WARN")

    builder = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.driver.memory", driver_memory)
        .config("spark.sql.shuffle.partitions", shuffle_partitions)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "false")
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
    )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(log_level)
    logger.info(
        "SparkSession initialized: app_name=%s, master=%s, driver_memory=%s, shuffle_partitions=%s, timeZone=UTC",
        app_name,
        master,
        driver_memory,
        shuffle_partitions,
    )
    return spark
