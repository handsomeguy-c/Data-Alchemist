from __future__ import annotations

from typing import Optional


class SparkSessionFactory:
    def __init__(
        self,
        spark_sql,
        master_url: Optional[str] = None,
        event_log_dir: Optional[str] = None,
        driver_host: Optional[str] = None,
        driver_port: Optional[int] = None,
    ):
        self._spark_sql = spark_sql
        self._master_url = master_url
        self._event_log_dir = event_log_dir
        self._driver_host = driver_host
        self._driver_port = driver_port

    def create(self, app_name: str):
        builder = self._spark_sql.SparkSession.builder.appName(app_name)
        if self._master_url:
            builder = builder.master(self._master_url)
        if self._event_log_dir:
            builder = builder.config("spark.eventLog.enabled", "true")
            builder = builder.config("spark.eventLog.dir", self._event_log_dir)
        if self._driver_host:
            builder = builder.config("spark.driver.bindAddress", "0.0.0.0")
            builder = builder.config("spark.driver.host", self._driver_host)
        if self._driver_port:
            builder = builder.config("spark.driver.port", str(self._driver_port))
        return builder.getOrCreate()
