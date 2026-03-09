from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional, Sequence, Dict

import pandas as pd
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS


class EquipmentCsvInfluxImporter:
    """
    For each equipment_id, loads <data_dir>/<equipment_id>.csv into a pandas DataFrame
    and writes it to InfluxDB using the DataFrame write API. [1](https://influxdb-client.readthedocs.io/en/stable/usage.html)[2](https://docs.influxdata.com/influxdb/v2/api-guide/client-libraries/python/)
    """

    def __init__(
        self,
        url: str,
        token: str,
        org: str,
        bucket: str,
        data_dir: str,
        time_col='timestamp'
    ) -> None:

        self.data_dir = Path(data_dir)
        self.client = InfluxDBClient(url=url, token=token, org=org)
        self.org = org
        self.bucket = bucket
        self.time_col = time_col

        # keep it clean: synchronous write (predictable + easy to reason about)
        self.write_api = self.client.write_api(write_options=SYNCHRONOUS)

    def close(self) -> None:
        # write_api should be closed/flushed at the end. [1](https://influxdb-client.readthedocs.io/en/stable/usage.html)
        self.write_api.close()
        self.client.close()

    def import_all(self, equipment_ids: Iterable[str]) -> dict[str, str]:
        """
        Returns a small status map:
          - "imported" if file found + written
          - "missing" if file doesn't exist
          - "skipped_empty" if CSV is empty after load
        """
        status: dict[str, str] = {}

        for eid in equipment_ids:
            csv_path = self.data_dir / f"{eid}.csv"
            if not csv_path.exists():
                status[eid] = "missing"
                continue

            df = pd.read_csv(csv_path)

            if df.empty:
                status[eid] = "skipped_empty"
                continue

            df = self._prepare_dataframe(df, equipment_id=eid)
            print(df.head())

            # DataFrame write: provide measurement name, and tag columns (includes equipment_id).
            # The python client supports writing Pandas DataFrames directly. [1](https://influxdb-client.readthedocs.io/en/stable/usage.html)[2](https://docs.influxdata.com/influxdb/v2/api-guide/client-libraries/python/)
            _cols = [_c for _c in list(df.columns) if _c not in [self.time_col, eid]]
            for column in _cols:
                self.write_api.write(
                    bucket = self.bucket,
                    record = df,
                    data_frame_measurement_name = eid,
                    data_frame_tag_columns = column ,
                    data_frame_timestamp_column = self.time_col
                )

            status[eid] = "imported"

        return status

    def _prepare_dataframe(self, df: pd.DataFrame, equipment_id: str) -> None:
        # Ensure timestamp column exists and is datetime
        if self.time_col not in df.columns:
            raise ValueError(f"Missing required time column '{self.time_col}'")

        df[self.time_col] = pd.to_datetime(df[self.time_col], utc=True, format='mixed')

        # InfluxDB client expects the DataFrame index to represent time for DataFrame writes,
        # so we set it here (keeps data clean and consistent).
        #df.set_index(self.time_col, inplace=True)

        # Add mandatory tag column
        #df[equipment_id] = equipment_id

        # Convert obvious numeric columns when possible (optional cleanliness)
        # (Leave non-numeric as-is; Influx will treat them as string fields.)

        mapper = {}
        for c in df.columns:
            if c == self.time_col:
                pass
            elif c == equipment_id:
                pass
            else:
                df[c] = pd.to_numeric(df[c], errors="ignore")
                mapper[c] = f"{equipment_id}.{c}"

        # df.rename(columns=mapper, inplace=True)

        return df

        