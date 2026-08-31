import pandas as pd
import numpy as np
import logging
import os

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("UsageProcessor")


class UsageProcessor:
    REQUIRED_CANONICAL_COLUMNS = [
        "timestamp", "grid_id", "country_code",
        "sms_in", "sms_out", "call_in", "call_out", "internet_activity"
    ]
    ACTIVITY_COLUMNS = ["sms_in", "sms_out", "call_in", "call_out", "internet_activity"]

    RAW_TO_CANONICAL = {
        "datetime": "timestamp",
        "CellID": "grid_id",
        "countrycode": "country_code",
        "smsin": "sms_in",
        "smsout": "sms_out",
        "callin": "call_in",
        "callout": "call_out",
        "internet": "internet_activity",
    }

    def __init__(self, source):
        """source: a file path (str) or an already-loaded DataFrame."""
        self.source = source
        self.raw_df = None
        self.clean_df = None
        self.grid_time_df = None
        self.daily_summary = None
        self.grid_summary = None

        # counters for the final audit log
        self._input_rows = 0
        self._rejected_missing_keys = 0
        self._rejected_negative = 0
        self._nulls_handled = 0
        self._agg_output_rows = 0

    # ------------------------------------------------------------------
    def load_data(self):
        if isinstance(self.source, pd.DataFrame):
            df = self.source.copy()
        else:
            df = pd.read_csv(self.source)

        df = df.rename(columns=self.RAW_TO_CANONICAL)

        missing_cols = [c for c in self.REQUIRED_CANONICAL_COLUMNS if c not in df.columns]
        if missing_cols:
            raise ValueError(f"Missing required columns after mapping: {missing_cols}")

        # raw_df is never mutated again — every later step works on copies
        self.raw_df = df
        self._input_rows = len(df)
        logger.info(f"load_data: loaded {self._input_rows} rows.")
        return self.raw_df

    # ------------------------------------------------------------------
    def clean_data(self):
        if self.raw_df is None:
            raise RuntimeError("load_data() must run before clean_data().")

        df = self.raw_df.copy()

        # Reject missing grid_id / timestamp
        before = len(df)
        df = df.dropna(subset=["grid_id", "timestamp"])
        self._rejected_missing_keys = before - len(df)

        # Reject negative activity values
        before = len(df)
        for col in self.ACTIVITY_COLUMNS:
            df = df[~(df[col] < 0)]
        self._rejected_negative = before - len(df)

        # Curated-layer null policy: blank activity measures -> 0, count handled
        nulls_before = int(df[self.ACTIVITY_COLUMNS].isnull().sum().sum())
        df[self.ACTIVITY_COLUMNS] = df[self.ACTIVITY_COLUMNS].fillna(0)
        self._nulls_handled = nulls_before

        self.clean_df = df
        logger.info(
            f"clean_data: rejected_missing_keys={self._rejected_missing_keys}, "
            f"rejected_negative={self._rejected_negative}, "
            f"nulls_handled={self._nulls_handled}"
        )
        return self.clean_df

    # ------------------------------------------------------------------
    def derive_time_features(self):
        if self.clean_df is None:
            raise RuntimeError("clean_data() must run before derive_time_features().")

        df = self.clean_df.copy()
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df["date"] = df["timestamp"].dt.date
        df["hour"] = df["timestamp"].dt.hour
        df["day_of_week"] = df["timestamp"].dt.dayofweek

        self.clean_df = df
        logger.info("derive_time_features: added date, hour, day_of_week.")
        return self.clean_df

    # ------------------------------------------------------------------
    def aggregate_to_grid_time(self):
        if self.clean_df is None:
            raise RuntimeError("derive_time_features() must run before aggregate_to_grid_time().")

        df = self.clean_df.copy()
        input_rows = len(df)

        grid_time = (
            df.groupby(["grid_id", "timestamp", "date", "hour", "day_of_week"], as_index=False)
              [self.ACTIVITY_COLUMNS]
              .sum()
        )
        # country_code intentionally dropped — analytics grain is grid + hour only

        output_rows = len(grid_time)
        duplicate_count = grid_time.duplicated(subset=["grid_id", "timestamp"]).sum()

        if duplicate_count != 0:
            raise RuntimeError(f"Aggregation failed: {duplicate_count} duplicates on (grid_id, timestamp).")
        if output_rows >= input_rows:
            raise RuntimeError("Aggregation failed: output rows not fewer than input rows.")
        if "country_code" in grid_time.columns:
            raise RuntimeError("Aggregation failed: country_code leaked into analytics grain.")

        self.grid_time_df = grid_time
        self._agg_output_rows = output_rows

        logger.info(f"aggregate_to_grid_time: input_rows={input_rows}, output_rows={output_rows}")
        return self.grid_time_df

    # ------------------------------------------------------------------
    def derive_activity_features(self):
        if self.grid_time_df is None:
            raise RuntimeError("aggregate_to_grid_time() must run before derive_activity_features().")

        df = self.grid_time_df.copy()
        df["total_sms"] = df["sms_in"] + df["sms_out"]
        df["total_calls"] = df["call_in"] + df["call_out"]
        df["total_activity"] = df["total_sms"] + df["total_calls"] + df["internet_activity"]

        self.grid_time_df = df
        logger.info("derive_activity_features: added total_sms, total_calls, total_activity.")
        return self.grid_time_df

    # ------------------------------------------------------------------
    def compute_kpis(self):
        if self.grid_time_df is None:
            raise RuntimeError("derive_activity_features() must run before compute_kpis().")

        df = self.grid_time_df

        daily_summary = (
            df.groupby("date", as_index=False)
              .agg(total_activity=("total_activity", "sum"),
                   avg_activity=("total_activity", "mean"),
                   active_grids=("grid_id", "nunique"))
        )

        busiest_hour_per_grid = (
            df.groupby(["grid_id", "hour"], as_index=False)["total_activity"].sum()
              .sort_values("total_activity", ascending=False)
              .drop_duplicates(subset="grid_id")
              .rename(columns={"hour": "busiest_hour"})[["grid_id", "busiest_hour"]]
        )

        grid_summary = (
            df.groupby("grid_id", as_index=False)
              .agg(total_activity=("total_activity", "sum"),
                   avg_activity=("total_activity", "mean"))
              .merge(busiest_hour_per_grid, on="grid_id", how="left")
        )

        self.daily_summary = daily_summary
        self.grid_summary = grid_summary
        logger.info(f"compute_kpis: daily_summary_rows={len(daily_summary)}, grid_summary_rows={len(grid_summary)}")
        return daily_summary, grid_summary

    # ------------------------------------------------------------------
    def export_summary(self, output_dir="../data/processed"):
        if self.daily_summary is None or self.grid_summary is None:
            raise RuntimeError("compute_kpis() must run before export_summary().")

        os.makedirs(output_dir, exist_ok=True)
        daily_path = os.path.join(output_dir, "daily_summary.csv")
        grid_path = os.path.join(output_dir, "grid_summary.csv")

        self.daily_summary.to_csv(daily_path, index=False)
        self.grid_summary.to_csv(grid_path, index=False)

        total_rejected = self._rejected_missing_keys + self._rejected_negative
        logger.info(
            f"export_summary: input_rows={self._input_rows}, "
            f"rejected_rows={total_rejected}, "
            f"nulls_handled={self._nulls_handled}, "
            f"output_rows={self._agg_output_rows}"
        )
        return daily_path, grid_path


processor = UsageProcessor("../data/raw/sms-call-internet-mi-2013-11-01.csv")
processor.load_data()
processor.clean_data()
processor.derive_time_features()
processor.aggregate_to_grid_time()
processor.derive_activity_features()
processor.compute_kpis()
processor.export_summary()
processor.grid_time_df.to_csv("../data/processed/grid_hour_activity.csv", index=False)



FILE_PATH = "../data/raw/sms-call-internet-mi-2013-11-01.csv"

def test_load_data():
    p = UsageProcessor(FILE_PATH)
    df = p.load_data()
    assert not df.empty

def test_clean_data():
    p = UsageProcessor(FILE_PATH)
    p.load_data()
    df = p.clean_data()
    assert (df[UsageProcessor.ACTIVITY_COLUMNS] >= 0).all().all()

def test_derive_time_features():
    p = UsageProcessor(FILE_PATH)
    p.load_data(); p.clean_data()
    df = p.derive_time_features()
    assert {"date", "hour", "day_of_week"}.issubset(df.columns)

def test_aggregate_to_grid_time():
    p = UsageProcessor(FILE_PATH)
    p.load_data(); p.clean_data(); p.derive_time_features()
    df = p.aggregate_to_grid_time()
    assert df.duplicated(subset=["grid_id", "timestamp"]).sum() == 0
    assert "country_code" not in df.columns

def test_derive_activity_features():
    p = UsageProcessor(FILE_PATH)
    p.load_data(); p.clean_data(); p.derive_time_features(); p.aggregate_to_grid_time()
    df = p.derive_activity_features()
    assert "total_activity" in df.columns

def test_compute_kpis():
    p = UsageProcessor(FILE_PATH)
    p.load_data(); p.clean_data(); p.derive_time_features()
    p.aggregate_to_grid_time(); p.derive_activity_features()
    daily, grid = p.compute_kpis()
    assert not daily.empty and not grid.empty

def test_export_summary():
    p = UsageProcessor(FILE_PATH)
    p.load_data(); p.clean_data(); p.derive_time_features()
    p.aggregate_to_grid_time(); p.derive_activity_features(); p.compute_kpis()
    daily_path, grid_path = p.export_summary()
    assert os.path.exists(daily_path) and os.path.exists(grid_path)

test_load_data()
test_clean_data()
test_derive_time_features()
test_aggregate_to_grid_time()
test_derive_activity_features()
test_compute_kpis()
test_export_summary()
print("All NP2 validations passed.")








