# +
from __future__ import annotations

import datetime
import time
from typing import Any, Optional

import numpy as np
import pyspark.sql.functions as F
from pyspark.sql import Window


class DelayModel:
    """
    Hierarchical fallback model for delay distributions.
    
    Tries the most specific available distribution first (trip + stop + weather),
    then progressively falls back to less specific levels: trip + stop,
    stop + weather, stop only, and finally the global delay distribution.
    This keeps the model robust when some combinations have too few observations.
    """

    def __init__(
        self,
        *,
        level2_df,
        level4_df,
        global_samples,
        min_obs: int = 20,
        level1_df=None,
        level3_df=None,
    ):
        # Minimum observation threshold required to trust a specific distribution layer
        self.min_obs = int(min_obs)
        
        # Level 5: absolute fallback using global network delay distribution
        self.global_samples = np.asarray(global_samples)

        # Level 4: standard localized distribution mapping using stop_id only
        self._l4 = {
            int(r.bpuic): r.delays
            for r in level4_df.itertuples(index=False)
            if len(r.delays) >= self.min_obs
        }

        # Level 2: standard granular distribution mapping using trip_id and stop_id
        self._l2 = {
            (r.tt_trip_id, int(r.bpuic)): r.delays
            for r in level2_df.itertuples(index=False)
            if len(r.delays) >= self.min_obs
        }

        # Weather-aware mode is enabled only when both contextual layers are provided 
        self.is_weather_aware = (level1_df is not None) and (level3_df is not None)

        if self.is_weather_aware:
            # Level 1: contextual specific layer using trip_id, stop_id and weather
            self._l1 = {
                (r.tt_trip_id, int(r.bpuic), r.weather_bucket): r.delays
                for r in level1_df.itertuples(index=False)
                if len(r.delays) >= self.min_obs
            }
            # Level 3: contextual localized layer using stop_id and weather
            self._l3 = {
                (int(r.bpuic), r.weather_bucket): r.delays
                for r in level3_df.itertuples(index=False)
                if len(r.delays) >= self.min_obs
            }
            # Track usage across all 5 layers
            self._level_usage = {1: 0, 2: 0, 3: 0, 4: 0, 5: 0}
        else:
            self._l1 = {}
            self._l3 = {}
            # Only track active layers
            self._level_usage = {2: 0, 4: 0, 5: 0}

    def _lookup_samples(self, tt_trip_id, bpuic, weather=None):
        """
        Internal routing mechanism executing the hierarchical fallback cascade.
        Bypasses contextual layers instantly if weather awareness is disabled.
        Returns (samples, level).
        """
        bpuic = int(bpuic)
        
        if self.is_weather_aware and weather is not None:
            if (samples := self._l1.get((tt_trip_id, bpuic, weather))) is not None:
                return samples, 1
            if (samples := self._l2.get((tt_trip_id, bpuic))) is not None:
                return samples, 2
            if (samples := self._l3.get((bpuic, weather))) is not None:
                return samples, 3
            if (samples := self._l4.get(bpuic)) is not None:
                return samples, 4
        else:
            if (samples := self._l2.get((tt_trip_id, bpuic))) is not None:
                return samples, 2
            if (samples := self._l4.get(bpuic)) is not None:
                return samples, 4
        
        return self.global_samples, 5

    def get_connection_probability(
        self,
        *,
        tt_trip_id,
        bpuic,
        margin_seconds,
        weather=None,
    ) -> float:
        """
        Computes the empirical Cumulative Distribution Function (eCDF) probability
        that the actual delay is within the given connection margin (in seconds).
        """
        samples, level = self._lookup_samples(tt_trip_id, bpuic, weather)
        self._level_usage[level] += 1
        
        # Binary search optimization for efficient quantile probability extraction
        return float(np.searchsorted(samples, margin_seconds, side="right") / len(samples))

    def get_quantile(self, tt_trip_id, bpuic, q, weather=None):
        """Returns the delay threshold in seconds corresponding to a probability quantile q."""
        samples, _ = self._lookup_samples(tt_trip_id, bpuic, weather)
        return float(np.quantile(samples, q))

    def reset_usage(self) -> None:
        """Resets all telemetry counters to zero."""
        self._level_usage = {k: 0 for k in self._level_usage}

    def usage_stats(self):
        """Generates a human-readable summary of the fallback layer usage."""
        total = sum(self._level_usage.values())
        if total == 0:
            return self._level_usage.copy()
        return {k: f"{v} ({100 * v / total:.1f}%)" for k, v in self._level_usage.items()}

    def summary(self) -> dict[str, int]:
        """Returns structural metrics highlighting metadata statistics of the loaded layers."""
        return {
            "min_obs": self.min_obs,
            "is_weather_aware": int(self.is_weather_aware),
            "level1_keys": len(self._l1),
            "level2_keys": len(self._l2),
            "level3_keys": len(self._l3),
            "level4_keys": len(self._l4),
            "global_observations": int(len(self.global_samples)),
        }


def _sorted_array(values):
    """Returns a sorted float array (required for binary-search lookups)."""
    return np.sort(np.asarray(values, dtype=float))


def _samples_to_pandas(sdf):
    """
    Converts a distributed Spark DataFrame into a local Pandas 
    DataFrame with the `delays` column pre-sorted.
    """

    df = sdf.toPandas()
    if len(df):
        df["delays"] = df["delays"].apply(_sorted_array)
    return df


def load_aligned_trips(
    *,
    spark,
    hadoopfs: str,
    username: str,
    stops_df,
    aligned_parquet_path: Optional[str] = None,
    recompute_alignment: bool = False,
    alignment_window_days: int = 365,
    match_threshold: float = 75.0,
    diagnostics: bool = False,
    verbose: bool = True,
):
    """
    Load (or compute) the mapping between actual transport operations (Istdaten)
    and scheduled trips (GTFS timetable).
    """

    def log(msg: str) -> None:
        if verbose:
            print(msg)

    if aligned_parquet_path is None:
        aligned_parquet_path = f"{hadoopfs}/user/{username}/final-project/aligned_trips.parquet"

    if not recompute_alignment:
        log(f"Loading cached alignment from: {aligned_parquet_path}")
        aligned_trips_df = spark.read.parquet(aligned_parquet_path)
        if diagnostics:
            log(f"Aligned trips count: {aligned_trips_df.count()}")
        return aligned_trips_df

    log("Computing Istdaten/timetable alignment...")
    t0 = time.time()

    last_day = spark.table("iceberg.sbb.istdaten").agg(F.max("operating_day").alias("last")).collect()[0]["last"]
    start_date = last_day - datetime.timedelta(days=int(alignment_window_days))
    max_pub_date = spark.table("iceberg.sbb.stops").agg(F.max("pub_date").alias("max_pub")).collect()[0]["max_pub"]
    region_bpuics = stops_df["stop_id"].astype(int).tolist()

    istdaten_target = (
        spark.table("iceberg.sbb.istdaten")
        .filter(F.col("operating_day").between(start_date, last_day))
        .filter(F.col("bpuic").isin(region_bpuics))
        .withColumn("actual_time_raw", F.coalesce(F.col("arr_time"), F.col("dep_time")))
        .filter(F.col("actual_time_raw").isNotNull())
        .withColumn("sched_time", F.date_format("actual_time_raw", "HH:mm"))
        .withColumn("agency_id", F.split(F.col("operator_id"), ":").getItem(1))
        .select(F.col("trip_id").alias("ist_trip_id"), "operating_day", "bpuic", "sched_time", "agency_id")
        .distinct()
    )

    timetable_target = (
        spark.table("iceberg.sbb.stop_times")
        .filter(F.col("pub_date") == max_pub_date)
        .withColumn("bpuic", F.split(F.col("stop_id"), ":").getItem(0).cast("integer"))
        .filter(F.col("bpuic").isin(region_bpuics))
        .withColumn("tt_time_raw", F.coalesce(F.col("arrival_time"), F.col("departure_time")))
        .filter(F.col("tt_time_raw").isNotNull())
        .withColumn("hours_raw", F.split(F.col("tt_time_raw"), ":").getItem(0).cast("int"))
        .withColumn("mins_raw", F.split(F.col("tt_time_raw"), ":").getItem(1))
        .withColumn("sched_time", F.concat_ws(":", F.lpad((F.col("hours_raw") % 24).cast("string"), 2, "0"), F.lpad(F.col("mins_raw"), 2, "0")))
        .join(spark.table("iceberg.sbb.trips").filter(F.col("pub_date") == max_pub_date), on="trip_id", how="inner")
        .join(spark.table("iceberg.sbb.routes").filter(F.col("pub_date") == max_pub_date), on="route_id", how="inner")
        .join(
            F.broadcast(
                spark.table("iceberg.sbb.stop_times")
                .filter(F.col("pub_date") == max_pub_date)
                .groupBy("trip_id")
                .agg(F.count("*").alias("total_tt_stops"))
            ),
            on="trip_id",
            how="left",
        )
        .select(F.col("trip_id").alias("tt_trip_id"), "bpuic", "sched_time", "total_tt_stops", "agency_id")
        .distinct()
    )

    trip_matches = (
        istdaten_target
        .join(timetable_target, on=["bpuic", "sched_time", "agency_id"], how="inner")
        .groupBy("ist_trip_id", "tt_trip_id", "operating_day")
        .agg(F.count("*").alias("shared_stops_count"), F.first("total_tt_stops").alias("total_tt_stops"))
        .withColumn("match_percentage", F.round(F.col("shared_stops_count") / F.col("total_tt_stops") * 100, 2))
    )

    best_match_window = Window.partitionBy("ist_trip_id", "operating_day").orderBy(
        F.col("match_percentage").desc(),
        F.col("total_tt_stops").desc(),
        F.col("tt_trip_id"),
    )

    aligned_trips_df = (
        trip_matches
        .withColumn("row_num", F.row_number().over(best_match_window))
        .filter(F.col("row_num") == 1)
        .drop("row_num")
        .filter(F.col("match_percentage") >= float(match_threshold))
    )

    log(f"Saving alignment mapping to: {aligned_parquet_path}")
    aligned_trips_df.write.mode("overwrite").parquet(aligned_parquet_path)
    aligned_trips_df = spark.read.parquet(aligned_parquet_path)
    
    if diagnostics:
        log(f"Aligned trips count: {aligned_trips_df.count()}")
    log(f"Alignment execution ready ({time.time() - t0:.1f}s).")
    
    return aligned_trips_df


def build_delay_model(
    *,
    spark,
    hadoopfs: str,
    username: str,
    stops_df,
    aligned_trips_df=None,
    aligned_parquet_path: Optional[str] = None,
    recompute_alignment: bool = False,
    alignment_window_days: int = 365,
    match_threshold: float = 75.0,
    raw_delays_path: Optional[str] = None,
    recompute_delays: bool = False,
    min_obs_for_ccdf: int = 20,
    use_weather_features: bool = False,
    weather_site: str = "LSGL",
    precip_threshold: float = 0.5,
    weather_start_date: str = "2024-12-01",
    diagnostics: bool = False,
    return_intermediates: bool = False,
    verbose: bool = True,
) -> dict[str, Any]:
    """
    Build a DelayModel: compile raw delays, aggregate by fallback level,
    and return the optimized wrapped model instance.
    """

    def log(msg: str) -> None:
        if verbose:
            print(msg)

    if aligned_trips_df is None or recompute_alignment:
        aligned_trips_df = load_aligned_trips(
            spark=spark,
            hadoopfs=hadoopfs,
            username=username,
            stops_df=stops_df,
            aligned_parquet_path=aligned_parquet_path,
            recompute_alignment=recompute_alignment,
            alignment_window_days=alignment_window_days,
            match_threshold=match_threshold,
            diagnostics=diagnostics,
            verbose=verbose,
        )

    if raw_delays_path is None:
        file_suffix = "_weather" if use_weather_features else ""
        raw_delays_path = f"{hadoopfs}/user/{username}/final-project/raw_delays{file_suffix}.parquet"

    if recompute_delays:
        log("Computing raw delays from source observations...")
        t0 = time.time()
        region_bpuics = stops_df["stop_id"].astype(int).tolist()

        istdaten_obs = (
            spark.table("iceberg.sbb.istdaten")
            .filter(F.col("bpuic").isin(region_bpuics))
            .filter((F.col("transit") == False) | F.col("transit").isNull())
            .filter((F.col("failed") == False) | F.col("failed").isNull())
            .filter((F.col("unplanned") == False) | F.col("unplanned").isNull())
            .withColumn("dep_delay_sec", F.when(
                F.col("dep_actual").isNotNull() & F.col("dep_time").isNotNull(),
                F.unix_timestamp("dep_actual") - F.unix_timestamp("dep_time"),
            ))
            .withColumn("arr_delay_sec", F.when(
                F.col("arr_actual").isNotNull() & F.col("arr_time").isNotNull(),
                F.unix_timestamp("arr_actual") - F.unix_timestamp("arr_time"),
            ))
            .withColumn("delay_sec", F.coalesce(F.col("dep_delay_sec"), F.col("arr_delay_sec")))
            .filter(F.col("delay_sec").between(-300, 7200))
            .select(F.col("trip_id").alias("ist_trip_id"), "operating_day", "bpuic", "delay_sec")
        )

        raw_delays_df = (
            aligned_trips_df
            .select("ist_trip_id", "tt_trip_id", "operating_day")
            .join(istdaten_obs, on=["ist_trip_id", "operating_day"], how="inner")
            .select("tt_trip_id", "bpuic", "operating_day", "delay_sec")
        )
        raw_delays_df.write.mode("overwrite").parquet(raw_delays_path)
        log(f"Raw delays saved to {raw_delays_path} ({time.time() - t0:.1f}s).")
    else:
        log(f"Loading cached raw delays from: {raw_delays_path}")

    raw_delays_df = spark.read.parquet(raw_delays_path)
    
    level2_sdf = raw_delays_df.groupBy("tt_trip_id", "bpuic").agg(
        F.collect_list("delay_sec").alias("delays"),
        F.count("*").alias("n_obs"),
    )
    level4_sdf = raw_delays_df.groupBy("bpuic").agg(
        F.collect_list("delay_sec").alias("delays"),
        F.count("*").alias("n_obs"),
    )

    log("Collecting empirical data distribution samples into RAM...")
    level2_df = _samples_to_pandas(level2_sdf)
    level4_df = _samples_to_pandas(level4_sdf)

    if len(level4_df) == 0:
        raise ValueError("Data distribution empty. Please verify ingestion parameters.")

    global_delays = np.sort(np.concatenate(level4_df["delays"].values))
    level1_df = None
    level3_df = None
    weather_daily_df = None

    if use_weather_features:
        log("Building contextual weather stratification matrix layers...")
        weather_daily_df = (
            spark.read.json(f"/data/com-490/bronze/weather/history/site={weather_site}/")
            .select(F.explode("observations").alias("obs"))
            .select(
                F.from_unixtime("obs.valid_time_gmt").cast("timestamp").alias("ts"),
                F.col("obs.precip_hrly").cast("double").alias("precip_raw"),
            )
            .filter(F.col("ts") >= weather_start_date)
            .filter(F.col("ts").isNotNull())
            .withColumn("operating_day", F.to_date("ts"))
            .groupBy("operating_day")
            .agg(F.max("precip_raw").alias("precip_max"))
            .withColumn("weather_bucket", F.when(F.col("precip_max") > precip_threshold, "adverse").otherwise("normal"))
            .select("operating_day", "weather_bucket")
            .toPandas()
        )

        weather_daily_sdf = spark.createDataFrame(weather_daily_df)
        raw_with_weather = raw_delays_df.join(F.broadcast(weather_daily_sdf), on="operating_day", how="inner")

        level1_df = _samples_to_pandas(
            raw_with_weather.groupBy("tt_trip_id", "bpuic", "weather_bucket").agg(
                F.collect_list("delay_sec").alias("delays"), F.count("*").alias("n_obs")
            )
        )
        level3_df = _samples_to_pandas(
            raw_with_weather.groupBy("bpuic", "weather_bucket").agg(
                F.collect_list("delay_sec").alias("delays"), F.count("*").alias("n_obs")
            )
        )

    delay_model = DelayModel(
        level1_df=level1_df,
        level2_df=level2_df,
        level3_df=level3_df,
        level4_df=level4_df,
        global_samples=global_delays,
        min_obs=min_obs_for_ccdf,
    )

    summary = delay_model.summary()
    if verbose:
        log("DelayModel initialization pipeline complete:")
        for k, v in summary.items():
            log(f"  {k}: {v}")

    out: dict[str, Any] = {
        "delay_model": delay_model,
        "aligned_trips_df": aligned_trips_df,
        "raw_delays_path": raw_delays_path,
        "summary": summary,
    }
    if return_intermediates:
        out.update({
            "raw_delays_df": raw_delays_df,
            "level2_df": level2_df,
            "level4_df": level4_df,
            "global_delays": global_delays,
            "level1_df": level1_df,
            "level3_df": level3_df,
            "weather_daily_df": weather_daily_df,
        })
    return out
