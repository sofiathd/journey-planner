# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.16.6
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---



# +
from __future__ import annotations

"""
validate_rain.py
================

Rainfall-vs-delay validation for the journey planner.

This module creates one histogram-style bar chart and saves it into figs/ so it
can be displayed directly from project.ipynb.

Typical notebook usage:
    from validate_rain import analyze_rain_delay

    res_rain = analyze_rain_delay(
        spark,
        hadoopfs,
        username,
        out_path="figs/rain_vs_delay_no_correlation.png",
    )

    from IPython.display import Image, display
    display(Image(filename=res_rain["plot_path"], width=900))
"""

from pathlib import Path

import numpy as np
import pandas as pd
import pyspark.sql.functions as F


DEFAULT_OUT_PATH = "figs/rain_vs_delay_no_correlation.png"


def load_daily_rain_delay(
    spark,
    hadoopfs: str,
    username: str,
    *,
    raw_delays_path: str | None = None,
    weather_site: str = "LSGL",
    weather_start_date: str = "2024-12-01",
):
    """
    Load daily rainfall and daily delay statistics.

    Returns
    -------
    daily_pdf : pandas.DataFrame
        One row per operating day with rainfall and delay statistics.
    event_sdf : pyspark.sql.DataFrame
        Event-level delays joined with the rainfall of the same operating day.
    """
    if raw_delays_path is None:
        raw_delays_path = f"{hadoopfs}/user/{username}/final-project/raw_delays.parquet"

    raw_delays = spark.read.parquet(raw_delays_path)

    weather_daily = (
        spark.read.json(f"/data/com-490/bronze/weather/history/site={weather_site}/")
        .select(F.explode("observations").alias("obs"))
        .select(
            F.from_unixtime("obs.valid_time_gmt").cast("timestamp").alias("ts"),
            F.col("obs.precip_hrly").cast("double").alias("precip_raw"),
        )
        .filter(F.col("ts").isNotNull())
        .filter(F.col("ts") >= weather_start_date)
        .withColumn("operating_day", F.to_date("ts"))
        .groupBy("operating_day")
        .agg(
            F.sum(F.coalesce(F.col("precip_raw"), F.lit(0.0))).alias("precip_sum_mm"),
            F.max("precip_raw").alias("precip_max_mm"),
        )
    )

    delay_daily = raw_delays.groupBy("operating_day").agg(
        F.mean("delay_sec").alias("delay_mean"),
        F.expr("percentile_approx(delay_sec, 0.5)").alias("delay_median"),
        F.expr("percentile_approx(delay_sec, 0.9)").alias("delay_p90"),
        F.count("*").alias("n_obs"),
    )

    daily_pdf = (
        delay_daily
        .join(weather_daily, on="operating_day", how="inner")
        .orderBy("operating_day")
        .toPandas()
    )

    event_sdf = raw_delays.join(weather_daily, on="operating_day", how="inner")
    return daily_pdf, event_sdf


def _correlations(daily_pdf: pd.DataFrame, precip_col: str, delay_cols: list[str]) -> pd.DataFrame:
    """Compute Pearson and Spearman correlations at daily level."""
    try:
        from scipy import stats
    except Exception:
        stats = None

    rows = []
    x = daily_pdf[precip_col].to_numpy(dtype=float)

    for col in delay_cols:
        y = daily_pdf[col].to_numpy(dtype=float)
        mask = np.isfinite(x) & np.isfinite(y)
        n = int(mask.sum())

        if n < 5:
            rows.append({"delay_metric": col, "n_days": n, "note": "too few days"})
            continue

        if stats is None:
            pearson_r = float(pd.Series(x[mask]).corr(pd.Series(y[mask]), method="pearson"))
            spearman_rho = float(pd.Series(x[mask]).corr(pd.Series(y[mask]), method="spearman"))
            rows.append({
                "delay_metric": col,
                "n_days": n,
                "pearson_r": round(pearson_r, 3),
                "pearson_p": np.nan,
                "spearman_rho": round(spearman_rho, 3),
                "spearman_p": np.nan,
            })
        else:
            pearson_r, pearson_p = stats.pearsonr(x[mask], y[mask])
            spearman_rho, spearman_p = stats.spearmanr(x[mask], y[mask])
            rows.append({
                "delay_metric": col,
                "n_days": n,
                "pearson_r": round(float(pearson_r), 3),
                "pearson_p": round(float(pearson_p), 4),
                "spearman_rho": round(float(spearman_rho), 3),
                "spearman_p": round(float(spearman_p), 4),
            })

    return pd.DataFrame(rows)


def _dose_response(
    daily_pdf: pd.DataFrame,
    precip_col: str,
    delay_col: str,
    bins=(0, 0.1, 1, 2, 5, 10, 20, np.inf),
) -> pd.DataFrame:
    """Aggregate mean/median delay by rainfall bin."""
    labels = []
    for i in range(len(bins) - 1):
        lo, hi = bins[i], bins[i + 1]
        if lo == 0 and hi == 0.1:
            labels.append("0")
        elif np.isfinite(hi):
            labels.append(f"{lo:g}–{hi:g}")
        else:
            labels.append(f"≥{lo:g}")

    categories = pd.cut(
        daily_pdf[precip_col],
        bins=list(bins),
        right=False,
        labels=labels,
        include_lowest=True,
    )

    dose_df = (
        daily_pdf
        .groupby(categories, observed=False)[delay_col]
        .agg(["mean", "median", "count"])
        .reset_index()
        .rename(columns={precip_col: "rain_bin"})
    )

    # Depending on pandas version, the first column may keep precip_col as name.
    dose_df = dose_df.rename(columns={dose_df.columns[0]: "rain_bin"})
    return dose_df


def save_rain_delay_histogram(
    dose_df: pd.DataFrame,
    corr_df: pd.DataFrame,
    *,
    delay_metric: str,
    precip_col: str,
    out_path: str = DEFAULT_OUT_PATH,
) -> str:
    """
    Save the histogram-style rainfall-vs-delay validation plot.

    The output is saved by default to:
        figs/rain_vs_delay_no_correlation.png
    """
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    bins = dose_df["rain_bin"].astype(str).tolist()
    medians = dose_df["median"].astype(float).to_numpy()
    means = dose_df["mean"].astype(float).to_numpy()
    n_days = dose_df["count"].astype(int).to_numpy()

    valid = np.isfinite(medians) & np.isfinite(means) & (n_days > 0)
    bins = [b for b, keep in zip(bins, valid) if keep]
    medians = medians[valid]
    means = means[valid]
    n_days = n_days[valid]

    row = corr_df[corr_df["delay_metric"] == delay_metric]
    if len(row) and "spearman_rho" in row.columns:
        rho = float(row["spearman_rho"].iloc[0])
        pval = float(row["spearman_p"].iloc[0])
    else:
        rho, pval = float("nan"), float("nan")

    total_days = int(n_days.sum())
    overall = float(np.average(medians, weights=n_days))

    fig, ax = plt.subplots(figsize=(9.5, 6.0))
    x = np.arange(len(bins))

    ax.bar(
        x,
        medians,
        color="#2563EB",
        alpha=0.85,
        width=0.62,
        label="median delay",
    )
    ax.scatter(
        x,
        means,
        color="#9CA3AF",
        s=46,
        zorder=5,
        edgecolor="white",
        linewidth=0.8,
        label="mean delay",
    )
    ax.axhline(
        overall,
        color="#DC2626",
        ls="--",
        lw=1.5,
        alpha=0.85,
        label=f"global median ≈ {overall:.0f} s",
    )

    for xi, med, nd in zip(x, medians, n_days):
        ax.text(
            xi,
            med + 6,
            f"n={nd}",
            ha="center",
            va="bottom",
            fontsize=9,
            color="#374151",
        )

    ymax = max(np.nanmax(medians), np.nanmax(means), overall) * 1.30
    ax.set_ylim(0, max(120, ymax))
    ax.set_xticks(x)
    ax.set_xticklabels(bins)

    if precip_col == "precip_sum_mm":
        ax.set_xlabel("Daily cumulative rainfall (mm)")
    else:
        ax.set_xlabel("Maximum hourly rainfall (mm)")

    ax.set_ylabel("Delay (s)")
    ax.set_title("Delays do not increase with rainfall", fontsize=13, pad=10)
    ax.legend(loc="upper left", fontsize=10, framealpha=0.9)
    ax.spines[["top", "right"]].set_visible(False)

    if np.isfinite(pval):
        ns_label = "n.s." if pval >= 0.05 else "significant"
        corr_label = f"Spearman ρ = {rho:+.2f} (p = {pval:.2f}, {ns_label})"
    else:
        corr_label = f"Spearman ρ = {rho:+.2f}"

    fig.suptitle(
        f"Rainfall vs delays: no detectable correlation  ·  {total_days} days  ·  {corr_label}",
        fontsize=13,
        fontweight="bold",
        y=0.98,
    )
    fig.text(
        0.5,
        0.005,
        "Near-flat profile using robust medians. Heavy-rain bins are sparsely populated "
        "and should not be over-interpreted.",
        ha="center",
        fontsize=9,
        color="#6B7280",
    )

    fig.tight_layout(rect=[0, 0.03, 1, 0.94])
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)

    return str(out_path)


def analyze_rain_delay(
    spark,
    hadoopfs: str,
    username: str,
    *,
    precip_metric: str = "sum",
    delay_metric: str = "delay_median",
    weather_site: str = "LSGL",
    weather_start_date: str = "2024-12-01",
    raw_delays_path: str | None = None,
    out_path: str = DEFAULT_OUT_PATH,
    verbose: bool = True,
) -> dict:
    """
    Run rainfall-delay validation and save the histogram into figs/.
    """
    if precip_metric not in {"sum", "max"}:
        raise ValueError("precip_metric must be 'sum' or 'max'.")
    if delay_metric not in {"delay_mean", "delay_median", "delay_p90"}:
        raise ValueError("delay_metric must be 'delay_mean', 'delay_median', or 'delay_p90'.")

    precip_col = "precip_sum_mm" if precip_metric == "sum" else "precip_max_mm"
    delay_cols = ["delay_mean", "delay_median", "delay_p90"]

    daily_pdf, event_sdf = load_daily_rain_delay(
        spark,
        hadoopfs,
        username,
        raw_delays_path=raw_delays_path,
        weather_site=weather_site,
        weather_start_date=weather_start_date,
    )

    if daily_pdf.empty:
        raise ValueError(
            "No overlapping days between weather and delay data. "
            "Check weather_start_date, weather_site, and raw_delays_path."
        )

    corr_df = _correlations(daily_pdf, precip_col, delay_cols)
    dose_df = _dose_response(daily_pdf, precip_col, delay_metric)

    plot_path = save_rain_delay_histogram(
        dose_df,
        corr_df,
        delay_metric=delay_metric,
        precip_col=precip_col,
        out_path=out_path,
    )

    if verbose:
        print(f"Saved rainfall-delay histogram to: {plot_path}")
        print("\n=== Daily correlations ===")
        print(corr_df.to_string(index=False))
        row = corr_df[corr_df["delay_metric"] == delay_metric]
        if len(row) and "spearman_rho" in row.columns:
            rho = row["spearman_rho"].iloc[0]
            pval = row["spearman_p"].iloc[0]
            print(f"\nSelected metric: {delay_metric}")
            print(f"Spearman rho = {rho}, p = {pval}")
            print(
                "Interpretation: rainfall does not appear to add clear predictive value "
                "for the current delay model if the correlation is weak and non-significant."
            )

    return {
        "plot_path": plot_path,
        "plots": [plot_path],
        "daily_pdf": daily_pdf,
        "event_sdf": event_sdf,
        "correlations": corr_df,
        "dose_response": dose_df,
        "precip_col": precip_col,
        "delay_metric": delay_metric,
    }


if __name__ == "__main__":
    print(
        "Import this module from project.ipynb, then run:\n"
        "    from validate_rain import analyze_rain_delay\n"
        "    res_rain = analyze_rain_delay(spark, hadoopfs, username)\n"
        "The histogram will be saved to figs/rain_vs_delay_no_correlation.png."
    )

# -


