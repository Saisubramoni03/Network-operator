# C3 - Long-Context Incident Investigation

## 1. Dump-Everything Context

# CURRENT EVIDENCE

For Grid 4821, as of the feature timestamp **2013-11-07 17:30:00**:

- **Latest hourly total_activity = 254.97**, down from a peak of **666.74** at 15:30 (two hours earlier) and **321.20** at 16:30 — a sharp two-step decline off a spike.
- **avg_activity (window) = 360.25**, so the current interval (254.97) is running *below* its own recent average, not above it.
- **activity_growth = -0.0225** — slightly negative, consistent with the just-observed decline.
- **peak_ratio = 1.85** — the window's peak (666.74) is ~1.85x a reference level, reflecting the mid-afternoon spike.
- **variability = 0.409** — moderate-to-high volatility across the 24-hour window.
- **internet_share = 0.827** — internet traffic dominates the activity mix throughout, consistent across all points (not a new shift).
- **active_hours = 24** — the grid shows activity in every hour of the window (continuous usage, not a dead grid).
- **Model risk_score = 0.573 ("medium")** — model estimates a 57.3% probability that next-interval activity will exceed this grid's own 90th-percentile training threshold. Top contributing features are **active_hours, peak_ratio, and variability** — i.e., the score is driven by volatility/spikiness, not raw current magnitude (current activity is actually below the window average).
- **No alert entry exists for grid 4821** in the 100-item alert batch provided for this timestamp. Other grids triggered ACTIVITY_DROP or HIGH_ACTIVITY alerts at this same run; 4821 did not cross either alerting threshold as configured in this pipeline.

**Interpretation vs. observation:** The observed facts are a mid-afternoon spike (15:30) followed by a fast decline into the current interval, with a medium (not high) model risk score. This is *not* a confirmed congestion or outage event — it is a volatility signal from a probabilistic model.

# HISTORICAL EVIDENCE

- The only "history" supplied is the same 24-hour hourly series feeding the current features (2013-11-06 18:30 through 2013-11-07 17:30). Within this window, the grid shows a normal-looking diurnal pattern: quiet overnight (~136–180 total activity), ramp-up starting ~02:30–03:30, a daytime plateau (~420–540), a pronounced spike at 15:30 (666.74), then a fast fall-off in the last two hours.
- **No longer-term (multi-day/week) history, and no prior-alert history specific to grid 4821, was provided.** The alerts object contains 100 items, but none reference grid 4821 — meaning we can only say this grid produced *zero* alerts at this run, not whether it has ever alerted historically.
- **No baseline comparison (e.g., grid 4821's own multi-week percentile thresholds) is included**, so we cannot state how the 15:30 spike or the current decline compares to this grid's typical variability over time.
- **Conclusion: there is insufficient historical evidence in this package to say whether this pattern has occurred before, or how often.** It cannot be confirmed as recurring or as anomalous relative to a longer baseline — only that it looks like a single spike-and-decline cycle within the one day of data supplied.

# UNCERTAINTY

- **Pipeline freshness is a material concern.** The pipeline record shows `"freshness": "177.1 hours since last run"` even though the data itself is timestamped 2013-11-07, and the run was executed in 2026 (`run_timestamp: 2026-09-01...`). This confirms the analytics layer is a batch/replay run against historical data, not a live monitoring feed. "Current" in this report means "current as of the fixed historical timestamp," not real-time — this should not be treated as an active, ongoing incident without further context on why this historical window was reprocessed now.
- **Large-scale null handling upstream.** `nulls_handled = 67,695,033` against `rows_in = 21,064,103` — meaning null/missing values were imputed at a scale exceeding the raw row count (likely across multiple fields per row). Despite `rows_rejected = 0` and `data_quality: "OK"` at the feature level, this volume of imputation means some portion of the sms/call/internet fields feeding Grid 4821's metrics may be filled/estimated values rather than raw observations. This is not quantified per-grid, so the precision of avg_activity, peak_ratio, and variability for 4821 specifically cannot be fully verified.
- **Heavy row reduction (21M → 1.68M published rows)** is consistent with aggregation to grid-hour granularity, but the aggregation/imputation logic isn't detailed, adding unquantified uncertainty to derived features.
- **The risk_score is a probability estimate, not a confirmed fault.** The model explicitly frames it as "an investigation signal, not a confirmed fault," predicting an exceed

## 2. Curated Context

CURRENT EVIDENCE

At 2013-11-07 17:30, Grid 4821 shows average activity of 360.25 (units as provided), with activity_growth of -0.0225 (essentially flat, a slight ~2.3% decline versus the prior interval). The grid was active for all 24 hours in the observed window (active_hours=24). Peak_ratio is 1.85, meaning the peak observed activity was about 1.85x the average. Variability is 0.409, indicating moderate interval-to-interval fluctuation. Internet_share is 0.827, so the large majority of recorded activity is internet traffic rather than voice/SMS. Data_quality is flagged "OK."

The risk model (logreg-balanced-v1) scores this grid at 0.573, classified "medium" risk, with active_hours, peak_ratio, and variability as the top contributing features — i.e., the model is responding to sustained full-day activity and the peak/variability pattern, not to a growth spike (growth is actually negative).

No prior alerts exist for this grid (count = 0).

HISTORICAL EVIDENCE

The only historical context supplied is a 24-hour window, which is the same window the current reading comes from (the historical average of 360.25 matches the current avg_activity exactly). Within this single day, activity ranged from a minimum of 136.58 to a peak of 666.74, with the peak occurring at 15:30 — two hours before the current 17:30 reading. Current activity (360.25) sits in the mid-range of that day, well below the day's peak.

There is no evidence of this pattern occurring on any other day: prior_alerts count is 0 with no alert types recorded. Based on the evidence provided, I cannot say whether this is a recurring pattern for Grid 4821 or a one-off — there is no multi-day baseline to compare against, and no alert history to indicate past occurrences of similar behavior.

UNCERTAINTY

- **Insufficient history for baselining**: Only 24 hours of history is available, and it is the same window as the current reading. There is no data to establish a longer-term normal range, weekly/seasonal pattern, or to confirm whether a peak_ratio of 1.85 or variability of 0.409 is typical or unusual for this specific grid.
- **No alert history to compare against**: Zero prior alerts means we cannot assess recurrence rate or judge whether "medium" risk previously correlated with any confirmed incident at this grid.
- **Pipeline data quality concerns**: The pipeline handled 67,695,033 nulls against only 21,064,103 rows ingested — implying multiple null fields per row on average across the source data. While the run is marked "healthy" with 0 rows rejected, this volume of null-handling is material and means the aggregated features (avg_activity, variability, peak_ratio, etc.) may rest on partially imputed or incomplete source data. The extent and method of null handling is not detailed here, so the precision of the current-interval figures should be treated with caution.
- **Freshness/timing inconsistency**: The pipeline's run_timestamp is 2026-09-01, while the data's as_of/feature_timestamp is 2013-11-07 — a multi-year gap — yet the reported "freshness" is only "177.1 hours since last run." This is internally inconsistent and suggests the run_timestamp does not reflect real-time monitoring of current conditions; this appears to be a backfill, historical replay, or test run rather than a live operational check. This materially limits confidence that "current evidence" reflects real-time network state as of now — it only reliably reflects a state as of Nov 2013.
- **No congestion or fault indicators provided**: Nothing in the evidence (activity volume, peak_ratio, variability) directly measures congestion, packet loss, or service degradation. High or full-day activity should not be interpreted as congestion or a network problem — that inference is not supported by the data given.
- **Model transparency limited**: Only the top three contributing features are listed, without underlying feature weights, decision threshold, or historical false-positive rate for "medium" risk classifications from logreg-balanced-v1. This limits how much weight should be given to the medium risk label alone.

## 3. Unhealthy Pipeline Simulation

CURRENT EVIDENCE
At the 2013-11-07 17:30 interval, Grid 4821 shows avg_activity = 360.25, a slightly negative activity_growth (-2.3%), and active_hours = 24 (active across the full day window). peak_ratio is 1.85 (peak-to-average), variability = 0.41 (moderate), and internet_share = 0.83 (internet traffic dominates the mix). Data_quality is tagged "OK" for this record. The model assigns a risk_score of 0.573 ("medium"), driven primarily by active_hours, peak_ratio, and variability — i.e., the model is reacting to sustained full-day activity and a moderately spiky peak, not to an extreme value. No congestion or capacity breach is indicated by these figures alone; peak_ratio and variability are within a range the model treats as medium, not high, risk.

HISTORICAL EVIDENCE
The only historical context supplied is a 24-hour window ending at the current timestamp, so "history" here means intraday history, not a multi-day baseline. Within that window, activity ranged from 136.58 to 666.74, with the peak occurring at 15:30 — two hours before the current reading — at a value consistent with the current peak_ratio (666.74 / 360.25 ≈ 1.85). This confirms the current peak_ratio is not new; it reflects the same peak already captured in the 24-hour summary, not a fresh spike.

Prior alerts for this grid number zero, with no alert types on record. Based on the evidence provided, there is no history of this grid triggering alerts, and no basis to say this pattern has "happened before" in an alerting sense. Longer-term recurrence (e.g., weekly or monthly patterns) cannot be assessed — no such data was supplied.

UNCERTAINTY
The pipeline status for this run is explicitly flagged unhealthy and stale ("SIMULATION: analytics pipeline unhealthy... cannot be considered fully trustworthy"). This is material:

- rows_published (1,679,994) is a small fraction (~8%) of rows_in (21,064,103), and nulls_handled (67,695,033) exceeds rows_in — indicating heavy null substitution/imputation across multiple fields per row. This means the current and historical aggregates (avg_activity, peak_ratio, variability, etc.) may be built on imputed or filtered data, not raw observed values, and could be biased in either direction.
- Despite rows_rejected = 0, the scale of null-handling combined with an explicit "unhealthy"/"stale" flag means the "OK" data_quality tag on the current record should not be taken as an independent confirmation of trustworthiness — it comes from the same unhealthy pipeline run.
- The historical window is only 24 hours; no baseline exists for comparing this day to prior weeks/months, so I cannot say whether this level of activity, variability, or peak_ratio is typical or unusual for this grid over time.
- The absence of prior alerts could mean the grid has genuinely never triggered one, or that alerting history/coverage is incomplete — the evidence does not distinguish between these.
- The medium risk score comes from a single logistic-regression model version; no confidence interval, calibration data, or comparison run is provided, so its reliability under a flagged-unhealthy pipeline is itself uncertain.

Given these gaps, this evidence supports describing the current state and its self-consistency with the 24-hour window, but does not support a confident conclusion about anomaly severity, root cause, or true historical rarity.

## 4. Comparison

- Both dump-everything and curated runs were executed.
- The curated package summarizes recent history instead of passing all hourly rows.
- The unhealthy-pipeline simulation was performed without modifying the real pipeline status.
- The UNCERTAINTY section should materially increase its concern when pipeline health is changed to false.
