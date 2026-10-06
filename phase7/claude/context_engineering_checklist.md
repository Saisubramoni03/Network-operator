\# C3 - Context Engineering Checklist



\## Purpose



Use this checklist when preparing evidence for a long-context

network incident investigation.



\## 1. Define the investigation question



\- \[ ] Clearly identify the grid, timestamp, and investigation question.

\- \[ ] Decide what the engineer needs to know before collecting context.

\- \[ ] Avoid collecting data that does not help answer the question.



\## 2. Collect relevant evidence



\- \[ ] Current grid metrics are included.

\- \[ ] Recent history is included.

\- \[ ] Prior alerts are included when available.

\- \[ ] Model risk/anomaly evidence is included.

\- \[ ] Pipeline status is included.

\- \[ ] Data freshness is included.



\## 3. Prefer summaries over raw data



\- \[ ] Summarize hourly history instead of passing every raw row.

\- \[ ] Include minimum, maximum, average, and peak information when useful.

\- \[ ] Include the timestamp of important peaks.

\- \[ ] Keep only fields relevant to the investigation.

\- \[ ] Avoid unnecessary API responses or unrelated grids.



\## 4. Separate evidence by purpose



\- \[ ] CURRENT EVIDENCE describes what is true at the investigated timestamp.

\- \[ ] HISTORICAL EVIDENCE describes what the available history supports.

\- \[ ] UNCERTAINTY describes what cannot be concluded.

\- \[ ] Do not mix assumptions with observed facts.



\## 5. Treat pipeline quality as evidence



\- \[ ] Check whether the pipeline is healthy.

\- \[ ] Check task statuses.

\- \[ ] Check rejected rows.

\- \[ ] Check null-handling information.

\- \[ ] Check analytics freshness.

\- \[ ] Treat stale analytics as a limitation on real-time conclusions.

\- \[ ] If the pipeline is unhealthy, explicitly increase uncertainty.

\- \[ ] Do not ignore pipeline status just because individual tasks succeeded.



\## 6. Avoid unsupported conclusions



\- \[ ] Do not claim congestion from activity volume alone.

\- \[ ] Do not claim an outage without service-impact evidence.

\- \[ ] Do not claim recurrence without sufficient historical data.

\- \[ ] Do not interpret zero returned alerts as proof of zero historical alerts

&#x20;     unless the alert query provides complete history.

\- \[ ] Do not treat a model risk score as a confirmed fault.

\- \[ ] Do not invent missing historical or operational evidence.



\## 7. Compare context strategies



\- \[ ] Run a dump-everything investigation.

\- \[ ] Run a curated-context investigation.

\- \[ ] Compare whether the conclusions are consistent.

\- \[ ] Check whether the curated response is easier to interpret.

\- \[ ] Remove irrelevant context and check whether the conclusion changes.



\## 8. Validate uncertainty



\- \[ ] Run the investigation with a healthy pipeline.

\- \[ ] Run a controlled unhealthy-pipeline simulation.

\- \[ ] Confirm that UNCERTAINTY changes materially.

\- \[ ] Confirm that the change is caused by pipeline evidence rather

&#x20;     than generic cautious wording.



\## 9. Final operator response



\- \[ ] State observed evidence first.

\- \[ ] Separate interpretation from evidence.

\- \[ ] State historical limitations.

\- \[ ] State pipeline/freshness limitations.

\- \[ ] State what remains unknown.

\- \[ ] Provide next checks only when supported by the evidence.



\## C3 Result



For Grid 4821:



\- Dump-everything investigation completed.

\- Curated-context investigation completed.

\- Unhealthy-pipeline simulation completed.

\- Curated evidence used summarized history rather than raw hourly rows.

\- The unhealthy-pipeline simulation materially increased uncertainty.

\- The investigation did not treat activity alone as proof of congestion.

