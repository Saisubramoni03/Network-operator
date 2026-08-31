
LIMITATION STATEMENT — Within-Day Baseline

This baseline compares each hour only against the same grid's own median
activity across the rest of the same day. It has no concept of what is
"normal" for a specific hour of day — it cannot distinguish "this grid is
always quiet at 03:00" from "this grid's activity just dropped."

Building a true hour-of-day baseline requires multiple days of history so
each (grid, hour-of-day) bucket has more than one observation. That
capability arrives at ML4, once Phase 2 (Spark) and Phase 3 (Data
Engineering) have accumulated multi-day history.
