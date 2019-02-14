# PromHouse

[![Build Status](https://travis-ci.org/Percona-Lab/PromHouse.svg?branch=master)](https://travis-ci.org/Percona-Lab/PromHouse)
[![codecov](https://codecov.io/gh/Percona-Lab/PromHouse/branch/master/graph/badge.svg)](https://codecov.io/gh/Percona-Lab/PromHouse)
[![Go Report Card](https://goreportcard.com/badge/github.com/Percona-Lab/PromHouse)](https://goreportcard.com/report/github.com/Percona-Lab/PromHouse)
[![CLA assistant](https://cla-assistant.percona.com/readme/badge/Percona-Lab/PromHouse)](https://cla-assistant.percona.com/Percona-Lab/PromHouse)

PromHouse is a long-term remote storage with built-in clustering and downsampling for 2.x on top of
[ClickHouse](https://clickhouse.yandex). Or, rather, it will be someday.
Feel free to ~~like, share, retweet,~~ star and watch it, but **do not use it in production** yet.

## Database Schema

```sql
CREATE TABLE time_series (
    date Date CODEC(Delta),
    fingerprint UInt64,
    labels String
)
ENGINE = ReplacingMergeTree
    PARTITION BY date
    ORDER BY fingerprint;

CREATE TABLE samples (
    fingerprint UInt64,
    timestamp_ms Int64 CODEC(Delta),
    value Float64 CODEC(Delta)
)
ENGINE = MergeTree
    PARTITION BY toDate(timestamp_ms / 1000)
    ORDER BY (fingerprint, timestamp_ms);
```

```sql
SELECT * FROM time_series WHERE fingerprint = 7975981685167825999;
```

```
┌───────date─┬─────────fingerprint─┬─labels─────────────────────────────────────────────────────────────────────────────────┐
│ 2017-12-31 │ 7975981685167825999 │ {"__name__":"up","instance":"promhouse_clickhouse_exporter_1:9116","job":"clickhouse"} │
└────────────┴─────────────────────┴────────────────────────────────────────────────────────────────────────────────────────┘
```

```sql
SELECT * FROM samples WHERE fingerprint = 7975981685167825999 LIMIT 3;
```

```
┌─────────fingerprint─┬──timestamp_ms─┬─value─┐
│ 7975981685167825999 │ 1514730532900 │     0 │
│ 7975981685167825999 │ 1514730533901 │     1 │
│ 7975981685167825999 │ 1514730534901 │     1 │
└─────────────────────┴───────────────┴───────┘
```

Time series in Prometheus are identified by label name/value pairs, including `__name__` label, which stores metric
name. Hash of those pairs is called a fingerprint. PromHouse uses the same hash algorithm as Prometheus to simplify data
migration. During the operation, all fingerprints and label name/value pairs a kept in memory for fast access. The new
time series are written to ClickHouse for persistence. They are also periodically read from it for discovering new time
series written by other ClickHouse instances. `ReplacingMergeTree` ensures there are no duplicates if several ClickHouses
wrote the same time series at the same time.

PromHouse currently stores 24 bytes per sample: 8 bytes for UInt64 fingerprint, 8 bytes for Int64 timestamp, and 8 bytes
for Float64 value. The actual compression rate is about 4.5:1, that's about 24/4.5 = 5.3 bytes per sample. Prometheus
local storage compresses 16 bytes (timestamp and value) per sample to [1.37](https://coreos.com/blog/prometheus-2.0-storage-layer-optimization), that's 12:1.

Since ClickHouse v19.3.3 it is possible to use delta and double delta for compression, which should make storage almost as efficient as TSDB's one.

## Outstanding features in the roadmap

- Downsampling (become possible since ClickHouse v18.12.14)
- Query Hints (become possible since [prometheus PR 4122](https://github.com/prometheus/prometheus/pull/4122), help wanted [issue #24](https://github.com/Percona-Lab/PromHouse/issues/24))

## SQL queries

The largest jobs and instances by time series count:

```sql
SELECT
    job,
    instance,
    COUNT(*) AS value
FROM time_series
GROUP BY
    visitParamExtractString(labels, 'job') AS job,
    visitParamExtractString(labels, 'instance') AS instance
ORDER BY value DESC LIMIT 10
```

The largest metrics by time series count (cardinality):

```sql
SELECT
    name,
    COUNT(*) AS value
FROM time_series
GROUP BY
    visitParamExtractString(labels, '__name__') AS name
ORDER BY value DESC LIMIT 10
```

The largest time series by samples count:

```sql
SELECT
    labels,
    value
FROM time_series
ANY INNER JOIN
(
    SELECT
        fingerprint,
        COUNT(*) AS value
    FROM samples
    GROUP BY fingerprint
    ORDER BY value DESC
    LIMIT 10
) USING (fingerprint)
```
