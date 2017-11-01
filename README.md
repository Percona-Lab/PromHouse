# PromHouse

[![Build Status](https://travis-ci.org/Percona-Lab/PromHouse.svg?branch=master)](https://travis-ci.org/Percona-Lab/PromHouse)
[![codecov](https://codecov.io/gh/Percona-Lab/PromHouse/branch/master/graph/badge.svg)](https://codecov.io/gh/Percona-Lab/PromHouse)
[![Go Report Card](https://goreportcard.com/badge/github.com/Percona-Lab/PromHouse)](https://goreportcard.com/report/github.com/Percona-Lab/PromHouse)

PromHouse is a long-term remote storage with built-in clustering and downsampling for Prometheus 1 and 2 on top of
[ClickHouse](https://clickhouse.yandex). Or, rather, it will be someday.
Feel free to ~~like, share, retweet,~~ star and watch it, but **do not use it in production** yet.

It requires Go 1.9+.

## SQL queries

The largest jobs and instances by time series count:
```sql
SELECT
    job,
    instance,
    COUNT(*) AS value
FROM timeseries
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
FROM timeseries
GROUP BY
    visitParamExtractString(labels, '__name__') AS name
ORDER BY value DESC LIMIT 10
```

The largest time series by samples count:
```sql
SELECT
    labels,
    value
FROM timeseries
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
