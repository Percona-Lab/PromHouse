// PromHouse
// Copyright (C) 2017 Percona LLC
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

package storages

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Percona-Lab/PromHouse/prompb"
	"github.com/Percona-Lab/PromHouse/storages/base"
	"github.com/Percona-Lab/PromHouse/storages/clickhouse"
	"github.com/Percona-Lab/PromHouse/storages/memory"
	"github.com/Percona-Lab/PromHouse/storages/test"
	"github.com/Percona-Lab/PromHouse/utils/timeseries"
)

func formatTS(ts *prompb.TimeSeries) string {
	res := test.MakeMetric(ts.Labels).String()
	for _, s := range ts.Samples {
		res += "\n\t" + model.SamplePair{
			Timestamp: model.Time(s.TimestampMs),
			Value:     model.SampleValue(s.Value),
		}.String()
	}
	return res
}

func messageTS(expected, actual *prompb.TimeSeries) string {
	return fmt.Sprintf("expected = %s\nactual  = %s", formatTS(expected), formatTS(actual))
}

func TestStorages(t *testing.T) {
	// level := logrus.GetLevel()
	// logrus.SetLevel(logrus.DebugLevel)
	// defer logrus.SetLevel(level)

	for storageName, newStorage := range map[string]func() (base.Storage, error){
		"Memory": func() (base.Storage, error) {
			return memory.New(), nil
		},
		"ClickHouseTempTable": func() (base.Storage, error) {
			params := &clickhouse.Params{
				DSN:          "tcp://127.0.0.1:9000/?database=prometheus_test",
				DropDatabase: true,
			}
			return clickhouse.New(params)
		},
		"ClickHouseQuery": func() (base.Storage, error) {
			params := &clickhouse.Params{
				DSN:                  "tcp://127.0.0.1:9000/?database=prometheus_test",
				DropDatabase:         true,
				MaxTimeSeriesInQuery: 1000,
			}
			return clickhouse.New(params)
		},
	} {
		t.Run(storageName, func(t *testing.T) {
			storage, err := newStorage()
			require.NoError(t, err)

			start := model.Now().Add(-time.Minute)
			end := model.Now()

			t.Run("Read", func(t *testing.T) {
				storedData := test.GetData()
				require.NoError(t, storage.Write(context.Background(), storedData))

				t.Run("ByName", func(t *testing.T) {
					// queries returning all data
					for _, q := range []base.Query{
						{
							Start: start,
							End:   end,
							Matchers: []base.Matcher{{
								Name:  "__name__",
								Type:  base.MatchEqual,
								Value: "http_requests_total",
							}},
						},
						{
							Start: start,
							End:   end,
							Matchers: []base.Matcher{{
								Name:  "__name__",
								Type:  base.MatchRegexp,
								Value: "http_requests_.+",
							}},
						},
					} {
						t.Run(q.String(), func(t *testing.T) {
							data, err := storage.Read(context.Background(), []base.Query{q})
							require.NoError(t, err)
							require.Len(t, data.Results, 1)
							require.Len(t, data.Results[0].TimeSeries, 3)
							timeseries.SortTimeSeriesSlow(data.Results[0].TimeSeries)
							for i, actual := range data.Results[0].TimeSeries {
								timeseries.SortLabels(actual.Labels)
								expected := storedData.TimeSeries[i]
								assert.Equal(t, expected, actual, messageTS(expected, actual))
							}
						})
					}

					// queries returning nothing
					for _, q := range []base.Query{
						{
							Start: start,
							End:   end,
							Matchers: []base.Matcher{{
								Name:  "__name__",
								Type:  base.MatchEqual,
								Value: "no_such_metric",
							}},
						},
						{
							Start: start,
							End:   end,
							Matchers: []base.Matcher{{
								Name:  "__name__",
								Type:  base.MatchRegexp,
								Value: "_requests_",
							}},
						},
						{
							// TODO should it return 3 series with 0 values, or 0 values?
							Start: 0,
							End:   0,
							Matchers: []base.Matcher{{
								Name:  "__name__",
								Type:  base.MatchEqual,
								Value: "http_requests_total",
							}},
						},
						{
							// TODO should it return 3 series with 0 values, or 0 values?
							Start: 0,
							End:   0,
							Matchers: []base.Matcher{{
								Name:  "__name__",
								Type:  base.MatchRegexp,
								Value: "http_requests_.+",
							}},
						},
					} {
						t.Run(q.String(), func(t *testing.T) {
							data, err := storage.Read(context.Background(), []base.Query{q})
							require.NoError(t, err)
							require.Len(t, data.Results, 1)
							require.Len(t, data.Results[0].TimeSeries, 0)
						})
					}
				})

				t.Run("ByNonExistingLabel", func(t *testing.T) {
					for _, q := range []base.Query{
						{
							Start: start,
							End:   end,
							Matchers: []base.Matcher{{
								Name:  "no_such_label",
								Type:  base.MatchEqual,
								Value: "value",
							}},
						},
						{
							Start: start,
							End:   end,
							Matchers: []base.Matcher{{
								Name:  "no_such_label",
								Type:  base.MatchRegexp,
								Value: "value",
							}},
						},
					} {
						t.Run(q.String(), func(t *testing.T) {
							data, err := storage.Read(context.Background(), []base.Query{q})
							require.NoError(t, err)
							require.Len(t, data.Results, 1)
							require.Len(t, data.Results[0].TimeSeries, 0)
						})
					}
				})

				t.Run("BySeveralMatchers", func(t *testing.T) {
					for _, q := range []base.Query{
						{
							Start: start,
							End:   end,
							Matchers: []base.Matcher{{
								Name:  "__name__",
								Type:  base.MatchEqual,
								Value: "http_requests_total",
							}, {
								Name:  "no_such_label",
								Type:  base.MatchNotEqual,
								Value: "no_such_value",
							}},
						},
						{
							Start: start,
							End:   end,
							Matchers: []base.Matcher{{
								Name:  "no_such_label",
								Type:  base.MatchNotEqual,
								Value: "no_such_value",
							}, {
								Name:  "__name__",
								Type:  base.MatchEqual,
								Value: "http_requests_total",
							}},
						},
						{
							Start: start,
							End:   end,
							Matchers: []base.Matcher{{
								Name:  "no_such_label",
								Type:  base.MatchNotEqual,
								Value: "no_such_value",
							}, {
								Name:  "no_this_label",
								Type:  base.MatchEqual,
								Value: "",
							}, {
								Name:  "__name__",
								Type:  base.MatchEqual,
								Value: "http_requests_total",
							}},
						},
					} {
						t.Run(q.String(), func(t *testing.T) {
							data, err := storage.Read(context.Background(), []base.Query{q})
							require.NoError(t, err)
							require.Len(t, data.Results, 1)
							require.Len(t, data.Results[0].TimeSeries, 3)
							timeseries.SortTimeSeriesSlow(data.Results[0].TimeSeries)
							for i, actual := range data.Results[0].TimeSeries {
								timeseries.SortLabels(actual.Labels)
								expected := storedData.TimeSeries[i]
								assert.Equal(t, expected, actual, messageTS(expected, actual))
							}
						})
					}
				})

				t.Run("Empty", func(t *testing.T) {
					// We can expect the following from Prometheus (from https://prometheus.io/docs/querying/basics/):
					//   * Label matchers that match empty label values also select all time series that do not have the specific label set at all.
					//   * At least one matcher should have non-empty label value.
					// See also test cases at https://github.com/prometheus/prometheus/blob/v2.2.1/promql/parse_test.go#L919-L939
					// But as an extension we allow such cases because they are useful for querying data and load tests.

					// queries returning all data
					for _, q := range []base.Query{
						{
							Start: start,
							End:   end,
						},
						{
							Start: start,
							End:   end,
							Matchers: []base.Matcher{{
								Name:  "__name__",
								Type:  base.MatchNotEqual,
								Value: "",
							}},
						},
						{
							Start: start,
							End:   end,
							Matchers: []base.Matcher{{
								Name:  "__name__",
								Type:  base.MatchNotEqual,
								Value: "no_such_metric",
							}},
						},
						{
							Start: start,
							End:   end,
							Matchers: []base.Matcher{{
								Name:  "no_such_label",
								Type:  base.MatchEqual,
								Value: "",
							}},
						},
						{
							Start: start,
							End:   end,
							Matchers: []base.Matcher{{
								Name:  "no_such_label",
								Type:  base.MatchNotEqual,
								Value: "value",
							}},
						},
					} {
						t.Run(q.String(), func(t *testing.T) {
							data, err := storage.Read(context.Background(), []base.Query{q})
							require.NoError(t, err)
							require.Len(t, data.Results, 1)
							require.Len(t, data.Results[0].TimeSeries, 3)
							timeseries.SortTimeSeriesSlow(data.Results[0].TimeSeries)
							for i, actual := range data.Results[0].TimeSeries {
								timeseries.SortLabels(actual.Labels)
								expected := storedData.TimeSeries[i]
								assert.Equal(t, expected, actual, messageTS(expected, actual))
							}
						})
					}

					// queries returning nothing
					for _, q := range []base.Query{
						{
							Start: start,
							End:   end,
							Matchers: []base.Matcher{{
								Name:  "__name__",
								Type:  base.MatchEqual,
								Value: "",
							}},
						},
						{
							Start: start,
							End:   end,
							Matchers: []base.Matcher{{
								Name:  "no_such_label",
								Type:  base.MatchNotEqual,
								Value: "",
							}},
						},
					} {
						t.Run(q.String(), func(t *testing.T) {
							data, err := storage.Read(context.Background(), []base.Query{q})
							require.NoError(t, err)
							require.Len(t, data.Results, 1)
							require.Len(t, data.Results[0].TimeSeries, 0)
						})
					}
				})

				if storageName == "ClickHouse" {
					t.Run("ByRawSQL", func(t *testing.T) {
						for _, q := range []base.Query{
							{
								Start: start,
								End:   end,
								Matchers: []base.Matcher{{
									Name:  "job",
									Type:  base.MatchEqual,
									Value: "rawsql",
								}, {
									Name:  "query",
									Type:  base.MatchEqual,
									Value: "SELECT * FROM samples ORDER BY fingerprint",
								}},
							},
						} {
							t.Run(q.String(), func(t *testing.T) {
								data, err := storage.Read(context.Background(), []base.Query{q})
								require.NoError(t, err)
								require.Len(t, data.Results, 1)

								// for _, ts := range data.Results[0].TimeSeries {
								// 	t.Log(formatTS(ts))
								// }

								require.Len(t, data.Results[0].TimeSeries, 15)
							})
						}
					})
				}
			})

			t.Run("WriteFunnyLabels", func(t *testing.T) {
				s := []*prompb.Sample{{Value: 1, TimestampMs: int64(start)}}
				storedData := &prompb.WriteRequest{
					TimeSeries: []*prompb.TimeSeries{
						{Labels: []*prompb.Label{{"__name__", "funny_1"}, {"label", ""}}, Samples: s},
						{Labels: []*prompb.Label{{"__name__", "funny_2"}, {"label", "'`\"\\"}}, Samples: s},
						{Labels: []*prompb.Label{{"__name__", "funny_3"}, {"label", "''``\"\"\\\\"}}, Samples: s},
						{Labels: []*prompb.Label{{"__name__", "funny_4"}, {"label", "'''```\"\"\"\\\\\\"}}, Samples: s},
						{Labels: []*prompb.Label{{"__name__", "funny_5"}, {"label", `\ \\ \\\\ \\\\`}}, Samples: s},
						{Labels: []*prompb.Label{{"__name__", "funny_6"}, {"label", "🆗"}}, Samples: s},
					},
				}
				require.NoError(t, storage.Write(context.Background(), storedData))

				q := base.Query{
					Start: start,
					End:   end,
					Matchers: []base.Matcher{{
						Name:  "__name__",
						Type:  base.MatchRegexp,
						Value: "funny_.+",
					}},
				}

				data, err := storage.Read(context.Background(), []base.Query{q})
				require.NoError(t, err)
				require.Len(t, data.Results, 1)
				require.Len(t, data.Results[0].TimeSeries, len(storedData.TimeSeries))
				timeseries.SortTimeSeriesSlow(data.Results[0].TimeSeries)
				for i, actual := range data.Results[0].TimeSeries {
					timeseries.SortLabels(actual.Labels)
					expected := storedData.TimeSeries[i]
					assert.Equal(t, expected, actual, messageTS(expected, actual))
				}
			})

			t.Run("Metrics", func(t *testing.T) {
				descCh := make(chan *prometheus.Desc)
				go func() {
					storage.Describe(descCh)
					close(descCh)
				}()

				var descs []*prometheus.Desc
				for d := range descCh {
					descs = append(descs, d)
				}

				metricsCh := make(chan prometheus.Metric)
				go func() {
					storage.Collect(metricsCh)
					close(metricsCh)
				}()

				for m := range metricsCh {
					var found bool
					for _, d := range descs {
						if m.Desc() == d {
							found = true
							break
						}
					}
					assert.True(t, found)
				}
			})
		})
	}
}

func BenchmarkStorages(b *testing.B) {
	for storageName, newStorage := range map[string]func() (base.Storage, error){
		"Memory": func() (base.Storage, error) {
			return memory.New(), nil
		},
		"ClickHouseTempTable": func() (base.Storage, error) {
			params := &clickhouse.Params{
				DSN:          "tcp://127.0.0.1:9000/?database=prometheus_test",
				DropDatabase: true,
			}
			return clickhouse.New(params)
		},
		"ClickHouseQuery": func() (base.Storage, error) {
			params := &clickhouse.Params{
				DSN:                  "tcp://127.0.0.1:9000/?database=prometheus_test",
				DropDatabase:         true,
				MaxTimeSeriesInQuery: 1000,
			}
			return clickhouse.New(params)
		},
	} {
		b.Run(storageName, func(b *testing.B) {
			storedData := test.GetData()
			storage, err := newStorage()
			require.NoError(b, err)

			b.Run("Write", func(b *testing.B) {
				var err error
				for i := 0; i < b.N; i++ {
					err = storage.Write(context.Background(), storedData)
				}
				require.NoError(b, err)
			})
		})
	}
}
