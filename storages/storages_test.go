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
	"sort"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Percona-Lab/PromHouse/prompb"
)

func getData() *prompb.WriteRequest {
	start := model.Now().Add(-6 * time.Second)

	return &prompb.WriteRequest{
		TimeSeries: []*prompb.TimeSeries{
			{
				Labels: []*prompb.Label{
					{Name: "__name__", Value: "http_requests_total"},
					{Name: "code", Value: "200"},
					{Name: "handler", Value: "query"},
				},
				Samples: []*prompb.Sample{
					{Value: 13, TimestampMs: int64(start)},
					{Value: 14, TimestampMs: int64(start.Add(1 * time.Second))},
					{Value: 14, TimestampMs: int64(start.Add(2 * time.Second))},
					{Value: 14, TimestampMs: int64(start.Add(3 * time.Second))},
					{Value: 15, TimestampMs: int64(start.Add(4 * time.Second))},
				},
			},
			{
				Labels: []*prompb.Label{
					{Name: "__name__", Value: "http_requests_total"},
					{Name: "code", Value: "400"},
					{Name: "handler", Value: "query_range"},
				},
				Samples: []*prompb.Sample{
					{Value: 9, TimestampMs: int64(start)},
					{Value: 9, TimestampMs: int64(start.Add(1 * time.Second))},
					{Value: 9, TimestampMs: int64(start.Add(2 * time.Second))},
					{Value: 11, TimestampMs: int64(start.Add(3 * time.Second))},
					{Value: 11, TimestampMs: int64(start.Add(4 * time.Second))},
				},
			},
			{
				Labels: []*prompb.Label{
					{Name: "__name__", Value: "http_requests_total"},
					{Name: "code", Value: "200"},
					{Name: "handler", Value: "prometheus"},
				},
				Samples: []*prompb.Sample{
					{Value: 591, TimestampMs: int64(start)},
					{Value: 592, TimestampMs: int64(start.Add(1 * time.Second))},
					{Value: 593, TimestampMs: int64(start.Add(2 * time.Second))},
					{Value: 594, TimestampMs: int64(start.Add(3 * time.Second))},
					{Value: 595, TimestampMs: int64(start.Add(4 * time.Second))},
				},
			},
		},
	}
}

// sortTimeSeries sorts timeseries by metric name and fingerprint.
func sortTimeSeries(timeSeries []*prompb.TimeSeries) {
	sort.Slice(timeSeries, func(i, j int) bool {
		var nameI, nameJ string
		for _, l := range timeSeries[i].Labels {
			if l.Name == model.MetricNameLabel {
				nameI = l.Value
				break
			}
		}
		for _, l := range timeSeries[j].Labels {
			if l.Name == model.MetricNameLabel {
				nameJ = l.Value
				break
			}
		}
		if nameI != nameJ {
			return nameI < nameJ
		}

		sortLabels(timeSeries[i].Labels)
		sortLabels(timeSeries[j].Labels)
		return fingerprint(timeSeries[i].Labels) < fingerprint(timeSeries[j].Labels)
	})
}

func makeMetric(labels []*prompb.Label) model.Metric {
	metric := make(model.Metric, len(labels))
	for _, l := range labels {
		metric[model.LabelName(l.Name)] = model.LabelValue(l.Value)
	}
	return metric
}

func formatTS(ts *prompb.TimeSeries) string {
	res := makeMetric(ts.Labels).String()
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

	for storageName, newStorage := range map[string]func() (Storage, error){
		"Memory": func() (Storage, error) {
			return NewMemory(), nil
		},
		"ClickHouse": func() (Storage, error) {
			return NewClickHouse("tcp://127.0.0.1:9000", "prometheus_test", true)
		},
	} {
		t.Run(storageName, func(t *testing.T) {
			// We expect that from Prometheus (from https://prometheus.io/docs/querying/basics/):
			// * Label matchers that match empty label values also select all time series that do not have the specific label set at all.
			// * At least one matcher should have non-empty label value.

			storage, err := newStorage()
			require.NoError(t, err)

			start := model.Now().Add(-time.Minute)
			end := model.Now()

			t.Run("Read", func(t *testing.T) {
				storedData := getData()
				require.NoError(t, storage.Write(context.Background(), storedData))

				t.Run("ByName", func(t *testing.T) {
					// queries returning all data
					for _, q := range []Query{
						{
							Start: start,
							End:   end,
							Matchers: []Matcher{{
								Name:  "__name__",
								Type:  MatchEqual,
								Value: "http_requests_total",
							}},
						},
						{
							Start: start,
							End:   end,
							Matchers: []Matcher{{
								Name:  "__name__",
								Type:  MatchNotEqual,
								Value: "no_such_metric",
							}},
						},
					} {
						t.Run(q.String(), func(t *testing.T) {
							data, err := storage.Read(context.Background(), []Query{q})
							require.NoError(t, err)
							require.Len(t, data.Results, 1)
							require.Len(t, data.Results[0].TimeSeries, 3)
							sortTimeSeries(data.Results[0].TimeSeries)
							for i, actual := range data.Results[0].TimeSeries {
								sortLabels(actual.Labels)
								expected := storedData.TimeSeries[i]
								assert.Equal(t, expected, actual, messageTS(expected, actual))
							}
						})
					}

					// queries returning nothing
					for _, q := range []Query{
						{
							Start: start,
							End:   end,
							Matchers: []Matcher{{
								Name:  "__name__",
								Type:  MatchEqual,
								Value: "no_such_metric",
							}},
						},
						{ // TODO should it return 3 series with 0 values, or 0 values?
							Start: 0,
							End:   0,
							Matchers: []Matcher{{
								Name:  "__name__",
								Type:  MatchEqual,
								Value: "http_requests_total",
							}},
						},
						{
							Start: start,
							End:   end,
							Matchers: []Matcher{{
								Name:  "__name__",
								Type:  MatchNotEqual,
								Value: "http_requests_total",
							}},
						},
					} {
						t.Run(q.String(), func(t *testing.T) {
							data, err := storage.Read(context.Background(), []Query{q})
							require.NoError(t, err)
							require.Len(t, data.Results, 1)
							require.Len(t, data.Results[0].TimeSeries, 0)
						})
					}
				})

				t.Run("ByNameRegexp", func(t *testing.T) {
					// queries returning all data
					for _, q := range []Query{
						{
							Start: start,
							End:   end,
							Matchers: []Matcher{{
								Name:  "__name__",
								Type:  MatchRegexp,
								Value: "http_requests_.+",
							}},
						},
						{
							Start: start,
							End:   end,
							Matchers: []Matcher{{
								Name:  "__name__",
								Type:  MatchNotRegexp,
								Value: "_requests_",
							}},
						},
					} {
						t.Run(q.String(), func(t *testing.T) {
							data, err := storage.Read(context.Background(), []Query{q})
							require.NoError(t, err)
							require.Len(t, data.Results, 1)
							require.Len(t, data.Results[0].TimeSeries, 3)
							sortTimeSeries(data.Results[0].TimeSeries)
							for i, actual := range data.Results[0].TimeSeries {
								sortLabels(actual.Labels)
								expected := storedData.TimeSeries[i]
								assert.Equal(t, expected, actual, messageTS(expected, actual))
							}
						})
					}

					// queries returning nothing
					for _, q := range []Query{
						{
							Start: start,
							End:   end,
							Matchers: []Matcher{{
								Name:  "__name__",
								Type:  MatchRegexp,
								Value: "_requests_",
							}},
						},
						{ // TODO should it return 3 series with 0 values, or 0 values?
							Start: 0,
							End:   0,
							Matchers: []Matcher{{
								Name:  "__name__",
								Type:  MatchRegexp,
								Value: "http_requests_.+",
							}},
						},
						{
							Start: start,
							End:   end,
							Matchers: []Matcher{{
								Name:  "__name__",
								Type:  MatchNotRegexp,
								Value: "http_requests_.+",
							}},
						},
					} {
						t.Run(q.String(), func(t *testing.T) {
							data, err := storage.Read(context.Background(), []Query{q})
							require.NoError(t, err)
							require.Len(t, data.Results, 1)
							require.Len(t, data.Results[0].TimeSeries, 0)
						})
					}
				})

				t.Run("ByNonExistingLabel", func(t *testing.T) {
					for _, q := range []Query{
						{
							Start: start,
							End:   end,
							Matchers: []Matcher{{
								Name:  "no_such_label",
								Type:  MatchEqual,
								Value: "query",
							}},
						},
					} {
						t.Run(q.String(), func(t *testing.T) {
							data, err := storage.Read(context.Background(), []Query{q})
							require.NoError(t, err)
							require.Len(t, data.Results, 1)
							require.Len(t, data.Results[0].TimeSeries, 0)
						})
					}
				})

				t.Run("BySeveralMatchers", func(t *testing.T) {
					for _, q := range []Query{
						{
							Start: start,
							End:   end,
							Matchers: []Matcher{{
								Name:  "__name__",
								Type:  MatchEqual,
								Value: "http_requests_total",
							}, {
								Name:  "no_such_label",
								Type:  MatchNotEqual,
								Value: "no_such_value",
							}},
						},
						{
							Start: start,
							End:   end,
							Matchers: []Matcher{{
								Name:  "no_such_label",
								Type:  MatchNotEqual,
								Value: "no_such_value",
							}, {
								Name:  "__name__",
								Type:  MatchEqual,
								Value: "http_requests_total",
							}},
						},
						{
							Start: start,
							End:   end,
							Matchers: []Matcher{{
								Name:  "no_such_label",
								Type:  MatchNotEqual,
								Value: "no_such_value",
							}, {
								Name:  "no_this_label",
								Type:  MatchEqual,
								Value: "",
							}, {
								Name:  "__name__",
								Type:  MatchEqual,
								Value: "http_requests_total",
							}},
						},
					} {
						t.Run(q.String(), func(t *testing.T) {
							data, err := storage.Read(context.Background(), []Query{q})
							require.NoError(t, err)
							require.Len(t, data.Results, 1)
							require.Len(t, data.Results[0].TimeSeries, 3)
							sortTimeSeries(data.Results[0].TimeSeries)
							for i, actual := range data.Results[0].TimeSeries {
								sortLabels(actual.Labels)
								expected := storedData.TimeSeries[i]
								assert.Equal(t, expected, actual, messageTS(expected, actual))
							}
						})
					}
				})

				if storageName == "ClickHouse" {
					t.Run("ByRawSQL", func(t *testing.T) {
						for _, q := range []Query{
							{
								Start: start,
								End:   end,
								Matchers: []Matcher{{
									Name:  "job",
									Type:  MatchEqual,
									Value: "rawsql",
								}, {
									Name:  "query",
									Type:  MatchEqual,
									Value: "SELECT * FROM samples ORDER BY fingerprint",
								}},
							},
						} {
							t.Run(q.String(), func(t *testing.T) {
								data, err := storage.Read(context.Background(), []Query{q})
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

				q := Query{
					Start: start,
					End:   end,
					Matchers: []Matcher{{
						Name:  "__name__",
						Type:  MatchRegexp,
						Value: "funny_.+",
					}},
				}

				data, err := storage.Read(context.Background(), []Query{q})
				require.NoError(t, err)
				require.Len(t, data.Results, 1)
				require.Len(t, data.Results[0].TimeSeries, len(storedData.TimeSeries))
				sortTimeSeries(data.Results[0].TimeSeries)
				for i, actual := range data.Results[0].TimeSeries {
					sortLabels(actual.Labels)
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
	for storageName, newStorage := range map[string]func() (Storage, error){
		"Memory":     func() (Storage, error) { return NewMemory(), nil },
		"ClickHouse": func() (Storage, error) { return NewClickHouse("tcp://127.0.0.1:9000", "prometheus_test", true) },
	} {
		b.Run(storageName, func(b *testing.B) {
			storedData := getData()
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
