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
	"sort"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func getData() *prompb.WriteRequest {
	start := model.Now().Add(-6 * time.Second)

	return &prompb.WriteRequest{
		Timeseries: []*prompb.TimeSeries{
			{
				Labels: []*prompb.Label{
					{Name: "__name__", Value: "http_requests_total"},
					{Name: "code", Value: "200"},
					{Name: "handler", Value: "query"},
				},
				Samples: []*prompb.Sample{
					{Value: 13, Timestamp: int64(start)},
					{Value: 14, Timestamp: int64(start.Add(1 * time.Second))},
					{Value: 14, Timestamp: int64(start.Add(2 * time.Second))},
					{Value: 14, Timestamp: int64(start.Add(3 * time.Second))},
					{Value: 15, Timestamp: int64(start.Add(4 * time.Second))},
				},
			},
			{
				Labels: []*prompb.Label{
					{Name: "__name__", Value: "http_requests_total"},
					{Name: "code", Value: "400"},
					{Name: "handler", Value: "query_range"},
				},
				Samples: []*prompb.Sample{
					{Value: 9, Timestamp: int64(start)},
					{Value: 9, Timestamp: int64(start.Add(1 * time.Second))},
					{Value: 9, Timestamp: int64(start.Add(2 * time.Second))},
					{Value: 11, Timestamp: int64(start.Add(3 * time.Second))},
					{Value: 11, Timestamp: int64(start.Add(4 * time.Second))},
				},
			},
			{
				Labels: []*prompb.Label{
					{Name: "__name__", Value: "http_requests_total"},
					{Name: "code", Value: "200"},
					{Name: "handler", Value: "prometheus"},
				},
				Samples: []*prompb.Sample{
					{Value: 591, Timestamp: int64(start)},
					{Value: 592, Timestamp: int64(start.Add(1 * time.Second))},
					{Value: 593, Timestamp: int64(start.Add(2 * time.Second))},
					{Value: 594, Timestamp: int64(start.Add(3 * time.Second))},
					{Value: 595, Timestamp: int64(start.Add(4 * time.Second))},
				},
			},
		},
	}
}

// sortTimeSeries sorts timeseries by a value of __name__ label.
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
		return nameI < nameJ
	})
}

// sortLabels sorts labels by name.
func sortLabels(labels []*prompb.Label) {
	sort.Slice(labels, func(i, j int) bool { return labels[i].Name < labels[j].Name })
}

func TestStorages(t *testing.T) {
	// level := logrus.GetLevel()
	// logrus.SetLevel(logrus.DebugLevel)
	// defer logrus.SetLevel(level)

	for storageName, newStorage := range map[string]func() (Storage, error){
		"Memory":     func() (Storage, error) { return NewMemory(), nil },
		"ClickHouse": func() (Storage, error) { return NewClickHouse("tcp://127.0.0.1:9000", "prometheus_test", true) },
	} {
		t.Run(storageName, func(t *testing.T) {
			// Label matchers that match empty label values also select all time series that do not have the specific label set at all.
			// At least one matcher should have non-empty label value.

			storedData := getData()
			storage, err := newStorage()
			require.NoError(t, err)
			require.NoError(t, storage.Write(context.Background(), storedData))

			start := model.Now().Add(-time.Minute)
			end := model.Now()

			t.Run("ReadByName", func(t *testing.T) {
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
							Value: "no_such_label",
						}},
					},
				} {
					t.Run(q.String(), func(t *testing.T) {
						data, err := storage.Read(context.Background(), []Query{q})
						assert.NoError(t, err)
						require.Len(t, data.Results, 1)
						require.Len(t, data.Results[0].Timeseries, 3)
						for i, ts := range data.Results[0].Timeseries {
							sortLabels(ts.Labels)
							assert.Equal(t, storedData.Timeseries[i], ts)
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
							Value: "no_such_label",
						}},
					},

					// TODO should it return 3 series with 0 values, or 0 values?
					{
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
						assert.NoError(t, err)
						require.Len(t, data.Results, 1)
						require.Len(t, data.Results[0].Timeseries, 0)
					})
				}
			})

			t.Run("ReadByNameRegexp", func(t *testing.T) {
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
						assert.NoError(t, err)
						require.Len(t, data.Results, 1)
						require.Len(t, data.Results[0].Timeseries, 3)
						for i, ts := range data.Results[0].Timeseries {
							sortLabels(ts.Labels)
							assert.Equal(t, storedData.Timeseries[i], ts)
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

					// TODO should it return 3 series with 0 values, or 0 values?
					{
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
						assert.NoError(t, err)
						require.Len(t, data.Results, 1)
						require.Len(t, data.Results[0].Timeseries, 0)
					})
				}
			})

			t.Run("WriteFunnyLabels", func(t *testing.T) {
				s := []*prompb.Sample{{Value: 1, Timestamp: int64(start)}}
				storedData := &prompb.WriteRequest{
					Timeseries: []*prompb.TimeSeries{
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
				assert.NoError(t, err)
				require.Len(t, data.Results, 1)
				require.Len(t, data.Results[0].Timeseries, len(storedData.Timeseries))
				sortTimeSeries(data.Results[0].Timeseries)
				for i, ts := range data.Results[0].Timeseries {
					sortLabels(ts.Labels)
					assert.Equal(t, storedData.Timeseries[i], ts)
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
