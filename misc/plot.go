package main

import (
	"fmt"
	"image/color"
	"math"
	"math/rand"
	"time"

	"github.com/gonum/plot"
	"github.com/gonum/plot/plotter"
	"github.com/gonum/plot/vg"
	"github.com/gonum/stat/distuv"
)

func main() {
	const mean = 5000.0
	const percent = 20.0
	const buckets = 10

	const fraction = percent / 100.0
	rand.Seed(time.Now().UnixNano())
	values := make(plotter.Values, 10000)
	stddev := math.Sqrt(mean * fraction)
	min := math.MaxFloat64
	max := -math.MaxFloat64
	for i := range values {
		v := rand.NormFloat64()*stddev + mean
		if v < min {
			min = v
		}
		if v > max {
			max = v
		}
		values[i] = v
	}

	// Make a plot and set its title.
	p, err := plot.New()
	if err != nil {
		panic(err)
	}
	p.Title.Text = fmt.Sprintf("%.f±%.f%%: stddev=%.1f, min=%.1f, max=%.1f", mean, percent, stddev, min, max)
	p.X.Min = mean - mean*fraction
	p.X.Max = mean + mean*fraction

	// Create a histogram of our values drawn
	// from the standard normal.
	h, err := plotter.NewHist(values, buckets)
	if err != nil {
		panic(err)
	}
	// Normalize the area under the histogram to
	// sum to one.
	h.Normalize(1)
	p.Add(h)

	// The normal distribution function
	norm := plotter.NewFunction(distuv.Normal{mean, stddev, nil}.Prob)
	norm.Color = color.RGBA{R: 255, A: 255}
	norm.Width = vg.Points(2)
	p.Add(norm)

	var ticks []plot.Tick
	add := func(x float64) {
		ticks = append(ticks, plot.Tick{
			Value: x,
			Label: fmt.Sprintf("%.1f", x),
		})
	}
	for x := mean; x < p.X.Max; x += (p.X.Max - mean) / buckets * 2 {
		add(x)
	}
	for x := mean; x > p.X.Min; x += (p.X.Min - mean) / buckets * 2 {
		add(x)
	}
	add(p.X.Min)
	add(p.X.Max)
	p.X.Tick.Marker = plot.ConstantTicks(ticks)

	if err := p.Save(20*vg.Centimeter, 20*vg.Centimeter, "plot.png"); err != nil {
		panic(err)
	}
}
