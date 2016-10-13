// Copyright 2014 Prometheus Team
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"flag"
	"fmt"
	"io"
	"mime"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/heroku/metaas/v2/api/client"
	"github.com/heroku/metaas/v2/schema"
	"github.com/matttproud/golang_protobuf_extensions/pbutil"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/log"

	dto "github.com/prometheus/client_model/go"
)

var skipLabels = map[string]bool{
	"handler": true,
}

const acceptHeader = `application/vnd.google.protobuf;proto=io.prometheus.client.MetricFamily;encoding=delimited;q=0.7,text/plain;version=0.0.4;q=0.3`

type metricFamily struct {
	Name    string        `json:"name"`
	Help    string        `json:"help"`
	Type    string        `json:"type"`
	Metrics []interface{} `json:"metrics,omitempty"` // Either metric or summary.
}

// metric is for all "single value" metrics.
type metric struct {
	Labels map[string]string `json:"labels,omitempty"`
	Value  string            `json:"value"`
}

type summary struct {
	Labels    map[string]string `json:"labels,omitempty"`
	Quantiles map[string]string `json:"quantiles,omitempty"`
	Count     string            `json:"count"`
	Sum       string            `json:"sum"`
}

type histogram struct {
	Labels  map[string]string `json:"labels,omitempty"`
	Buckets map[string]string `json:"buckets,omitempty"`
	Count   string            `json:"count"`
	Sum     string            `json:"sum"`
}

func newObservation(ts time.Time, dtoMF *dto.MetricFamily) *schema.Observation {
	name := dtoMF.GetName()
	obs := &schema.Observation{
		Timestamp:    ts.Unix(),
		Measurements: &schema.Measurements{},
	}

	switch dtoMF.GetType() {
	case dto.MetricType_GAUGE:
		obs.Type = schema.MetricType_GAUGE
		obs.Measurements.Names = make([]string, len(dtoMF.Metric))
		obs.Measurements.Values = make([]float64, len(dtoMF.Metric))

		for idx, m := range dtoMF.Metric {
			obs.Measurements.Names[idx] = name + suffixFor(m)
			obs.Measurements.Values[idx] = getValue(m)
		}
		return obs

	case dto.MetricType_COUNTER:
		obs.Type = schema.MetricType_COUNTER
		obs.Measurements.Names = make([]string, len(dtoMF.Metric))
		obs.Measurements.Values = make([]float64, len(dtoMF.Metric))

		for idx, m := range dtoMF.Metric {
			obs.Measurements.Names[idx] = name + suffixFor(m)
			obs.Measurements.Values[idx] = getValue(m)
		}

		return obs

	case dto.MetricType_SUMMARY:
		obs.Type = schema.MetricType_GAUGE

		for _, m := range dtoMF.Metric {
			suffix := suffixFor(m)

			obs.Measurements.Names = append(obs.Measurements.Names, name+suffix+"_count")
			obs.Measurements.Values = append(obs.Measurements.Values, float64(m.GetSummary().GetSampleCount()))

			obs.Measurements.Names = append(obs.Measurements.Names, name+suffix+"_sum")
			obs.Measurements.Values = append(obs.Measurements.Values, m.GetSummary().GetSampleSum())

			for _, q := range m.GetSummary().Quantile {
				obs.Measurements.Names = append(obs.Measurements.Names, name+suffix+"_p"+strconv.Itoa(int(100*q.GetQuantile())))
				obs.Measurements.Values = append(obs.Measurements.Values, q.GetValue())
			}
		}

		return obs

	case dto.MetricType_HISTOGRAM:
		log.Println("Skipping HISTOGRAM")
	case dto.MetricType_UNTYPED:
		log.Println("Skipping UNTYPED")
	}
	return nil
}

func newMetricFamily(dtoMF *dto.MetricFamily) *metricFamily {
	mf := &metricFamily{
		Name:    dtoMF.GetName(),
		Help:    dtoMF.GetHelp(),
		Type:    dtoMF.GetType().String(),
		Metrics: make([]interface{}, len(dtoMF.Metric)),
	}
	for i, m := range dtoMF.Metric {
		if dtoMF.GetType() == dto.MetricType_SUMMARY {
			mf.Metrics[i] = summary{
				Labels:    makeLabels(m),
				Quantiles: makeQuantiles(m),
				Count:     fmt.Sprint(m.GetSummary().GetSampleCount()),
				Sum:       fmt.Sprint(m.GetSummary().GetSampleSum()),
			}
		} else if dtoMF.GetType() == dto.MetricType_HISTOGRAM {
			mf.Metrics[i] = histogram{
				Labels:  makeLabels(m),
				Buckets: makeBuckets(m),
				Count:   fmt.Sprint(m.GetHistogram().GetSampleCount()),
				Sum:     fmt.Sprint(m.GetSummary().GetSampleSum()),
			}
		} else {
			mf.Metrics[i] = metric{
				Labels: makeLabels(m),
				Value:  fmt.Sprint(getValue(m)),
			}
		}
	}
	return mf
}

func getValue(m *dto.Metric) float64 {
	if m.Gauge != nil {
		return m.GetGauge().GetValue()
	}
	if m.Counter != nil {
		return m.GetCounter().GetValue()
	}
	if m.Untyped != nil {
		return m.GetUntyped().GetValue()
	}
	return 0.
}

func suffixFor(m *dto.Metric) string {
	result := make([]string, 0, len(m.Label))

	for _, lp := range m.Label {
		if skipLabels[lp.GetName()] {
			continue
		}
		result = append(result, lp.GetName()+"_"+lp.GetValue())
	}

	if len(result) == 0 {
		return ""
	}
	return "_" + strings.Join(result, "_")
}

func makeLabels(m *dto.Metric) map[string]string {
	result := map[string]string{}
	for _, lp := range m.Label {
		result[lp.GetName()] = lp.GetValue()
	}
	return result
}

func makeQuantiles(m *dto.Metric) map[string]string {
	result := map[string]string{}
	for _, q := range m.GetSummary().Quantile {
		result[fmt.Sprint(q.GetQuantile())] = fmt.Sprint(q.GetValue())
	}
	return result
}

func makeBuckets(m *dto.Metric) map[string]string {
	result := map[string]string{}
	for _, b := range m.GetHistogram().Bucket {
		result[fmt.Sprint(b.GetUpperBound())] = fmt.Sprint(b.GetCumulativeCount())
	}
	return result
}

func fetchMetricFamilies(url string, ch chan<- *dto.MetricFamily) {
	defer close(ch)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		log.Fatalf("creating GET request for URL %q failed: %s", url, err)
	}
	req.Header.Add("Accept", acceptHeader)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Fatalf("executing GET request for URL %q failed: %s", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		log.Fatalf("GET request for URL %q returned HTTP status %s", url, resp.Status)
	}

	mediatype, params, err := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if err == nil && mediatype == "application/vnd.google.protobuf" &&
		params["encoding"] == "delimited" &&
		params["proto"] == "io.prometheus.client.MetricFamily" {
		for {
			mf := &dto.MetricFamily{}
			if _, err = pbutil.ReadDelimited(resp.Body, mf); err != nil {
				if err == io.EOF {
					break
				}
				log.Fatalln("reading metric family protocol buffer failed:", err)
			}
			ch <- mf
		}
	} else {
		// We could do further content-type checks here, but the
		// fallback for now will anyway be the text format
		// version 0.0.4, so just go for it and see if it works.
		var parser expfmt.TextParser
		metricFamilies, err := parser.TextToMetricFamilies(resp.Body)
		if err != nil {
			log.Fatalln("reading text format failed:", err)
		}
		for _, mf := range metricFamilies {
			ch <- mf
		}
	}
}

var (
	scrapeURL = flag.String("scrape-url", "", "Scrape URL")
	sinkURL   = flag.String("url", "", "Destination URL")
	instance  = flag.String("instance", "", "Instance label to utilize")
	interval  = flag.Int("interval", 5, "Default interval for waiting between posts")
)

func main() {
	// Limit processing power to max 2 CPUs.
	runtime.GOMAXPROCS(2)

	flag.Parse()

	if *sinkURL == "" || *scrapeURL == "" || *instance == "" {
		flag.Usage()
		os.Exit(2)
	}

	interval := time.Second * time.Duration(*interval)
	timer := time.NewTimer(interval)

	for {
		select {
		case <-timer.C:
			mfChan := make(chan *dto.MetricFamily, 1024)
			go fetchMetricFamilies(*scrapeURL, mfChan)

			now := time.Now()
			var observations []*schema.Observation
			for mf := range mfChan {
				if o := newObservation(now, mf); o != nil {
					observations = append(observations, o)
				}
			}

			c := client.New(*sinkURL)
			err := c.PostObservations(&schema.Observations{Observations: observations}, client.Opts{Dyno: *instance})
			if err != nil {
				log.Printf("err=%s", err)
			}
		}
		timer.Reset(interval)
	}
}
