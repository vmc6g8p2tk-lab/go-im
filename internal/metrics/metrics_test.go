package metrics

import (
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestMetricsExpositionUsesActualObservations(t *testing.T) {
	m := New()
	m.RegisterHubGauges(func() float64 { return 2 }, func() float64 { return 3 })
	m.Messages.WithLabelValues("direct", "created").Inc()
	m.DeliveryConfirmed.Inc()
	m.ObserveRedis("ping", time.Now().Add(-time.Millisecond), nil)
	recorder := httptest.NewRecorder()
	m.Handler().ServeHTTP(recorder, httptest.NewRequest("GET", "/metrics", nil))
	if recorder.Code != 200 {
		t.Fatalf("metrics status=%d", recorder.Code)
	}
	body := recorder.Body.String()
	for _, wanted := range []string{
		`im_websocket_connections 2`,
		`im_websocket_send_queue_frames 3`,
		`im_messages_total{kind="direct",result="created"} 1`,
		`im_delivery_confirmed_total 1`,
		`im_redis_operation_seconds_count{operation="ping",result="ok"} 1`,
	} {
		if !strings.Contains(body, wanted) {
			t.Errorf("missing %q", wanted)
		}
	}
}
