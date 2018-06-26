package allreduce

import "testing"

func TestNaiveAllreducer(t *testing.T) {
	RunAllreducerTests(t, NaiveAllreducer{})
}
