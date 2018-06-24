package simulator

import (
	"testing"
)

func TestConnMatSums(t *testing.T) {
	mat := NewConnMat(4)
	mat.Set(1, 2, 3.0)
	mat.Set(0, 2, 2.0)
	mat.Set(2, 3, 4.0)
	if res := mat.SumDest(0); res != 0.0 {
		t.Errorf("expected sum of 0.0 but got %f", res)
	}
	if res := mat.SumDest(1); res != 0.0 {
		t.Errorf("expected sum of 0.0 but got %f", res)
	}
	if res := mat.SumDest(2); res != 5.0 {
		t.Errorf("expected sum of 5.0 but got %f", res)
	}
	if res := mat.SumDest(3); res != 4.0 {
		t.Errorf("expected sum of 4.0 but got %f", res)
	}
	if res := mat.SumSource(0); res != 2.0 {
		t.Errorf("expected sum of 2.0 but got %f", res)
	}
	if res := mat.SumSource(1); res != 3.0 {
		t.Errorf("expected sum of 3.0 but got %f", res)
	}
	if res := mat.SumSource(2); res != 4.0 {
		t.Errorf("expected sum of 4.0 but got %f", res)
	}
	if res := mat.SumSource(3); res != 0.0 {
		t.Errorf("expected sum of 0.0 but got %f", res)
	}
}

func TestConnMatScales(t *testing.T) {
	mat := NewConnMat(4)
	mat.Set(1, 2, 3.0)
	mat.Set(1, 3, 5.0)
	mat.Set(0, 2, 2.0)
	mat.Set(2, 3, 4.0)

	mat.ScaleSource(1, 2.0)
	for i, expected := range []float64{0, 0, 3.0 * 2.0, 5.0 * 2.0} {
		if res := mat.Get(1, i); res != expected {
			t.Errorf("column %d: expected %f but got %f", i, expected, res)
		}
	}

	mat.ScaleDest(3, 3.0)
	for i, expected := range []float64{0, 5.0 * 2.0 * 3.0, 4.0 * 3.0, 0} {
		if res := mat.Get(i, 3); res != expected {
			t.Errorf("row %d: expected %f but got %f", i, expected, res)
		}
	}
}
