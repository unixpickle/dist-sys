package simulator

import (
	"math"
	"testing"
)

func TestGreedyDropSwitcher(t *testing.T) {
	switcher := &GreedyDropSwitcher{
		SendRates: []float64{1.0, 2.0, 3.0},
		RecvRates: []float64{2.0, 1.0, 1.0},
	}
	inputMatrices := [][]float64{
		{
			0.0, 1.0, 0.0,
			0.0, 0.0, 1.0,
			1.0, 0.0, 0.0,
		},
		{
			1.0, 0.0, 0.0,
			1.0, 0.0, 0.0,
			1.0, 0.0, 0.0,
		},
		{
			1.0, 1.0, 1.0,
			1.0, 1.0, 1.0,
			1.0, 1.0, 1.0,
		},
	}
	outputMatrices := [][]float64{
		{
			0.0, 1.0, 0.0,
			0.0, 0.0, 1.0,
			2.0, 0.0, 0.0,
		},
		{
			1.0 / 3.0, 0.0, 0.0,
			2.0 / 3.0, 0.0, 0.0,
			3.0 / 3.0, 0.0, 0.0,
		},
		{
			1.0 / 3.0, 1.0 / 6.0, 1.0 / 6.0,
			2.0 / 3.0, 2.0 / 6.0, 2.0 / 6.0,
			3.0 / 3.0, 3.0 / 6.0, 3.0 / 6.0,
		},
	}
	for i, input := range inputMatrices {
		output := outputMatrices[i]
		connMat := &ConnMat{numNodes: 3, rates: input}
		switcher.SwitchedRates(connMat)
		for j, actual := range connMat.rates {
			if math.Abs(actual-output[j]) > 0.001 {
				t.Errorf("test %d: expected %v but got %v", i, output, connMat.rates)
				break
			}
		}
	}
}
