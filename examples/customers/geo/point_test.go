package geo

import "testing"

func TestPoint_String(t *testing.T) {
	p := Point{Latitude: 12.3456, Longitude: -65.4321}
	if s := p.String(); s != "[12.3456, -65.4321]" {
		t.Errorf("unexpected result: " + s)
	}
}
