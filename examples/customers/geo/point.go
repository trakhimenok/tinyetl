package geo

import "fmt"

// Point represents a physical point in geographic notation [latitude, longitude].
type Point struct {
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
}

// String renders geo point as "[latitude, longitude]""
func (p Point) String() string {
	return fmt.Sprintf("[%v, %v]", p.Latitude, p.Longitude)
}
