package models

//go:generate ffjson $GOFILE

import (
	"errors"
	"github.com/astec/tinyetl/examples/customers/geo"
	"strconv"
)

type CustomerShort struct {
	UserID int    `json:"user_id"`
	Name   string `json:"name"`
}

type CustomerExtended struct {
	CustomerShort
	Latitude  string `json:"latitude"`
	Longitude string `json:"longitude"`
}

func (j CustomerExtended) GeoPoint() (point geo.Point, err error) {
	if point.Latitude, err = strconv.ParseFloat(j.Latitude, 64); err != nil {
		err = errors.New("failed to parse latitude to float")
		return
	}
	if point.Longitude, err = strconv.ParseFloat(j.Longitude, 64); err != nil {
		err = errors.New("failed to parse longitude to float")
		return
	}
	return
}
