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

func (customer CustomerExtended) GeoPoint() (point geo.Point, err error) {
	if point.Latitude, err = strconv.ParseFloat(customer.Latitude, 64); err != nil {
		err = errors.New("failed to parse latitude to float")
		return
	}
	if point.Longitude, err = strconv.ParseFloat(customer.Longitude, 64); err != nil {
		err = errors.New("failed to parse latitude to float")
		return
	}
	return
}
