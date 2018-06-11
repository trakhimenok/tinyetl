package models

import (
	"testing"
	"encoding/json"
)

func TestCustomerExtended_UnmarshalJSON(t *testing.T) {
	var customer CustomerExtended
	err := json.Unmarshal([]byte(`{"latitude": "52.986375", "user_id": 12, "name": "Christina McArdle", "longitude": "-6.043701"}`), &customer)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if customer.UserID != 12 {
		t.Errorf("unexpected UserID: %v", customer.UserID)
	}
	if customer.Name != "Christina McArdle" {
		t.Errorf("unexpected Name: %v", customer.Name)
	}
	if customer.Latitude != "52.986375" {
		t.Errorf("unexpected Latitude: %v", customer.Latitude)
	}
	if customer.Longitude != "-6.043701" {
		t.Errorf("unexpected Latitude: %v", customer.Longitude)
	}
}
