package customerscli

import "testing"

func TestGetSortBy(t *testing.T) {
	if sortBy := GetSortBy("ort=name"); sortBy != "name" {
		t.Errorf("kingpin bug not fixed")
	}
}
