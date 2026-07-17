package tpcc

import (
	"reflect"
	"testing"
)

func TestBuildStockSelectArgs(t *testing.T) {
	const homeWID = 3
	const remoteWID = 7

	items := []orderItem{
		{olIID: 100, olSupplyWID: homeWID},
		{olIID: 200, olSupplyWID: remoteWID, remoteWarehouse: 1},
		{olIID: 300, olSupplyWID: homeWID},
	}

	got := buildStockSelectArgs(items)
	want := []interface{}{
		homeWID, 100,
		remoteWID, 200,
		homeWID, 300,
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("buildStockSelectArgs() = %v, want %v", got, want)
	}
}

func TestStockUpdateArgs(t *testing.T) {
	const homeWID = 3
	const remoteWID = 7

	cases := []struct {
		name string
		item orderItem
		want []interface{}
	}{
		{
			name: "home warehouse",
			item: orderItem{
				olIID:           100,
				olSupplyWID:     homeWID,
				olQuantity:      5,
				remoteWarehouse: 0,
				sQuantity:       42,
			},
			want: []interface{}{42, 5, 0, 100, homeWID},
		},
		{
			name: "remote warehouse",
			item: orderItem{
				olIID:           200,
				olSupplyWID:     remoteWID,
				olQuantity:      2,
				remoteWarehouse: 1,
				sQuantity:       17,
			},
			want: []interface{}{17, 2, 1, 200, remoteWID},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := stockUpdateArgs(&tc.item)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("stockUpdateArgs() = %v, want %v", got, tc.want)
			}
		})
	}
}
