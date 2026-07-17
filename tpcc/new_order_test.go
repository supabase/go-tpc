package tpcc

import (
	"reflect"
	"testing"
)

func TestNewOrderStockArgsUseSupplyWarehouse(t *testing.T) {
	items := []orderItem{
		{
			olSupplyWID:     1,
			olIID:           101,
			olQuantity:      3,
			sQuantity:       42,
			remoteWarehouse: 0,
		},
		{
			olSupplyWID:     7,
			olIID:           202,
			olQuantity:      5,
			sQuantity:       36,
			remoteWarehouse: 1,
		},
	}

	wantSelect := []interface{}{1, 101, 7, 202}
	if got := newOrderSelectStockArgs(items); !reflect.DeepEqual(got, wantSelect) {
		t.Fatalf("newOrderSelectStockArgs() = %#v, want %#v", got, wantSelect)
	}

	wantUpdate := []interface{}{36, 5, 1, 202, 7}
	if got := newOrderUpdateStockArgs(&items[1]); !reflect.DeepEqual(got, wantUpdate) {
		t.Fatalf("newOrderUpdateStockArgs() = %#v, want %#v", got, wantUpdate)
	}
}
