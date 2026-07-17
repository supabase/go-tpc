package tpcc

import (
	"math/rand"
	"testing"

	"github.com/supabase/go-tpc/pkg/util"
)

// refer 2.1.6: NURand(A, x, y) = (((random(0, A) | random(x, y)) + C) % (y - x + 1)) + x

func TestRandCustomerIDRange(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	for i := 0; i < 100000; i++ {
		id := randCustomerID(r)
		if id < 1 || id > 3000 {
			t.Fatalf("randCustomerID() = %d, want within [1, 3000]", id)
		}
	}
}

func TestRandItemIDRange(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	for i := 0; i < 100000; i++ {
		id := randItemID(r)
		if id < 1 || id > 100000 {
			t.Fatalf("randItemID() = %d, want within [1, 100000]", id)
		}
	}
}

// TestRandCustomerIDFormula computes the expected NURand value directly from
// the spec formula using an independently seeded RNG that draws numbers in
// the same order as randCustomerID, guarding against regressing to the
// operator-precedence bug from issue #22 (where C was added inside the OR).
func TestRandCustomerIDFormula(t *testing.T) {
	origC := cCustomerID
	defer func() { cCustomerID = origC }()
	cCustomerID = 42

	r := rand.New(rand.NewSource(7))
	rExpected := rand.New(rand.NewSource(7))

	for i := 0; i < 1000; i++ {
		got := randCustomerID(r)

		a := rExpected.Intn(1024)     // random(0, 1023), A=1023
		x := rExpected.Intn(3000) + 1 // random(1, 3000)
		want := (((a | x) + cCustomerID) % 3000) + 1

		if got != want {
			t.Fatalf("iteration %d: randCustomerID() = %d, want %d", i, got, want)
		}
	}
}

func TestRandItemIDFormula(t *testing.T) {
	origC := cItemID
	defer func() { cItemID = origC }()
	cItemID = 99

	r := rand.New(rand.NewSource(11))
	rExpected := rand.New(rand.NewSource(11))

	for i := 0; i < 1000; i++ {
		got := randItemID(r)

		a := rExpected.Intn(8192)       // random(0, 8191), A=8191
		x := rExpected.Intn(100000) + 1 // random(1, 100000)
		want := (((a | x) + cItemID) % 100000) + 1

		if got != want {
			t.Fatalf("iteration %d: randItemID() = %d, want %d", i, got, want)
		}
	}
}

// TestConstantsInitializedInRange guards against issue #24, where cCustomerID
// and cItemID were assigned from each other's ranges.
func TestConstantsInitializedInRange(t *testing.T) {
	if cCustomerID < 0 || cCustomerID > 1023 {
		t.Fatalf("cCustomerID = %d, want within [0, 1023] (A=1023 for C_ID)", cCustomerID)
	}
	if cItemID < 0 || cItemID > 8191 {
		t.Fatalf("cItemID = %d, want within [0, 8191] (A=8191 for OL_I_ID)", cItemID)
	}
}

func TestRandCLast(t *testing.T) {
	b := util.NewBufAllocator()

	valid := make(map[string]bool, 1000)
	for n := 0; n < 1000; n++ {
		valid[randCLastSyllables(n, b)] = true
	}

	r := rand.New(rand.NewSource(3))
	for i := 0; i < 1000; i++ {
		name := randCLast(r, b)
		if !valid[name] {
			t.Fatalf("randCLast() = %q, not one of the 1000 valid C_LAST values", name)
		}
	}
}
