package dgobloom

import (
	"fmt"
	"math/rand"
	"testing"
)

func TestSerial(t *testing.T) {

	saltsNeeded := SaltsRequired2(CAPACITY, ERRPCT)

	t.Log("generating", saltsNeeded, "salts")

	salts := make([]uint32, saltsNeeded)

	for i := uint(0); i < saltsNeeded; i++ {
		salts[i] = rand.Uint32()
	}

	b := NewBloomFilter2(CAPACITY, ERRPCT, salts)
	fmt.Println(CAPACITY)

	a := []byte{1, 2, 3, 4, 5, 6}
	b.Insert(a)
	t.Logf("before: %d", b.Len())

	fn := "bloom2.gpkl"
	b.Serialization(fn)

	b2, err := UnSerialization(fn)
	if err == nil {
		t.Logf("before: %d %v", b2.Len(), b2.Exists(a))
	}

}
