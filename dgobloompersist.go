/*
Package dgobloom implements a simple Bloom Filter for strings.

A Bloom Filter is a probablistic data structure which allows testing set membership.
A negative answer means the value is not in the set.  A positive answer means the element
is probably is the set.  The desired rate false positives can be set at Filter construction time.

Copyright (c) 2011 Damian Gryski <damian@gryski.com>

Licensed under the GPLv3, or at your option any later version.
*/
package dgobloom

import (
	"encoding/gob"
	"hash/fnv"
	"math"
	"os"
)

// Internal routines for the bit vector

type bitvector2 []uint32

// get bit 'bit' in the bitvector2 d
func (d bitvector2) get(bit uint32) uint {

	shift := bit % 32
	bb := d[bit/32]
	bb &= (1 << shift)

	return uint(bb >> shift)
}

// set bit 'bit' in the bitvector2 d
func (d bitvector2) set(bit uint32) {
	d[bit/32] |= (1 << (bit % 32))
}

// 32-bit, which is why it only goes up to 16
// return the integer >= i which is a power of two
func nextPowerOfTwo2(i uint64) uint64 {
	n := i - 1
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	n |= n >> 32
	n++
	return n
}

// BloomFilter2 allow probabilistic membership tests
type BloomFilter2 interface {
	// Insert an element into the set.
	Insert(b []byte) bool

	// Determine if an element is in the set
	Exists(b []byte) bool

	// Return the number of Elements currently stored in the set
	Len() uint32

	// Merge two bloom Filters
	Merge(BloomFilter2)

	// Compress a bloom Filter
	Compress()

	Serialization(file string) error
}

// Internal struct for our bloom Filter
type bloomFilter2 struct {
	Capacity uint32
	Elements uint32
	Bits     uint64     // size of bit vector in Bits
	Filter   bitvector2 // our Filter bit vector
	Salts    [][]byte
}

func (bf *bloomFilter2) Len() uint32 { return bf.Elements }

// FilterBits2 returns the number of Bits required for the desired Capacity and false positive rate.
func FilterBits2(Capacity uint32, falsePositiveRate float64) uint64 {
	Bits := float64(Capacity) * -math.Log(falsePositiveRate) / (math.Log(2.0) * math.Log(2.0)) // in Bits
	m := nextPowerOfTwo2(uint64(Bits))

	if m < 1024 {
		return 1024
	}

	return m
}

// SaltsRequired2 returns the number of Salts required by the constructor for the desired Capacity and false positive rate.
func SaltsRequired2(Capacity uint32, falsePositiveRate float64) uint {
	m := FilterBits2(Capacity, falsePositiveRate)
	Salts := uint(0.7 * float32(float64(m)/float64(Capacity)))
	if Salts < 2 {
		return 2
	}
	return Salts
}

func uint32ToByteArray2(salt uint32) []byte {
	p := make([]byte, 4)
	p[0] = byte(salt >> 24)
	p[1] = byte(salt >> 16)
	p[2] = byte(salt >> 8)
	p[3] = byte(salt)
	return p
}

// NewBloomFilter2 returns a new bloom Filter with the specified Capacity and false positive rate.
// The hash function h will be salted with the array of Salts.
func NewBloomFilter2(Capacity uint32, falsePositiveRate float64, Salts []uint32) BloomFilter2 {

	bf := new(bloomFilter2)

	bf.Capacity = Capacity
	bf.Bits = FilterBits2(Capacity, falsePositiveRate)
	bf.Filter = make([]uint32, uint(bf.Bits+31)/32)

	bf.Salts = make([][]byte, len(Salts))
	for i, s := range Salts {
		bf.Salts[i] = uint32ToByteArray2(s)
	}

	return bf
}

// Insert inserts the byte array b into the bloom Filter.
// If the function returns false, the Capacity of the bloom Filter has been reached.  Further inserts will increase the rate of false positives.
func (bf *bloomFilter2) Insert(b []byte) bool {
	h := fnv.New32()

	bf.Elements++

	for _, s := range bf.Salts {
		h.Reset()
		h.Write(s)
		h.Write(b)
		bf.Filter.set(uint32(uint64(h.Sum32()) % bf.Bits))
	}

	return bf.Elements < bf.Capacity
}

// Exists checks the bloom Filter for the byte array b
func (bf *bloomFilter2) Exists(b []byte) bool {
	h := fnv.New32()

	for _, s := range bf.Salts {
		h.Reset()
		h.Write(s)
		h.Write(b)

		if bf.Filter.get(uint32(uint64(h.Sum32())%bf.Bits)) == 0 {
			return false
		}
	}

	return true
}

// Merge adds bf2 into the current bloom Filter.  They must have the same dimensions and be constructed with identical random seeds.
func (bf *bloomFilter2) Merge(bf2 BloomFilter2) {

	other := bf2.(*bloomFilter2)

	for i, v := range other.Filter {
		bf.Filter[i] |= v
	}
}

// Compress halves the space used by the bloom Filter, at the cost of increased error rate.
func (bf *bloomFilter2) Compress() {

	w := len(bf.Filter)

	if w&(w-1) != 0 {
		panic("width must be a power of two")
	}

	neww := w / 2

	// We allocate a new array here so old space can actually be garbage collected.
	// TODO(dgryski): reslice and only reallocate every few compressions
	row := make([]uint32, neww)
	for j := 0; j < neww; j++ {
		row[j] = bf.Filter[j] | bf.Filter[j+neww]
	}
	bf.Filter = row
	bf.Bits /= 2
}

func UnSerialization(file string) (BloomFilter2, error) {
	bf := new(bloomFilter2)
	fp, err := os.Open(file)
	if err != nil {
		return bf, err
	}

	dec := gob.NewDecoder(fp)
	err = dec.Decode(&bf)
	if err != nil {
		return bf, err
	}

	//fmt.Println(bf.Capacity)

	return bf, nil
}

func (bf *bloomFilter2) Serialization(file string) error {
	fp, err := os.Create(file)
	if err != nil {
		return err
	}
	enc := gob.NewEncoder(fp)
	err = enc.Encode(bf)
	if err != nil {
		return err
	}
	return nil
}
