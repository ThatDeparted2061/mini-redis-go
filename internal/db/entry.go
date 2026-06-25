package db

import "time"

// kind discriminates the type of value an Entry holds. Redis keys are typed: a
// key holds a string, OR a list, OR a hash, and so on — never a mix. Commands
// for one type reject keys holding another, which is what the WRONGTYPE error
// means on the wire. Storing the kind alongside the value is what lets the
// store enforce that.
type kind uint8

const (
	kindString kind = iota // value lives in Entry.str
	kindList               // value lives in Entry.list
	kindHash               // value lives in Entry.hash
	kindSet                // value lives in Entry.set
)

// Entry is the value stored under a single key. It generalises the store from
// raw []byte values to typed values: beyond a plain string a key can now hold a
// list, with room for more Redis types later. Exactly one of the typed fields
// is meaningful, selected by kind.
//
// expireAt carries the key's optional expiry. The zero Time means "no expiry"
// (the key is persistent). The field is part of the data model now so the store
// shape is stable; the commands that set and enforce it (EXPIRE/TTL and lazy
// eviction on access) arrive in a later phase, so today it is always zero.
type Entry struct {
	kind     kind
	str      []byte              // meaningful when kind == kindString
	list     [][]byte            // meaningful when kind == kindList (index 0 is the head)
	hash     map[string][]byte   // meaningful when kind == kindHash (field -> value)
	set      map[string]struct{} // meaningful when kind == kindSet (struct{} is a value-less placeholder)
	expireAt time.Time
}
