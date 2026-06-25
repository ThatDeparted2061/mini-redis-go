package cmd

import (
	"github.com/ThatDeparted2061/mini-redis-go/internal/db"
	"github.com/ThatDeparted2061/mini-redis-go/internal/protocol"
)

// SAdd implements SADD key member [member ...]: it adds each member to the set
// (creating it if needed) and replies with the number of members newly added —
// members already in the set, including duplicates within the same call, are
// counted once. WRONGTYPE if the key holds a non-set value.
func SAdd(database *db.DB, args []protocol.Value) protocol.Value {
	if len(args) < 2 {
		return wrongArgs("sadd")
	}

	n, err := database.SAdd(string(args[0].Bulk), bulkArgs(args[1:])...)
	if err != nil {
		return replyForErr(err)
	}
	return integerValue(int64(n))
}

// SRem implements SREM key member [member ...]: it removes the named members and
// replies with the count actually removed (members not present and a missing key
// contribute nothing). WRONGTYPE if the key holds a non-set value.
func SRem(database *db.DB, args []protocol.Value) protocol.Value {
	if len(args) < 2 {
		return wrongArgs("srem")
	}

	n, err := database.SRem(string(args[0].Bulk), bulkArgs(args[1:])...)
	if err != nil {
		return replyForErr(err)
	}
	return integerValue(int64(n))
}

// SIsMember implements SISMEMBER key member: it replies with 1 if member is in
// the set and 0 otherwise (a missing key counts as not a member). WRONGTYPE if
// the key holds a non-set value.
func SIsMember(database *db.DB, args []protocol.Value) protocol.Value {
	if len(args) != 2 {
		return wrongArgs("sismember")
	}

	member, err := database.SIsMember(string(args[0].Bulk), args[1].Bulk)
	if err != nil {
		return replyForErr(err)
	}
	return boolReply(member)
}

// SMembers implements SMEMBERS key: it replies with an array of all members of
// the set (unspecified order), or an empty array for a missing key. WRONGTYPE if
// the key holds a non-set value.
func SMembers(database *db.DB, args []protocol.Value) protocol.Value {
	if len(args) != 1 {
		return wrongArgs("smembers")
	}

	members, err := database.SMembers(string(args[0].Bulk))
	if err != nil {
		return replyForErr(err)
	}
	return bulkArrayValue(members)
}

// SCard implements SCARD key: it replies with the set's cardinality as an
// integer, or 0 if the key does not exist. WRONGTYPE if the key holds a non-set
// value.
func SCard(database *db.DB, args []protocol.Value) protocol.Value {
	if len(args) != 1 {
		return wrongArgs("scard")
	}

	n, err := database.SCard(string(args[0].Bulk))
	if err != nil {
		return replyForErr(err)
	}
	return integerValue(int64(n))
}
