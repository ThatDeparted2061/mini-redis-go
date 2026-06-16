package protocol

// RESP type discriminators. Each corresponds to the first byte of a RESP
// frame on the wire (https://redis.io/docs/reference/protocol-spec/).
const (
	TypeSimpleString byte = '+'
	TypeError        byte = '-'
	TypeInteger      byte = ':'
	TypeBulkString   byte = '$'
	TypeArray        byte = '*'
)

// Value is a tagged union representing a single RESP value. The Type field is
// the discriminator; only the field matching Type is meaningful:
//
//	TypeSimpleString -> Str
//	TypeError        -> Str
//	TypeInteger      -> Int
//	TypeBulkString   -> Bulk (nil means RESP null bulk string, "$-1\r\n")
//	TypeArray        -> Array (nil means RESP null array, "*-1\r\n")
type Value struct {
	Type  byte
	Str   string
	Int   int64
	Bulk  []byte
	Array []Value
}
