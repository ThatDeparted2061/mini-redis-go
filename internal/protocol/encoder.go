package protocol

import (
	"fmt"
	"io"
	"strconv"
)

// Encode serializes v to w in RESP wire format. For aggregate types it writes
// the elements recursively. A nil Bulk encodes as a null bulk string and a nil
// Array as a null array.
func Encode(w io.Writer, v Value) error {
	switch v.Type {
	case TypeSimpleString:
		return writeAll(w, "+"+v.Str+"\r\n")

	case TypeError:
		return writeAll(w, "-"+v.Str+"\r\n")

	case TypeInteger:
		return writeAll(w, ":"+strconv.FormatInt(v.Int, 10)+"\r\n")

	case TypeBulkString:
		if v.Bulk == nil {
			return writeAll(w, "$-1\r\n")
		}
		if err := writeAll(w, "$"+strconv.Itoa(len(v.Bulk))+"\r\n"); err != nil {
			return err
		}
		if _, err := w.Write(v.Bulk); err != nil {
			return err
		}
		return writeAll(w, "\r\n")

	case TypeArray:
		if v.Array == nil {
			return writeAll(w, "*-1\r\n")
		}
		if err := writeAll(w, "*"+strconv.Itoa(len(v.Array))+"\r\n"); err != nil {
			return err
		}
		for _, e := range v.Array {
			if err := Encode(w, e); err != nil {
				return err
			}
		}
		return nil

	default:
		return fmt.Errorf("encode: unknown value type %q", v.Type)
	}
}

func writeAll(w io.Writer, s string) error {
	_, err := io.WriteString(w, s)
	return err
}
