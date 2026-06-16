package protocol

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
)

// Decode reads a single RESP frame from r and returns it as a Value.
//
// It relies entirely on bufio.Reader for framing: ReadBytes and io.ReadFull
// block until the requested data has arrived, so a frame split across multiple
// TCP segments is reassembled transparently.
//
// A clean connection close (EOF before any byte of a frame) is reported as
// io.EOF so callers can distinguish it from a protocol error. An EOF in the
// middle of a frame is reported as io.ErrUnexpectedEOF.
func Decode(r *bufio.Reader) (Value, error) {
	prefix, err := r.ReadByte()
	if err != nil {
		return Value{}, err // io.EOF here means a clean close.
	}

	switch prefix {
	case TypeSimpleString:
		line, err := readLine(r)
		if err != nil {
			return Value{}, err
		}
		return Value{Type: TypeSimpleString, Str: string(line)}, nil

	case TypeError:
		line, err := readLine(r)
		if err != nil {
			return Value{}, err
		}
		return Value{Type: TypeError, Str: string(line)}, nil

	case TypeInteger:
		n, err := readInt(r)
		if err != nil {
			return Value{}, err
		}
		return Value{Type: TypeInteger, Int: n}, nil

	case TypeBulkString:
		return decodeBulk(r)

	case TypeArray:
		return decodeArray(r)

	default:
		return Value{}, fmt.Errorf("protocol error: unknown type byte %q", prefix)
	}
}

func decodeBulk(r *bufio.Reader) (Value, error) {
	n, err := readInt(r)
	if err != nil {
		return Value{}, err
	}
	if n < 0 {
		return Value{Type: TypeBulkString, Bulk: nil}, nil // null bulk string
	}

	// Read exactly n bytes of payload plus the trailing CRLF.
	buf := make([]byte, n+2)
	if _, err := io.ReadFull(r, buf); err != nil {
		return Value{}, unexpectEOF(err)
	}
	if buf[n] != '\r' || buf[n+1] != '\n' {
		return Value{}, errors.New("protocol error: bulk string not terminated by CRLF")
	}
	return Value{Type: TypeBulkString, Bulk: buf[:n]}, nil
}

func decodeArray(r *bufio.Reader) (Value, error) {
	n, err := readInt(r)
	if err != nil {
		return Value{}, err
	}
	if n < 0 {
		return Value{Type: TypeArray, Array: nil}, nil // null array
	}

	arr := make([]Value, n)
	for i := int64(0); i < n; i++ {
		v, err := Decode(r)
		if err != nil {
			return Value{}, unexpectEOF(err)
		}
		arr[i] = v
	}
	return Value{Type: TypeArray, Array: arr}, nil
}

// readLine reads up to and including the next \n, then strips the trailing
// CRLF. It errors if the line is not CRLF-terminated.
func readLine(r *bufio.Reader) ([]byte, error) {
	line, err := r.ReadBytes('\n')
	if err != nil {
		return nil, unexpectEOF(err)
	}
	if len(line) < 2 || line[len(line)-2] != '\r' {
		return nil, errors.New("protocol error: line not terminated by CRLF")
	}
	return line[:len(line)-2], nil
}

// readInt reads a CRLF-terminated line and parses it as a base-10 int64.
func readInt(r *bufio.Reader) (int64, error) {
	line, err := readLine(r)
	if err != nil {
		return 0, err
	}
	n, err := strconv.ParseInt(string(line), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("protocol error: invalid integer %q", line)
	}
	return n, nil
}

// unexpectEOF converts a mid-frame io.EOF into io.ErrUnexpectedEOF; only a
// frame-boundary EOF (handled in Decode) should surface as a clean io.EOF.
func unexpectEOF(err error) error {
	if err == io.EOF {
		return io.ErrUnexpectedEOF
	}
	return err
}
