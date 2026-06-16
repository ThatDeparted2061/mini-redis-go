package protocol

import (
	"bufio"
	"bytes"
	"io"
	"reflect"
	"strings"
	"testing"
)

func TestDecode(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  Value
	}{
		{
			name:  "simple string",
			input: "+OK\r\n",
			want:  Value{Type: TypeSimpleString, Str: "OK"},
		},
		{
			name:  "empty simple string",
			input: "+\r\n",
			want:  Value{Type: TypeSimpleString, Str: ""},
		},
		{
			name:  "error",
			input: "-ERR unknown command\r\n",
			want:  Value{Type: TypeError, Str: "ERR unknown command"},
		},
		{
			name:  "integer positive",
			input: ":1000\r\n",
			want:  Value{Type: TypeInteger, Int: 1000},
		},
		{
			name:  "integer negative",
			input: ":-42\r\n",
			want:  Value{Type: TypeInteger, Int: -42},
		},
		{
			name:  "bulk string",
			input: "$5\r\nhello\r\n",
			want:  Value{Type: TypeBulkString, Bulk: []byte("hello")},
		},
		{
			name:  "empty bulk string",
			input: "$0\r\n\r\n",
			want:  Value{Type: TypeBulkString, Bulk: []byte{}},
		},
		{
			name:  "bulk string with embedded crlf",
			input: "$7\r\nfoo\r\nba\r\n",
			want:  Value{Type: TypeBulkString, Bulk: []byte("foo\r\nba")},
		},
		{
			name:  "null bulk string",
			input: "$-1\r\n",
			want:  Value{Type: TypeBulkString, Bulk: nil},
		},
		{
			name:  "empty array",
			input: "*0\r\n",
			want:  Value{Type: TypeArray, Array: []Value{}},
		},
		{
			name:  "null array",
			input: "*-1\r\n",
			want:  Value{Type: TypeArray, Array: nil},
		},
		{
			name:  "array of bulk strings",
			input: "*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n",
			want: Value{Type: TypeArray, Array: []Value{
				{Type: TypeBulkString, Bulk: []byte("GET")},
				{Type: TypeBulkString, Bulk: []byte("key")},
			}},
		},
		{
			name:  "nested array",
			input: "*2\r\n:1\r\n*1\r\n+ok\r\n",
			want: Value{Type: TypeArray, Array: []Value{
				{Type: TypeInteger, Int: 1},
				{Type: TypeArray, Array: []Value{
					{Type: TypeSimpleString, Str: "ok"},
				}},
			}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := bufio.NewReader(strings.NewReader(tt.input))
			got, err := Decode(r)
			if err != nil {
				t.Fatalf("Decode() error = %v", err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Decode() = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestDecodeErrors(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantEOF bool
	}{
		{name: "clean eof", input: "", wantEOF: true},
		{name: "unknown type byte", input: "!nope\r\n"},
		{name: "missing crlf", input: "+OK\n"},
		{name: "bad integer", input: ":notanum\r\n"},
		{name: "bad bulk length", input: "$abc\r\nhi\r\n"},
		{name: "truncated mid frame", input: "$5\r\nhel"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := bufio.NewReader(strings.NewReader(tt.input))
			_, err := Decode(r)
			if err == nil {
				t.Fatal("Decode() expected error, got nil")
			}
			if tt.wantEOF && err != io.EOF {
				t.Errorf("Decode() error = %v, want io.EOF", err)
			}
		})
	}
}

func TestEncode(t *testing.T) {
	tests := []struct {
		name string
		in   Value
		want string
	}{
		{
			name: "simple string",
			in:   Value{Type: TypeSimpleString, Str: "OK"},
			want: "+OK\r\n",
		},
		{
			name: "error",
			in:   Value{Type: TypeError, Str: "ERR boom"},
			want: "-ERR boom\r\n",
		},
		{
			name: "integer",
			in:   Value{Type: TypeInteger, Int: -7},
			want: ":-7\r\n",
		},
		{
			name: "bulk string",
			in:   Value{Type: TypeBulkString, Bulk: []byte("hello")},
			want: "$5\r\nhello\r\n",
		},
		{
			name: "empty bulk string",
			in:   Value{Type: TypeBulkString, Bulk: []byte{}},
			want: "$0\r\n\r\n",
		},
		{
			name: "null bulk string",
			in:   Value{Type: TypeBulkString, Bulk: nil},
			want: "$-1\r\n",
		},
		{
			name: "null array",
			in:   Value{Type: TypeArray, Array: nil},
			want: "*-1\r\n",
		},
		{
			name: "array",
			in: Value{Type: TypeArray, Array: []Value{
				{Type: TypeBulkString, Bulk: []byte("GET")},
				{Type: TypeBulkString, Bulk: []byte("key")},
			}},
			want: "*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			if err := Encode(&buf, tt.in); err != nil {
				t.Fatalf("Encode() error = %v", err)
			}
			if got := buf.String(); got != tt.want {
				t.Errorf("Encode() = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestRoundTrip ensures Encode followed by Decode yields the original value.
func TestRoundTrip(t *testing.T) {
	values := []Value{
		{Type: TypeSimpleString, Str: "PONG"},
		{Type: TypeError, Str: "ERR nope"},
		{Type: TypeInteger, Int: 123456},
		{Type: TypeBulkString, Bulk: []byte("payload")},
		{Type: TypeBulkString, Bulk: nil},
		{Type: TypeArray, Array: []Value{
			{Type: TypeBulkString, Bulk: []byte("SET")},
			{Type: TypeBulkString, Bulk: []byte("k")},
			{Type: TypeBulkString, Bulk: []byte("v")},
		}},
	}

	for _, v := range values {
		var buf bytes.Buffer
		if err := Encode(&buf, v); err != nil {
			t.Fatalf("Encode(%#v) error = %v", v, err)
		}
		got, err := Decode(bufio.NewReader(&buf))
		if err != nil {
			t.Fatalf("Decode after Encode(%#v) error = %v", v, err)
		}
		if !reflect.DeepEqual(got, v) {
			t.Errorf("round trip = %#v, want %#v", got, v)
		}
	}
}

// chunkReader returns its data one byte at a time to simulate TCP delivering
// a frame in arbitrary chunks. bufio.Reader must transparently reassemble it.
type chunkReader struct {
	data []byte
	pos  int
}

func (c *chunkReader) Read(p []byte) (int, error) {
	if c.pos >= len(c.data) {
		return 0, io.EOF
	}
	if len(p) == 0 {
		return 0, nil
	}
	p[0] = c.data[c.pos]
	c.pos++
	return 1, nil
}

func TestDecodeChunkedInput(t *testing.T) {
	input := "*2\r\n$3\r\nGET\r\n$5\r\nmykey\r\n"
	r := bufio.NewReader(&chunkReader{data: []byte(input)})
	got, err := Decode(r)
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	want := Value{Type: TypeArray, Array: []Value{
		{Type: TypeBulkString, Bulk: []byte("GET")},
		{Type: TypeBulkString, Bulk: []byte("mykey")},
	}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Decode() = %#v, want %#v", got, want)
	}
}
