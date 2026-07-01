package persistence

import (
	"bufio"
	"errors"
	"io"
	"os"

	"github.com/ThatDeparted2061/mini-redis-go/internal/protocol"
)

// Replay reads every command frame from the AOF at path, in the order they were
// written, and calls apply once per command. It returns the number of commands
// applied.
//
// apply is where the caller re-executes the command — in this server, by handing
// it to the normal command dispatcher against a fresh database. Because the log
// is just the original commands in order, replaying it from an empty store
// rebuilds exactly the state that produced the log (see the package comment).
//
// Two conditions are deliberately NOT treated as failures:
//
//   - A MISSING file means there is nothing to recover — a first-ever start, or a
//     run with persistence previously disabled — so Replay returns (0, nil).
//   - A TRUNCATED trailing frame means the process was killed partway through
//     writing the last command (a torn tail). Everything before it is intact, so
//     Replay applies those commands and stops cleanly at the tear rather than
//     refusing to start. This mirrors Redis's aof-load-truncated behaviour and is
//     the normal shape of a log left behind by kill -9.
//
// Any OTHER decode error is genuine corruption mid-log and is returned, along
// with the count of commands successfully applied before it.
func Replay(path string, apply func(cmd protocol.Value) error) (int, error) {
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	defer func() { _ = f.Close() }()

	r := bufio.NewReader(f)
	applied := 0
	for {
		cmd, err := protocol.Decode(r)
		switch {
		case errors.Is(err, io.EOF):
			// Clean end of log at a frame boundary: every command was read.
			return applied, nil
		case errors.Is(err, io.ErrUnexpectedEOF):
			// A half-written final command from a crash. The prefix is sound;
			// stop here and keep what we have.
			return applied, nil
		case err != nil:
			return applied, err
		}

		if err := apply(cmd); err != nil {
			return applied, err
		}
		applied++
	}
}
