package util

import (
	"os"

	"golang.org/x/term"
)

// IsInteractiveStdout reports whether stdout is attached to an interactive
// terminal, as opposed to being redirected to a file/pipe (e.g. in CI).
func IsInteractiveStdout() bool {
	return term.IsTerminal(int(os.Stdout.Fd()))
}
