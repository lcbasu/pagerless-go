package pagerless

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"path"
	"regexp"
	"runtime"
	"strconv"
	"strings"
)

const (
	sdkPkg         = "github.com/lcbasu/pagerless-go."
	maxStackFrames = 64
	// fingerprintFrames is how many in-app frames feed the grouping key.
	fingerprintFrames = 5
)

type frame struct {
	Function string
	File     string
	Line     int
}

// callers captures the stack above the SDK, skipping runtime and SDK frames at the top so the
// first frame is the caller's code (or the panic site).
func callers(skip int) []frame {
	pcs := make([]uintptr, maxStackFrames)
	n := runtime.Callers(skip+2, pcs)
	it := runtime.CallersFrames(pcs[:n])
	out := make([]frame, 0, n)
	top := true
	for {
		f, more := it.Next()
		if top && (isRuntime(f.Function) || isSDK(f)) {
			if !more {
				break
			}
			continue
		}
		top = false
		out = append(out, frame{Function: f.Function, File: f.File, Line: f.Line})
		if !more {
			break
		}
	}
	return out
}

func isRuntime(fn string) bool { return strings.HasPrefix(fn, "runtime.") }

// isSDK matches SDK frames but not the SDK's own tests, so tests see their frames as in-app.
func isSDK(f runtime.Frame) bool {
	return strings.HasPrefix(f.Function, sdkPkg) && !strings.HasSuffix(f.File, "_test.go")
}

// isFramework marks frames that are never the cause: stdlib and router plumbing below user handlers.
func isFramework(f frame) bool {
	for _, p := range []string{"runtime.", "net/http.", "testing.", "reflect.", "github.com/gin-gonic/gin."} {
		if strings.HasPrefix(f.Function, p) {
			return true
		}
	}
	return strings.HasPrefix(f.Function, sdkPkg) && !strings.HasSuffix(f.File, "_test.go")
}

// formatStack renders frames like a Go panic trace.
func formatStack(frames []frame) string {
	var b strings.Builder
	for _, f := range frames {
		b.WriteString(f.Function)
		b.WriteString("()\n\t")
		b.WriteString(f.File)
		b.WriteByte(':')
		b.WriteString(strconv.Itoa(f.Line))
		b.WriteByte('\n')
	}
	return b.String()
}

// fingerprint is the stable grouping key: error type + top in-app frames + route.
// Line numbers and messages are excluded on purpose: they change with unrelated edits and carry
// per-request data, which would split one problem into many.
func fingerprint(kind, errType, route string, frames []frame) string {
	h := sha256.New()
	h.Write([]byte(kind))
	h.Write([]byte{0})
	h.Write([]byte(errType))
	h.Write([]byte{0})
	h.Write([]byte(route))
	h.Write([]byte{0})
	used := 0
	for _, f := range frames {
		if used == fingerprintFrames {
			break
		}
		if isFramework(f) {
			continue
		}
		h.Write([]byte(normalizeFunc(f.Function)))
		h.Write([]byte{0})
		h.Write([]byte(path.Base(f.File)))
		h.Write([]byte{0})
		used++
	}
	return hex.EncodeToString(h.Sum(nil))[:32]
}

// Closures are named func1, func2… by declaration order; treat them as their enclosing function
// so adding a closure elsewhere in the function doesn't regroup.
var closureSuffix = regexp.MustCompile(`(\.func\d+)+$|\.gowrap\d+$`)

func normalizeFunc(fn string) string { return closureSuffix.ReplaceAllString(fn, "") }

// Generic wrapper types that say nothing about the cause; errType looks through them.
var genericErrTypes = map[string]bool{
	"*errors.errorString": true,
	"*fmt.wrapError":      true,
	"*fmt.wrapErrors":     true,
	"*errors.joinError":   true,
}

// errType names the most specific error type in the Unwrap chain: the outermost non-generic
// type, else the innermost type (stable however many times the error was wrapped with %w).
func errType(err error) string {
	last := fmt.Sprintf("%T", err)
	for e := err; e != nil; e = errors.Unwrap(e) {
		t := fmt.Sprintf("%T", e)
		if !genericErrTypes[t] {
			return t
		}
		last = t
	}
	return last
}

// panicInfo turns a recovered value into (type, message).
func panicInfo(v any) (string, string) {
	if err, ok := v.(error); ok {
		return "panic: " + errType(err), err.Error()
	}
	return fmt.Sprintf("panic: %T", v), fmt.Sprint(v)
}
