package skiplistmap

import (
	"fmt"
	"io"
)

type statKey byte

var EnableStats bool = false

var DebugStats map[statKey]int = map[statKey]int{}

func ResetStats() {
	DebugStats = map[statKey]int{}
}

type LogLevel byte

const (
	LogDebug LogLevel = iota
	LogInfo
	LogWarn
	LogError
	LogFatal
)

const CurrentLogLevel LogLevel = LogWarn

var logio io.Writer = io.Discard

func SetLogIO(w io.Writer) {
	logio = w
}

func Log(l LogLevel, s string, args ...interface{}) {

	if l == LogFatal {
		panic(fmt.Sprintf(s, args...))
	}

	if CurrentLogLevel >= l {
		fmt.Fprintf(logio, s, args...)
	}

}

func IsDebug() bool {
	return CurrentLogLevel == LogDebug
}
