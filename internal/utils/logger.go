package utils

import (
	"os"

	"github.com/rs/zerolog"
)

var Logger zerolog.Logger

func Init(level string) {
	l, err := zerolog.ParseLevel(level)
	if err != nil {
		l = zerolog.InfoLevel
	}
	zerolog.SetGlobalLevel(l)

	if isTerminal() {
		Logger = zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: "15:04:05.000"}).
			With().Timestamp().Logger()
	} else {
		Logger = zerolog.New(os.Stderr).With().Timestamp().Logger()
	}
}

func isTerminal() bool {
	fi, err := os.Stdout.Stat()
	if err != nil {
		return false
	}
	return (fi.Mode() & os.ModeCharDevice) != 0
}
