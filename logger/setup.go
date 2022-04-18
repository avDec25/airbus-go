package logger

import (
	"github.com/op/go-logging"
	"os"
)

var log = logging.MustGetLogger("airbus")

func LogInit(level logging.Level) {
	backend := logging.NewLogBackend(os.Stderr, "", 0)
	var format = logging.MustStringFormatter(
		`%{color}%{time:2006-01-02 15:04:05Z-07:00} %{shortfunc} ▶ %{level:.4s} %{id:03x}%{color:reset} %{message}`,
	)
	formatter := logging.NewBackendFormatter(backend, format)
	backendLeveled := logging.AddModuleLevel(formatter)
	backendLeveled.SetLevel(level, "airbus")
	log.SetBackend(backendLeveled)
	logging.SetBackend(backendLeveled)
}

func GetLogger() *logging.Logger {
	return log
}
