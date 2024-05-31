package client

import (
	"fmt"
	"net/http"
	"time"

	"github.com/Trendyol/go-dcp/logger"
)

type LoggerAdapter struct {
	Logger logger.Logger
}

func (l *LoggerAdapter) LogRoundTrip(req *http.Request, res *http.Response, err error, time time.Time, dur time.Duration) error {
	if err == nil {
		return nil
	}

	l.Logger.Error(fmt.Sprintf("elasticsearch error: %s", err.Error()))
	return nil
}

func (l *LoggerAdapter) RequestBodyEnabled() bool {
	return true
}

func (l *LoggerAdapter) ResponseBodyEnabled() bool {
	return true
}
