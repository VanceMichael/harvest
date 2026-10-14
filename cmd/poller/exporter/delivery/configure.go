/*
Copyright NetApp Inc, 2021 All rights reserved
*/

package delivery

import (
	"log/slog"
	"time"

	"github.com/netapp/harvest/v2/pkg/conf"
)

// Configure builds a Queue for one push exporter instance from harvest
// configuration. Each exporter instance gets its own spool directory and
// sender goroutine, so multiple exporters of the same type are fully
// isolated from each other.
func Configure(
	poller, class, name string,
	params *conf.DeliveryConfig,
	logger *slog.Logger,
	push PushFunc,
	setStatus func(uint8, string),
) (*Queue, error) {
	base := conf.Path("spool")
	cfg := Config{
		Poller:    poller,
		Class:     class,
		Name:      name,
		Logger:    logger,
		Push:      push,
		SetStatus: setStatus,
	}

	if params != nil {
		if params.SpoolPath != "" {
			base = params.SpoolPath
		}
		if params.QueueMax != nil && *params.QueueMax > 0 {
			cfg.QueueMax = *params.QueueMax
		}
		if params.SpoolMaxBatches != nil && *params.SpoolMaxBatches > 0 {
			cfg.SpoolMaxBatches = *params.SpoolMaxBatches
		}
		if params.RetryMinInterval != "" {
			if d, err := time.ParseDuration(params.RetryMinInterval); err == nil && d > 0 {
				cfg.RetryMin = d
			} else {
				logger.Warn("invalid delivery.retry_min_interval, using default",
					slog.String("value", params.RetryMinInterval), slog.Any("err", err))
			}
		}
		if params.RetryMaxInterval != "" {
			if d, err := time.ParseDuration(params.RetryMaxInterval); err == nil && d > 0 {
				cfg.RetryMax = d
			} else {
				logger.Warn("invalid delivery.retry_max_interval, using default",
					slog.String("value", params.RetryMaxInterval), slog.Any("err", err))
			}
		}
	}

	cfg.Dir = SpoolDir(base, poller, name)
	return New(cfg)
}
