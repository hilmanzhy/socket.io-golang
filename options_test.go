package socketio

import (
	"io"
	"log/slog"
	"testing"
	"time"
)

func TestOptionsDefaults(t *testing.T) {
	t.Parallel()
	custom := slog.New(slog.NewTextHandler(io.Discard, nil))
	cases := []struct {
		name string
		in   Options
		want Options
		logg *slog.Logger
	}{
		{
			"the zero value",
			Options{},
			Options{ConnectTimeout: 45 * time.Second, MaxAttachments: 10, EventBacklog: 64},
			slog.Default(),
		},
		{
			"explicit values are kept",
			Options{ConnectTimeout: time.Second, MaxAttachments: 3, EventBacklog: 7, Logger: custom},
			Options{ConnectTimeout: time.Second, MaxAttachments: 3, EventBacklog: 7},
			custom,
		},
		{
			"only the missing values are filled in",
			Options{EventBacklog: 5},
			Options{ConnectTimeout: 45 * time.Second, MaxAttachments: 10, EventBacklog: 5},
			slog.Default(),
		},
		{
			"negative values are replaced by the defaults",
			Options{ConnectTimeout: -time.Second, EventBacklog: -3},
			Options{ConnectTimeout: 45 * time.Second, MaxAttachments: 10, EventBacklog: 64},
			slog.Default(),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := tc.in.withDefaults()
			if got.ConnectTimeout != tc.want.ConnectTimeout || got.MaxAttachments != tc.want.MaxAttachments || got.EventBacklog != tc.want.EventBacklog {
				t.Errorf("timeout %v, attachments %d, backlog %d; want %v, %d, %d",
					got.ConnectTimeout, got.MaxAttachments, got.EventBacklog,
					tc.want.ConnectTimeout, tc.want.MaxAttachments, tc.want.EventBacklog)
			}
			if got.Logger != tc.logg {
				t.Errorf("logger = %p, want %p", got.Logger, tc.logg)
			}
		})
	}
}
