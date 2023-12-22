package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/redis/rueidis"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/DeNA/ommonitor"
	"github.com/DeNA/ommonitor/ui"
	tea "github.com/charmbracelet/bubbletea"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	log.SetFlags(0)
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Usage of %s [options...] <REDIS_ADDR>\n", os.Args[0])
		flag.PrintDefaults()
	}
	var extColsStr string
	flag.StringVar(&extColsStr, "ext-cols", "", "extension keys to add table columns")
	var refreshInterval time.Duration
	flag.DurationVar(&refreshInterval, "refresh", 2*time.Second, "Specify the default refresh rate")
	var isMinimatch bool
	flag.BoolVar(&isMinimatch, "minimatch", false, "minimatch mode")
	flag.Parse()
	extCols := strings.Split(extColsStr, ",")

	addr := flag.Arg(0)
	if addr == "" {
		flag.Usage()
		os.Exit(2)
	}
	client, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress:  []string{addr},
		DisableCache: true,
	})
	if err != nil {
		log.Fatalf("failed to create redis client: %+v", err)
	}
	resp := client.Do(ctx, client.B().Ping().Build())
	if err := resp.Error(); err != nil {
		log.Fatalf("failed to connect to Redis %s: %+v", addr, err)
	}

	var options []ommonitor.MonitorOption
	if isMinimatch {
		options = append(options, ommonitor.WithMinimatch())
	}
	monitor := ommonitor.NewMonitor(addr, client, options...)
	ticketsTable := ui.NewTicketTableModel(extCols)
	ticketDetail, err := ui.NewTicketDetailModel()
	if err != nil {
		log.Fatalf("failed to create ticket detail UI: %+v", err)
	}
	if _, err := tea.NewProgram(ui.NewAppRootModel(ctx, monitor, ticketsTable, ticketDetail, refreshInterval), tea.WithAltScreen()).Run(); err != nil {
		log.Fatalf("Error running program: %+v", err)
	}
}
