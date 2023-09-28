package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/redis/go-redis/v9"

	"github.com/DeNA/ommonitor"
	"github.com/DeNA/ommonitor/ui"
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
	flag.Parse()
	extCols := strings.Split(extColsStr, ",")

	addr := flag.Arg(0)
	if addr == "" {
		flag.Usage()
		os.Exit(2)
	}
	client := redis.NewClient(&redis.Options{
		Addr: addr,
	})

	if _, err := client.Ping(ctx).Result(); err != nil {
		log.Fatalf("failed to connect to Redis %s: %+v", addr, err)
	}
	monitor := ommonitor.NewMonitor(client)
	ticketsTable := ui.NewTicketTableModel(extCols)
	ticketDetail, err := ui.NewTicketDetailModel()
	if err != nil {
		log.Fatalf("failed to create ticket detail UI: %+v", err)
	}
	if _, err := tea.NewProgram(ui.NewAppRootModel(ctx, monitor, ticketsTable, ticketDetail, refreshInterval), tea.WithAltScreen()).Run(); err != nil {
		log.Fatalf("Error running program: %+v", err)
	}
}
