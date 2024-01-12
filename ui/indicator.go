package ui

import (
	"fmt"
	"time"

	"github.com/charmbracelet/bubbles/spinner"
	tea "github.com/charmbracelet/bubbletea"

	"github.com/DeNA/ommonitor"
)

type indicatorModel struct {
	spinner         spinner.Model
	ticketCount     int
	redisAddr       string
	refreshInterval time.Duration
	err             error
	loading         bool
	progress        ommonitor.MonitorFetchProgress
}

func (m indicatorModel) Init() tea.Cmd {
	return m.spinner.Tick
}

func (m indicatorModel) Update(msg tea.Msg) (indicatorModel, tea.Cmd) {
	var (
		cmd  tea.Cmd
		cmds []tea.Cmd
	)
	m.spinner, cmd = m.spinner.Update(msg)
	cmds = append(cmds, cmd)

	switch msg := msg.(type) {
	case triggerTicketFetchMsg:
		m.loading = true
	case reportFetchProgressMsg:
		m.progress = msg.progress
	case ticketsLoadedMsg:
		m.loading = false
		if msg.Error != nil {
			m.err = msg.Error
		}
		m.ticketCount = len(msg.Tickets)
	}
	return m, tea.Batch(cmds...)
}

func (m indicatorModel) View() string {
	v := fmt.Sprintf("ommonitor - %d ticket(s), redis: %s, refresh: %v",
		m.ticketCount, m.redisAddr, m.refreshInterval)
	if m.err != nil {
		v += fmt.Sprintf("    error: %+v", m.err)
	} else if m.loading {
		v += fmt.Sprintf("    %s Loading...", m.spinner.View())
	}
	switch m.progress.State {
	case ommonitor.MonitorFetchProgressStateScanning:
		v += fmt.Sprintf(" (%d scanned)", m.progress.TicketsScanned)
	case ommonitor.MonitorFetchProgressStateFetching:
		v += fmt.Sprintf(" (%d fetched)", m.progress.TicketsFetched)
	case ommonitor.MonitorFetchProgressStateDone:
		v += fmt.Sprintf(" (%d added, %d expired)", m.progress.TicketsAdded, m.progress.TicketsExpired)
	}
	return v
}

func newIndicator(redisAddr string, refreshInterval time.Duration) indicatorModel {
	return indicatorModel{
		spinner:         spinner.New(),
		redisAddr:       redisAddr,
		refreshInterval: refreshInterval,
		err:             nil,
		loading:         false,
	}
}
