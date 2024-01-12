package ui

import (
	"context"
	"fmt"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"github.com/DeNA/ommonitor"
)

const (
	fetchTicketsTimeout = 5 * time.Minute
)

type triggerTicketFetchMsg struct{}

type reportFetchProgressMsg struct {
	ch       <-chan ommonitor.MonitorFetchProgress
	progress ommonitor.MonitorFetchProgress
}

type ticketsLoadedMsg struct {
	Tickets []ommonitor.Ticket
	Error   error
}

type AppRootModel struct {
	ctx             context.Context
	monitor         *ommonitor.Monitor
	ticketsTable    TicketTableModel
	ticketDetail    TicketDetailModel
	indicator       indicatorModel
	refreshInterval time.Duration
	progress        <-chan ommonitor.MonitorFetchProgress
}

func NewAppRootModel(ctx context.Context, monitor *ommonitor.Monitor, table TicketTableModel, detail TicketDetailModel, refreshInterval time.Duration) *AppRootModel {
	return &AppRootModel{
		ctx:             ctx,
		monitor:         monitor,
		ticketsTable:    table,
		ticketDetail:    detail,
		indicator:       newIndicator(monitor.RedisAddr(), refreshInterval),
		refreshInterval: refreshInterval,
	}
}

func (m AppRootModel) Init() tea.Cmd {
	return tea.Batch(func() tea.Msg {
		return triggerTicketFetchMsg{}
	}, m.indicator.Init())
}

func (m AppRootModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var (
		cmd  tea.Cmd
		cmds []tea.Cmd
	)

	// update model
	m.indicator, cmd = m.indicator.Update(msg)
	cmds = append(cmds, cmd)
	m.ticketsTable, cmd = m.ticketsTable.Update(msg)
	cmds = append(cmds, cmd)
	if ticketSelected, ok := m.ticketsTable.TicketSelected(); ok {
		m.ticketDetail.SetTicket(&ticketSelected)
	}
	m.ticketDetail, cmd = m.ticketDetail.Update(msg)
	cmds = append(cmds, cmd)

	// handle message
	switch msg := msg.(type) {
	case triggerTicketFetchMsg:
		cmd, prog := m.fetchTickets()
		cmds = append(cmds, cmd, func() tea.Msg {
			return reportFetchProgressMsg{ch: prog, progress: <-prog}
		})
	case reportFetchProgressMsg:
		if msg.progress.State != ommonitor.MonitorFetchProgressStateDone {
			cmds = append(cmds, func() tea.Msg {
				return reportFetchProgressMsg{ch: msg.ch, progress: <-msg.ch}
			})
		}
	case ticketsLoadedMsg:
		// trigger fetch tickets periodically
		cmds = append(cmds, tea.Tick(m.refreshInterval, func(t time.Time) tea.Msg {
			return triggerTicketFetchMsg{}
		}))
	case tea.WindowSizeMsg:
		width := msg.Width
		height := msg.Height
		titleBarHeight := lipgloss.Height(m.indicator.View())
		statusBarHeight := lipgloss.Height(m.statusBarView())
		tableWidth := int(float64(width)*(1-ticketDetailProportion)) - ticketTableCellStyle.GetHorizontalFrameSize()
		tableHeight := height - (titleBarHeight + statusBarHeight) - 1
		m.ticketsTable.SetSize(tableWidth, tableHeight)
		detailWidth := int(float64(width)*ticketDetailProportion) - ticketDetailStyle.GetHorizontalFrameSize() + 30
		detailHeight := tableHeight
		m.ticketDetail.SetSize(detailWidth, detailHeight)
	case tea.KeyMsg:
		switch msg.String() {
		case "ctrl+c":
			return m, tea.Quit
		}
	}

	return m, tea.Batch(cmds...)
}

func (m AppRootModel) View() string {
	titleBar := titleBarStyle.Render(m.indicator.View())
	mainView := lipgloss.JoinHorizontal(lipgloss.Top,
		m.ticketsTable.View(),
		m.ticketDetail.View(),
	)
	statusBar := statusBarStyle.Render(m.statusBarView())
	return lipgloss.JoinVertical(lipgloss.Left, titleBar, mainView, statusBar)
}

func (m AppRootModel) statusBarView() string {
	return fmt.Sprintf("j/k: move down/up")
}

func (m AppRootModel) fetchTickets() (tea.Cmd, <-chan ommonitor.MonitorFetchProgress) {
	progCh := make(chan ommonitor.MonitorFetchProgress, 1)
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(m.ctx, fetchTicketsTimeout)
		defer cancel()
		tickets, err := m.monitor.FetchAllTickets(ctx, func(progress ommonitor.MonitorFetchProgress) {
			progCh <- progress
		})
		return ticketsLoadedMsg{Tickets: tickets, Error: err}
	}, progCh
}
