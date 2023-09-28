package ui

import (
	"context"
	"fmt"
	"time"

	"github.com/charmbracelet/bubbles/spinner"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"github.com/DeNA/ommonitor"
	"github.com/DeNA/ommonitor/viewutils"
)

const (
	fetchTicketsTimeout = 30 * time.Second
)

type AppStatus string

const (
	AppStatusLoading AppStatus = "Loading"
	AppStatusError   AppStatus = "Error"
	AppStatusReady   AppStatus = "Ready"
)

type TriggerTicketFetchMsg struct{}

type TicketsLoadedMsg struct {
	Tickets []ommonitor.Ticket
	Error   error
}

type AppRootModel struct {
	ctx             context.Context
	monitor         *ommonitor.Monitor
	ticketsTable    TicketTableModel
	ticketDetail    TicketDetailModel
	status          AppStatus
	error           error
	spinner         spinner.Model
	refreshInterval time.Duration
}

func NewAppRootModel(ctx context.Context, monitor *ommonitor.Monitor, table TicketTableModel, detail TicketDetailModel, refreshInterval time.Duration) *AppRootModel {
	return &AppRootModel{
		ctx:             ctx,
		monitor:         monitor,
		ticketsTable:    table,
		ticketDetail:    detail,
		status:          AppStatusLoading,
		error:           nil,
		spinner:         spinner.New(),
		refreshInterval: refreshInterval,
	}
}

func (m AppRootModel) Init() tea.Cmd {
	return tea.Batch(func() tea.Msg {
		return TriggerTicketFetchMsg{}
	}, m.spinner.Tick)
}

func (m AppRootModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var (
		cmd  tea.Cmd
		cmds []tea.Cmd
	)

	// update model
	switch m.status {
	case AppStatusLoading:
		m.spinner, cmd = m.spinner.Update(msg)
		cmds = append(cmds, cmd)
	}
	m.ticketsTable, cmd = m.ticketsTable.Update(msg)
	cmds = append(cmds, cmd)
	if ticketSelected, ok := m.ticketsTable.TicketSelected(); ok {
		m.ticketDetail.SetTicket(&ticketSelected)
	}
	m.ticketDetail, cmd = m.ticketDetail.Update(msg)
	cmds = append(cmds, cmd)

	// handle message
	switch msg := msg.(type) {
	case TriggerTicketFetchMsg:
		m.status = AppStatusLoading
		cmds = append(cmds, m.fetchTicketsCmd)
	case TicketsLoadedMsg:
		if msg.Error != nil {
			m.status = AppStatusError
			m.error = msg.Error
		} else {
			m.status = AppStatusReady
		}
		// trigger fetch tickets periodically
		cmds = append(cmds, tea.Tick(m.refreshInterval, func(t time.Time) tea.Msg {
			return TriggerTicketFetchMsg{}
		}))
	case tea.WindowSizeMsg:
		width := msg.Width
		height := msg.Height
		titleBarHeight := lipgloss.Height(m.titleBarView())
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
	titleBar := titleBarStyle.Render(m.titleBarView())
	mainView := lipgloss.JoinHorizontal(lipgloss.Top,
		m.ticketsTable.View(),
		m.ticketDetail.View(),
	)
	statusBar := statusBarStyle.Render(m.statusBarView())
	return lipgloss.JoinVertical(lipgloss.Left, titleBar, mainView, statusBar)
}

func (m AppRootModel) titleBarView() string {
	return fmt.Sprintf("ommonitor - %d ticket(s), redis: %s, refresh: %v    %s",
		m.ticketsTable.TicketCount(), m.monitor.RedisAddr(), viewutils.HumanDuration(m.refreshInterval), m.indicatorView())
}

func (m AppRootModel) indicatorView() string {
	switch m.status {
	case AppStatusLoading:
		return m.spinner.View() + " Loading..."
	case AppStatusError:
		return fmt.Sprintf("error: %+v", m.error)
	default:
		return ""
	}
}

func (m AppRootModel) statusBarView() string {
	return fmt.Sprintf("j/k: move up/down")
}

func (m AppRootModel) fetchTicketsCmd() tea.Msg {
	ctx, cancel := context.WithTimeout(m.ctx, fetchTicketsTimeout)
	defer cancel()
	tickets, err := m.monitor.FetchAllTickets(ctx)
	return TicketsLoadedMsg{Tickets: tickets, Error: err}
}
