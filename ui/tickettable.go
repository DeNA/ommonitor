package ui

import (
	"time"

	"github.com/charmbracelet/bubbles/table"
	tea "github.com/charmbracelet/bubbletea"

	"github.com/DeNA/ommonitor"
	"github.com/DeNA/ommonitor/viewutils"
)

type TicketTableModel struct {
	extColumns []string

	cursor  int
	tickets []ommonitor.Ticket
	table   table.Model
}

func NewTicketTableModel(extColumns []string) TicketTableModel {
	cols := []table.Column{
		{Title: "ID", Width: 22},
		{Title: "Status", Width: 8},
		{Title: "Age", Width: 6},
		{Title: "TTL", Width: 6},
	}
	for _, col := range extColumns {
		cols = append(cols, table.Column{Title: col, Width: 30})
	}
	tbl := table.New(
		table.WithColumns(cols),
		table.WithFocused(true),
		table.WithStyles(table.Styles{
			Header:   ticketTableHeaderStyle,
			Cell:     ticketTableCellStyle,
			Selected: ticketTableSelectedStyle,
		}),
	)
	return TicketTableModel{
		extColumns: extColumns,
		cursor:     -1,
		table:      tbl,
	}
}

func (m TicketTableModel) Update(msg tea.Msg) (TicketTableModel, tea.Cmd) {
	var (
		cmd  tea.Cmd
		cmds []tea.Cmd
	)

	// update model
	m.table, cmd = m.table.Update(msg)
	cmds = append(cmds, cmd)
	if m.cursor != m.table.Cursor() {
		m.cursor = m.table.Cursor()
	}

	// handle message
	switch msg := msg.(type) {
	case ticketsLoadedMsg:
		if msg.Error != nil {
			break
		}
		m.tickets = msg.Tickets
		m.table.SetRows(ticketsToRows(m.tickets, m.extColumns))
	}

	return m, tea.Batch(cmds...)
}

func (m TicketTableModel) TicketSelected() (ommonitor.Ticket, bool) {
	cursorAvailable := m.cursor >= 0 && m.cursor < len(m.tickets)
	if !cursorAvailable {
		return ommonitor.Ticket{}, false
	}
	return m.tickets[m.cursor], true
}

func (m *TicketTableModel) SetSize(width, height int) {
	m.table.SetWidth(width)
	m.table.SetHeight(height)
}

func (m TicketTableModel) View() string {
	return m.table.View()
}

func (m TicketTableModel) TicketCount() int {
	return len(m.tickets)
}

func ticketsToRows(tickets []ommonitor.Ticket, extCols []string) []table.Row {
	var rows []table.Row
	for _, ticket := range tickets {
		rows = append(rows, ticketToRow(&ticket, extCols))
	}
	return rows
}

func ticketToRow(ticket *ommonitor.Ticket, extCols []string) table.Row {
	ttl := ""
	if ticket.ExpiredAt != nil {
		ttl = viewutils.HumanDuration((*ticket.ExpiredAt).Sub(time.Now()))
	}
	row := table.Row{
		ticket.TicketID,       // ID
		string(ticket.Status), // Status
		viewutils.HumanDuration(time.Since(ticket.CreatedAt)), // Age
		ttl, // TTL
	}
	if len(extCols) > 0 {
		valMap := viewutils.ExtensionsToStrMap(ticket.Extensions)
		for _, col := range extCols {
			val, _ := valMap[col]
			row = append(row, val)
		}
	}
	return row
}
