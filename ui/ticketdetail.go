package ui

import (
	"fmt"
	"time"

	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/glamour"

	"github.com/DeNA/ommonitor"
	"github.com/DeNA/ommonitor/viewutils"
)

type TicketDetailModel struct {
	ticket   *ommonitor.Ticket
	renderer *glamour.TermRenderer
	viewport viewport.Model
}

func NewTicketDetailModel() (TicketDetailModel, error) {
	rd, err := glamour.NewTermRenderer()
	if err != nil {
		return TicketDetailModel{}, err
	}
	vp := viewport.New(30, 30)
	vp.SetContent("detail")
	return TicketDetailModel{
		renderer: rd,
		viewport: vp,
	}, nil
}

func (m TicketDetailModel) Update(msg tea.Msg) (TicketDetailModel, tea.Cmd) {
	var cmd tea.Cmd

	// update model
	m.viewport, cmd = m.viewport.Update(msg)

	return m, cmd
}

func (m *TicketDetailModel) SetTicket(ticket *ommonitor.Ticket) {
	m.ticket = ticket
	m.viewport.SetContent(m.viewportContent())
}

func (m TicketDetailModel) View() string {
	return ticketDetailStyle.Render(m.viewport.View())
}

func (m *TicketDetailModel) SetSize(width, height int) {
	rd, err := glamour.NewTermRenderer(glamour.WithAutoStyle(), glamour.WithWordWrap(width))
	if err != nil {
		m.viewport.SetContent(fmt.Sprintf("render error: %+v", err))
		return
	}
	m.renderer = rd
	m.viewport = viewport.New(width, height)
	m.viewport.SetContent(m.viewportContent())
}

func (m TicketDetailModel) viewportContent() string {
	if m.ticket == nil {
		return "<ticket detail area>"
	}
	searchFields := viewutils.PrettyJSON(m.ticket.SearchFields)
	assignment := viewutils.AssignmentToJSON(m.ticket.Assignment)
	extensions := viewutils.ExtensionsToJSON(m.ticket.Extensions)
	createdAt := viewutils.HumanTime(m.ticket.CreatedAt)
	elapsed := viewutils.HumanDuration(time.Since(m.ticket.CreatedAt))
	expiredAt := "<none>"
	if m.ticket.ExpiredAt != nil {
		expiredAt = viewutils.HumanTime(*m.ticket.ExpiredAt)
	}
	str, err := m.renderer.Render(fmt.Sprintf(`
# Ticket: %s
- Created at: %s (%s ago)
- Expired at: %s
## Search Fields
%s
## Assignment
%s
## Extensions
%s
`, m.ticket.TicketID, createdAt, elapsed, expiredAt, mdJSONCodeBlock(searchFields), mdJSONCodeBlock(assignment), mdJSONCodeBlock(extensions)))
	if err != nil {
		return fmt.Sprintf("details render error: %+v", err)
	}
	return str
}

func mdJSONCodeBlock(s string) string {
	return fmt.Sprintf("```json\n%s\n```", s)
}
