package ui

import (
	"github.com/charmbracelet/bubbles/table"
	"github.com/charmbracelet/lipgloss"
)

const (
	ticketDetailProportion = 0.3
)

var (
	titleBarStyle            = lipgloss.NewStyle().Foreground(lipgloss.AdaptiveColor{Light: "#9B9B9B", Dark: "#5C5C5C"})
	ticketTableHeaderStyle   = table.DefaultStyles().Header
	ticketTableCellStyle     = table.DefaultStyles().Cell
	ticketTableSelectedStyle = table.DefaultStyles().Selected
	ticketDetailStyle        = lipgloss.NewStyle().Border(lipgloss.ThickBorder(), false, false, false, true)
	statusBarStyle           = lipgloss.NewStyle().Foreground(lipgloss.AdaptiveColor{Light: "#9B9B9B", Dark: "#5C5C5C"})
)
