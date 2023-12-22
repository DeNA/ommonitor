package ommonitor

import (
	"context"
	"fmt"
	"github.com/redis/rueidis"
	"sort"
	"strings"
	"time"

	"github.com/rs/xid"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"open-match.dev/open-match/pkg/pb"
)

type TicketStatus string

const (
	TicketStatusActive   TicketStatus = "Active"
	TicketStatusProposed TicketStatus = "Proposed"
	TicketStatusAssigned TicketStatus = "Assigned"
)

const (
	allTicketsKey      = "allTickets"
	proposedTicketsKey = "proposed_ticket_ids"
)

type Ticket struct {
	TicketID     string
	Status       TicketStatus
	CreatedAt    time.Time
	SearchFields *pb.SearchFields
	Assignment   *pb.Assignment
	ExpiredAt    *time.Time
	Extensions   map[string]*anypb.Any
}

func (t *Ticket) HasExpired(now time.Time) bool {
	if t.ExpiredAt == nil {
		return false
	}
	return now.After(*t.ExpiredAt)
}

type Monitor struct {
	redisAddr      string
	client         rueidis.Client
	ticketsCache   map[string]*Ticket
	firstFetchDone bool
	options        *monitorOptions
}

type MonitorOption interface {
	apply(options *monitorOptions)
}

type MonitorOptionFunc func(options *monitorOptions)

func (f MonitorOptionFunc) apply(options *monitorOptions) {
	f(options)
}

func WithMinimatch() MonitorOption {
	return MonitorOptionFunc(func(options *monitorOptions) {
		options.isMinimatch = true
	})
}

type monitorOptions struct {
	isMinimatch bool
}

func defaultMonitorOptions() *monitorOptions {
	return &monitorOptions{
		isMinimatch: false,
	}
}

func NewMonitor(redisAddr string, client rueidis.Client, opts ...MonitorOption) *Monitor {
	options := defaultMonitorOptions()
	for _, o := range opts {
		o.apply(options)
	}
	return &Monitor{
		redisAddr:    redisAddr,
		client:       client,
		ticketsCache: map[string]*Ticket{},
		options:      options,
	}
}

func (m *Monitor) RedisAddr() string {
	return m.redisAddr
}

func (m *Monitor) FetchAllTickets(ctx context.Context) ([]Ticket, error) {
	// First, scan the entire Redis space and fetch all ticketsMap.
	if !m.firstFetchDone {
		tids, err := m.scanAllTicketOrBackfillIDs(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to scan ticketsMap: %w", err)
		}
		ticketsMap, err := m.fetchTicketsAsMap(ctx, tids)
		if err != nil {
			return nil, err
		}
		for _, ticket := range ticketsMap {
			m.ticketsCache[ticket.TicketID] = ticket
		}
		m.firstFetchDone = true
	}

	// From the second time on, monitor indexed ticketsMap and fetch only newly created ticketsMap.
	resp := m.client.Do(ctx, m.client.B().Smembers().Key(allTicketsKey).Build())
	if err := resp.Error(); err != nil {
		return nil, fmt.Errorf("failed to fetch '%s': %w", allTicketsKey, err)
	}
	indexedTicketIDs, err := resp.AsStrSlice()
	if err != nil {
		return nil, fmt.Errorf("failed to fetch indexedTicketIDs as str slice: %w", err)
	}
	indexedTicketsIDMap := make(map[string]struct{}, len(indexedTicketIDs))
	var fetchKeys []string
	for _, tid := range indexedTicketIDs {
		if _, ok := m.ticketsCache[tid]; !ok {
			fetchKeys = append(fetchKeys, tid)
		}
		indexedTicketsIDMap[tid] = struct{}{}
	}
	ticketsMap, err := m.fetchTicketsAsMap(ctx, fetchKeys)
	if err != nil {
		return nil, err
	}
	for _, ticket := range ticketsMap {
		m.ticketsCache[ticket.TicketID] = ticket
	}

	// get proposed ids
	resp = m.client.Do(ctx, m.client.B().Zrevrange().Key(proposedTicketsKey).Start(0).Stop(-1).Build())
	if err := resp.Error(); err != nil {
		return nil, fmt.Errorf("failed to fetch '%s': %w", proposedTicketsKey, err)
	}
	proposedIDs, err := resp.AsStrSlice()
	if err != nil {
	}
	proposedIDMap := make(map[string]struct{}, len(proposedIDs))
	for _, pid := range proposedIDs {
		proposedIDMap[pid] = struct{}{}
	}

	var deIndexedTicketIDs []string
	// check de-indexed (Assigned or Deleted) ticketsMap
	for _, ticket := range m.ticketsCache {
		if _, ok := indexedTicketsIDMap[ticket.TicketID]; !ok {
			deIndexedTicketIDs = append(deIndexedTicketIDs, ticket.TicketID)
		}
	}
	deIndexedTickets, err := m.fetchTicketsAsMap(ctx, deIndexedTicketIDs)
	if err != nil {
		return nil, err
	}

	for _, ticketID := range deIndexedTicketIDs {
		t, ok := deIndexedTickets[ticketID]
		if !ok {
			delete(m.ticketsCache, ticketID)
			continue
		}
		if _, proposed := proposedIDMap[ticketID]; t.Assignment == nil && proposed {
			t.Status = TicketStatusProposed
		}
		m.ticketsCache[ticketID] = t
	}

	var tickets []Ticket
	for _, ticket := range m.ticketsCache {
		tickets = append(tickets, *ticket)
	}
	sort.SliceStable(tickets, func(i, j int) bool {
		return tickets[i].CreatedAt.UnixNano() > tickets[j].CreatedAt.UnixNano()
	})
	return tickets, nil
}

func (m *Monitor) scanAllTicketOrBackfillIDs(ctx context.Context) ([]string, error) {
	var ticketIDs []string
	cursor := uint64(0)
	for {
		resp := m.client.Do(ctx, m.client.B().Scan().Cursor(cursor).Count(10).Build())
		if err := resp.Error(); err != nil {
			return nil, err
		}
		entry, err := resp.AsScanEntry()
		if err != nil {
			return nil, err
		}
		for _, key := range entry.Elements {
			if _, err := xid.FromString(key); err == nil {
				ticketIDs = append(ticketIDs, key)
			}
		}
		if entry.Cursor == 0 {
			break
		}
		cursor = entry.Cursor
	}
	return ticketIDs, nil
}

func (m *Monitor) fetchTicketsAsMap(ctx context.Context, ticketIDs []string) (map[string]*Ticket, error) {
	if len(ticketIDs) == 0 {
		return map[string]*Ticket{}, nil
	}
	keys := make([]string, 0, len(ticketIDs))
	for _, ticketID := range ticketIDs {
		keys = append(keys, redisKeyTicketData(ticketID))
	}
	mGet, err := rueidis.MGet(m.client, ctx, keys)
	if err != nil {
		return nil, err
	}
	tickets := map[string]*Ticket{}
	for _, resp := range mGet {
		if err := resp.Error(); err != nil {
			if rueidis.IsRedisNil(err) {
				// key not exists
				continue
			}
			return nil, fmt.Errorf("failed to fetch Tickets: %w", err)
		}
		data, err := resp.AsBytes()
		if err != nil {
			return nil, fmt.Errorf("failed to get ticket data as bytes: %w", err)
		}
		t, err := decodeTicket(data)
		if err != nil {
			// not Ticket but Backfill?
			continue
		}
		status := TicketStatusActive
		var expiredAt *time.Time
		if exp, ok := m.getExpiredAt(ctx, t.Id); ok {
			expiredAt = &exp
		}
		tickets[t.Id] = &Ticket{
			TicketID:     t.Id,
			Status:       status,
			CreatedAt:    t.CreateTime.AsTime(),
			SearchFields: t.SearchFields,
			Extensions:   t.Extensions,
			Assignment:   t.Assignment,
			ExpiredAt:    expiredAt,
		}
	}
	if m.options.isMinimatch {
		keys := make([]string, 0, len(tickets))
		for _, ticket := range tickets {
			keys = append(keys, redisKeyAssignmentData(ticket.TicketID))
		}
		mGet, err := rueidis.MGet(m.client, ctx, keys)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch assignemnts: %w", err)
		}
		for key, resp := range mGet {
			if err := resp.Error(); err != nil {
				if rueidis.IsRedisNil(err) {
					continue
				}
				return nil, fmt.Errorf("failed to fetch assignment: %w", err)
			}
			data, err := resp.AsBytes()
			if err != nil {
				return nil, fmt.Errorf("failed to get assignment data as bytes: %w", err)
			}
			as, err := decodeAssignment(data)
			if err != nil {
				return nil, fmt.Errorf("failed to decode assignment data: %w", err)
			}
			ticketID := ticketIDFromAssignmentKey(key)
			if _, ok := tickets[ticketID]; ok {
				tickets[ticketID].Assignment = as
			}
		}
	}
	for _, ticket := range tickets {
		if ticket.Assignment != nil {
			ticket.Status = TicketStatusAssigned
		}
	}
	return tickets, nil
}

func (m *Monitor) getExpiredAt(ctx context.Context, ticketID string) (time.Time, bool) {
	key := redisKeyTicketData(ticketID)
	resp := m.client.Do(ctx, m.client.B().Ttl().Key(key).Build())
	if err := resp.Error(); err != nil {
		return time.Time{}, false
	}
	ttl, err := resp.AsInt64()
	if err != nil || ttl < 0 {
		return time.Time{}, false
	}
	return time.Now().Add(time.Duration(ttl) * time.Second), true
}

func decodeTicket(data []byte) (*pb.Ticket, error) {
	var t pb.Ticket
	if err := proto.Unmarshal(data, &t); err != nil {
		return nil, fmt.Errorf("failed to unmarshal Ticket: %w", err)
	}
	return &t, nil
}

func decodeAssignment(data []byte) (*pb.Assignment, error) {
	var as pb.Assignment
	if err := proto.Unmarshal(data, &as); err != nil {
		return nil, fmt.Errorf("failed to unmarshal Assignment: %w", err)
	}
	return &as, nil
}

func redisKeyTicketData(ticketID string) string {
	return ticketID
}

func redisKeyAssignmentData(ticketID string) string {
	return fmt.Sprintf("assign:%s", ticketID)
}

func ticketIDFromAssignmentKey(key string) string {
	if cut, ok := strings.CutPrefix(key, "assign:"); ok {
		return cut
	}
	return key
}
