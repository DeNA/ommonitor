package ommonitor

import (
	"context"
	"encoding/base64"
	"fmt"
	"sort"
	"time"

	"github.com/redis/go-redis/v9"
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
	client         *redis.Client
	ticketsCache   map[string]*Ticket
	firstFetchDone bool
}

func NewMonitor(client *redis.Client) *Monitor {
	return &Monitor{
		client:       client,
		ticketsCache: map[string]*Ticket{},
	}
}

func (m *Monitor) RedisAddr() string {
	return m.client.Options().Addr
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
	indexedTicketIDs, err := m.client.SMembers(ctx, allTicketsKey).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to fetch %s: %w", allTicketsKey, err)
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
	proposedIDs, err := m.client.ZRevRange(ctx, proposedTicketsKey, 0, -1).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to fetch %s: %w", proposedTicketsKey, err)
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
		keys, cur, err := m.client.Scan(ctx, cursor, "", 10).Result()
		if err != nil {
			return nil, err
		}
		for _, key := range keys {
			if _, err := xid.FromString(key); err == nil {
				ticketIDs = append(ticketIDs, key)
			}
		}
		if cur == 0 {
			break
		}
		cursor = cur
	}
	return ticketIDs, nil
}

func (m *Monitor) fetchTicketsAsMap(ctx context.Context, ids []string) (map[string]*Ticket, error) {
	if len(ids) == 0 {
		return map[string]*Ticket{}, nil
	}

	datas, err := m.client.MGet(ctx, ids...).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to fetch Tickets: %w", err)
	}
	tickets := map[string]*Ticket{}
	for _, data := range datas {
		if data == nil {
			// key not exists
			continue
		}
		t, err := decodeTicket(data.(string))
		if err != nil {
			// not Ticket but Backfill?
			continue
		}
		var expiredAt *time.Time
		status := TicketStatusActive
		if t.Assignment != nil {
			status = TicketStatusAssigned
			if exp, ok := m.getExpiredAt(ctx, t.Id); ok {
				expiredAt = &exp
			}
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
	return tickets, nil
}

func (m *Monitor) getAssignment(ctx context.Context, ticketID string) (*pb.Assignment, error) {
	data, err := m.client.Get(ctx, ticketID).Result()
	if err != nil {
		return nil, err
	}
	ticket, err := decodeTicket(data)
	if err != nil {
		return nil, err
	}
	return ticket.Assignment, nil
}

func (m *Monitor) getExpiredAt(ctx context.Context, ticketID string) (time.Time, bool) {
	ttl, err := m.client.TTL(ctx, ticketID).Result()
	if err != nil || ttl < 0 {
		return time.Time{}, false
	}
	return time.Now().Add(ttl), true
}

func decodeTicket(data string) (*pb.Ticket, error) {
	var t pb.Ticket
	b := []byte(data)
	// HACK: miniredis support
	if decoded, err := base64.StdEncoding.DecodeString(data); err == nil {
		b = decoded
	}
	if err := proto.Unmarshal(b, &t); err != nil {
		return nil, fmt.Errorf("failed to unmarshal Ticket: %w", err)
	}
	return &t, nil
}
