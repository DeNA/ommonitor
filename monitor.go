package ommonitor

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/redis/rueidis"
	"github.com/rs/xid"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"open-match.dev/open-match/pkg/pb"
)

type TicketStatus string

const (
	TicketStatusActive   TicketStatus = "Active"
	TicketStatusProposed TicketStatus = "Proposed"
	TicketStatusAssigned TicketStatus = "Assigned"
	scanPageSize                      = 1000
	fetchChunkSize                    = 1000
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

func WithRedisKeyPrefix(prefix string) MonitorOption {
	return MonitorOptionFunc(func(options *monitorOptions) {
		options.redisKeyPrefix = prefix
	})
}

type monitorOptions struct {
	isMinimatch    bool
	redisKeyPrefix string
}

func defaultMonitorOptions() *monitorOptions {
	return &monitorOptions{
		isMinimatch:    false,
		redisKeyPrefix: "",
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

func (m *Monitor) TicketCount() int {
	return len(m.ticketsCache)
}

func (m *Monitor) RedisAddr() string {
	return m.redisAddr
}

func (m *Monitor) FetchAllTickets(ctx context.Context, progress func(MonitorFetchProgress)) ([]Ticket, error) {
	// First, scan the entire Redis space and fetch all tickets.
	var fetchTicketIDs []string
	if !m.firstFetchDone {
		tids, err := m.scanAllTicketOrBackfillIDs(ctx, progress)
		if err != nil {
			return nil, fmt.Errorf("failed to scan all ticket IDs in redis space: %w", err)
		}
		fetchTicketIDs = tids
	}

	indexedTicketsMap, err := m.getIndexedTicketIDMap(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to scan ticket index: %w", err)
	}
	if m.firstFetchDone {
		// fetch only newly created tickets after first-scan.
		for tid := range indexedTicketsMap {
			if _, ok := m.ticketsCache[tid]; !ok {
				fetchTicketIDs = append(fetchTicketIDs, tid)
			}
		}
	}

	fetchedTickets, err := m.fetchTicketsAsMap(ctx, fetchTicketIDs, progress)
	if err != nil {
		return nil, err
	}
	var addedTicketIDs []string
	for _, ticket := range fetchedTickets {
		if _, ok := m.ticketsCache[ticket.TicketID]; !ok {
			addedTicketIDs = append(addedTicketIDs, ticket.TicketID)
			m.ticketsCache[ticket.TicketID] = ticket
		}
	}

	var expiredTicketIDs []string
	// deIndexed: (Assigned or Deleted)
	var deIndexedTicketIDs []string
	for _, ticket := range m.ticketsCache {
		if ticket.HasExpired(time.Now()) {
			expiredTicketIDs = append(expiredTicketIDs, ticket.TicketID)
		} else if _, ok := indexedTicketsMap[ticket.TicketID]; !ok {
			deIndexedTicketIDs = append(deIndexedTicketIDs, ticket.TicketID)
		}
	}
	for _, ticketID := range expiredTicketIDs {
		delete(m.ticketsCache, ticketID)
	}

	deIndexedTickets, err := m.fetchTicketsAsMap(ctx, deIndexedTicketIDs, nil)
	if err != nil {
		return nil, err
	}
	for _, ticket := range m.ticketsCache {
		if updated, ok := deIndexedTickets[ticket.TicketID]; ok {
			m.ticketsCache[ticket.TicketID] = updated
		}
	}

	proposedTicketIDs, err := m.getProposedTicketIDMap(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get proposed ticket ids: %w", err)
	}

	var tickets []Ticket
	for _, ticket := range m.ticketsCache {
		if _, ok := proposedTicketIDs[ticket.TicketID]; ok {
			ticket.Status = TicketStatusProposed
		}
		tickets = append(tickets, *ticket)
	}
	sort.SliceStable(tickets, func(i, j int) bool {
		return tickets[i].CreatedAt.UnixNano() > tickets[j].CreatedAt.UnixNano()
	})
	if progress != nil {
		progress(MonitorFetchProgress{
			State:          MonitorFetchProgressStateDone,
			TicketsAdded:   len(addedTicketIDs),
			TicketsExpired: len(expiredTicketIDs),
		})
	}
	if !m.firstFetchDone {
		m.firstFetchDone = true
	}
	return tickets, nil
}

type MonitorFetchProgressState string

const (
	MonitorFetchProgressStateScanning MonitorFetchProgressState = "scanning"
	MonitorFetchProgressStateFetching MonitorFetchProgressState = "fetching"
	MonitorFetchProgressStateDone     MonitorFetchProgressState = "done"
)

type MonitorFetchProgress struct {
	State          MonitorFetchProgressState
	TicketsScanned int
	TicketsFetched int
	// --- done
	TicketsAdded   int
	TicketsExpired int
}

// Indexed: (Active or Proposed)
func (m *Monitor) getIndexedTicketIDMap(ctx context.Context) (map[string]struct{}, error) {
	key := redisKeyAllTickets(m.options.redisKeyPrefix)
	resp := m.client.Do(ctx, m.client.B().Smembers().Key(key).Build())
	if err := resp.Error(); err != nil {
		return nil, fmt.Errorf("failed to fetch '%s': %w", key, err)
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
	return indexedTicketsIDMap, nil
}

func (m *Monitor) getProposedTicketIDMap(ctx context.Context) (map[string]struct{}, error) {
	key := redisKeyProposedTickets(m.options.redisKeyPrefix)
	resp := m.client.Do(ctx, m.client.B().Zrevrange().Key(key).Start(0).Stop(-1).Build())
	if err := resp.Error(); err != nil {
		return nil, fmt.Errorf("failed to fetch '%s': %w", key, err)
	}
	proposedIDs, err := resp.AsStrSlice()
	if err != nil {
		return nil, fmt.Errorf("failed to decode proposed IDs as str slice: %w", err)
	}
	proposedIDMap := make(map[string]struct{}, len(proposedIDs))
	for _, pid := range proposedIDs {
		proposedIDMap[pid] = struct{}{}
	}
	return proposedIDMap, nil
}

func (m *Monitor) scanAllTicketOrBackfillIDs(ctx context.Context, progress func(MonitorFetchProgress)) ([]string, error) {
	var ticketIDs []string
	cursor := uint64(0)
	for {
		resp := m.client.Do(ctx, m.client.B().Scan().
			Cursor(cursor).
			Match(fmt.Sprintf("%s*", m.options.redisKeyPrefix)).
			Count(scanPageSize).Build())
		if err := resp.Error(); err != nil {
			return nil, err
		}
		entry, err := resp.AsScanEntry()
		if err != nil {
			return nil, err
		}
		for _, key := range entry.Elements {
			key = strings.TrimPrefix(key, m.options.redisKeyPrefix)
			if _, err := xid.FromString(key); err == nil {
				ticketIDs = append(ticketIDs, key)
			}
		}
		if progress != nil {
			progress(MonitorFetchProgress{State: MonitorFetchProgressStateScanning, TicketsScanned: len(ticketIDs)})
		}
		if entry.Cursor == 0 {
			break
		}
		cursor = entry.Cursor
	}
	return ticketIDs, nil
}

func (m *Monitor) fetchTicketsAsMap(ctx context.Context, ticketIDs []string, progress func(MonitorFetchProgress)) (map[string]*Ticket, error) {
	if len(ticketIDs) == 0 {
		return map[string]*Ticket{}, nil
	}
	keys := make([]string, 0, len(ticketIDs))
	for _, ticketID := range ticketIDs {
		keys = append(keys, redisKeyTicketData(m.options.redisKeyPrefix, ticketID))
	}

	tickets := map[string]*Ticket{}
	for _, chunkedKeys := range chunkBy(keys, fetchChunkSize) {
		mGet, err := rueidis.MGet(m.client, ctx, chunkedKeys)
		if err != nil {
			return nil, err
		}
		ticketCh := make(chan *Ticket, len(chunkedKeys))
		eg, _ := errgroup.WithContext(ctx)
		for _, resp := range mGet {
			if err := resp.Error(); err != nil {
				if rueidis.IsRedisNil(err) {
					// key not exists
					continue
				}
			}
			r := resp
			eg.Go(func() error {
				ticket, err := m.decodeTicketResp(ctx, &r)
				if err != nil {
					if errors.Is(err, errNotTicket) {
						// skip when backfill
						return nil
					}
					return err
				}
				ticketCh <- ticket
				return nil
			})
		}
		if err := eg.Wait(); err != nil {
			return nil, fmt.Errorf("failed to fetch Tickets: %w", err)
		}
		close(ticketCh)
		var chunkedTicketIDs []string
		for ticket := range ticketCh {
			tickets[ticket.TicketID] = ticket
			chunkedTicketIDs = append(chunkedTicketIDs, ticket.TicketID)
		}
		if m.options.isMinimatch {
			if err := m.fetchMinimatchAssignments(ctx, chunkedTicketIDs, tickets); err != nil {
				return nil, fmt.Errorf("failed to fetch minimatch assignments: %w", err)
			}
		}
		if progress != nil {
			progress(MonitorFetchProgress{
				State:          MonitorFetchProgressStateFetching,
				TicketsFetched: len(tickets),
			})
		}
	}
	return tickets, nil
}

var (
	errNotTicket = errors.New("is not ticket")
)

func (m *Monitor) decodeTicketResp(ctx context.Context, resp *rueidis.RedisMessage) (*Ticket, error) {
	data, err := resp.AsBytes()
	if err != nil {
		return nil, fmt.Errorf("failed to get ticket data as bytes: %w", err)
	}
	t, err := decodeTicket(data)
	if err != nil {
		// not Ticket but Backfill?
		return nil, errNotTicket
	}
	status := TicketStatusActive
	var expiredAt *time.Time
	if exp, ok := m.getExpiredAt(ctx, t.Id); ok {
		expiredAt = &exp
	}
	return &Ticket{
		TicketID:     t.Id,
		Status:       status,
		CreatedAt:    t.CreateTime.AsTime(),
		SearchFields: t.SearchFields,
		Extensions:   t.Extensions,
		Assignment:   t.Assignment,
		ExpiredAt:    expiredAt,
	}, nil
}

func (m *Monitor) fetchMinimatchAssignments(ctx context.Context, ticketIDs []string, tickets map[string]*Ticket) error {
	keys := make([]string, 0, len(ticketIDs))
	for _, ticketID := range ticketIDs {
		keys = append(keys, redisKeyAssignmentData(m.options.redisKeyPrefix, ticketID))
	}
	mGet, err := rueidis.MGet(m.client, ctx, keys)
	if err != nil {
		return fmt.Errorf("failed to fetch assignemnts: %w", err)
	}

	asCh := make(chan struct {
		ticketID string
		as       *pb.Assignment
	}, len(keys))
	eg, _ := errgroup.WithContext(ctx)
	for key, resp := range mGet {
		if err := resp.Error(); err != nil {
			if rueidis.IsRedisNil(err) {
				continue
			}
			return fmt.Errorf("failed to fetch assignment: %w", err)
		}
		r := resp
		eg.Go(func() error {
			data, err := r.AsBytes()
			if err != nil {
				return fmt.Errorf("failed to get assignment data as bytes: %w", err)
			}
			as, err := decodeAssignment(data)
			if err != nil {
				return fmt.Errorf("failed to decode assignment data: %w", err)
			}
			ticketID := ticketIDFromAssignmentKey(m.options.redisKeyPrefix, key)
			asCh <- struct {
				ticketID string
				as       *pb.Assignment
			}{ticketID: ticketID, as: as}
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return err
	}
	close(asCh)
	for v := range asCh {
		if _, ok := tickets[v.ticketID]; ok {
			tickets[v.ticketID].Assignment = v.as
			tickets[v.ticketID].Status = TicketStatusAssigned
		}
	}
	return nil
}

func (m *Monitor) getExpiredAt(ctx context.Context, ticketID string) (time.Time, bool) {
	key := redisKeyTicketData(m.options.redisKeyPrefix, ticketID)
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

func redisKeyAllTickets(prefix string) string {
	return fmt.Sprintf("%sallTickets", prefix)
}

func redisKeyProposedTickets(prefix string) string {
	return fmt.Sprintf("%sproposed_ticket_ids", prefix)
}

func redisKeyTicketData(prefix, ticketID string) string {
	return fmt.Sprintf("%s%s", prefix, ticketID)
}

func redisKeyAssignmentData(prefix, ticketID string) string {
	return fmt.Sprintf("%sassign:%s", prefix, ticketID)
}

func ticketIDFromAssignmentKey(prefix, key string) string {
	key = strings.TrimPrefix(key, prefix)
	if cut, ok := strings.CutPrefix(key, "assign:"); ok {
		return cut
	}
	return key
}

// https://stackoverflow.com/a/72408490
func chunkBy[T any](items []T, chunkSize int) (chunks [][]T) {
	for chunkSize < len(items) {
		items, chunks = items[chunkSize:], append(chunks, items[0:chunkSize:chunkSize])
	}
	return append(chunks, items)
}
