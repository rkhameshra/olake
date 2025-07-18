package waljs

import (
	"context"
	"crypto/tls"
	"fmt"
	"strings"
	"time"

	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jmoiron/sqlx"
)

const (
	ReplicationSlotTempl = "SELECT plugin, slot_type, confirmed_flush_lsn, pg_current_wal_lsn() as current_lsn FROM pg_replication_slots WHERE slot_name = '%s'"
	AdvanceLSNTemplate   = "SELECT * FROM pg_replication_slot_advance('%s', '%s')"
	noRecordErr          = "no record found"
)

var pluginArguments = []string{
	"\"include-lsn\" 'on'",
	"\"pretty-print\" 'off'",
	"\"include-timestamp\" 'on'",
}

// Socket represents a connection to PostgreSQL's logical replication stream
type Socket struct {
	// pgConn is the underlying PostgreSQL replication connection
	pgConn *pgconn.PgConn
	// clientXLogPos tracks the current position in the Write-Ahead Log (WAL)
	ClientXLogPos pglogrepl.LSN
	// changeFilter filters WAL changes based on configured tables
	changeFilter ChangeFilter
	// confirmedLSN is the position from which replication should start (Prev marked lsn)
	ConfirmedFlushLSN pglogrepl.LSN
	// wal position at a point of time
	CurrentWalPosition pglogrepl.LSN
	// replicationSlot is the name of the PostgreSQL replication slot being used
	replicationSlot string
	// initialWaitTime is the duration to wait for initial data before timing out
	initialWaitTime time.Duration
}

func NewConnection(ctx context.Context, db *sqlx.DB, config *Config, typeConverter func(value interface{}, columnType string) (interface{}, error)) (*Socket, error) {
	// Build PostgreSQL connection config
	connURL := config.Connection
	q := connURL.Query()
	q.Set("replication", "database")
	connURL.RawQuery = q.Encode()

	cfg, err := pgconn.ParseConfig(connURL.String())
	if err != nil {
		return nil, fmt.Errorf("failed to parse connection url: %s", err)
	}

	cfg.OnNotice = func(_ *pgconn.PgConn, n *pgconn.Notice) {
		logger.Warnf("notice received from pg conn: %s", n.Message)
	}

	cfg.OnNotification = func(_ *pgconn.PgConn, n *pgconn.Notification) {
		logger.Warnf("notification received from pg conn: %s", n.Payload)
	}

	cfg.OnPgError = func(_ *pgconn.PgConn, pe *pgconn.PgError) bool {
		logger.Warnf("pg conn thrown code[%s] and error: %s", pe.Code, pe.Message)
		// close connection and fail sync
		return false
	}
	if config.TLSConfig != nil {
		// TODO: use proper TLS Configurations
		cfg.TLSConfig = &tls.Config{InsecureSkipVerify: false, MinVersion: tls.VersionTLS12}
	}

	// Establish PostgreSQL connection
	pgConn, err := pgconn.ConnectConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create postgres connection: %s", err)
	}

	// System identification
	sysident, err := pglogrepl.IdentifySystem(ctx, pgConn)
	if err != nil {
		return nil, fmt.Errorf("failed to indentify system: %s", err)
	}
	logger.Infof("SystemID:%s Timeline:%d XLogPos:%s Database:%s",
		sysident.SystemID, sysident.Timeline, sysident.XLogPos, sysident.DBName)

	// Get replication slot position
	var slot ReplicationSlot
	if err := db.Get(&slot, fmt.Sprintf(ReplicationSlotTempl, config.ReplicationSlotName)); err != nil {
		return nil, fmt.Errorf("failed to get replication slot: %s", err)
	}

	// Create and return final connection object
	return &Socket{
		pgConn:             pgConn,
		changeFilter:       NewChangeFilter(typeConverter, config.Tables.Array()...),
		ConfirmedFlushLSN:  slot.LSN,
		ClientXLogPos:      slot.LSN,
		CurrentWalPosition: slot.CurrentLSN,
		replicationSlot:    config.ReplicationSlotName,
		initialWaitTime:    config.InitialWaitTime,
	}, nil
}

// advanceLSN advances the logical replication position to the current WAL position.
func (s *Socket) AdvanceLSN(ctx context.Context, db *sqlx.DB) error {
	// Get replication slot position
	if _, err := db.ExecContext(ctx, fmt.Sprintf(AdvanceLSNTemplate, s.replicationSlot, s.CurrentWalPosition.String())); err != nil {
		return fmt.Errorf("failed to advance replication slot: %s", err)
	}
	logger.Debugf("advanced LSN to %s", s.CurrentWalPosition.String())
	return nil
}

// Confirm that Logs has been recorded
func (s *Socket) AcknowledgeLSN(ctx context.Context, fakeAck bool) error {
	walPosition := s.ClientXLogPos
	if fakeAck {
		walPosition = s.ConfirmedFlushLSN
	}
	err := pglogrepl.SendStandbyStatusUpdate(ctx, s.pgConn, pglogrepl.StandbyStatusUpdate{
		WALWritePosition: walPosition,
		WALFlushPosition: walPosition,
	})
	if err != nil {
		return fmt.Errorf("failed to send standby status message on wal position[%s]: %s", walPosition.String(), err)
	}

	// Update local pointer and state
	logger.Debugf("sent standby status message at LSN#%s", walPosition.String())
	return nil
}

func (s *Socket) StreamMessages(ctx context.Context, db *sqlx.DB, callback abstract.CDCMsgFn) error {
	// update current lsn information
	var slot ReplicationSlot
	if err := db.Get(&slot, fmt.Sprintf(ReplicationSlotTempl, s.replicationSlot)); err != nil {
		return fmt.Errorf("failed to get replication slot: %s", err)
	}

	// update current wal lsn
	s.CurrentWalPosition = slot.CurrentLSN

	// Start logical replication with wal2json plugin arguments.
	var tables []string
	for key := range s.changeFilter.tables {
		tables = append(tables, key)
	}
	pluginArguments = append(pluginArguments, fmt.Sprintf("\"add-tables\" '%s'", strings.Join(tables, ",")))
	if err := pglogrepl.StartReplication(
		ctx,
		s.pgConn,
		s.replicationSlot,
		s.ConfirmedFlushLSN,
		pglogrepl.StartReplicationOptions{PluginArgs: pluginArguments},
	); err != nil {
		return fmt.Errorf("starting replication slot failed: %s", err)
	}
	logger.Infof("Started logical replication for slot[%s] from lsn[%s] to lsn[%s]", s.replicationSlot, s.ConfirmedFlushLSN, s.CurrentWalPosition)
	messageReceived := false
	cdcStartTime := time.Now()
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			if !messageReceived && s.initialWaitTime > 0 && time.Since(cdcStartTime) > s.initialWaitTime {
				logger.Warnf("no records found in given initial wait time, try increasing it or do full load")
				return nil
			}

			if s.ClientXLogPos >= s.CurrentWalPosition {
				logger.Infof("finishing sync, reached wal position: %s", s.CurrentWalPosition)
				return nil
			}

			msg, err := s.pgConn.ReceiveMessage(ctx)
			if err != nil {
				if strings.Contains(err.Error(), "EOF") {
					return nil
				}
				return fmt.Errorf("failed to receive message from wal logs: %s", err)
			}

			// Process only CopyData messages.
			copyData, ok := msg.(*pgproto3.CopyData)
			if !ok {
				return fmt.Errorf("unexpected message type: %T", msg)
			}

			switch copyData.Data[0] {
			case pglogrepl.PrimaryKeepaliveMessageByteID:
				// For keepalive messages, process them (but no ack is sent here).
				pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(copyData.Data[1:])
				if err != nil {
					return fmt.Errorf("failed to parse primary keepalive message: %s", err)
				}
				s.ClientXLogPos = pkm.ServerWALEnd
				if pkm.ReplyRequested {
					logger.Debugf("keep alive message received: %v", pkm)
					// send fake acknowledgement
					err := s.AcknowledgeLSN(ctx, true)
					if err != nil {
						return fmt.Errorf("failed to ack lsn: %s", err)
					}
				}
			case pglogrepl.XLogDataByteID:
				// Reset the idle timer on receiving WAL data.
				xld, err := pglogrepl.ParseXLogData(copyData.Data[1:])
				if err != nil {
					return fmt.Errorf("failed to parse XLogData: %s", err)
				}
				// Process change with the provided callback.
				nextLSN, records, err := s.changeFilter.FilterChange(xld.WALData, callback)
				if err != nil {
					return fmt.Errorf("failed to filter change: %s", err)
				}
				messageReceived = records > 0 || messageReceived
				s.ClientXLogPos = *nextLSN
			default:
				logger.Warnf("received unhandled message type: %v", copyData.Data[0])
			}
		}
	}
}

// cleanUpOnFailure drops replication slot and publication if database snapshotting was failed for any reason
func (s *Socket) Cleanup(ctx context.Context) {
	if s.pgConn != nil {
		_ = s.pgConn.Close(ctx)
	}
}
