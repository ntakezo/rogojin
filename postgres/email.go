package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/ntakezo/rogojin/email"
)

// Emails is the email.Repository: one row per email, its inbox credentials
// in a JSON text column, in the clear. The listener claim lives in columns
// the Email model never sees, so a Save can neither read nor clobber a
// claim; the claim predicates compare against the server's now().
type Emails struct {
	db *sql.DB
}

// NewEmails builds the email store on db, bringing its tables up to the
// current schema.
func NewEmails(db *DB) (email.Repository, error) {
	if err := migrate(db.db, "email", emailMigrations); err != nil {
		return nil, err
	}
	return &Emails{db: db.db}, nil
}

// emailMigrations is the ordered schema history of the email store — a fresh
// baseline, since this adapter was born on the current contract.
var emailMigrations = []migration{
	{
		Name: "create emails table",
		SQL: `CREATE TABLE emails (
			id                  TEXT PRIMARY KEY,
			address             TEXT NOT NULL DEFAULT '',
			vendor              TEXT NOT NULL DEFAULT '',
			auth                TEXT NOT NULL DEFAULT '',
			last_uid            BIGINT NOT NULL DEFAULT 0,
			uid_validity        BIGINT NOT NULL DEFAULT 0,
			listener_node       TEXT NOT NULL DEFAULT '',
			listener_expires_at TIMESTAMPTZ,
			created_at          TEXT NOT NULL DEFAULT '',
			updated_at          TEXT NOT NULL DEFAULT ''
		)`,
	},
}

// List returns every stored email in stable id order, so the manager's
// inventory order is deterministic.
func (s *Emails) List(ctx context.Context) ([]email.Email, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, address, vendor, auth, last_uid, uid_validity, created_at, updated_at
		 FROM emails ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("list emails: %w", err)
	}
	defer rows.Close()

	listed := make([]email.Email, 0)
	for rows.Next() {
		var e email.Email
		var vendor, auth, created, updated string
		var lastUID, uidValidity uint32
		if err := rows.Scan(&e.ID, &e.Address, &vendor, &auth, &lastUID, &uidValidity, &created, &updated); err != nil {
			return nil, fmt.Errorf("list emails: %w", err)
		}
		if e.Inbox, err = parseInbox(e.ID, vendor, auth, lastUID, uidValidity); err != nil {
			return nil, fmt.Errorf("list emails: %w", err)
		}
		if e.CreatedAt, err = parseTime(created); err != nil {
			return nil, fmt.Errorf("list emails: %w", err)
		}
		if e.UpdatedAt, err = parseTime(updated); err != nil {
			return nil, fmt.Errorf("list emails: %w", err)
		}
		listed = append(listed, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list emails: %w", err)
	}
	return listed, nil
}

// Save upserts the email's record: address, inbox credentials, cursor, and
// updated_at. created_at is written on insert and never overwritten, and the
// listener claim columns are untouched — a Save cannot take or drop a claim.
func (s *Emails) Save(ctx context.Context, e email.Email) error {
	vendor, auth, lastUID, uidValidity, err := formatInbox(e.Inbox)
	if err != nil {
		return fmt.Errorf("save email %s: %w", e.ID, err)
	}
	_, err = s.db.ExecContext(ctx,
		`INSERT INTO emails (id, address, vendor, auth, last_uid, uid_validity, created_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		 ON CONFLICT (id) DO UPDATE SET address = EXCLUDED.address,
		 vendor = EXCLUDED.vendor, auth = EXCLUDED.auth,
		 last_uid = EXCLUDED.last_uid, uid_validity = EXCLUDED.uid_validity,
		 updated_at = EXCLUDED.updated_at`,
		e.ID, e.Address, vendor, auth, lastUID, uidValidity,
		formatTime(e.CreatedAt), formatTime(e.UpdatedAt))
	if err != nil {
		return fmt.Errorf("save email %s: %w", e.ID, err)
	}
	return nil
}

// Delete removes the email's record; absent rows are a no-op.
func (s *Emails) Delete(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM emails WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("delete email %s: %w", id, err)
	}
	return nil
}

// ClaimListener takes the inbox's listener claim in one conditional UPDATE:
// it lands iff the claim is unheld, already node's, or expired by the
// server's clock. Claim bookkeeping never touches updated_at: a heartbeat is
// not an inventory change.
func (s *Emails) ClaimListener(ctx context.Context, emailID, node string, ttl time.Duration) error {
	res, err := s.db.ExecContext(ctx,
		`UPDATE emails SET listener_node = $1, listener_expires_at = `+fmt.Sprintf(ttlExpr, 2)+`
		 WHERE id = $3 AND (listener_node = '' OR listener_node = $1 OR listener_expires_at < now())`,
		node, ttl.Milliseconds(), emailID)
	if err != nil {
		return fmt.Errorf("claim listener of email %s: %w", emailID, err)
	}
	return s.refusedClaim(ctx, "claim listener", emailID, res)
}

// RenewListener extends the claim iff node holds it, expired or not — a late
// but unusurped renewal wins.
func (s *Emails) RenewListener(ctx context.Context, emailID, node string, ttl time.Duration) error {
	res, err := s.db.ExecContext(ctx,
		`UPDATE emails SET listener_expires_at = `+fmt.Sprintf(ttlExpr, 1)+` WHERE id = $2 AND listener_node = $3`,
		ttl.Milliseconds(), emailID, node)
	if err != nil {
		return fmt.Errorf("renew listener of email %s: %w", emailID, err)
	}
	return s.refusedClaim(ctx, "renew listener", emailID, res)
}

// ReleaseListener clears the claim iff node holds it; a release after being
// usurped is a silent no-op.
func (s *Emails) ReleaseListener(ctx context.Context, emailID, node string) error {
	res, err := s.db.ExecContext(ctx,
		`UPDATE emails SET listener_node = '', listener_expires_at = NULL
		 WHERE id = $1 AND listener_node = $2`,
		emailID, node)
	if err != nil {
		return fmt.Errorf("release listener of email %s: %w", emailID, err)
	}
	if n, err := res.RowsAffected(); err != nil {
		return fmt.Errorf("release listener of email %s: %w", emailID, err)
	} else if n > 0 {
		return nil
	}
	if _, err := s.holderOf(ctx, emailID); errors.Is(err, email.ErrEmailNotFound) {
		return fmt.Errorf("release listener of email %s: %w", emailID, email.ErrEmailNotFound)
	} else if err != nil {
		return fmt.Errorf("release listener of email %s: %w", emailID, err)
	}
	return nil
}

// AdvanceCursor moves the cursor under the claim holder's hand only, and
// only forward: same validity with a higher UID, or a changed validity — the
// reset. The holder's refused non-forward write is a silent no-op.
func (s *Emails) AdvanceCursor(ctx context.Context, emailID, node string, uidValidity, lastUID uint32) error {
	res, err := s.db.ExecContext(ctx,
		`UPDATE emails SET last_uid = $1, uid_validity = $2, updated_at = $3
		 WHERE id = $4 AND listener_node = $5 AND (uid_validity != $2 OR last_uid < $1)`,
		int64(lastUID), int64(uidValidity), formatTime(time.Now()), emailID, node)
	if err != nil {
		return fmt.Errorf("advance cursor of email %s: %w", emailID, err)
	}
	if n, err := res.RowsAffected(); err != nil {
		return fmt.Errorf("advance cursor of email %s: %w", emailID, err)
	} else if n > 0 {
		return nil
	}
	holder, err := s.holderOf(ctx, emailID)
	if err != nil {
		return fmt.Errorf("advance cursor of email %s: %w", emailID, err)
	}
	if holder != node {
		return fmt.Errorf("advance cursor of email %s: %w", emailID, email.ErrListenerHeld)
	}
	return nil // the holder's non-forward move: a late duplicate, dropped
}

// refusedClaim turns a zero-row claim UPDATE into its meaning: no such
// email, or a claim held elsewhere.
func (s *Emails) refusedClaim(ctx context.Context, op, emailID string, res sql.Result) error {
	if n, err := res.RowsAffected(); err != nil {
		return fmt.Errorf("%s of email %s: %w", op, emailID, err)
	} else if n > 0 {
		return nil
	}
	if _, err := s.holderOf(ctx, emailID); err != nil {
		return fmt.Errorf("%s of email %s: %w", op, emailID, err)
	}
	return fmt.Errorf("%s of email %s: %w", op, emailID, email.ErrListenerHeld)
}

// holderOf reads the claim column, distinguishing a missing row from a
// refused write.
func (s *Emails) holderOf(ctx context.Context, emailID string) (string, error) {
	var node string
	err := s.db.QueryRowContext(ctx,
		`SELECT listener_node FROM emails WHERE id = $1`, emailID).Scan(&node)
	if errors.Is(err, sql.ErrNoRows) {
		return "", email.ErrEmailNotFound
	}
	if err != nil {
		return "", err
	}
	return node, nil
}

// formatInbox flattens the optional inbox into its columns; a nil inbox
// stores an empty vendor, the address-only marker.
func formatInbox(in *email.Inbox) (vendor, auth string, lastUID, uidValidity int64, err error) {
	if in == nil {
		return "", "", 0, 0, nil
	}
	encoded, err := json.Marshal(in.Auth)
	if err != nil {
		return "", "", 0, 0, fmt.Errorf("encode auth: %w", err)
	}
	return string(in.Vendor), string(encoded), int64(in.LastUID), int64(in.UIDValidity), nil
}

// parseInbox is the inverse of formatInbox.
func parseInbox(id, vendor, auth string, lastUID, uidValidity uint32) (*email.Inbox, error) {
	if vendor == "" {
		return nil, nil
	}
	in := &email.Inbox{Vendor: email.Vendor(vendor), LastUID: lastUID, UIDValidity: uidValidity}
	if auth != "" {
		if err := json.Unmarshal([]byte(auth), &in.Auth); err != nil {
			return nil, fmt.Errorf("decode auth of email %s: %w", id, err)
		}
	}
	return in, nil
}
