package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/ntakezo/rogojin/email"
)

// Emails is the email.Repository: one row per email, its inbox credentials
// in a JSON text column, in the clear. An empty vendor column marks an
// address-only email — one with no inbox at all.
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

// emailMigrations is the ordered schema history of the email store. Append
// new steps to the end; never edit or reorder shipped ones: the ledger records
// which of them have already run on existing databases by position.
var emailMigrations = []migration{
	{
		Name: "create emails table",
		SQL: `CREATE TABLE IF NOT EXISTS emails (
			id           TEXT PRIMARY KEY,
			address      TEXT NOT NULL DEFAULT '',
			vendor       TEXT NOT NULL DEFAULT '',
			auth         TEXT NOT NULL DEFAULT '',
			last_uid     INTEGER NOT NULL DEFAULT 0,
			uid_validity INTEGER NOT NULL DEFAULT 0,
			created_at   TEXT NOT NULL DEFAULT '',
			updated_at   TEXT NOT NULL DEFAULT ''
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
// updated_at. created_at is written on insert and never overwritten, so a
// cursor advance cannot revise it.
func (s *Emails) Save(ctx context.Context, e email.Email) error {
	vendor, auth, lastUID, uidValidity, err := formatInbox(e.Inbox)
	if err != nil {
		return fmt.Errorf("save email %s: %w", e.ID, err)
	}
	_, err = s.db.ExecContext(ctx,
		`INSERT INTO emails (id, address, vendor, auth, last_uid, uid_validity, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(id) DO UPDATE SET address = excluded.address,
		 vendor = excluded.vendor, auth = excluded.auth,
		 last_uid = excluded.last_uid, uid_validity = excluded.uid_validity,
		 updated_at = excluded.updated_at`,
		e.ID, e.Address, vendor, auth, lastUID, uidValidity,
		formatTime(e.CreatedAt), formatTime(e.UpdatedAt))
	if err != nil {
		return fmt.Errorf("save email %s: %w", e.ID, err)
	}
	return nil
}

// Delete removes the email's record; absent rows are a no-op.
func (s *Emails) Delete(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM emails WHERE id = ?`, id)
	if err != nil {
		return fmt.Errorf("delete email %s: %w", id, err)
	}
	return nil
}

// formatInbox flattens the optional inbox into its columns; a nil inbox
// stores an empty vendor, the address-only marker.
func formatInbox(in *email.Inbox) (vendor, auth string, lastUID, uidValidity uint32, err error) {
	if in == nil {
		return "", "", 0, 0, nil
	}
	encoded, err := json.Marshal(in.Auth)
	if err != nil {
		return "", "", 0, 0, fmt.Errorf("encode auth: %w", err)
	}
	return string(in.Vendor), string(encoded), in.LastUID, in.UIDValidity, nil
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
