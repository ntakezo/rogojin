package sqlite

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/ntakezo/rogojin/cards"
)

// Cards is the cards.Repository: one row per card, its checkout-defined
// fields — number, expiry, CVV, billing address — in a JSON text column so
// the schema knows nothing about what any workflow's cards contain.
//
// They are stored in the clear. This is the plain-file default, not a vault:
// a database holding real card data belongs behind a store that seals
// cards.Card.Fields on the way down and opens them on the way up, which the
// Repository port is shaped to allow. Wrap this one, or write your own.
type Cards struct {
	db *sql.DB
}

// NewCards builds the cards store on db, bringing its tables up to the
// current schema.
func NewCards(db *DB) (cards.Repository, error) {
	if err := migrate(db.db, "cards", cardMigrations); err != nil {
		return nil, err
	}
	return &Cards{db: db.db}, nil
}

// cardMigrations is the ordered schema history of the cards store. Append new
// steps to the end; never edit or reorder shipped ones: the ledger records
// which of them have already run on existing databases by position.
var cardMigrations = []migration{
	{
		Name: "create cards table",
		SQL: `CREATE TABLE IF NOT EXISTS cards (
			id          TEXT PRIMARY KEY,
			group_id    TEXT NOT NULL DEFAULT 'global',
			owner_id    TEXT NOT NULL DEFAULT '',
			max_holders INTEGER NOT NULL DEFAULT 0,
			successes   INTEGER NOT NULL DEFAULT 0,
			failures    INTEGER NOT NULL DEFAULT 0,
			fields      TEXT NOT NULL DEFAULT '',
			created_at  TEXT NOT NULL DEFAULT '',
			updated_at  TEXT NOT NULL DEFAULT ''
		)`,
	},
	{
		Name: "create card_groups table",
		SQL: `CREATE TABLE IF NOT EXISTS card_groups (
			id          TEXT PRIMARY KEY,
			max_holders INTEGER NOT NULL DEFAULT 0,
			created_at  TEXT NOT NULL DEFAULT '',
			updated_at  TEXT NOT NULL DEFAULT ''
		)`,
	},
	{
		Name: "add strategy column to card_groups",
		SQL:  `ALTER TABLE card_groups ADD COLUMN strategy TEXT NOT NULL DEFAULT ''`,
	},
	{
		Name: "add refs column to card_groups",
		SQL:  `ALTER TABLE card_groups ADD COLUMN refs TEXT NOT NULL DEFAULT ''`,
	},
}

// List returns every stored card in stable id order, so the manager's pool
// order is deterministic. The successes and failures columns are legacy —
// cards keep no lease outcomes — and are left unread.
func (s *Cards) List(ctx context.Context) ([]cards.Card, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, group_id, owner_id, max_holders, fields, created_at, updated_at
		 FROM cards ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("list cards: %w", err)
	}
	defer rows.Close()

	listed := make([]cards.Card, 0)
	for rows.Next() {
		var c cards.Card
		var fields, created, updated string
		if err := rows.Scan(&c.ID, &c.GroupID, &c.OwnerID, &c.MaxHolders, &fields, &created, &updated); err != nil {
			return nil, fmt.Errorf("list cards: %w", err)
		}
		c.Fields = parseFields(fields)
		if c.CreatedAt, err = parseTime(created); err != nil {
			return nil, fmt.Errorf("list cards: %w", err)
		}
		if c.UpdatedAt, err = parseTime(updated); err != nil {
			return nil, fmt.Errorf("list cards: %w", err)
		}
		listed = append(listed, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list cards: %w", err)
	}
	return listed, nil
}

// Save upserts the card's record: group, holder policy, lock owner, fields, and
// updated_at. created_at is written on insert and never overwritten, so a later
// lock cannot revise it.
func (s *Cards) Save(ctx context.Context, c cards.Card) error {
	fields, err := formatFields(c.Fields)
	if err != nil {
		return fmt.Errorf("save card %s: %w", c.ID, err)
	}
	_, err = s.db.ExecContext(ctx,
		`INSERT INTO cards (id, group_id, owner_id, max_holders, fields, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(id) DO UPDATE SET group_id = excluded.group_id,
		 owner_id = excluded.owner_id, max_holders = excluded.max_holders,
		 fields = excluded.fields,
		 updated_at = excluded.updated_at`,
		c.ID, c.GroupID, c.OwnerID, c.MaxHolders, fields,
		formatTime(c.CreatedAt), formatTime(c.UpdatedAt))
	if err != nil {
		return fmt.Errorf("save card %s: %w", c.ID, err)
	}
	return nil
}

// Delete removes the card's record; absent rows are a no-op.
func (s *Cards) Delete(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM cards WHERE id = ?`, id)
	if err != nil {
		return fmt.Errorf("delete card %s: %w", id, err)
	}
	return nil
}

// ListGroups returns every stored card group in stable id order. The
// max_holders column is legacy — holder policy lives on the card — and is
// left unread.
func (s *Cards) ListGroups(ctx context.Context) ([]cards.Group, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, strategy, refs, created_at, updated_at FROM card_groups ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("list card groups: %w", err)
	}
	defer rows.Close()

	listed := make([]cards.Group, 0)
	for rows.Next() {
		var g cards.Group
		var refs, created, updated string
		if err := rows.Scan(&g.ID, &g.Strategy, &refs, &created, &updated); err != nil {
			return nil, fmt.Errorf("list card groups: %w", err)
		}
		if g.Refs, err = parseMap[string](refs); err != nil {
			return nil, fmt.Errorf("list card groups: decode refs of %s: %w", g.ID, err)
		}
		if g.CreatedAt, err = parseTime(created); err != nil {
			return nil, fmt.Errorf("list card groups: %w", err)
		}
		if g.UpdatedAt, err = parseTime(updated); err != nil {
			return nil, fmt.Errorf("list card groups: %w", err)
		}
		listed = append(listed, g)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list card groups: %w", err)
	}
	return listed, nil
}

// SaveGroup upserts the group's record. created_at is written on insert and
// never overwritten: when a group was created is not something a later save
// gets to revise.
func (s *Cards) SaveGroup(ctx context.Context, g cards.Group) error {
	refs, err := formatMap(g.Refs)
	if err != nil {
		return fmt.Errorf("save card group %s: encode refs: %w", g.ID, err)
	}
	_, err = s.db.ExecContext(ctx,
		`INSERT INTO card_groups (id, strategy, refs, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?)
		 ON CONFLICT(id) DO UPDATE SET strategy = excluded.strategy,
		 refs = excluded.refs, updated_at = excluded.updated_at`,
		g.ID, g.Strategy, refs, formatTime(g.CreatedAt), formatTime(g.UpdatedAt))
	if err != nil {
		return fmt.Errorf("save card group %s: %w", g.ID, err)
	}
	return nil
}

// DeleteGroup removes the group's record; absent rows are a no-op. Member
// cards are the manager's to delete — the store cascades nothing.
func (s *Cards) DeleteGroup(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM card_groups WHERE id = ?`, id)
	if err != nil {
		return fmt.Errorf("delete card group %s: %w", id, err)
	}
	return nil
}
