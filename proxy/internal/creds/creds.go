// Package creds keeps the credentials of organizations' databases the proxy needs (block 10,
// stage 9a, tasks/direct-profile.md): the DSN of the role the direct path runs under and of
// the owner of the moved tables (DDL replay, move-back), in lokadb_cp.org_credentials of the
// control plane database, sealed with AES-256-GCM under a key only the proxy has (the file
// credentials_key of its config). The control plane's database and its administrators see
// ciphertext alone; a row cannot be moved to another organization, role, database or cloud:
// they are the additional data of its seal. The cloud is its cluster's system_identifier
// (installation; review of admin, A14): two clouds with a database of the same name, one
// control plane and one key keep rows of their own, neither opens the other's.
package creds

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"runtime"
	"strings"

	"github.com/jackc/pgx/v5"
)

// Kinds of credentials.
const (
	Direct = "direct" // the role of DML alone the direct path runs the application's statements under
	Owner  = "owner"  // the owner of the moved tables: DDL replay, move-back
)

// TableSQL creates the table (the proxy's code makes it: the decision of 23.10 with block 8).
// A table of before A14 gets its column installation (” for its rows: sealed without one) and
// the primary key with it.
const TableSQL = `CREATE SCHEMA IF NOT EXISTS lokadb_cp;
CREATE TABLE IF NOT EXISTS lokadb_cp.org_credentials (
  installation text NOT NULL DEFAULT '',
  db text NOT NULL,
  org bigint NOT NULL,
  kind text NOT NULL CHECK (kind IN ('direct', 'owner')),
  key_id text NOT NULL,
  ciphertext bytea NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (installation, db, org, kind));
ALTER TABLE lokadb_cp.org_credentials ADD COLUMN IF NOT EXISTS installation text NOT NULL DEFAULT '';
DO $lokadb$ BEGIN
  IF (SELECT array_length(conkey, 1) FROM pg_constraint WHERE conrelid = 'lokadb_cp.org_credentials'::regclass AND contype = 'p') = 3 THEN
    ALTER TABLE lokadb_cp.org_credentials DROP CONSTRAINT org_credentials_pkey, ADD PRIMARY KEY (installation, db, org, kind);
  END IF;
END $lokadb$`

// InstallationSQL: the cloud's installation, its cluster's system_identifier (any role reads
// it: pg_control_system is granted to PUBLIC). A replica has its primary's.
const InstallationSQL = `SELECT system_identifier::text FROM pg_control_system()`

// LoadKey reads a key file: 32 bytes, or 64 hexadecimal digits (a trailing newline allowed).
// Outside Windows a file others may read is refused, as ssh refuses a private key.
func LoadKey(path string) ([]byte, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if runtime.GOOS != "windows" && fi.Mode().Perm()&0o077 != 0 {
		return nil, fmt.Errorf("credentials key %s: mode %o, want 0600 (no access for group and others)", path, fi.Mode().Perm())
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if len(b) == 32 {
		return b, nil
	}
	if k, err := hex.DecodeString(strings.TrimSpace(string(b))); err == nil && len(k) == 32 {
		return k, nil
	}
	return nil, fmt.Errorf("credentials key %s: want 32 bytes or 64 hexadecimal digits", path)
}

// KeyID names a key in the rows it sealed (the first 8 bytes of its SHA-256): a row sealed with
// an older key is opened with that one (key rotation) until Reseal seals it with the new one.
func KeyID(key []byte) string {
	h := sha256.Sum256(key)
	return hex.EncodeToString(h[:8])
}

// aad: a row's additional data. inst "": a row sealed before A14, by the database's name alone.
func aad(inst, db, org, kind string) []byte {
	if inst == "" {
		return []byte("lokadb org_credentials\x00" + db + "\x00" + org + "\x00" + kind)
	}
	return []byte("lokadb org_credentials v2\x00" + inst + "\x00" + db + "\x00" + org + "\x00" + kind)
}

func gcm(key []byte) (cipher.AEAD, error) {
	if len(key) != 32 {
		return nil, errors.New("credentials key: want 32 bytes")
	}
	b, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(b)
}

// Seal: plain sealed for (inst, db, org, kind): nonce ‖ ciphertext ‖ tag.
func Seal(key []byte, inst, db, org, kind, plain string) ([]byte, error) {
	a, err := gcm(key)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, a.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	return a.Seal(nonce, nonce, []byte(plain), aad(inst, db, org, kind)), nil
}

// Open: what Seal sealed for (inst, db, org, kind); an error for another key, another row's
// additional data (another cloud's too) or a changed blob.
func Open(key []byte, inst, db, org, kind string, blob []byte) (string, error) {
	a, err := gcm(key)
	if err != nil {
		return "", err
	}
	if len(blob) < a.NonceSize()+a.Overhead() {
		return "", errors.New("credentials: a sealed value too short")
	}
	plain, err := a.Open(nil, blob[:a.NonceSize()], blob[a.NonceSize():], aad(inst, db, org, kind))
	if err != nil {
		return "", errors.New("credentials: cannot be opened with this key for this organization and kind")
	}
	return string(plain), nil
}

// Put seals dsn and writes it for (inst, db, org, kind), replacing what was there. A row of
// before A14 for (db, org, kind) stays: it may be another cloud's (ClaimLegacy).
func Put(ctx context.Context, cp *pgx.Conn, key []byte, inst, db, org, kind, dsn string) error {
	if kind != Direct && kind != Owner {
		return fmt.Errorf("kind %q: direct or owner", kind)
	}
	blob, err := Seal(key, inst, db, org, kind, dsn)
	if err != nil {
		return err
	}
	if _, err := cp.Exec(ctx, TableSQL); err != nil {
		return err
	}
	_, err = cp.Exec(ctx, `INSERT INTO lokadb_cp.org_credentials (installation, db, org, kind, key_id, ciphertext) VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (installation, db, org, kind) DO UPDATE SET key_id = excluded.key_id, ciphertext = excluded.ciphertext, updated_at = now()`,
		inst, db, org, kind, KeyID(key), blob)
	return err
}

// Set: the credentials read, Set[db][org][kind] = DSN.
type Set map[string]map[string]map[string]string

// DSN: the credentials of (db, org, kind), "" for none.
func (s Set) DSN(db, org, kind string) string { return s[db][org][kind] }

// Load reads and opens the rows of installation inst with the keys given (the current one and
// older ones, by KeyID). The rows of other installations are not opened, nor the rows of before
// A14 (no installation): which cloud's they are, the row does not say, and two clouds with a
// database of one name would get each other's (second review of admin, R3-7). The operator
// gives them to the cloud they belong to (ClaimLegacy); legacy counts them meanwhile. A row
// none of the keys opens is left out and reported in errs (the others still load).
func Load(ctx context.Context, cp *pgx.Conn, inst string, keys ...[]byte) (set Set, errs []error, err error) {
	set, errs, _, err = LoadAll(ctx, cp, inst, keys...)
	return set, errs, err
}

// LoadAll is Load, and how many rows of before A14 each database has (no cloud's yet).
func LoadAll(ctx context.Context, cp *pgx.Conn, inst string, keys ...[]byte) (set Set, errs []error, legacy map[string]int, err error) {
	byID := map[string][]byte{}
	for _, k := range keys {
		if k != nil {
			byID[KeyID(k)] = k
		}
	}
	var there bool
	if err := cp.QueryRow(ctx, `SELECT to_regclass('lokadb_cp.org_credentials') IS NOT NULL`).Scan(&there); err != nil || !there {
		return Set{}, nil, nil, err // no table yet: no credentials there
	}
	// installation by to_jsonb: a table of before A14 has no such column
	rows, err := cp.Query(ctx, `SELECT coalesce(to_jsonb(c) ->> 'installation', ''), db, org::text, kind, key_id, ciphertext FROM lokadb_cp.org_credentials c`)
	if err != nil {
		return nil, nil, nil, err
	}
	defer rows.Close()
	set, legacy = Set{}, map[string]int{}
	for rows.Next() {
		var rowInst, db, org, kind, id string
		var blob []byte
		if err := rows.Scan(&rowInst, &db, &org, &kind, &id, &blob); err != nil {
			return nil, nil, nil, err
		}
		if rowInst == "" {
			legacy[db]++ // no cloud's: ClaimLegacy
			continue
		}
		if rowInst != inst {
			continue // another cloud's
		}
		k := byID[id]
		if k == nil {
			errs = append(errs, fmt.Errorf("credentials of %s organization %s (%s): sealed with key %s, which the proxy does not have", db, org, kind, id))
			continue
		}
		dsn, err := Open(k, rowInst, db, org, kind, blob)
		if err != nil {
			errs = append(errs, fmt.Errorf("credentials of %s organization %s (%s): %w", db, org, kind, err))
			continue
		}
		set.put(db, org, kind, dsn)
	}
	return set, errs, legacy, rows.Err()
}

func (s Set) put(db, org, kind, dsn string) {
	if s[db] == nil {
		s[db] = map[string]map[string]string{}
	}
	if s[db][org] == nil {
		s[db][org] = map[string]string{}
	}
	s[db][org][kind] = dsn
}

// Reseal seals again with key every row of installation inst sealed with another key (one of
// old, by KeyID): the rotation's second step (credentials_key the new key, credentials_key_old
// the old one, then Reseal, then credentials_key_old can go). The rows of before A14 are left
// alone: they become a cloud's by ClaimLegacy only. One transaction; a row none of the keys
// opens is left as it is and reported in left. resealed: the rows rewritten.
func Reseal(ctx context.Context, cp *pgx.Conn, key []byte, inst string, old ...[]byte) (resealed int, left []error, err error) {
	return rewrite(ctx, cp, key, inst, "", old)
}

// ClaimLegacy makes the rows of before A14 of the cloud database db (no installation) inst's,
// sealed with key (the operator says they are this cloud's: second review of admin, R3-7). A
// row whose (db, org, kind) inst has already stays as it is (another cloud's, maybe) and is
// reported in left; the others are deleted as their copies for inst are written.
func ClaimLegacy(ctx context.Context, cp *pgx.Conn, key []byte, inst, db string, old ...[]byte) (claimed int, left []error, err error) {
	if inst == "" || db == "" {
		return 0, nil, fmt.Errorf("claim: the cloud's installation and database are needed")
	}
	return rewrite(ctx, cp, key, inst, db, old)
}

// rewrite: Reseal (legacyDB "") or ClaimLegacy (the rows of before A14 of legacyDB).
func rewrite(ctx context.Context, cp *pgx.Conn, key []byte, inst, legacyDB string, old [][]byte) (n int, left []error, err error) {
	byID := map[string][]byte{KeyID(key): key}
	for _, k := range old {
		if k != nil {
			byID[KeyID(k)] = k
		}
	}
	id := KeyID(key)
	var there bool
	if err := cp.QueryRow(ctx, `SELECT to_regclass('lokadb_cp.org_credentials') IS NOT NULL`).Scan(&there); err != nil || !there {
		return 0, nil, err
	}
	if _, err := cp.Exec(ctx, TableSQL); err != nil { // the column installation, on a table of before A14
		return 0, nil, err
	}
	tx, err := cp.Begin(ctx)
	if err != nil {
		return 0, nil, err
	}
	defer tx.Rollback(context.Background())
	q := `SELECT installation, db, org::text, kind, key_id, ciphertext FROM lokadb_cp.org_credentials WHERE installation = $1 AND key_id <> $2 FOR UPDATE`
	args := []any{inst, id}
	if legacyDB != "" {
		q = `SELECT installation, db, org::text, kind, key_id, ciphertext FROM lokadb_cp.org_credentials WHERE installation = '' AND db = $1 FOR UPDATE`
		args = []any{legacyDB}
	}
	rows, err := tx.Query(ctx, q, args...)
	if err != nil {
		return 0, nil, err
	}
	type row struct {
		inst, db, org, kind, keyID string
		blob                       []byte
	}
	var todo []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.inst, &r.db, &r.org, &r.kind, &r.keyID, &r.blob); err != nil {
			rows.Close()
			return 0, nil, err
		}
		todo = append(todo, r)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, nil, err
	}
	for _, r := range todo {
		k := byID[r.keyID]
		if k == nil {
			left = append(left, fmt.Errorf("credentials of %s organization %s (%s): sealed with key %s, not given", r.db, r.org, r.kind, r.keyID))
			continue
		}
		dsn, err := Open(k, r.inst, r.db, r.org, r.kind, r.blob)
		if err != nil {
			left = append(left, fmt.Errorf("credentials of %s organization %s (%s): %w", r.db, r.org, r.kind, err))
			continue
		}
		blob, err := Seal(key, inst, r.db, r.org, r.kind, dsn)
		if err != nil {
			return 0, nil, err
		}
		if r.inst == inst {
			if _, err = tx.Exec(ctx, `UPDATE lokadb_cp.org_credentials SET key_id = $1, ciphertext = $2, updated_at = now() WHERE installation = $3 AND db = $4 AND org = $5::bigint AND kind = $6`,
				id, blob, inst, r.db, r.org, r.kind); err != nil {
				return 0, nil, err
			}
			n++
			continue
		}
		// a row of before A14, claimed: written as inst's unless inst has its own (then kept)
		tag, err := tx.Exec(ctx, `INSERT INTO lokadb_cp.org_credentials (installation, db, org, kind, key_id, ciphertext) VALUES ($1, $2, $3::bigint, $4, $5, $6) ON CONFLICT DO NOTHING`,
			inst, r.db, r.org, r.kind, id, blob)
		if err != nil {
			return 0, nil, err
		}
		if tag.RowsAffected() == 0 {
			left = append(left, fmt.Errorf("credentials of %s organization %s (%s): the cloud has its own already, the row of before A14 is kept (another cloud's, maybe)", r.db, r.org, r.kind))
			continue
		}
		if _, err := tx.Exec(ctx, `DELETE FROM lokadb_cp.org_credentials WHERE installation = '' AND db = $1 AND org = $2::bigint AND kind = $3`, r.db, r.org, r.kind); err != nil {
			return 0, nil, err
		}
		n++
	}
	return n, left, tx.Commit(ctx)
}
