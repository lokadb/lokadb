package admin

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"

	"github.com/jackc/pgx/v5"

	"github.com/lokadb/lokadb/proxy/internal/creds"
)

// CredentialsOptions: lokadb org-credentials (stage 9a, tasks/direct-profile.md).
type CredentialsOptions struct {
	CloudDSN     string // the cloud database: its name is the proxy's database name
	ControlPlane string // DSN of the control plane database (lokadb_cp.org_credentials)
	KeyFile      string // the proxy's credentials_key
	Org          int64
	Kind         string // creds.Direct or creds.Owner
	DSN          string // the credentials to store
	Log          *slog.Logger
}

// StoreCredentials checks dsn by connecting (a direct path role: DML alone, as the proxy checks
// it), seals it with the key and writes it for the cloud database's name, the organization and
// the kind. The proxies with that control plane and key read it within credentialsRefresh.
func StoreCredentials(ctx context.Context, o CredentialsOptions) error {
	key, err := creds.LoadKey(o.KeyFile)
	if err != nil {
		return err
	}
	cfg, err := pgx.ParseConfig(o.CloudDSN)
	if err != nil {
		return fmt.Errorf("cloud DSN: %w", err)
	}
	if o.Kind != creds.Direct && o.Kind != creds.Owner {
		return fmt.Errorf("-kind %q: direct or owner", o.Kind)
	}
	org, err := pgx.Connect(ctx, o.DSN)
	if err != nil {
		return fmt.Errorf("the credentials do not connect to the organization's database: %w", err)
	}
	defer org.Close(context.Background())
	if o.Kind == creds.Direct {
		var risky bool
		if err := org.QueryRow(ctx, `SELECT rolsuper OR rolbypassrls OR rolcreaterole OR rolcreatedb OR rolreplication FROM pg_roles WHERE rolname = current_user`).Scan(&risky); err != nil {
			return err
		}
		if risky {
			return fmt.Errorf("the direct path's role is a superuser or has BYPASSRLS, CREATEROLE, CREATEDB or REPLICATION: it needs DML alone (org-profile -direct-role grants it)")
		}
	} else {
		// the owner: the DDL replay runs under it, so it owns the moved tables and no more
		// (second review of admin, R3-15): a superuser of the customer's server would let the
		// cloud's DDL do anything there
		var risky bool
		if err := org.QueryRow(ctx, `SELECT rolsuper OR rolbypassrls OR rolreplication FROM pg_roles WHERE rolname = current_user`).Scan(&risky); err != nil {
			return err
		}
		if risky {
			return fmt.Errorf("the owner's role is a superuser or has BYPASSRLS or REPLICATION: give the role that owns the moved tables (move-org's -customer-app), not an administrator of the customer's server")
		}
	}
	inst, err := CloudInstallation(ctx, o.CloudDSN)
	if err != nil {
		return err
	}
	cp, err := pgx.Connect(ctx, o.ControlPlane)
	if err != nil {
		return fmt.Errorf("control plane: %w", err)
	}
	defer cp.Close(context.Background())
	if err := creds.Put(ctx, cp, key, inst, cfg.Database, strconv.FormatInt(o.Org, 10), o.Kind, o.DSN); err != nil {
		return err
	}
	if o.Log != nil {
		o.Log.Info("organization's credentials stored, sealed", "db", cfg.Database, "org", o.Org, "kind", o.Kind, "key_id", creds.KeyID(key))
	}
	return nil
}

// ListCredentials: the rows of the control plane, without opening them.
func ListCredentials(ctx context.Context, controlPlane string) ([][]string, error) {
	cp, err := pgx.Connect(ctx, controlPlane)
	if err != nil {
		return nil, err
	}
	defer cp.Close(context.Background())
	var there bool
	if err := cp.QueryRow(ctx, `SELECT to_regclass('lokadb_cp.org_credentials') IS NOT NULL`).Scan(&there); err != nil || !there {
		return nil, err
	}
	// the installation (the cloud's system_identifier, '' before A14) by to_jsonb: a table of
	// before A14 has no such column
	return rowsOf(ctx, cp, `SELECT coalesce(to_jsonb(c) ->> 'installation', ''), db, org::text, kind, key_id, updated_at::text FROM lokadb_cp.org_credentials c ORDER BY 1, 2, 3, 4`)
}

// ResealCredentials: lokadb org-credentials -reseal (review of admin, A8): every row of the cloud
// sealed with one of the old keys is sealed with keyFile's; with claim, the rows of before A14
// of the cloud's database (sealed by its name alone, which cloud's the row does not say) become
// the cloud's (-claim-legacy: the operator says they are; second review of admin, R3-7). It
// returns how many, and the rows left as they are (an error each).
func ResealCredentials(ctx context.Context, cloudDSN, controlPlane, keyFile string, oldKeyFiles []string, claim bool) (int, []error, error) {
	key, err := creds.LoadKey(keyFile)
	if err != nil {
		return 0, nil, err
	}
	var old [][]byte
	for _, f := range oldKeyFiles {
		k, err := creds.LoadKey(f)
		if err != nil {
			return 0, nil, err
		}
		old = append(old, k)
	}
	cp, err := pgx.Connect(ctx, controlPlane)
	if err != nil {
		return 0, nil, fmt.Errorf("control plane: %w", err)
	}
	defer cp.Close(context.Background())
	inst, err := CloudInstallation(ctx, cloudDSN)
	if err != nil {
		return 0, nil, err
	}
	n, left, err := creds.Reseal(ctx, cp, key, inst, old...)
	if err != nil || !claim {
		return n, left, err
	}
	db, err := dbName(cloudDSN)
	if err != nil {
		return n, left, err
	}
	c, cleft, err := creds.ClaimLegacy(ctx, cp, key, inst, db, old...)
	return n + c, append(left, cleft...), err
}

// CloudInstallation: the cloud's installation, its cluster's system_identifier (review of
// admin, A14): the credentials of the organizations are sealed for it.
func CloudInstallation(ctx context.Context, cloudDSN string) (string, error) {
	c, err := pgx.Connect(ctx, cloudDSN)
	if err != nil {
		return "", fmt.Errorf("cloud database: %w", err)
	}
	defer c.Close(context.Background())
	var inst string
	if err := c.QueryRow(ctx, creds.InstallationSQL).Scan(&inst); err != nil {
		return "", fmt.Errorf("the cloud's system_identifier (the credentials are sealed for it): %w", err)
	}
	return inst, nil
}
