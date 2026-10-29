// Package cloudproto: what the runner (lokadb runner, at the SaaS) and the LokaDB Cloud service
// exchange. Open, like the runner: the SaaS can read every job the service may send it.
// Protocol 1 (private/cloud/DESIGN.md §4).
package cloudproto

import (
	"encoding/json"
	"fmt"
	"net/netip"
	"strings"
	"time"
)

const (
	// RunnerProtocol: the version of this protocol the runner speaks (header RunnerHeader).
	RunnerProtocol = 1
	RunnerHeader   = "Lokadb-Runner-Protocol"
)

// Job kinds and their version (a runner lists "kind@v" in its capabilities).
const (
	KindInventory   = "inventory"
	KindLookup      = "org.lookup"
	KindCheck       = "check"
	KindEnable      = "enable"
	KindDisable     = "disable"
	KindOrgTables   = "org.tables"
	KindOrgList     = "org.list"
	KindPrepare     = "agent.prepare"
	KindCredentials = "credentials.store"
	KindMove        = "move"
	KindMoveBack    = "move_back"
	KindRelease     = "org.release"
	KindExtSync     = "extensions.sync"
)

// Kinds: what a runner of this version does, as capabilities.
var Kinds = []string{KindInventory + "@1", KindLookup + "@1", KindCheck + "@1", KindEnable + "@1", KindDisable + "@1", KindOrgTables + "@1", KindOrgList + "@1", KindPrepare + "@1",
	KindCredentials + "@1", KindMove + "@1", KindMoveBack + "@1", KindRelease + "@1", KindExtSync + "@1", KindStatements + "@1", KindDecide + "@1"}

// Exclusive: kinds of which a runner runs one at a time (the open module's own locks say so too).
var Exclusive = map[string]bool{KindMove: true, KindMoveBack: true, KindEnable: true, KindDisable: true}

// Idempotent: kinds the service may hand out again when their lease expired.
var Idempotent = map[string]bool{KindOrgTables: true, KindOrgList: true, KindInventory: true, KindLookup: true, KindCheck: true, KindPrepare: true,
	KindCredentials: true, KindRelease: true, KindExtSync: true, KindStatements: true}

// --- registration and polling ---

type RegisterRequest struct {
	Name         string   `json:"name"`
	Hostname     string   `json:"hostname"`
	Version      string   `json:"version"`
	Protocol     int      `json:"protocol"`
	PublicKey    string   `json:"public_key"` // X25519, base64 (package seal)
	Capabilities []string `json:"capabilities"`
}

type RegisterResponse struct {
	RunnerID    string `json:"runner_id"`
	Secret      string `json:"secret"` // lrs_…, shown once
	Account     string `json:"account"`
	Fingerprint string `json:"fingerprint"`
}

type PollRequest struct {
	Version       string   `json:"version"`
	Running       []string `json:"running"`
	InventoryHash string   `json:"inventory_hash"`
	// Problems: what keeps the runner from working (its config, a file, a database); while
	// there are any, it takes no jobs and sends no inventory. None: ready.
	Problems []Problem `json:"problems,omitempty"`
	// Info: facts for the setup wizard, nothing secret ("database": "pgsql:5432/app as forge",
	// "relay": "up", "proxy": "up", ...): what the runner found and started.
	Info map[string]string `json:"info,omitempty"`
}

// Problem: one thing to fix on the runner's side, in words for the panel.
type Problem struct {
	Key     string `json:"key"`               // the runner.json key: "spec", "relay.ca", "db"…
	Message string `json:"message"`           // what is wrong, the path included
	Fix     string `json:"fix,omitempty"`     // what to do
	Path    string `json:"path,omitempty"`    // the file as the runner sees it (in its container)
	Host    string `json:"host,omitempty"`    // the same file on the host, from the container's mounts
	Mounted *bool  `json:"mounted,omitempty"` // in a container: whether Path is under a mount from the host
}

// Settings: what the person chose in the panel, sent with every poll answer (the service keeps
// them for the account; nothing secret is in them). An embedded runner (one container with the
// relay and the proxy, no files typed by hand) takes them as its config.
type Settings struct {
	RelayPublic string `json:"relay_public,omitempty"` // host:port the customers' agents dial (7443)
	RelayName   string `json:"relay_name,omitempty"`   // the relay's name inside the runner's network (default lokadb)
	Owner       string `json:"owner,omitempty"`        // the role that owns the application's tables
	// Spec: the tables that move with an organization, built from the person's choice.
	Spec json.RawMessage `json:"spec,omitempty"`
	// OnlyKeys: the organizations list shows keys only, no names (strict SaaSes).
	OnlyKeys bool `json:"only_keys,omitempty"`
	// ProxyPublic: the proxy listens to the internet, for an application on a platform: TLS is
	// required, and only the addresses in Allow may connect (never the whole internet).
	ProxyPublic *PublicProxy `json:"proxy_public,omitempty"`
	// Fanout: what the proxy does with a statement of the application's database that names no
	// organization (reject, reject-writes, log, allow); empty: whatever proxy.json says. The runner
	// writes it into proxy.json and leaves the other keys alone.
	Fanout string `json:"fanout,omitempty"`
	// OrgTable and OrgKey: the table of customers and its key column (what the person chose in
	// step 2); OrgLabel: the column to show as the customer's name (found by name when empty).
	OrgTable string `json:"org_table,omitempty"`
	OrgKey   string `json:"org_key,omitempty"`
	OrgLabel string `json:"org_label,omitempty"`
}

// PublicProxy: Name is the public name of this server (the certificate is made for it), Allow the
// addresses or CIDR ranges of the platform's outgoing traffic.
type PublicProxy struct {
	Name  string   `json:"name"`
	Allow []string `json:"allow"`
}

// OrgListArgs asks for a page of the customers table, read live; the service keeps nothing of
// the answer but the job's result for a few minutes.
type OrgListArgs struct {
	Q    string `json:"q,omitempty"`
	Page int    `json:"page,omitempty"`
	Mode string `json:"mode,omitempty"` // "smallest": the customers with the fewest rows
}

type OrgListResult struct {
	Orgs        []OrgListItem `json:"orgs"`
	Total       int64         `json:"total"`
	Page        int           `json:"page"`
	PageSize    int           `json:"page_size"`
	LabelColumn string        `json:"label_column,omitempty"`
	OnlyKeys    bool          `json:"only_keys,omitempty"`
}

type OrgListItem struct {
	Key   string `json:"key"`
	Label string `json:"label,omitempty"`
	Rows  int64  `json:"rows"` // in the spec's root tables
}

type PollResponse struct {
	Settings        *Settings `json:"settings,omitempty"`
	Jobs            []Job     `json:"jobs"`
	Cancel          []string  `json:"cancel"`
	WantInventory   bool      `json:"want_inventory"`
	StatusIntervalS int       `json:"status_interval_s"`
	LatestVersion   string    `json:"latest_version,omitempty"`
	MinVersion      string    `json:"min_version,omitempty"`
	RunnerImage     string    `json:"runner_image,omitempty"` // the latest release's image, tag included
}

type Job struct {
	ID      string          `json:"id"`
	Kind    string          `json:"kind"`
	V       int             `json:"v"`
	Attempt int             `json:"attempt"`
	MoveID  string          `json:"move_id,omitempty"`
	Args    json.RawMessage `json:"args"`
	LeaseS  int             `json:"lease_s"`
}

// Event: one line of a job's progress (the open module's log records, passwords redacted).
type Event struct {
	Seq   int64          `json:"seq"`
	At    time.Time      `json:"at"`
	Kind  string         `json:"kind"` // progress, warning, error
	Msg   string         `json:"msg"`
	Attrs map[string]any `json:"attrs,omitempty"`
}

type EventsRequest struct {
	Events []Event `json:"events"`
}

type EventsResponse struct {
	Cancel bool `json:"cancel"`
}

// Job results.
const (
	StatusOK          = "ok"
	StatusFailed      = "failed"
	StatusCancelled   = "cancelled"
	StatusUnsupported = "unsupported"
)

type Result struct {
	Status string          `json:"status"`
	Result json.RawMessage `json:"result,omitempty"`
	Error  *JobError       `json:"error,omitempty"`
}

// JobError: why a job failed. Code "interrupted": the runner restarted while it ran (the
// inventory is attached as Result); "retryable": the same job may work in a moment.
type JobError struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	Hint      string `json:"hint,omitempty"`
	Retryable bool   `json:"retryable,omitempty"`
}

func (e *JobError) Error() string { return e.Message }

// --- job arguments and results ---

// Roles: the organization's two roles at the customer.
type Roles struct {
	App string `json:"app"`
	DML string `json:"dml"`
}

type LookupArgs struct {
	Org    int64  `json:"org"`
	Tenant string `json:"tenant,omitempty"`
}

type LookupResult struct {
	Org int64 `json:"org"`
}

// CheckArgs: Org 0 checks the SaaS database alone (before enable).
type CheckArgs struct {
	Org int64 `json:"org"`
}

type CheckResult struct {
	Exit    int             `json:"exit"`
	Verdict string          `json:"verdict"`
	Report  json.RawMessage `json:"report"` // check -json
}

type EnableArgs struct {
	ConfirmDB string `json:"confirm_db"` // the SaaS database's name, as the person typed it
}

// OrgTablesArgs asks which table is "one customer of yours". Without OrgTable: the candidates,
// best first, each with the number of tables it would split. With it: the tables that would move
// with an organization taken from that table, and the spec to enable.
type OrgTablesArgs struct {
	OrgTable string `json:"org_table,omitempty"`
}

// OrgTablesResult: Candidates is a list of check.OrgCandidate, Discovery a check.Discovery (the
// spec and for every table why), kept as JSON so this package stays free of the check module.
type OrgTablesResult struct {
	Candidates json.RawMessage `json:"candidates,omitempty"`
	Discovery  json.RawMessage `json:"discovery,omitempty"`
}

// DisableArgs: turn LokaDB off again (while no organization is moved). DryRun changes nothing and
// answers with a DisableResult: what would be copied and for how long the tables would wait.
type DisableArgs struct {
	ConfirmDB string `json:"confirm_db"`
	DryRun    bool   `json:"dry_run"`
}

// DisableResult is the answer of a dry run.
type DisableResult struct {
	// Rows: rows outside the default partition (in partitions of organizations, or of no
	// organization); disable copies them back, and the tables wait meanwhile. NoOrgRows: of those,
	// the rows of no organization: on a database where the organizations table was chosen wrong,
	// these are most of the rows.
	Tables    []DisableTable `json:"tables"`
	Rows      int64          `json:"rows_to_copy"`
	Pause     string         `json:"expected_pause"`
	NoOrgRows int64          `json:"rows_without_organization"`
}

type DisableTable struct {
	Name      string `json:"name"`
	Rows      int64  `json:"rows_to_copy"`
	NoOrgRows int64  `json:"rows_without_organization"`
}

type PrepareArgs struct {
	Org          int64  `json:"org"`
	InstallerKey string `json:"installer_key"` // the installer's one-time public key
	// Fresh: the installer has no certificate: the organization's earlier ones are revoked
	// and an enrollment token is made.
	Fresh bool `json:"fresh"`
	Port  int  `json:"port,omitempty"` // the relay port the move holds already
}

type PrepareResult struct {
	EnrollTokenSealed string      `json:"enroll_token_sealed,omitempty"` // sealed to InstallerKey
	Port              int         `json:"port"`
	Relay             RelayInfo   `json:"relay"`
	Extensions        []Extension `json:"extensions"`
}

type RelayInfo struct {
	Addr       string `json:"addr"`
	Enroll     string `json:"enroll"`
	ServerName string `json:"server_name,omitempty"`
	CAPEM      string `json:"ca_pem"`
}

// Extension: one the LokaDB tables use in the SaaS database.
type Extension struct {
	Name    string `json:"name"`
	Schema  string `json:"schema"`
	Version string `json:"version"`
	Trusted bool   `json:"trusted"` // a role with CREATE on the database may create it
}

// Extension states at a customer.
const (
	ExtPresent     = "present"
	ExtCreated     = "created"
	ExtNeedsAdmin  = "needs_admin"
	ExtUnavailable = "unavailable"
	ExtFailed      = "failed"
)

type ExtensionStatus struct {
	Name         string `json:"name"`
	Status       string `json:"status"`
	Version      string `json:"version,omitempty"`
	CloudVersion string `json:"cloud_version,omitempty"`
	Error        string `json:"error,omitempty"`
}

type CredentialsArgs struct {
	Org      int64  `json:"org"`
	Database string `json:"database"`
	Port     int    `json:"port"`
	Roles    Roles  `json:"roles"`
	Sealed   string `json:"sealed"` // Credentials, sealed to the runner's key
}

// Credentials: what the installer seals to the runner.
type Credentials struct {
	AppPassword string `json:"app_password"`
	DMLPassword string `json:"dml_password"`
}

type MoveArgs struct {
	Org   int64 `json:"org"`
	Roles Roles `json:"roles"`
}

type MoveResult struct {
	Profile string `json:"profile"`
	Warning string `json:"warning,omitempty"`
}

type OrgArgs struct {
	Org int64 `json:"org"`
}

type ExtSyncResult struct {
	Extensions []ExtensionStatus `json:"extensions"`
}

// Inventory: the SaaS database as the runner sees it. The truth about where an organization is.
type Inventory struct {
	Installation string      `json:"installation"` // admin.CloudInstallation
	Database     string      `json:"database"`
	Enabled      bool        `json:"enabled"`
	Tables       []string    `json:"tables"`
	Extensions   []Extension `json:"extensions"`
	Orgs         []OrgState  `json:"orgs"` // the organizations not in the SaaS database (lokadb.orgs)
	Relay        RelayInfo   `json:"relay"`
	Hash         string      `json:"hash"`
}

type OrgState struct {
	Org      int64  `json:"org"`
	Location string `json:"location"` // customer
	Profile  string `json:"profile,omitempty"`
}

// --- status (DESIGN §9.1) ---

type Status struct {
	At           time.Time         `json:"at"`
	Runner       RunnerState       `json:"runner"`
	Installation InstallationState `json:"installation"`
	Relays       []Component       `json:"relays"`
	Proxies      []Component       `json:"proxies"`
	Orgs         []OrgStatus       `json:"orgs"`
}

type RunnerState struct {
	Version      string `json:"version"`
	UptimeS      int64  `json:"uptime_s"`
	JobsRunning  int    `json:"jobs_running"`
	LastJobError string `json:"last_job_error,omitempty"`
}

type InstallationState struct {
	DBOK           bool   `json:"db_ok"`
	ControlPlaneOK bool   `json:"control_plane_ok"`
	OrgsCustomer   int    `json:"orgs_customer"`
	ExtensionsHash string `json:"extensions_hash"`
	Error          string `json:"error,omitempty"`
}

// Component: a relay's or a proxy's own status document, as it served it.
type Component struct {
	Addr   string          `json:"addr"`
	Status json.RawMessage `json:"status,omitempty"`
	Error  string          `json:"error,omitempty"`
}

type OrgStatus struct {
	Org      int64  `json:"org"`
	Location string `json:"location"`
	Profile  string `json:"profile,omitempty"`
	Probe    Probe  `json:"probe"`
}

type Probe struct {
	OK        bool    `json:"ok"`
	RTTMS     float64 `json:"rtt_ms,omitempty"`
	PGVersion string  `json:"pg_version,omitempty"`
	DBBytes   int64   `json:"db_bytes,omitempty"`
	Error     string  `json:"error,omitempty"`
}

// Quote: a value of a libpq key=value DSN.
func Quote(v string) string {
	return "'" + strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(v) + "'"
}

// Validate: a public proxy needs a name (a DNS name or an IP address, no port), and addresses that
// may connect, none of which is the whole internet.
func (p *PublicProxy) Validate() error {
	if p == nil {
		return nil
	}
	if strings.TrimSpace(p.Name) == "" || strings.ContainsAny(p.Name, " /@:") {
		return fmt.Errorf("the public name of this server: a DNS name or an IP address, without a port")
	}
	if len(p.Allow) == 0 {
		return fmt.Errorf("name the addresses that may connect: a proxy open to every address is not offered")
	}
	for _, e := range p.Allow {
		e = strings.TrimSpace(e)
		if pfx, err := netip.ParsePrefix(e); err == nil {
			if pfx.Bits() == 0 {
				return fmt.Errorf("%q is the whole internet: name the addresses that may connect", e)
			}
			continue
		}
		if _, err := netip.ParseAddr(e); err != nil {
			return fmt.Errorf("%q is not an IP address or a CIDR range", e)
		}
	}
	return nil
}

// FanoutModes: the policies the proxy knows (proxy.FanoutPolicy.Mode), the panel's choices.
var FanoutModes = []string{"reject", "reject-writes", "log", "allow"}

// ValidFanout: "" (not chosen) or one of FanoutModes.
func ValidFanout(mode string) bool {
	if mode == "" {
		return true
	}
	for _, m := range FanoutModes {
		if m == mode {
			return true
		}
	}
	return false
}

// --- statements without a customer (the fanout review) ---

// Kinds of the review: fanout.review lists the statements the proxy saw that name no customer;
// fanout.decide gives some of them a way to be routed, or allows them by name.
const (
	KindStatements = "fanout.review"
	KindDecide     = "fanout.decide"
)

// StatementsArgs: nothing yet; the runner answers from what its proxy has seen.
type StatementsArgs struct{}

// Statement: one statement shape (its literals are $n: no customer's data), how often it ran
// without naming a customer, why, and what can be done with it.
type Statement struct {
	Fingerprint string   `json:"fingerprint"`
	SQL         string   `json:"sql"`
	Table       string   `json:"table,omitempty"`
	Column      string   `json:"column,omitempty"`
	Reason      string   `json:"reason"` // in words
	Write       bool     `json:"write,omitempty"`
	Refused     bool     `json:"refused,omitempty"` // the last time the application got an error
	Count       int64    `json:"count"`
	Options     []string `json:"options"` // "key", "cloud", "allow"
}

type StatementsResult struct {
	Policy   string      `json:"policy"` // the fanout policy in force
	Items    []Statement `json:"items"`
	Overflow int64       `json:"overflow,omitempty"` // shapes that did not fit
}

// Decision: Choice is "key" (the column names the customer: a unique column such as a slug),
// "cloud" (the table is not per customer; it lives with LokaDB) or "allow" (run this statement in
// every customer's database, by name).
type Decision struct {
	Fingerprint string `json:"fingerprint"`
	Choice      string `json:"choice"`
}

type DecideArgs struct {
	Decisions []Decision `json:"decisions"`
}

type DecideResult struct {
	Applied []string          `json:"applied"` // fingerprints
	Failed  map[string]string `json:"failed,omitempty"`
	// Spec: the spec with the routing declarations now in force; the panel saves it so that the
	// service holds what the runner holds.
	Spec json.RawMessage `json:"spec,omitempty"`
}
