// Package cloudproto: what the runner (lokadb runner, at the SaaS) and the LokaDB Cloud service
// exchange. Open, like the runner: the SaaS can read every job the service may send it.
// Protocol 1 (private/cloud/DESIGN.md §4).
package cloudproto

import (
	"encoding/json"
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
	KindPrepare     = "agent.prepare"
	KindCredentials = "credentials.store"
	KindMove        = "move"
	KindMoveBack    = "move_back"
	KindRelease     = "org.release"
	KindExtSync     = "extensions.sync"
)

// Kinds: what a runner of this version does, as capabilities.
var Kinds = []string{KindInventory + "@1", KindLookup + "@1", KindCheck + "@1", KindEnable + "@1", KindPrepare + "@1",
	KindCredentials + "@1", KindMove + "@1", KindMoveBack + "@1", KindRelease + "@1", KindExtSync + "@1"}

// Exclusive: kinds of which a runner runs one at a time (the open module's own locks say so too).
var Exclusive = map[string]bool{KindMove: true, KindMoveBack: true, KindEnable: true}

// Idempotent: kinds the service may hand out again when their lease expired.
var Idempotent = map[string]bool{KindInventory: true, KindLookup: true, KindCheck: true, KindPrepare: true,
	KindCredentials: true, KindRelease: true, KindExtSync: true}

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
}

type PollResponse struct {
	Jobs            []Job    `json:"jobs"`
	Cancel          []string `json:"cancel"`
	WantInventory   bool     `json:"want_inventory"`
	StatusIntervalS int      `json:"status_interval_s"`
	LatestVersion   string   `json:"latest_version,omitempty"`
	MinVersion      string   `json:"min_version,omitempty"`
	RunnerImage     string   `json:"runner_image,omitempty"` // the latest release's image, tag included
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
