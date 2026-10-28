package check

// The latency section of the report (check -rtt): check/ext fills it (tasks/check-report-format.md).

// LatencyReport: the -rtt section of the report.
type LatencyReport struct {
	RTTms float64 `json:"rtt_ms"`
	// Model: "estimate": exchange counts from the measurements, not from this database.
	Model        string           `json:"model"`
	Exchanges    LatencyExchanges `json:"exchanges"`
	Transactions []LatencyTx      `json:"transactions"` // most expensive (per transaction, fdw) first
	// TotalFDW, TotalDirect: the recorded transactions together, in ms (count × per-transaction time).
	TotalFDWms    float64 `json:"total_fdw_ms"`
	TotalDirectMs float64 `json:"total_direct_ms"`
	// TotalTargetMs: the same at the direct path's limit, a goal (see Target in LatencyTx).
	TotalTargetMs float64 `json:"total_target_ms"`
}

// LatencyExchanges: the model's counts, as used.
type LatencyExchanges struct {
	Transaction  int `json:"fdw_transaction"`
	Select       int `json:"fdw_select"`
	Insert       int `json:"fdw_insert"`
	UpdateDirect int `json:"fdw_update_partition"`
	UpdateParent int `json:"fdw_update_delete_parent"`
	Direct       int `json:"direct_statement"`
	// TargetCommit: at the direct path's limit, a transaction of several statements pays one
	// exchange per statement and this one for COMMIT (a goal, not the proxy's behaviour now).
	TargetCommit int `json:"target_commit"`
}

// LatencyTx: one recorded transaction shape.
type LatencyTx struct {
	Count      int64              `json:"count"` // transactions of this shape seen
	Statements []LatencyStatement `json:"statements"`
	FDW        float64            `json:"exchanges_fdw"`
	Direct     float64            `json:"exchanges_direct"`
	FDWms      float64            `json:"ms_fdw"`
	DirectMs   float64            `json:"ms_direct"`
	// Target, TargetMs: at the direct path's limit, where the proxy is heading (block 10, items
	// 6-9 and 8a.2), not what it does now: one exchange per statement, one for COMMIT.
	Target   float64 `json:"exchanges_target"`
	TargetMs float64 `json:"ms_target"`
	Single   bool    `json:"single_statement"` // a statement on its own: the direct path can take it
	AllOrgs  bool    `json:"all_organizations"`
}

// LatencyStatement: a statement of a shape and what it costs.
type LatencyStatement struct {
	Fingerprint string `json:"fingerprint"`
	SQL         string `json:"sql"`
	// Kind: select, insert, update, delete, cloud (no LokaDB table: no exchange), other.
	Kind string `json:"kind"`
	// Repeats: how many times it runs per transaction. Counted (Counted true) when the
	// recording keeps it (RecordedTx.Repeats); otherwise estimated: a statement on its own
	// counts once per transaction, the rest of its calls are spread over the transactions of
	// the other shapes that ran it.
	Repeats float64 `json:"repeats"`
	Counted bool    `json:"repeats_counted"`
	FDW     float64 `json:"exchanges_fdw"`
	Direct  float64 `json:"exchanges_direct"`
	Target  float64 `json:"exchanges_target"` // at the direct path's limit: one per run (a goal)
	// AllOrgs: it names no organization, so it runs in every organization's database.
	AllOrgs bool `json:"all_organizations,omitempty"`
}
