package admin

import (
	"context"
	"errors"
	"strconv"
)

// The SaaS (cloud) database runs PostgreSQL 15 or later; the organizations' databases 13 to 18.
// Before 15 the cloud gets enable through but not the moves: on 13 move-org cannot give the FDW
// server async_capable (14), on 14 move-back makes its bridge views with security_invoker (15),
// without which they would read the organization's rows with their owner's rights, past row
// security. check reports it as a blocker (cloud_version_unsupported), and enable, move-org and
// move-back refuse it before they change anything, with the same words.

// MinCloudVersion: the oldest PostgreSQL of the SaaS database (server_version_num).
const MinCloudVersion = 150000

// CloudVersionProblem: why the SaaS database of version num (server_version_num) cannot run
// LokaDB, "" when it can.
func CloudVersionProblem(num int) string {
	if num >= MinCloudVersion {
		return ""
	}
	return "the SaaS database runs PostgreSQL " + strconv.Itoa(num/10000) + ", and LokaDB needs PostgreSQL 15 or later there " +
		"(the organizations' databases may run 13 to 18): on 13 move-org cannot give the FDW server async_capable (PostgreSQL 14), " +
		"on 14 move-back cannot make its bridge views with security_invoker (PostgreSQL 15), without which they would read " +
		"an organization's rows with their owner's rights, past row security. Upgrade the SaaS database to PostgreSQL 15 or later"
}

// requireCloudVersion: the refusal of a command (what: "enable", "move-org" ...) on a SaaS
// database older than MinCloudVersion, before it changes anything.
func requireCloudVersion(ctx context.Context, conn querier, what string) error {
	r, err := rowsOf(ctx, conn, `SELECT current_setting('server_version_num')`)
	if err != nil {
		return err
	}
	num, _ := strconv.Atoi(r[0][0])
	if p := CloudVersionProblem(num); p != "" {
		return errors.New(what + " changed nothing: " + p)
	}
	return nil
}
