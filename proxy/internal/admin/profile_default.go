package admin

import (
	"context"
	"fmt"
	"strconv"

	"github.com/lokadb/lokadb/proxy/internal/creds"
)

// The profile a moved organization gets (decision of 26.10, after block 14's measurements on
// a real network, the organizations 9-49 ms away from the cloud): fdw costs about 5 exchanges
// with the organization's database a statement (postgres_fdw), 350-440 ms an ORM operation;
// fdw+direct and direct about 1.5 (a read 88 ms against 437, an update 45 against 351). So
// move-org sets fdw+direct when the proxy can take the direct path, and says why not when it
// cannot. A profile given explicitly (move-org -profile, org-profile) is as before.

// FDWCost: what profile fdw costs, for the warnings of move-org and check.
const FDWCost = "about 5 exchanges with the organization's database a statement (postgres_fdw; 350-440 ms an ORM operation on a network 9-49 ms away, against about 1.5 exchanges on fdw+direct)"

// MovedProfileOptions: move-org's choice of the moved organization's profile.
type MovedProfileOptions struct {
	OrgProfileOptions
	// Explicit: the profile move-org was given (-profile); "": the default, fdw+direct when the
	// direct path can be set up.
	Explicit string
	// ProxyConfig: move-org was given the proxy's config (-proxy-config): without it, whether
	// the proxy has the direct path's credentials is not known. ProxyCustomersApp: its
	// customers_app of the organization (the control plane's, ControlPlane and KeyFile).
	ProxyConfig       bool
	ProxyCustomersApp string
}

// SetMovedProfile gives the organization move-org moved its profile: the explicit one, or
// fdw+direct when the proxy has the direct path's credentials of it (customers_app of its
// config, or the direct row of its control plane). Otherwise fdw stays, and warning says why
// and what it costs. A failure to set fdw+direct is a warning too: the move is done.
func SetMovedProfile(ctx context.Context, o MovedProfileOptions) (profile, warning string, err error) {
	if o.Explicit != "" {
		if o.Explicit == ProfileFDW {
			return ProfileFDW, "", nil // the default of the row (NULL)
		}
		o.Profile = o.Explicit
		_, err := SetOrgProfile(ctx, o.OrgProfileOptions)
		return o.Explicit, "", err
	}
	why := ""
	switch {
	case !o.ProxyConfig:
		why = "move-org was not given the proxy's config (-proxy-config): whether the proxy has the direct path's credentials of the organization is not known"
	case o.ProxyCustomersApp != "":
	case o.ControlPlane != "" && o.KeyFile != "":
		set, lerr := loadCredentials(ctx, o.CloudDSN, o.ControlPlane, o.KeyFile, o.KeyFileOld)
		db, derr := dbName(o.CloudDSN)
		switch {
		case lerr != nil:
			why = "the proxy's control plane is not read (" + lerr.Error() + ")"
		case derr != nil:
			why = derr.Error()
		case set.DSN(db, strconv.FormatInt(o.Org, 10), creds.Direct) == "":
			why = "the proxy has no direct path credentials of the organization (neither customers_app in its config nor lokadb org-credentials -kind direct)"
		}
	default:
		why = "the proxy has no direct path credentials of the organization (neither customers_app in its config nor lokadb org-credentials -kind direct)"
	}
	if why == "" {
		o.Profile = ProfileFDWDirect
		if _, err := SetOrgProfile(ctx, o.OrgProfileOptions); err != nil {
			why = "profile fdw+direct was not set (" + err.Error() + ")"
		} else {
			return ProfileFDWDirect, "", nil
		}
	}
	return ProfileFDW, fmt.Sprintf("organization %d stays on profile fdw: %s. Profile fdw costs %s; on a far network give the proxy the direct path's credentials (customers_app, or lokadb org-credentials -kind direct) and run lokadb org-profile -org %d fdw+direct",
		o.Org, why, FDWCost, o.Org), nil
}
