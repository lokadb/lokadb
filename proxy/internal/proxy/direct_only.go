package proxy

// The profile direct (block 10, stage 9b, tasks/direct-profile.md): an organization with no
// way through the FDW. Its statements on LokaDB tables go to its database or fail: one the
// direct path does not take is refused with its reason instead of going the cloud's way (the
// cloud may have no user mapping of it: org-profile -drop-fdw-credentials). A read needs the
// organization named (lokadb.tenant): a guessed read from the wrong database would come back
// empty, silently (RD §7.3). DDL goes the cloud's way as before and is replayed there.

// ProfileDirect: Meta.Profiles of an organization with no FDW way.
const ProfileDirect = "direct"

// directProfile: p lets statements go straight to the organization's database.
func directProfile(p string) bool { return p == ProfileFDWDirect || p == ProfileDirect }

// directOnly: org's profile is direct.
func (s *session) directOnly(org string) bool {
	if s.meta == nil || !realOrg(org) {
		return false
	}
	num := s.meta.orgNum(org)
	return num != "" && s.meta.Profiles[num] == ProfileDirect
}

// directOnlyRefusal: why a statement on LokaDB tables of org, about to go the cloud's way,
// cannot (org in direct): "" when it may (another profile, DDL).
func (s *session) directOnlyRefusal(sql string, an *Analysis, org string, named bool) string {
	if !an.Lokadb || len(an.DDL) > 0 || !s.directOnly(org) {
		return ""
	}
	v := directInWith(sql, an, s.meta, named, s.pathFor()) // an: the session's, by the same path
	di := s.directInfo(sql, an)
	switch {
	case v.Reason == DirectCloudTable:
		return "organization " + org + " is on profile direct: its statement joins a cloud table (" + joinCloud(v.Cloud) + ") with no copy in its database (routing.mirrors)"
	case s.tenant.Value() == TenantAll:
		return "organization " + org + " is on profile direct: a statement across organizations (lokadb.tenant = '*') cannot reach its database"
	case !named && di.ok && di.write == nil && di.update == "":
		return "organization " + org + " is on profile direct: a read must name the organization (SET LOCAL lokadb.tenant): a guessed one would read nothing from another database"
	case !di.ok:
		why := string(v.Reason)
		if why == "" {
			why = "a form the direct path does not take: MERGE, a cursor WITH HOLD, a table with a row trigger of the cloud (an FK onto a cloud table among them), a type of the application"
		}
		return "organization " + org + " is on profile direct: no FDW way, and the direct path does not take this statement (" + why + ")"
	}
	return "organization " + org + " is on profile direct: the statement cannot go to its database (its credentials, its partition, the gate)"
}

func joinCloud(ts []string) string {
	out := ""
	for i, t := range ts {
		if i > 0 {
			out += ", "
		}
		out += t
	}
	return out
}
