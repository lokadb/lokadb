package check

import (
	"fmt"
	"net"
	"strings"
)

// cloudDirectAccess (BACKLOG block 8, item 11): the proxy keeps the organizations apart, so
// a connection of the application's role to the cloud database that does not go through the
// proxy sees every organization. The cloud's pg_hba must let that role in from the proxy (and
// the relay) only. The rules come from pg_hba_file_rules (a superuser, or a role granted
// EXECUTE on pg_hba_file_rules()); -proxy-hosts lists the addresses allowed. Without
// -proxy-hosts a rule is reported when it admits the role from more than one host.
func (c *checker) cloudDirectAccess(roles []string) error {
	if len(roles) == 0 {
		return nil
	}
	if _, err := c.tx.Exec(c.ctx, "SAVEPOINT lokadb_hba"); err != nil {
		return err
	}
	rules, err := c.rows(`SELECT type, array_to_string(user_name, ','), coalesce(address, ''), coalesce(netmask, ''), auth_method, line_number::text
		FROM pg_hba_file_rules h WHERE error IS NULL ORDER BY h.line_number`) // the number, not its text (1, 120, 2...)
	if err != nil {
		if _, rerr := c.tx.Exec(c.ctx, "ROLLBACK TO SAVEPOINT lokadb_hba"); rerr != nil {
			return rerr
		}
		c.add(Info, "cloud_direct_access_unknown", "", "pg_hba_file_rules cannot be read with this role (a superuser, or GRANT EXECUTE ON FUNCTION pg_hba_file_rules() to it): it is not checked that the application's role reaches the cloud database only from the proxy", []string{err.Error()})
		return nil
	}
	members, err := c.rows(`SELECT r.rolname::text, g.rolname::text FROM pg_roles r JOIN pg_roles g ON pg_has_role(r.oid, g.oid, 'MEMBER') WHERE r.rolname = ANY($1)`, roles)
	if err != nil {
		return err
	}
	inGroup := map[string]bool{} // role "+group"
	for _, m := range members {
		inGroup[m[0]+" +"+m[1]] = true
	}
	allowed, bad := parseHosts(c.o.ProxyHosts)
	if len(bad) > 0 {
		c.add(Warning, "cloud_direct_access", "", "-proxy-hosts: these entries are not IPs or CIDRs", bad)
	}
	for _, role := range roles {
		if open := hbaOpen(rules, role, inGroup, allowed); len(open) > 0 {
			c.add(Warning, "cloud_direct_access", role, fmt.Sprintf("the application's role %s can connect to the cloud database from addresses other than the proxy's: there, without the proxy, a query sees and changes every organization's rows. Let it in from the proxy and the relay only (pg_hba: host … %s <proxy>/32 …, then a reject for it), and keep the database off other networks (firewall, security group)", role, role), open)
		}
	}
	return nil
}

// hbaOpen: the rules (type, users, address, netmask, method, line) that admit role from
// outside allowed, in pg_hba's order: the first rule that matches every address for the
// role ends it (a reject included).
func hbaOpen(rules [][]string, role string, inGroup map[string]bool, allowed []*net.IPNet) []string {
	var open []string
	for _, r := range rules {
		typ, users, addr, mask, method, line := r[0], strings.Split(r[1], ","), r[2], r[3], r[4], r[5]
		if typ == "local" || !hbaUser(users, role, inGroup) {
			continue
		}
		if method != "reject" {
			if why := hbaTooWide(addr, mask, allowed); why != "" {
				open = append(open, fmt.Sprintf("pg_hba.conf line %s: %s %s %s %s (%s)", line, typ, r[1], hbaAddr(addr, mask), method, why))
			}
		}
		if hbaMatchesAll(addr, mask) {
			break
		}
	}
	return open
}

// parseHosts: -proxy-hosts as networks (an IP is its /32 or /128).
func parseHosts(list []string) (nets []*net.IPNet, bad []string) {
	for _, h := range list {
		h = strings.TrimSpace(h)
		if h == "" {
			continue
		}
		if !strings.Contains(h, "/") {
			if ip := net.ParseIP(h); ip != nil {
				bits := 128
				if ip.To4() != nil {
					bits = 32
				}
				nets = append(nets, &net.IPNet{IP: ip, Mask: net.CIDRMask(bits, bits)})
				continue
			}
			bad = append(bad, h)
			continue
		}
		if _, n, err := net.ParseCIDR(h); err == nil {
			nets = append(nets, n)
		} else {
			bad = append(bad, h)
		}
	}
	return nets, bad
}

// hbaUser: a rule's user list names role (all, the role, or +group it belongs to).
func hbaUser(users []string, role string, inGroup map[string]bool) bool {
	for _, u := range users {
		switch {
		case u == "all", u == role:
			return true
		case strings.HasPrefix(u, "+") && inGroup[role+" "+u]:
			return true
		}
	}
	return false
}

func hbaAddr(addr, mask string) string {
	if mask != "" {
		return addr + " " + mask
	}
	return addr
}

// hbaNet: the network of a rule's address ("" for all, samehost, samenet, a host name).
func hbaNet(addr, mask string) *net.IPNet {
	ip := net.ParseIP(addr)
	if ip == nil {
		return nil
	}
	if m := net.ParseIP(mask); m != nil {
		if v4 := m.To4(); v4 != nil && ip.To4() != nil {
			return &net.IPNet{IP: ip.To4().Mask(net.IPMask(v4)), Mask: net.IPMask(v4)}
		}
		return &net.IPNet{IP: ip.Mask(net.IPMask(m.To16())), Mask: net.IPMask(m.To16())}
	}
	bits := 128
	if ip.To4() != nil {
		bits = 32
	}
	return &net.IPNet{IP: ip, Mask: net.CIDRMask(bits, bits)}
}

func hbaMatchesAll(addr, mask string) bool {
	if addr == "all" {
		return true
	}
	n := hbaNet(addr, mask)
	if n == nil {
		return false
	}
	ones, _ := n.Mask.Size()
	return ones == 0
}

// hbaTooWide: why a rule admits more than allowed ("" when it does not). Without allowed,
// a rule wider than one host is too wide.
func hbaTooWide(addr, mask string, allowed []*net.IPNet) string {
	switch addr {
	case "all":
		return "every address"
	case "samenet":
		return "every host of the server's networks"
	case "samehost":
		return ""
	}
	n := hbaNet(addr, mask)
	if n == nil {
		return "a host name: not compared"
	}
	if n.IP.IsLoopback() {
		return "" // the database's own host, like samehost
	}
	if len(allowed) == 0 {
		if ones, bits := n.Mask.Size(); ones < bits {
			return "more than one host (give -proxy-hosts to compare with the proxy's addresses)"
		}
		return ""
	}
	for _, a := range allowed {
		aOnes, _ := a.Mask.Size()
		nOnes, _ := n.Mask.Size()
		if a.Contains(n.IP) && nOnes >= aOnes {
			return ""
		}
	}
	return "not within -proxy-hosts"
}
