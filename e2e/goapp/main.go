// A small GORM application. It knows nothing about where the rows physically live.
package main

import (
	"embed"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
)

//go:embed migrations/*.sql
var migrations embed.FS

type Organization struct {
	ID   int64
	Name string
}

type Project struct {
	ID             int64
	OrganizationID int64
	Name           string
}

type Document struct {
	ID             int64
	OrganizationID int64
	ProjectID      int64
	Slug           string
	Title          string
	Body           string
	Project        Project
	Versions       []DocumentVersion
	Comments       []Comment
}

type DocumentVersion struct {
	ID         int64
	DocumentID int64
	Content    string
}

type Comment struct {
	ID         int64
	DocumentID int64
	Author     string
	Body       string
}

// Share is keyed by a uuid the database makes (block 17); its visits reach it by a uuid FK.
type Share struct {
	ID             string `gorm:"type:uuid;default:gen_random_uuid()"`
	OrganizationID int64
	DocumentID     int64
	Token          string
	Visits         []ShareVisit
}

type ShareVisit struct {
	ID      string `gorm:"type:uuid;default:gen_random_uuid()"`
	ShareID string `gorm:"type:uuid"`
	Who     string
}

// DocumentR2 is the release-2 model of the documents table.
type DocumentR2 struct {
	ID             int64
	OrganizationID int64
	ProjectID      int64
	Slug           string
	Title          string
	Body           string
	Status         string
}

func (DocumentR2) TableName() string { return "documents" }

var fails int

func check(name string, fn func() error) {
	err := func() (err error) {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("panic: %v", r)
			}
		}()
		return fn()
	}()
	if err != nil {
		fails++
		fmt.Printf("FAIL  %s: %v\n", name, err)
		return
	}
	fmt.Printf("PASS  %s\n", name)
}

func expect(ok bool, format string, args ...any) error {
	if ok {
		return nil
	}
	return fmt.Errorf(format, args...)
}

func main() {
	if len(os.Args) > 2 && os.Args[1] == "uuid" { // block 18: the application numbered by uuid (uuid.go)
		os.Exit(uuidMain(os.Args[2:]))
	}
	db, err := gorm.Open(postgres.Open(os.Getenv("DATABASE_URL")), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		panic(err)
	}
	switch os.Args[1] {
	case "migrate":
		err = migrate(db, os.Args[2])
	case "seed":
		err = seed(db)
	case "scenario":
		var a, b Organization
		db.Where("name = ?", "Org A").First(&a)
		db.Where("name = ?", "Org B").First(&b)
		switch os.Args[2] {
		case "r1":
			r1(db, a, b)
		case "r2":
			r2(db, a, b)
		case "offline":
			offline(db, a, b)
		case "coop":
			coop(db, a, b)
		}
		if fails > 0 {
			os.Exit(1)
		}
	}
	if err != nil {
		fmt.Println("error:", err)
		os.Exit(1)
	}
}

// migrate applies migrations/NNN_*.sql up to the target version, one transaction per file.
func migrate(db *gorm.DB, target string) error {
	if err := db.Exec("CREATE TABLE IF NOT EXISTS schema_migrations (version text PRIMARY KEY)").Error; err != nil {
		return err
	}
	entries, _ := migrations.ReadDir("migrations")
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	for _, e := range entries {
		version := e.Name()[:3]
		if version > fmt.Sprintf("%03s", target) {
			break
		}
		var n int64
		db.Table("schema_migrations").Where("version = ?", version).Count(&n)
		if n > 0 {
			continue
		}
		body, _ := migrations.ReadFile("migrations/" + e.Name())
		err := db.Transaction(func(tx *gorm.DB) error {
			for _, stmt := range strings.Split(string(body), ";") {
				if strings.TrimSpace(stmt) == "" {
					continue
				}
				if err := tx.Exec(stmt).Error; err != nil {
					return err
				}
			}
			return tx.Exec("INSERT INTO schema_migrations VALUES (?)", version).Error
		})
		if err != nil {
			return fmt.Errorf("%s: %w", e.Name(), err)
		}
		fmt.Println("migrated", e.Name())
	}
	return nil
}

func seed(db *gorm.DB) error {
	for _, l := range []string{"A", "B"} {
		org := Organization{Name: "Org " + l}
		if err := db.Create(&org).Error; err != nil {
			return err
		}
		project := Project{OrganizationID: org.ID, Name: l + "-Project"}
		if err := db.Create(&project).Error; err != nil {
			return err
		}
		for i := 1; i <= 3; i++ {
			doc := Document{
				OrganizationID: org.ID, ProjectID: project.ID,
				Slug: fmt.Sprintf("%s-%d", strings.ToLower(l), i), Title: fmt.Sprintf("%s doc %d", l, i),
				Body:     fmt.Sprintf("secret %s %d", l, i),
				Versions: []DocumentVersion{{Content: "v1"}, {Content: "v2"}},
				Comments: []Comment{{Author: "alice", Body: "looks good"}},
			}
			if err := db.Omit("Project").Create(&doc).Error; err != nil {
				return err
			}
			if i <= 2 {
				share := Share{OrganizationID: org.ID, DocumentID: doc.ID, Token: fmt.Sprintf("%s-share-%d", strings.ToLower(l), i),
					Visits: []ShareVisit{{Who: "alice"}}}
				if err := db.Create(&share).Error; err != nil {
					return err
				}
			}
		}
	}
	fmt.Println("seeded")
	return nil
}

func projectOf(db *gorm.DB, org Organization) int64 {
	var p Project
	db.Where("organization_id = ?", org.ID).First(&p)
	return p.ID
}

// countByIDs runs `id = ANY($1)` with a Go slice as one array parameter (pgx sends int8[]).
// GORM would expand the slice into a list, so this goes through database/sql.
func countByIDs(db *gorm.DB, ids []int64) (int, error) {
	sqlDB, err := db.DB()
	if err != nil {
		return 0, err
	}
	var n int
	err = sqlDB.QueryRow(`SELECT count(*) FROM documents WHERE id = ANY($1)`, ids).Scan(&n)
	return n, err
}

func idsOf(db *gorm.DB, org Organization) []int64 {
	var ids []int64
	db.Table("documents").Where("organization_id = ?", org.ID).Order("id").Pluck("id", &ids)
	return ids
}

// shareIDs: the uuid keys of an organization's shares.
func shareIDs(db *gorm.DB, org Organization) []string {
	var ids []string
	db.Table("shares").Where("organization_id = ?", org.ID).Order("id").Pluck("id", &ids)
	return ids
}

// countSharesByIDs runs `id = ANY($1)` with a []string as one uuid[] parameter.
func countSharesByIDs(db *gorm.DB, ids []string) (int, error) {
	sqlDB, err := db.DB()
	if err != nil {
		return 0, err
	}
	var n int
	err = sqlDB.QueryRow(`SELECT count(*) FROM shares WHERE id = ANY($1)`, ids).Scan(&n)
	return n, err
}

// shares: an organization's statements on tables keyed by a uuid (block 17). Each one names
// the organization by a uuid alone, or, for the visits, by the FK onto the uuid key alone.
func shares(db *gorm.DB, org Organization, l string) {
	var created Share
	check(l+": shares: create one without a key (gen_random_uuid() in the cloud), with a visit", func() error {
		var doc Document
		db.Where("organization_id = ?", org.ID).Order("id").First(&doc)
		created = Share{OrganizationID: org.ID, DocumentID: doc.ID, Token: strings.ToLower(l) + "-share-new", Visits: []ShareVisit{{Who: "carol"}}}
		if err := db.Create(&created).Error; err != nil {
			return err
		}
		return expect(len(created.ID) == 36 && len(created.Visits) == 1 && len(created.Visits[0].ID) == 36, "share %q, visits %+v", created.ID, created.Visits)
	})
	check(l+": shares: First by the uuid key, Preload(Visits) by share_id (the FK onto it)", func() error {
		var s Share
		if err := db.Preload("Visits").First(&s, "id = ?", created.ID).Error; err != nil {
			return err
		}
		return expect(s.Token == strings.ToLower(l)+"-share-new" && len(s.Visits) == 1 && s.Visits[0].Who == "carol", "share %+v", s)
	})
	check(l+": shares: id = ANY($1) with a []string, and id IN (...)", func() error {
		ids := shareIDs(db, org)
		n, err := countSharesByIDs(db, ids)
		if err != nil {
			return err
		}
		var m int64
		if err := db.Model(&Share{}).Where("id IN ?", ids).Count(&m).Error; err != nil {
			return err
		}
		return expect(n == len(ids) && int(m) == len(ids) && n == 3, "ANY %d, IN %d, of %d shares", n, m, len(ids))
	})
	check(l+": shares: update a visit found by share_id alone, and a share by its key", func() error {
		res := db.Model(&ShareVisit{}).Where("share_id = ?", created.ID).Update("who", "dave")
		if res.Error != nil {
			return res.Error
		}
		res2 := db.Model(&Share{}).Where("id = ?", created.ID).Update("token", strings.ToLower(l)+"-share-edited")
		if res2.Error != nil {
			return res2.Error
		}
		return expect(res.RowsAffected == 1 && res2.RowsAffected == 1, "visits updated %d, shares updated %d", res.RowsAffected, res2.RowsAffected)
	})
}

func r1(db *gorm.DB, a, b Organization) {
	for _, o := range []struct {
		org Organization
		l   string
	}{{a, "A"}, {b, "B"}} {
		check(o.l+": list with Preload(Project, Versions)", func() error {
			var docs []Document
			if err := db.Preload("Project").Preload("Versions").Where("organization_id = ?", o.org.ID).Order("id").Find(&docs).Error; err != nil {
				return err
			}
			if len(docs) != 3 {
				return fmt.Errorf("got %d docs", len(docs))
			}
			for _, d := range docs {
				if d.Project.Name != o.l+"-Project" || len(d.Versions) != 2 {
					return fmt.Errorf("doc %d: project %q, %d versions", d.ID, d.Project.Name, len(d.Versions))
				}
			}
			return nil
		})
		check(o.l+": First by primary key (no tenant in the query)", func() error {
			var first, found Document
			db.Where("organization_id = ?", o.org.ID).Order("id").First(&first)
			if err := db.First(&found, first.ID).Error; err != nil {
				return err
			}
			return expect(found.Title == o.l+" doc 1", "title %q", found.Title)
		})
		check(o.l+": id = ANY($1) with a slice (one array parameter)", func() error {
			ids := idsOf(db, o.org)
			n, err := countByIDs(db, ids)
			if err != nil {
				return err
			}
			return expect(n == len(ids) && n > 0, "%d of %d documents", n, len(ids))
		})
		shares(db, o.org, o.l)
	}

	var created Document
	check("A: create document with version and comment (associations)", func() error {
		created = Document{OrganizationID: a.ID, ProjectID: projectOf(db, a), Slug: "a-new", Title: "New A", Body: "secret A new",
			Versions: []DocumentVersion{{Content: "first draft"}}, Comments: []Comment{{Author: "bob", Body: "hi"}}}
		if err := db.Omit("Project").Create(&created).Error; err != nil {
			return err
		}
		var fresh Document
		if err := db.Preload("Versions").Preload("Comments").First(&fresh, created.ID).Error; err != nil {
			return err
		}
		return expect(len(fresh.Versions) == 1 && len(fresh.Comments) == 1, "versions=%d comments=%d", len(fresh.Versions), len(fresh.Comments))
	})
	check("A: update by primary key", func() error {
		var d Document
		db.Where("organization_id = ? AND slug = ?", a.ID, "a-2").First(&d)
		if err := db.Model(&d).Update("title", "A doc 2 (edited)").Error; err != nil {
			return err
		}
		var again Document
		db.First(&again, d.ID)
		return expect(again.Title == "A doc 2 (edited)", "title %q", again.Title)
	})
	check("A: transaction commit", func() error {
		err := db.Transaction(func(tx *gorm.DB) error {
			d := Document{OrganizationID: a.ID, ProjectID: projectOf(tx, a), Slug: "a-tx", Title: "Tx", Body: "secret tx"}
			if err := tx.Omit("Project").Create(&d).Error; err != nil {
				return err
			}
			return tx.Create(&DocumentVersion{DocumentID: d.ID, Content: "in tx"}).Error
		})
		if err != nil {
			return err
		}
		var d Document
		if err := db.Preload("Versions").Where("organization_id = ? AND slug = ?", a.ID, "a-tx").First(&d).Error; err != nil {
			return err
		}
		return expect(len(d.Versions) == 1, "versions=%d", len(d.Versions))
	})
	check("A: transaction rollback", func() error {
		_ = db.Transaction(func(tx *gorm.DB) error {
			tx.Omit("Project").Create(&Document{OrganizationID: a.ID, ProjectID: projectOf(tx, a), Slug: "a-rollback", Title: "Rollback"})
			return errors.New("boom")
		})
		var n int64
		db.Model(&Document{}).Where("organization_id = ? AND slug = ?", a.ID, "a-rollback").Count(&n)
		return expect(n == 0, "rolled back row is visible")
	})
	for _, o := range []struct {
		org Organization
		l   string
	}{{a, "a"}, {b, "b"}} {
		check(strings.ToUpper(o.l)+": upsert with clause.OnConflict returns ids", func() error {
			pid := projectOf(db, o.org)
			docs := []Document{
				{OrganizationID: o.org.ID, ProjectID: pid, Slug: o.l + "-1", Title: o.l + "-1 upserted"},
				{OrganizationID: o.org.ID, ProjectID: pid, Slug: o.l + "-upsert", Title: "brand new"},
			}
			err := db.Omit("Project").Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "organization_id"}, {Name: "slug"}},
				DoUpdates: clause.AssignmentColumns([]string{"title"}),
			}).Create(&docs).Error
			if err != nil {
				return err
			}
			var existing Document
			db.Where("organization_id = ? AND slug = ?", o.org.ID, o.l+"-1").First(&existing)
			return expect(existing.Title == o.l+"-1 upserted" && docs[0].ID == existing.ID && docs[1].ID != 0,
				"title=%q ids=%d/%d existing=%d", existing.Title, docs[0].ID, docs[1].ID, existing.ID)
		})
	}
	check("A: delete cascades to versions (customer FK) and comments (cloud FK)", func() error {
		if err := db.Delete(&Document{}, created.ID).Error; err != nil {
			return err
		}
		var docs, versions, comments int64
		db.Model(&Document{}).Where("id = ?", created.ID).Count(&docs)
		db.Model(&DocumentVersion{}).Where("document_id = ?", created.ID).Count(&versions)
		db.Model(&Comment{}).Where("document_id = ?", created.ID).Count(&comments)
		return expect(docs+versions+comments == 0, "left: docs=%d versions=%d comments=%d", docs, versions, comments)
	})
	check("A: join + group by", func() error {
		var rows []struct {
			Name string
			C    int64
		}
		err := db.Table("documents").Select("projects.name, count(*) AS c").
			Joins("JOIN projects ON projects.id = documents.project_id").
			Where("documents.organization_id = ?", a.ID).Group("projects.name").Scan(&rows).Error
		if err != nil {
			return err
		}
		return expect(len(rows) == 1 && rows[0].Name == "A-Project" && rows[0].C == 5, "rows=%+v", rows)
	})
	check("B: lookup by slug only (all tenants scanned)", func() error {
		var d Document
		if err := db.Where("slug = ?", "b-2").First(&d).Error; err != nil {
			return err
		}
		return expect(d.OrganizationID == b.ID, "org %d", d.OrganizationID)
	})
}

func r2(db *gorm.DB, a, b Organization) {
	check("A+B: new column has its default", func() error {
		var da, dbb DocumentR2
		db.Where("organization_id = ? AND slug = ?", a.ID, "a-1").First(&da)
		db.Where("organization_id = ? AND slug = ?", b.ID, "b-1").First(&dbb)
		return expect(da.Status == "draft" && dbb.Status == "draft", "statuses %q %q", da.Status, dbb.Status)
	})
	check("A: write and read the new column", func() error {
		d := DocumentR2{OrganizationID: a.ID, ProjectID: projectOf(db, a), Slug: "a-r2", Title: "R2", Status: "published"}
		if err := db.Create(&d).Error; err != nil {
			return err
		}
		var again DocumentR2
		db.First(&again, d.ID)
		return expect(again.Status == "published", "status %q", again.Status)
	})
	check("B: update the new column", func() error {
		if err := db.Model(&DocumentR2{}).Where("organization_id = ? AND slug = ?", b.ID, "b-2").Update("status", "archived").Error; err != nil {
			return err
		}
		var again DocumentR2
		db.Where("organization_id = ? AND slug = ?", b.ID, "b-2").First(&again)
		return expect(again.Status == "archived", "status %q", again.Status)
	})
}

func offline(db *gorm.DB, a, b Organization) {
	check("B: works while A's database is down (list, First, create)", func() error {
		var docs []DocumentR2
		if err := db.Where("organization_id = ?", b.ID).Find(&docs).Error; err != nil {
			return err
		}
		var one DocumentR2
		if err := db.First(&one, docs[0].ID).Error; err != nil {
			return err
		}
		d := DocumentR2{OrganizationID: b.ID, ProjectID: projectOf(db, b), Slug: fmt.Sprintf("b-offline-%d", time.Now().UnixNano()), Title: "while A is down", Status: "draft"}
		return db.Create(&d).Error
	})
	// Sent to every organization's database, this would fail: A's is down.
	check("B: id = ANY($1) works while A's database is down (the ids name the organization)", func() error {
		ids := idsOf(db, b)
		n, err := countByIDs(db, ids)
		if err != nil {
			return err
		}
		return expect(n == len(ids) && n > 0, "%d of %d documents", n, len(ids))
	})
	// block 17: a uuid key names the organization the same way
	check("B: a share by its uuid key and its visits by share_id work while A's database is down", func() error {
		ids := shareIDs(db, b)
		if len(ids) == 0 {
			return errors.New("no shares of B")
		}
		var s Share
		if err := db.Preload("Visits").First(&s, "id = ?", ids[0]).Error; err != nil {
			return err
		}
		n, err := countSharesByIDs(db, ids)
		if err != nil {
			return err
		}
		return expect(s.OrganizationID == b.ID && len(s.Visits) > 0 && n == len(ids), "share %+v, %d of %d by ANY", s, n, len(ids))
	})
	check("A: fails with an error instead of falling back to the cloud", func() error {
		var docs []DocumentR2
		err := db.Where("organization_id = ?", a.ID).Find(&docs).Error
		if err == nil {
			return errors.New("query unexpectedly succeeded")
		}
		fmt.Println("     error:", err)
		return nil
	})
}

// inOrg is the cooperative mode's middleware (block 10, tasks/cooperative-mode.md): the one
// line an application adds where it already knows the organization of the request. The
// proxy then takes that organization instead of guessing it from each statement.
func inOrg(db *gorm.DB, org int64, fn func(tx *gorm.DB) error) error {
	return db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Exec("SELECT set_config('lokadb.tenant', ?, true)", fmt.Sprint(org)).Error; err != nil {
			return err
		}
		return fn(tx)
	})
}

// coop: the same application with the middleware. Statements the proxy cannot route by
// themselves (the fanout policy is reject) run in the named organization; a statement that
// names another one fails; a row of another organization is not found by its id.
func coop(db *gorm.DB, a, b Organization) {
	check("coop: a condition on a column without a lookup runs in the named organization (1.3)", func() error {
		var named, want int64
		if err := inOrg(db, a.ID, func(tx *gorm.DB) error {
			return tx.Model(&Document{}).Where("title LIKE ?", "%").Count(&named).Error
		}); err != nil {
			return err
		}
		if err := db.Model(&Document{}).Where("organization_id = ?", a.ID).Count(&want).Error; err != nil {
			return err
		}
		return expect(named == want && want > 0, "counted %d, organization A has %d", named, want)
	})
	check("coop: an insert with an explicit id, read back by id (P4: no refs lookup)", func() error {
		id := 9_000_000 + a.ID
		doc := Document{ID: id, OrganizationID: a.ID, ProjectID: projectOf(db, a), Slug: "a-coop", Title: "Coop"}
		var got Document
		err := inOrg(db, a.ID, func(tx *gorm.DB) error {
			if err := tx.Omit("Project").Create(&doc).Error; err != nil {
				return err
			}
			return tx.First(&got, id).Error
		})
		if err != nil {
			return err
		}
		defer inOrg(db, a.ID, func(tx *gorm.DB) error { return tx.Delete(&Document{}, id).Error }) //nolint:errcheck
		return expect(got.Title == "Coop", "read back %+v", got)
	})
	check("coop: eager loading by parent ids", func() error {
		var docs []Document
		if err := inOrg(db, a.ID, func(tx *gorm.DB) error {
			return tx.Preload("Versions").Where("organization_id = ?", a.ID).Order("id").Find(&docs).Error
		}); err != nil {
			return err
		}
		return expect(len(docs) > 0 && len(docs[0].Versions) > 0, "%d documents, versions of the first: %d", len(docs), len(docs[0].Versions))
	})
	check("coop: a statement that names another organization fails (42501)", func() error {
		var docs []Document
		err := inOrg(db, a.ID, func(tx *gorm.DB) error {
			return tx.Where("organization_id = ?", b.ID).Find(&docs).Error
		})
		return expect(err != nil && strings.Contains(err.Error(), "42501") && len(docs) == 0, "error %v, %d rows", err, len(docs))
	})
	check("coop: a document of another organization is not found by its id", func() error {
		var other Document
		if err := db.Where("organization_id = ?", b.ID).Order("id").First(&other).Error; err != nil {
			return err
		}
		var got Document
		err := inOrg(db, a.ID, func(tx *gorm.DB) error { return tx.First(&got, other.ID).Error })
		return expect(errors.Is(err, gorm.ErrRecordNotFound), "error %v, row %+v", err, got)
	})
}
