import sys
import time

from django.core.management.base import BaseCommand
from django.db import DatabaseError, transaction
from django.db.models import Count

from docs.models import Comment, Document, DocumentVersion, Organization, Project

# Ordinary Django ORM code: it knows nothing about where the rows physically live.


class Command(BaseCommand):
    help = "Run the demo scenario (r1, r2, offline)"

    def add_arguments(self, parser):
        parser.add_argument("phase")

    def verify(self, name, fn):
        try:
            ok = fn()
        except Exception as e:  # noqa: BLE001
            ok = f"{type(e).__name__}: {e}".splitlines()[0]
        if ok is True:
            self.stdout.write(f"PASS  {name}")
        else:
            self.fails += 1
            self.stdout.write(f"FAIL  {name}" + ("" if ok is False else f": {ok}"))

    def handle(self, *args, phase, **options):
        self.fails = 0
        a = Organization.objects.get(name="Org A")
        b = Organization.objects.get(name="Org B")
        getattr(self, phase)(a, b)
        if self.fails:
            sys.exit(1)

    def r1(self, a, b):
        for org, letter in [(a, "A"), (b, "B")]:
            def listing(org=org, letter=letter):
                docs = list(Document.objects.select_related("project").prefetch_related("versions")
                            .filter(organization=org).order_by("id"))
                return len(docs) == 3 and all(
                    d.project.name == f"{letter}-Project" and len(d.versions.all()) == 2 for d in docs)

            def find(org=org, letter=letter):
                pk = Document.objects.filter(organization=org).order_by("id").values_list("id", flat=True).first()
                return Document.objects.get(pk=pk).title == f"{letter} doc 1"

            self.verify(f"{letter}: list with select_related + prefetch_related", listing)
            self.verify(f"{letter}: get by pk (no tenant in the query)", find)

        project_a = Project.objects.get(organization=a)
        state = {}

        def create():
            doc = Document.objects.create(organization=a, project=project_a, slug="a-new", title="New A", body="secret A new")
            doc.versions.create(content="first draft")
            Comment.objects.create(document=doc, author="bob", body="hi")
            state["new"] = doc
            fresh = Document.objects.prefetch_related("versions", "comments").get(pk=doc.pk)
            return len(fresh.versions.all()) == 1 and len(fresh.comments.all()) == 1

        def update():
            doc = Document.objects.get(organization=a, slug="a-2")
            doc.title = "A doc 2 (edited)"
            doc.save()
            return Document.objects.get(pk=doc.pk).title == "A doc 2 (edited)"

        def tx_commit():
            with transaction.atomic():
                doc = Document.objects.create(organization=a, project=project_a, slug="a-tx", title="Tx", body="secret tx")
                doc.versions.create(content="in tx")
            return Document.objects.get(organization=a, slug="a-tx").versions.count() == 1

        def tx_rollback():
            try:
                with transaction.atomic():
                    Document.objects.create(organization=a, project=project_a, slug="a-rollback", title="Rollback")
                    raise RuntimeError("boom")
            except RuntimeError:
                pass
            return not Document.objects.filter(organization=a, slug="a-rollback").exists()

        self.verify("A: create document with version and comment", create)
        self.verify("A: update by pk", update)
        self.verify("A: transaction commit", tx_commit)
        self.verify("A: transaction rollback", tx_rollback)

        for org, letter in [(a, "a"), (b, "b")]:
            def upsert(org=org, letter=letter):
                project = Project.objects.get(organization=org)
                objs = Document.objects.bulk_create(
                    [Document(organization=org, project=project, slug=f"{letter}-1", title=f"{letter}-1 upserted"),
                     Document(organization=org, project=project, slug=f"{letter}-upsert", title="brand new")],
                    update_conflicts=True, unique_fields=["organization", "slug"], update_fields=["title"])
                existing = Document.objects.get(organization=org, slug=f"{letter}-1")
                return (existing.title == f"{letter}-1 upserted" and objs[0].pk == existing.pk
                        and Document.objects.filter(organization=org, slug=f"{letter}-upsert").exists())

            self.verify(f"{letter.upper()}: bulk_create(update_conflicts=True) returns the right pks", upsert)

        def delete():
            pk = state["new"].pk
            state["new"].delete()
            return (not Document.objects.filter(pk=pk).exists()
                    and DocumentVersion.objects.filter(document_id=pk).count() == 0
                    and Comment.objects.filter(document_id=pk).count() == 0)

        def aggregate():
            rows = list(Document.objects.filter(organization=a).values("project__name").annotate(c=Count("id")))
            return rows == [{"project__name": "A-Project", "c": 5}] or str(rows)

        self.verify("A: delete with ORM cascade", delete)
        self.verify("A: join + group by", aggregate)
        self.verify("B: lookup by slug only (all tenants scanned)",
                   lambda: Document.objects.get(slug="b-2").organization_id == b.id)

    def r2(self, a, b):
        self.verify("A+B: new column has its default",
                   lambda: Document.objects.get(organization=a, slug="a-1").status == "draft"
                   and Document.objects.get(organization=b, slug="b-1").status == "draft")

        def write_a():
            doc = Document.objects.create(organization=a, project=Project.objects.get(organization=a),
                                          slug="a-r2", title="R2", status="published")
            return Document.objects.get(pk=doc.pk).status == "published"

        def update_b():
            Document.objects.filter(organization=b, slug="b-2").update(status="archived")
            return Document.objects.get(organization=b, slug="b-2").status == "archived"

        self.verify("A: write and read the new column", write_a)
        self.verify("B: update the new column", update_b)

    def offline(self, a, b):
        def b_works():
            docs = list(Document.objects.prefetch_related("versions").filter(organization=b))
            doc = Document.objects.get(pk=docs[0].pk)
            created = Document.objects.create(organization=b, project=Project.objects.get(organization=b),
                                              slug=f"b-offline-{time.time_ns()}", title="while A is down")
            return len(docs) > 0 and doc is not None and Document.objects.filter(pk=created.pk).exists()

        def a_fails():
            try:
                list(Document.objects.filter(organization=a))
                return "query unexpectedly succeeded"
            except DatabaseError as e:
                self.stdout.write("     error: " + str(e).splitlines()[0])
                return True

        # Sent to every organization's database, the rewritten upsert would fail: A's is down.
        def b_bulk_upsert():
            project = Project.objects.get(organization=b)
            slug = f"b-bulk-{time.time_ns()}"
            objs = Document.objects.bulk_create(
                [Document(organization=b, project=project, slug="b-1", title="b-1 upserted offline"),
                 Document(organization=b, project=project, slug=slug, title="bulk while A is down")],
                update_conflicts=True, unique_fields=["organization", "slug"], update_fields=["title"])
            return (Document.objects.get(organization=b, slug="b-1").title == "b-1 upserted offline"
                    and Document.objects.filter(organization=b, slug=slug).exists() and all(o.pk for o in objs))

        self.verify("B: works while A's database is down (list, get, create)", b_works)
        self.verify("B: bulk_create(update_conflicts=True) works while A's database is down (UNNEST arrays name the organization)", b_bulk_upsert)
        self.verify("A: fails with an error instead of falling back to the cloud", a_fails)
