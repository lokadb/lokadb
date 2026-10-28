from urllib.parse import parse_qs
import uuid

from django.db import connection, transaction
from django.http import Http404, JsonResponse
from django.views.decorators.csrf import csrf_exempt

from docs.models import Document, DocumentVersion, Project

# What an ordinary Django page does: the queries are the ORM's, nothing is tuned for LokaDB.


def _body(request):
    return {k: v[0] for k, v in parse_qs(request.body.decode()).items()}


@csrf_exempt
def doc(request, org, id):
    if request.method == "PUT":  # edit a title: read the document, save it, in a transaction
        with transaction.atomic():
            d = Document.objects.filter(organization_id=org, id=id).first()
            if d is None:
                raise Http404()
            d.title = _body(request).get("title", "bench")
            d.save()
        return JsonResponse({"ok": True})
    # a document page: the document, its project, how many versions it has
    d = Document.objects.filter(organization_id=org, id=id).first()
    if d is None:
        raise Http404()
    p = Project.objects.filter(organization_id=org, id=d.project_id).first()
    return JsonResponse({"title": d.title, "project": p.name if p else None, "versions": d.versions.count()})


def feed(request, org):  # the latest documents of the organization with their projects
    docs = Document.objects.select_related("project").filter(organization_id=org).order_by("-id")[:20]
    return JsonResponse([[d.id, d.title, d.project.name] for d in docs], safe=False)


@csrf_exempt
def insert(request, org):  # a new document and its first version in one transaction
    with transaction.atomic():
        p = Project.objects.filter(organization_id=org).first()
        if p is None:
            raise Http404()
        d = Document.objects.create(organization_id=org, project=p, slug=uuid.uuid4().hex[:12],
                                    title=_body(request).get("title", "bench"), body="b")
        DocumentVersion.objects.create(document=d, content="v1")
    return JsonResponse({"id": d.id}, status=201)


def report(request):  # a report over all organizations: names no organization (the fanout policy decides)
    with connection.cursor() as cur:
        cur.execute("SELECT project_id, count(*) AS n FROM docs_document GROUP BY project_id ORDER BY n DESC LIMIT 10")
        return JsonResponse(cur.fetchall(), safe=False)
