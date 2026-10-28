from django.conf import settings
from django.db import models


class Organization(models.Model):
    name = models.CharField(max_length=100)


class Project(models.Model):
    organization = models.ForeignKey(Organization, on_delete=models.CASCADE)
    name = models.CharField(max_length=100)


class Document(models.Model):
    organization = models.ForeignKey(Organization, on_delete=models.CASCADE)
    project = models.ForeignKey(Project, on_delete=models.CASCADE)
    slug = models.CharField(max_length=100)
    title = models.CharField(max_length=200)
    body = models.TextField(blank=True, default="")
    if settings.APP_RELEASE >= 2:
        status = models.CharField(max_length=20, default="draft")

    class Meta:
        constraints = [models.UniqueConstraint(fields=["organization", "slug"], name="doc_org_slug_uniq")]


class DocumentVersion(models.Model):
    document = models.ForeignKey(Document, on_delete=models.CASCADE, related_name="versions")
    content = models.TextField()


class Comment(models.Model):
    document = models.ForeignKey(Document, on_delete=models.CASCADE, related_name="comments")
    author = models.CharField(max_length=100)
    body = models.TextField()
