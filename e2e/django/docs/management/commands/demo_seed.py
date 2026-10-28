from django.core.management.base import BaseCommand

from docs.models import Comment, Document, Organization, Project


class Command(BaseCommand):
    help = "Seed two organizations with documents"

    def handle(self, *args, **options):
        for letter in ["A", "B"]:
            org = Organization.objects.create(name=f"Org {letter}")
            project = Project.objects.create(organization=org, name=f"{letter}-Project")
            for i in range(1, 4):
                doc = Document.objects.create(
                    organization=org, project=project, slug=f"{letter.lower()}-{i}",
                    title=f"{letter} doc {i}", body=f"secret {letter} {i}",
                )
                doc.versions.create(content="v1")
                doc.versions.create(content="v2")
                Comment.objects.create(document=doc, author="alice", body="looks good")
        self.stdout.write("seeded")
