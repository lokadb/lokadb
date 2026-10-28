from django.http import JsonResponse
from django.urls import path

from . import views

urlpatterns = [
    path("doc/<int:org>/<int:id>", views.doc),
    path("doc/<int:org>", views.insert),
    path("feed/<int:org>", views.feed),
    path("report", views.report),
    path("", lambda r: JsonResponse({"ok": True})),
]
