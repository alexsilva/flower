from django.http import Http404

from ..views import BaseHandler


class NotFoundErrorHandler(BaseHandler):
    def get(self, request):
        raise Http404

    def post(self, request):
        raise Http404
