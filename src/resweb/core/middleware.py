from django.conf import settings
from pyres import ResQ

class ResQMiddleware(object):
    def process_request(self, request):
        resq = ResQ(settings.RESQ_HOST)
        request.resq = resq
    def process_response(self,request, response):
        del request.resq
        return response