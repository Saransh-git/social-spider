from rest_framework.response import Response
from rest_framework.views import APIView


class TwitterCallbackView(APIView):

    def get(self, request, *args, **kwargs):
        return Response(data={})

    def post(self, request, *args, **kwargs):
        return Response(data={})
