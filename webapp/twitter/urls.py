from django.urls import re_path

from .views import TwitterCallbackView

urlpatterns = [
    re_path(r'^activity/(?P<env_name>.+)/callback/?$', TwitterCallbackView.as_view())
]
