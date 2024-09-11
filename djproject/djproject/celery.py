# coding=utf-8
import os

project_name = os.path.basename(os.path.dirname(os.path.abspath(__file__)))

os.environ['DJANGO_SETTINGS_MODULE'] = project_name + '.settings'

from celery import Celery

app = Celery(project_name)

# Using a string here means the worker will not have to
# pickle the object when using Windows.
app.config_from_object('django.conf:settings', namespace='CELERY')


def lazy_installed_apps():
    from django.conf import settings
    return settings.INSTALLED_APPS


app.autodiscover_tasks(lazy_installed_apps)
