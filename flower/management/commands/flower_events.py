# coding=utf-8
from django.core.management import BaseCommand

from flower.events import Events
from flower.options import options as settings


class Command(BaseCommand):
    # db can not be used
    leave_locale_alone = True
    requires_system_checks = ()

    def handle(self, *args, **options):
        events = Events(settings.app, settings)
        # rpc server
        events.start()
