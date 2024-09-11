# coding=utf-8

import gevent.monkey

gevent.monkey.patch_all()

from django.core.management import BaseCommand

from flower.events import Events, logger
from flower.options import options as settings


class Command(BaseCommand):
    # db can not be used
    leave_locale_alone = True
    requires_system_checks = ()

    def handle(self, *args, **options):
        events = Events(settings.app, settings)
        try:
            settings.app.control.enable_events()
        except Exception as e:
            logger.debug("Failed to enable events: '%s'", e)
        events.run()
