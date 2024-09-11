import logging
import socket
import sys
from collections import OrderedDict
from functools import partial

from django.utils.decorators import method_decorator
from django.views.generic import View

from flower.utils import login_required_admin
from ..api.workers import ListWorkers
from ..views import BaseHandler

logger = logging.getLogger(__name__)


class DashboardView(BaseHandler):

    @method_decorator(login_required_admin)
    def get(self, request, *args, **kwargs):
        refresh = self.get_argument('refresh', default=False, type=bool)
        status = self.get_argument('status', default=False, type=bool)
        json = self.get_argument('json', default=False, type=bool)
        app = self.capp

        # When rpc fails to connect (check the flower_events command is running).
        try:
            events = self.settings.state
        except socket.error:
            return self.write_error(500, message='RPC connection failed',
                                    exc_info=sys.exc_info())

        if refresh:
            try:
                ListWorkers.update_workers(settings=self.settings)
            except Exception as e:
                logger.exception('Failed to update workers: %s', e)
            if status:  # update workers only!
                return self.write('ok')

        workers = {}
        for name, values in events.counter.items():
            if name not in events.workers:
                continue
            worker = events.workers[name]
            info = dict(values)
            info.update(self._as_dict(worker))
            info.update(status=worker.alive)
            workers[name] = info

        if json:
            return self.write(dict(data=list(workers.values())))
        else:
            broker = app.connection().as_uri()

            def lazy_alive_workers():
                return sum(map(lambda x: x.get('active') or 0, workers.values()))

            def lazy_task_received():
                return sum(map(lambda x: x.get('task-received') or 0, workers.values()))

            def lazy_task_failed():
                return sum(map(lambda x: x.get('task-failed') or 0, workers.values()))

            def lazy_task_succeeded():
                return sum(map(lambda x: x.get('task-succeeded') or 0, workers.values()))

            def lazy_task_retried():
                return sum(map(lambda x: x.get('task-retried') or 0, workers.values()))

            context = dict(
                alive_workers=lazy_alive_workers,
                task_received=lazy_task_received,
                task_failed=lazy_task_failed,
                task_succeeded=lazy_task_succeeded,
                task_retried=lazy_task_retried,
                broker=broker,
                workers=workers
            )
            return self.render("flower/dashboard.html", context)

    @classmethod
    def _as_dict(cls, worker):
        if hasattr(worker, '_fields'):
            return dict((k, worker.__getattribute__(k)) for k in worker._fields)
        else:
            return cls._info(worker)

    @classmethod
    def _info(cls, worker):
        _fields = ('hostname', 'pid', 'freq', 'heartbeats', 'clock',
                   'active', 'processed', 'loadavg', 'sw_ident',
                   'sw_ver', 'sw_sys')

        def _keys():
            for key in _fields:
                value = getattr(worker, key, None)
                if value is not None:
                    yield key, value

        return dict(_keys())


class DashboardUpdateHandler(View):
    listeners = []
    periodic_callback = None
    workers = None
    page_update_interval = 2000

    def open(self):
        app = self.app_options
        if not app.auto_refresh:
            self.write_message({})
            return

        if not self.listeners:
            if self.periodic_callback is None:
                cls = DashboardUpdateHandler
                cls.periodic_callback = PeriodicCallback(
                    partial(cls.on_update_time, app),
                    self.page_update_interval)
            if not self.periodic_callback._running:
                logger.debug('Starting a timer for dashboard updates')
                self.periodic_callback.start()
        self.listeners.append(self)

    def on_message(self, message):
        pass

    def on_close(self):
        if self in self.listeners:
            self.listeners.remove(self)
        if not self.listeners and self.periodic_callback:
            logger.debug('Stopping dashboard updates timer')
            self.periodic_callback.stop()

    @classmethod
    def on_update_time(cls, app):
        update = cls.dashboard_update(app)
        if update:
            for l in cls.listeners:
                l.write_message(update)

    @classmethod
    def dashboard_update(cls, app):
        state = app.events.state
        workers = OrderedDict()

        for name, worker in sorted(state.workers.items()):
            counter = state.counter[name]
            started = counter.get('task-started', 0)
            processed = counter.get('task-received', 0)
            failed = counter.get('task-failed', 0)
            succeeded = counter.get('task-succeeded', 0)
            retried = counter.get('task-retried', 0)
            active = started - succeeded - failed - retried
            if active < 0:
                active = 'N/A'

            workers[name] = dict(
                name=name,
                status=worker.alive,
                active=active,
                processed=processed,
                failed=failed,
                succeeded=succeeded,
                retried=retried,
                loadavg=getattr(worker, 'loadavg', None))
        return workers

    def check_origin(self, origin):
        return True
