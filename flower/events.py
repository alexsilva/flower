import collections
import logging
import time

import gevent.monkey
import rpyc
from celery.events import EventReceiver
from celery.events.state import State
from rpyc.utils import factory
from rpyc.utils.classic import DEFAULT_SERVER_PORT
from rpyc.utils.helpers import classpartial
from rpyc.utils.server import GeventServer

try:
    from collections import Counter
except ImportError:
    from .utils.backports.collections import Counter

logger = logging.getLogger(__name__)


class CeleryStateService(rpyc.Service):

    def __init__(self, state):
        super(CeleryStateService, self).__init__()
        self.state = state

    def exposed_get_state(self):
        return self.state

    def on_connect(self, conn):
        conn._config.update({
            'allow_public_attrs': True,
            'allow_pickle': True,
            'allow_all_attrs': True
        })


class EventsState(State):
    # EventsState object is created and accessed only from ioloop thread

    def __init__(self, *args, **kwargs):
        super(EventsState, self).__init__(*args, **kwargs)
        self.counter = collections.defaultdict(Counter)

    def event(self, event):
        worker_name = event['hostname']
        event_type = event['type']

        self.counter[worker_name][event_type] += 1

        # Send event to api subscribers (via websockets)
        # classname = api.events.getClassName(event_type)
        # cls = getattr(api.events, classname, None)
        # if cls:
        #     cls.send_message(event)

        # Save the event
        return super(EventsState, self).event(event)


class RpcClient:

    def __init__(self, service):
        self.service = service

    def connect(self, host, port=DEFAULT_SERVER_PORT, ipv6=False, keepalive=False):
        """
        Creates a socket connection to the given host and port.

        :param host: the host to connect to
        :param port: the TCP port
        :param ipv6: whether to create an IPv6 socket or IPv4

        :returns: an RPyC connection exposing ``Service``
        """
        return factory.connect(host, port, self.service, ipv6=ipv6, keepalive=keepalive)


class Events:

    rpc_client_connection = None

    def __init__(self, app, options):
        self.state = EventsState()
        self.options = options
        self.app = app
        self.server = None

        self.service = classpartial(CeleryStateService, self.state)
        self.client = RpcClient(self.service)

    def get_remote_state(self, retry=False):
        if self.rpc_client_connection is None or retry:
            self.rpc_client_connection = self.client.connect(self.options.rpc_host,
                                                             port=self.options.rpc_port)
        try:
            return self.rpc_client_connection.root.get_state()
        except EOFError:
            if not retry:
                return self.get_remote_state(retry=True)
            else:
                raise

    def start_rpc(self):
        self.server = GeventServer(self.service,
                                   hostname=self.options.rpc_host,
                                   port=self.options.rpc_port,
                                   auto_register=False,
                                   logger=logger)
        self.server._listen()
        gevent.spawn(self.server.start)
        return self.server

    def __del__(self):
        if self.rpc_client_connection is not None:
            self.rpc_client_connection.close()

    def run(self):
        self.start_rpc()
        try_interval = 1
        while True:
            try:
                try_interval *= 2

                with self.app.connection() as conn:
                    recv = EventReceiver(conn,
                                         handlers={"*": self.on_shutter},
                                         app=self.app)
                    try_interval = 1
                    recv.capture(limit=None, timeout=None, wakeup=True)
            except Exception as e:
                logger.error("Failed to capture events: '%s', "
                             "trying again in %s seconds.",
                             e, try_interval)
                logger.debug(e, exc_info=True)
                time.sleep(try_interval)

    def enable_events(self):
        # Periodically enable events for workers
        # launched after flower
        try:
            self.app.control.enable_events()
        except Exception as e:
            logger.debug("Failed to enable events: '%s'", e)

    def on_shutter(self, event):
        self.state.event(event)

        if not self.state.event_count:
            # No new events since last snapshot.
            return
