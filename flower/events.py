import collections
import logging
import time

import rpyc
from celery.events import EventReceiver
from celery.events.state import State
from django.utils.functional import cached_property
from rpyc.utils import factory
from rpyc.utils.classic import DEFAULT_SERVER_PORT
from rpyc.utils.helpers import classpartial
from rpyc.utils.server import ThreadedServer
from collections import Counter
import threading

logger = logging.getLogger(__name__)


class CeleryStateService(rpyc.Service):

    def __init__(self, state):
        super(CeleryStateService, self).__init__()
        self.state = state

    def exposed_get_state(self):
        return self.state


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


class Events(threading.Thread):

    def __init__(self, app, options):
        super().__init__()

        self.state = EventsState()
        self.options = options
        self.app = app

        self.service = classpartial(CeleryStateService, self.state)
        self.client = RpcClient(self.service)

    def _get_connection(self):
        """Client connection to rpc server"""
        return self.client.connect(self.options.rpc_host, port=self.options.rpc_port)

    def get_remote_state(self, retry=False):
        """Connects to the server started by 'start_server'"""
        conn = self._get_connection()
        try:
            return conn.root.get_state()
        except EOFError:
            if not retry:
                conn.close()
                return self.get_remote_state(retry=True)
            else:
                raise

    def _get_server(self):
        """Starts the rpc server that exposes the 'state' object"""
        server = ThreadedServer(
            self.service,
            hostname=self.options.rpc_host,
            port=self.options.rpc_port,
            auto_register=False,
            logger=logger,
            protocol_config={
                'allow_public_attrs': True,
                'allow_pickle': True,
                'allow_all_attrs': True
            }
        )
        return server

    @cached_property
    def server(self):
        """Starts the rpc server that exposes the 'state' object"""
        return self._get_server()

    def start(self):
        try:
            self.enable_events()
        except Exception as e:
            logger.debug("Failed to enable events: '%s'", e)
        # starts the events thread
        super().start()
        # start the rpc server
        self.server.start()

    def run(self):
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
        # Periodically enable events for workers launched after flower
        try:
            self.app.control.enable_events()
        except Exception as e:
            logger.debug("Failed to enable events: '%s'", e)

    def on_shutter(self, event):
        self.state.event(event)

        if not self.state.event_count:
            # No new events since last snapshot.
            return
