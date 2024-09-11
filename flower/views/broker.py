import logging
import sys

import kombu.exceptions
from flower.utils import login_required_admin
from django.utils.decorators import method_decorator

from ..api.control import ControlHandler
from ..utils.broker import Broker
from ..views import BaseHandler

logger = logging.getLogger(__name__)


class BrokerView(BaseHandler):

    @method_decorator(login_required_admin)
    def get(self, request):
        app = self.capp
        broker_options = app.conf.BROKER_TRANSPORT_OPTIONS
        http_api = None

        from celery.backends.amqp import AMQPBackend
        if isinstance(app.backend, AMQPBackend) and app.options.broker_api:
            http_api = app.options.broker_api

        try:
            broker = Broker(app.connection().as_uri(include_password=True),
                            http_api=http_api, broker_options=broker_options)
        except NotImplementedError:
            return self.write_error(404, message="'%s' broker is not supported" % app.transport)

        # noinspection PyBroadException
        try:
            queue_names = ControlHandler.get_active_queue_names()
            if not queue_names:
                queue_names = {app.conf.CELERY_DEFAULT_QUEUE} | \
                              set([q.name for q in app.conf.CELERY_QUEUES or [] if q.name])
            queues = list(broker.queues(sorted(queue_names)))

        except kombu.exceptions.OperationalError as e:
            return self.write_error(500, message="Unable to connect to broker",
                                    exc_info=sys.exc_info())
        except Exception:
            return self.write_error(500, message="Unable to get queues",
                                    exc_info=sys.exc_info())

        return self.render("flower/broker.html",
                           context={
                               'broker_url': app.connection().as_uri(),
                               'queues': queues
                           })
