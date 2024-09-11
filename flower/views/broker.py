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
        broker_options = self.capp.conf.BROKER_TRANSPORT_OPTIONS
        try:
            http_api = self.settings.broker_api
        except AttributeError:
            http_api = None
        try:
            broker = Broker(self.capp.connection().as_uri(include_password=True),
                            http_api=http_api, broker_options=broker_options)
        except NotImplementedError:
            return self.write_error(404, message="'%s' broker is not supported" % self.capp.transport)

        # noinspection PyBroadException
        try:
            queue_names = ControlHandler.get_active_queue_names()
            if not queue_names:
                queue_names = {self.capp.conf.CELERY_DEFAULT_QUEUE} | \
                              set([q.name for q in self.capp.conf.CELERY_QUEUES or [] if q.name])
            queues = list(broker.queues(sorted(queue_names)))

        except kombu.exceptions.OperationalError as exc:
            return self.write_error(500, message="Unable to connect to broker",
                                    exc_info=sys.exc_info())
        except Exception:
            return self.write_error(500, message="Unable to get queues",
                                    exc_info=sys.exc_info())

        return self.render("flower/broker.html",
                           context={
                               'broker_url': self.capp.connection().as_uri(),
                               'queues': queues
                           })
