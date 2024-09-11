import collections
import logging
import time
from multiprocessing.pool import ThreadPool

from django.http import Http404, JsonResponse, HttpResponse
from django.utils.decorators import method_decorator

from flower.exceptions import HTTPError
from flower.utils import login_required_admin
from ..views import BaseHandler

logger = logging.getLogger(__name__)


def inspect_method_async(method):
    """Asynchronous results"""
    return method()


class ControlHandler(BaseHandler):
    INSPECT_METHODS = ('stats', 'active_queues', 'registered', 'scheduled',
                       'active', 'reserved', 'revoked', 'conf')
    worker_cache = collections.defaultdict(dict)

    def update_cache(self, workername=None):
        return self.update_workers(
            settings=self.settings,
            workername=workername
        )

    @classmethod
    def update_workers(cls, settings, workername=None):
        logger.debug("Updating %s worker's cache...", workername or 'all')

        app = settings.app
        destination = [workername] if workername else None
        timeout = settings.inspect_timeout / 1000.0
        inspect = app.control.inspect(timeout=timeout,
                                      destination=destination)

        pool = ThreadPool(len(cls.INSPECT_METHODS))
        methods = []
        for method in cls.INSPECT_METHODS:
            methods.append(getattr(inspect, method))

        results = pool.map(inspect_method_async, methods)
        results = dict(zip(cls.INSPECT_METHODS, results))

        for method in results:
            result = results[method]
            if result is None:
                logger.warning("'%s' inspect method failed", method)
                continue
            for worker, response in result.items():
                if response is not None:
                    info = cls.worker_cache[worker]
                    info[method] = response
                    info['timestamp'] = time.time()

        return cls.worker_cache

    def is_worker(self, workername):
        return workername and workername in self.worker_cache

    def error_reason(self, workername, response):
        """extracts error message from response"""
        for r in response:
            try:
                return r[workername].get('error', 'Unknown reason')
            except KeyError:
                pass
        logger.error("Failed to extract error reason from '%s'", response)
        return 'Unknown reason'

    @classmethod
    def get_active_queue_names(cls):
        queues = set([])
        for worker, info in cls.worker_cache.items():
            for q in info.get('active_queues', []):
                queues.add(q['name'])
        return queues


class WorkerQueuePurge(ControlHandler):

    @method_decorator(login_required_admin)
    def post(self, request, queues):
        """
Shut down a worker

**Example request**:

.. sourcecode:: http

  POST /api/worker/queue/purge/queues HTTP/1.1
  Content-Length: 0
  Host: localhost:5555

**Example response**:

.. sourcecode:: http

  HTTP/1.1 200 OK
  Content-Length: 29
  Content-Type: application/json; charset=UTF-8

  {
      "message": "Purge 0 messages from 'queues'"
  }

:reqheader Authorization: optional OAuth token to authenticate
:statuscode 200: no error
:statuscode 401: unauthorized request
:statuscode 404: unknown worker
        """
        logger.info("Purge queues '%s", queues)

        total = self.purge(queues)

        logger.info("Purge total %d messages", total)

        return JsonResponse(dict(message="Purge %d messages from '%s'" % (total, queues)))

    def purge(self, queues):
        """Discard all waiting tasks.

        This will ignore all tasks waiting for execution, and they will
        be deleted from the messaging server.

        Arguments:
            :param queues: queue list

        Returns:
            int: the number of tasks discarded.
        """
        app = self.capp.control.app
        queues = queues.split(',')

        with app.connection_for_write() as conn:
            return sum(self._purge(conn, queue) for queue in queues)

    def _purge(self, conn, queue):
        try:
            return conn.default_channel.queue_purge(queue) or 0
        except conn.channel_errors:
            return 0


class WorkerShutDown(ControlHandler):

    @method_decorator(login_required_admin)
    def post(self, request, workername):
        """
Shut down a worker

**Example request**:

.. sourcecode:: http

  POST /api/worker/shutdown/celery@worker2 HTTP/1.1
  Content-Length: 0
  Host: localhost:5555

**Example response**:

.. sourcecode:: http

  HTTP/1.1 200 OK
  Content-Length: 29
  Content-Type: application/json; charset=UTF-8

  {
      "message": "Shutting down!"
  }

:reqheader Authorization: optional OAuth token to authenticate
:statuscode 200: no error
:statuscode 401: unauthorized request
:statuscode 404: unknown worker
        """
        if not self.is_worker(workername):
            raise Http404("Unknown worker '%s'" % workername)

        logger.info("Shutting down '%s' worker", workername)
        self.capp.control.broadcast('shutdown', destination=[workername])
        return JsonResponse(dict(message="Shutting down!"))


class WorkerPoolRestart(ControlHandler):

    @method_decorator(login_required_admin)
    def post(self, request, workername):
        """
Restart worker's pool

**Example request**:

.. sourcecode:: http

  POST /api/worker/pool/restart/celery@worker2 HTTP/1.1
  Content-Length: 0
  Host: localhost:5555

**Example response**:

.. sourcecode:: http

  HTTP/1.1 200 OK
  Content-Length: 56
  Content-Type: application/json; charset=UTF-8

  {
      "message": "Restarting 'celery@worker2' worker's pool"
  }

:reqheader Authorization: optional OAuth token to authenticate
:statuscode 200: no error
:statuscode 401: unauthorized request
:statuscode 403: pool restart is not enabled (see CELERYD_POOL_RESTARTS)
:statuscode 404: unknown worker
        """
        if not self.is_worker(workername):
            raise Http404("Unknown worker '%s'" % workername)

        logger.info("Restarting '%s' worker's pool", workername)
        response = self.capp.control.broadcast(
            'pool_restart', arguments={'reload': False},
            destination=[workername], reply=True)
        if response and 'ok' in response[0][workername]:
            return JsonResponse(dict(message="Restarting '%s' worker's pool" % workername))
        else:
            logger.error(response)
            return HttpResponse("Failed to restart the '%s' pool: %s" % (
                workername, self.error_reason(workername, response)),
                                status=403)


class WorkerPoolGrow(ControlHandler):

    @method_decorator(login_required_admin)
    def post(self, request, workername):
        """
Grow worker's pool

**Example request**:

.. sourcecode:: http

  POST /api/worker/pool/grow/celery@worker2?n=3 HTTP/1.1
  Content-Length: 0
  Host: localhost:5555

**Example response**:

.. sourcecode:: http

  HTTP/1.1 200 OK
  Content-Length: 58
  Content-Type: application/json; charset=UTF-8

  {
      "message": "Growing 'celery@worker2' worker's pool by 3"
  }

:query n: number of pool processes to grow, default is 1
:reqheader Authorization: optional OAuth token to authenticate
:statuscode 200: no error
:statuscode 401: unauthorized request
:statuscode 403: failed to grow
:statuscode 404: unknown worker
        """

        if not self.is_worker(workername):
            raise Http404("Unknown worker '%s'" % workername)

        n = self.get_argument('n', default=1, type=int)

        logger.info("Growing '%s' worker's pool by '%s'", workername, n)
        response = self.capp.control.pool_grow(
            n=n, reply=True, destination=[workername])
        if response and 'ok' in response[0][workername]:
            return JsonResponse(dict(message="Growing '%s' worker's pool by %s" % (workername, n)))
        else:
            logger.error(response)
            return HttpResponse("Failed to grow '%s' worker's pool: %s" % (
                workername, self.error_reason(workername, response)),
                                status=403)


class WorkerPoolShrink(ControlHandler):

    @method_decorator(login_required_admin)
    def post(self, request, workername):
        """
Shrink worker's pool

**Example request**:

.. sourcecode:: http

  POST /api/worker/pool/shrink/celery@worker2 HTTP/1.1
  Content-Length: 0
  Host: localhost:5555

**Example response**:

.. sourcecode:: http

  HTTP/1.1 200 OK
  Content-Length: 60
  Content-Type: application/json; charset=UTF-8

  {
      "message": "Shrinking 'celery@worker2' worker's pool by 1"
  }

:query n: number of pool processes to shrink, default is 1
:reqheader Authorization: optional OAuth token to authenticate
:statuscode 200: no error
:statuscode 401: unauthorized request
:statuscode 403: failed to shrink
:statuscode 404: unknown worker
        """

        if not self.is_worker(workername):
            raise Http404("Unknown worker '%s'" % workername)

        n = self.get_argument('n', default=1, type=int)

        logger.info("Shrinking '%s' worker's pool by '%s'", workername, n)
        response = self.capp.control.pool_shrink(
            n=n, reply=True, destination=[workername])
        if response and 'ok' in response[0][workername]:
            return JsonResponse(dict(
                message="Shrinking '%s' worker's pool by %s" % (
                    workername, n)))
        else:
            logger.error(response)
            return HttpResponse("Failed to shrink '%s' worker's pool: %s" % (
                workername, self.error_reason(workername, response)
            ), status=403)


class WorkerPoolAutoscale(ControlHandler):

    @method_decorator(login_required_admin)
    def post(self, request, workername):
        """
Autoscale worker pool

**Example request**:

.. sourcecode:: http

  POST /api/worker/pool/autoscale/celery@worker2?min=3&max=10 HTTP/1.1
  Content-Length: 0
  Content-Type: application/x-www-form-urlencoded; charset=utf-8
  Host: localhost:5555

**Example response**:

.. sourcecode:: http

  HTTP/1.1 200 OK
  Content-Length: 66
  Content-Type: application/json; charset=UTF-8

  {
      "message": "Autoscaling 'celery@worker2' worker (min=3, max=10)"
  }

:query min: minimum number of pool processes
:query max: maximum number of pool processes
:reqheader Authorization: optional OAuth token to authenticate
:statuscode 200: no error
:statuscode 401: unauthorized request
:statuscode 403: autoscaling is not enabled (see CELERYD_AUTOSCALER)
:statuscode 404: unknown worker
        """

        if not self.is_worker(workername):
            raise Http404("Unknown worker '%s'" % workername)

        min = self.get_argument('min', type=int)
        max = self.get_argument('max', type=int)

        logger.info("Autoscaling '%s' worker by '%s'",
                    workername, (min, max))
        response = self.capp.control.broadcast(
            'autoscale', arguments={'min': min, 'max': max},
            destination=[workername], reply=True)
        if response and 'ok' in response[0][workername]:
            return JsonResponse(dict(
                message="Autoscaling '%s' worker (min=%s, max=%s)" % (workername, min, max)
            ))
        else:
            logger.error(response)
            return HttpResponse("Failed to autoscale '%s' worker: %s" % (
                workername, self.error_reason(workername, response)
            ), status=403)


class WorkerQueueAddConsumer(ControlHandler):

    @method_decorator(login_required_admin)
    def post(self, request, workername):
        """
Start consuming from a queue

**Example request**:

.. sourcecode:: http

  POST /api/worker/queue/add-consumer/celery@worker2?queue=sample-queue
  Content-Length: 0
  Content-Type: application/x-www-form-urlencoded; charset=utf-8
  Host: localhost:5555

**Example response**:

.. sourcecode:: http

  HTTP/1.1 200 OK
  Content-Length: 40
  Content-Type: application/json; charset=UTF-8

  {
      "message": "add consumer sample-queue"
  }

:query queue: the name of a new queue
:reqheader Authorization: optional OAuth token to authenticate
:statuscode 200: no error
:statuscode 401: unauthorized request
:statuscode 403: failed to add consumer
:statuscode 404: unknown worker
        """
        if not self.is_worker(workername):
            raise Http404("Unknown worker '%s'" % workername)

        queue = self.get_argument('queue')

        logger.info("Adding consumer '%s' to worker '%s'",
                    queue, workername)
        response = self.capp.control.broadcast(
            'add_consumer', arguments={'queue': queue},
            destination=[workername], reply=True)
        if response and 'ok' in response[0][workername]:
            return JsonResponse(dict(message=response[0][workername]['ok']))
        else:
            logger.error(response)
            return HttpResponse("Failed to add '%s' consumer to '%s' worker: %s" % (
                queue, workername, self.error_reason(workername, response)
            ), status=403)


class WorkerQueueCancelConsumer(ControlHandler):

    @method_decorator(login_required_admin)
    def post(self, request, workername):
        """
Stop consuming from a queue

**Example request**:

.. sourcecode:: http

  POST /api/worker/queue/cancel-consumer/celery@worker2?queue=sample-queue
  Content-Length: 0
  Content-Type: application/x-www-form-urlencoded; charset=utf-8
  Host: localhost:5555

**Example response**:

.. sourcecode:: http

  HTTP/1.1 200 OK
  Content-Length: 52
  Content-Type: application/json; charset=UTF-8

  {
      "message": "no longer consuming from sample-queue"
  }

:query queue: the name of queue
:reqheader Authorization: optional OAuth token to authenticate
:statuscode 200: no error
:statuscode 401: unauthorized request
:statuscode 403: failed to cancel consumer
:statuscode 404: unknown worker
        """
        if not self.is_worker(workername):
            raise Http404("Unknown worker '%s'" % workername)

        queue = self.get_argument('queue')

        logger.info("Canceling consumer '%s' from worker '%s'",
                    queue, workername)
        response = self.capp.control.broadcast(
            'cancel_consumer', arguments={'queue': queue},
            destination=[workername], reply=True)
        if response and 'ok' in response[0][workername]:
            return JsonResponse(dict(message=response[0][workername]['ok']))
        else:
            logger.error(response)
            return HttpResponse(
                "Failed to cancel '%s' consumer from '%s' worker: %s" % (
                    queue, workername, self.error_reason(workername, response)
                ), status=403)


class TaskRevoke(ControlHandler):

    @method_decorator(login_required_admin)
    def post(self, request, taskid):
        """
Revoke a task

**Example request**:

.. sourcecode:: http

  POST /api/task/revoke/1480b55c-b8b2-462c-985e-24af3e9158f9?terminate=true
  Content-Length: 0
  Content-Type: application/x-www-form-urlencoded; charset=utf-8
  Host: localhost:5555

**Example response**:

.. sourcecode:: http

  HTTP/1.1 200 OK
  Content-Length: 61
  Content-Type: application/json; charset=UTF-8

  {
      "message": "Revoked '1480b55c-b8b2-462c-985e-24af3e9158f9'"
  }

:query terminate: terminate the task if it is running
:query signal: name of signal to send to process if terminate (default: 'SIGTERM')
:reqheader Authorization: optional OAuth token to authenticate
:statuscode 200: no error
:statuscode 401: unauthorized request
        """
        logger.info("Revoking task '%s'", taskid)
        terminate = self.get_argument('terminate', default=False, type=bool)
        signal = self.get_argument('signal', default='SIGTERM', type=str)
        self.capp.control.revoke(taskid, terminate=terminate, signal=signal)
        return JsonResponse(dict(message="Revoked '%s'" % taskid))


class TaskTimout(ControlHandler):

    @method_decorator(login_required_admin)
    def post(self, request, taskname):
        """
Change soft and hard time limits for a task

**Example request**:

.. sourcecode:: http

    POST /api/task/timeout/tasks.sleep HTTP/1.1
    Content-Length: 44
    Content-Type: application/x-www-form-urlencoded; charset=utf-8
    Host: localhost:5555

    soft=30&hard=100&workername=celery%40worker1

**Example response**:

.. sourcecode:: http

    HTTP/1.1 200 OK
    Content-Length: 46
    Content-Type: application/json; charset=UTF-8

    {
        "message": "time limits set successfully"
    }

:query workername: worker name
:reqheader Authorization: optional OAuth token to authenticate
:statuscode 200: no error
:statuscode 401: unauthorized request
:statuscode 404: unknown task/worker
        """
        workername = self.get_argument('workername')
        hard = self.get_argument('hard', default=None, type=float)
        soft = self.get_argument('soft', default=None, type=float)

        if taskname not in self.capp.tasks:
            raise Http404("Unknown task '%s'" % taskname)
        if workername is not None and not self.is_worker(workername):
            raise Http404("Unknown worker '%s'" % workername)

        logger.info("Setting timeouts for '%s' task (%s, %s)",
                    taskname, soft, hard)
        destination = [workername] if workername is not None else None
        response = self.capp.control.time_limit(
            taskname, reply=True, hard=hard, soft=soft,
            destination=destination)

        if response and 'ok' in response[0][workername]:
            return JsonResponse(dict(message=response[0][workername]['ok']))
        else:
            logger.error(response)
            return HttpResponse("Failed to set timeouts: '%s'" %
                                self.error_reason(taskname, response), status=403)


class TaskRateLimit(ControlHandler):

    @method_decorator(login_required_admin)
    def post(self, request, taskname):
        """
Change rate limit for a task

**Example request**:

.. sourcecode:: http

    POST /api/task/rate-limit/tasks.sleep HTTP/1.1
    Content-Length: 41
    Content-Type: application/x-www-form-urlencoded; charset=utf-8
    Host: localhost:5555

    ratelimit=200&workername=celery%40worker1

**Example response**:

.. sourcecode:: http

  HTTP/1.1 200 OK
  Content-Length: 61
  Content-Type: application/json; charset=UTF-8

  {
      "message": "new rate limit set successfully"
  }

:query workername: worker name
:reqheader Authorization: optional OAuth token to authenticate
:statuscode 200: no error
:statuscode 401: unauthorized request
:statuscode 404: unknown task/worker
        """
        workername = self.get_argument('workername')
        ratelimit = self.get_argument('ratelimit')

        if taskname not in self.capp.tasks:
            raise HTTPError(404, "Unknown task '%s'" % taskname)
        if workername is not None and not self.is_worker(workername):
            raise HTTPError(404, "Unknown worker '%s'" % workername)

        logger.info("Setting '%s' rate limit for '%s' task", ratelimit, taskname)
        destination = [workername] if workername is not None else None
        response = self.capp.control.rate_limit(
            taskname, ratelimit, reply=True, destination=destination)
        if response and 'ok' in response[0][workername]:
            response = self.write(dict(message=response[0][workername]['ok']))
        else:
            logger.error(response)
            response = HttpResponse("Failed to set rate limit: '%s'" % self.error_reason(taskname, response))
        return response
