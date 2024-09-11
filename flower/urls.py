try:
    from django.urls import re_path as url, include
except ImportError:
    from django.conf.urls import url, include
from django.views.decorators.cache import never_cache

from .api import control
from .api import tasks
from .api import workers
from .views import monitor
from .views.broker import BrokerView
from .views.dashboard import DashboardView
from .views.error import NotFoundErrorHandler
from .views.tasks import TaskView, TasksView, TasksDataTable
from .views.workers import WorkerView


ns_urlpatterns = ([
    # App
    url(r"^$", never_cache(DashboardView.as_view()), name='main'),
    url(r"^dashboard/$", never_cache(DashboardView.as_view()), name='dashboard'),
    url(r"^worker/(.+)", WorkerView.as_view(), name='worker'),
    url(r"^task/(.+)", TaskView.as_view(), name='task'),
    url(r"^tasks/$", never_cache(TasksView.as_view()), name='tasks'),
    url(r"^tasks/datatable/$", TasksDataTable.as_view()),
    url(r"^broker/$", BrokerView.as_view(), name='broker'),

    # # Worker API
    url(r"^api/workers/$", workers.ListWorkers.as_view()),
    url(r"^api/worker/shutdown/(.+)", control.WorkerShutDown.as_view()),
    url(r"^api/worker/pool/restart/(.+)", control.WorkerPoolRestart.as_view()),
    url(r"^api/worker/pool/grow/(.+)", control.WorkerPoolGrow.as_view()),
    url(r"^api/worker/pool/shrink/(.+)", control.WorkerPoolShrink.as_view()),
    url(r"^api/worker/pool/autoscale/(.+)", control.WorkerPoolAutoscale.as_view()),
    url(r"^api/worker/queue/add-consumer/(.+)", control.WorkerQueueAddConsumer.as_view()),
    url(r"^api/worker/queue/cancel-consumer/(.+)", control.WorkerQueueCancelConsumer.as_view()),
    url(r"^api/worker/queue/purge/(.+)", control.WorkerQueuePurge.as_view()),

    # Task API
    url(r"^api/tasks/$", tasks.ListTasks.as_view()),
    url(r"^api/task/types/$", tasks.ListTaskTypes.as_view()),
    url(r"^api/queues/length/$", tasks.GetQueueLengths.as_view()),
    url(r"^api/task/info/(.*)", tasks.TaskInfo.as_view()),
    url(r"^api/task/apply/(.+)", tasks.TaskApply.as_view()),
    url(r"^api/task/async-apply/(.+)", tasks.TaskAsyncApply.as_view()),
    url(r"^api/task/send-task/(.+)", tasks.TaskSend.as_view()),
    url(r"^api/task/result/(.+)", tasks.TaskResult.as_view()),
    url(r"^api/task/abort/(.+)", tasks.TaskAbort.as_view()),
    url(r"^api/task/timeout/(.+)", control.TaskTimout.as_view()),
    url(r"^api/task/rate-limit/(.+)", control.TaskRateLimit.as_view()),
    url(r"^api/task/revoke/(.+)", control.TaskRevoke.as_view()),
    # Events WebSocket API
    # url(r"api/task/events/task-sent/(.*)", events.TaskSent),
    # url(r"api/task/events/task-received/(.*)", events.TaskReceived),
    # url(r"api/task/events/task-started/(.*)", events.TaskStarted),
    # url(r"api/task/events/task-succeeded/(.*)", events.TaskSucceeded),
    # url(r"api/task/events/task-failed/(.*)", events.TaskFailed),
    # url(r"api/task/events/task-revoked/(.*)", events.TaskRevoked),
    # url(r"api/task/events/task-retried/(.*)", events.TaskRetried),
    # url(r"api/task/events/task-custom/(.*)", events.TaskCustom),
    # # WebSocket Updates
    # url(r"update-dashboard", DashboardUpdateHandler),
    # # Monitors
    url(r"^monitor/$", monitor.Monitor.as_view(), name='monitor'),
    url(r"^monitor/succeeded-tasks/$", monitor.SucceededTaskMonitor.as_view()),
    url(r"^monitor/failed-tasks/$", monitor.FailedTaskMonitor.as_view()),
    url(r"^monitor/completion-time/$", monitor.TimeToCompletionMonitor.as_view()),
    url(r"^monitor/broker/$", monitor.BrokerMonitor.as_view()),
    # Error
    url(r"^.*/$", NotFoundErrorHandler.as_view()),
], 'flower')


urlpatterns = [url("^", include(ns_urlpatterns))]