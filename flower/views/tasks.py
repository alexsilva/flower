import copy
import logging
from functools import total_ordering

from flower.utils import login_required_admin
from django.utils.decorators import method_decorator

from flower.exceptions import HTTPError

try:
    from itertools import imap
except ImportError:
    imap = map

from ..views import BaseHandler
from ..utils.tasks import iter_tasks, get_task_by_id, as_dict

logger = logging.getLogger(__name__)


class TaskView(BaseHandler):

    @method_decorator(login_required_admin)
    def get(self, request, task_id):
        try:
            task = get_task_by_id(self.settings.state, task_id)
        except Exception:
            raise HTTPError(404, "Unknown task '%s'" % task_id)

        return self.render("flower/task.html", context={'task': task})


@total_ordering
class Comparable(object):
    """
    Compare two objects, one or more of which may be None.  If one of the
    values is None, the other will be deemed greater.
    """

    def __init__(self, value):
        self.value = value

    def __eq__(self, other):
        return self.value == other.value

    def __lt__(self, other):
        try:
            return self.value < other.value
        except TypeError:
            return self.value is None


class TasksDataTable(BaseHandler):

    @method_decorator(login_required_admin)
    def get(self, request):
        state = self.settings.state

        draw = self.get_argument('draw', type=int)
        start = self.get_argument('start', type=int)
        length = self.get_argument('length', type=int)
        search = self.get_argument('search[value]', type=str)

        column = self.get_argument('order[0][column]', type=int)
        sort_by = self.get_argument('columns[%s][data]' % column, type=str)
        sort_order = self.get_argument('order[0][dir]', type=str) == 'desc'

        def key(item):
            return Comparable(getattr(item[1], sort_by))

        sorted_tasks = sorted(
            iter_tasks(state, search=search),
            key=key,
            reverse=sort_order
        )

        filtered_tasks = []
        for task in sorted_tasks[start:start + length]:
            task_dict = as_dict(self.format_task(task)[1])
            if task_dict.get('worker'):
                task_dict['worker'] = task_dict['worker'].hostname

            filtered_tasks.append(task_dict)

        return self.write(dict(draw=draw, data=filtered_tasks,
                          recordsTotal=len(sorted_tasks),
                          recordsFiltered=len(sorted_tasks)))

    @method_decorator(login_required_admin)
    def post(self, request):
        return self.get(request)

    def format_task(self, args):
        uuid, task = args
        custom_format_task = self.settings.format_task
        if custom_format_task:
            try:
                task = custom_format_task(copy.copy(task))
            except:
                logger.exception("Failed to format '%s' task", uuid)
        return uuid, task


class TasksView(BaseHandler):

    @method_decorator(login_required_admin)
    def get(self, request, *args, **kwargs):
        settings = self.settings
        app = settings.app
        time = 'natural-time' if settings.natural_time else 'time'
        if app.conf.CELERY_TIMEZONE:
            time += '-' + str(app.conf.CELERY_TIMEZONE)
        # state_config = self.get_argument("state", default=None)
        context = dict(
            tasks=[],
            columns=settings.tasks_columns,
            time=time,
        )
        return self.render("flower/tasks.html", context=context)
