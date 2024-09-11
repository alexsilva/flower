import threading
import types

from celery import current_app
from django.conf import settings
from rpyc.utils.classic import DEFAULT_SERVER_PORT

from flower.events import Events


class Options:
    ns_local = threading.local()

    def __init__(self, namespace=None):
        self.namespace = namespace.upper()
        self.app = current_app

    def namespace_with(self, name):
        return self.namespace + "_" + name.upper()

    def define(self, name, default=None, **kwargs):
        value = getattr(settings, self.namespace_with(name), default)
        setattr(self, name, value)

    @property
    def state(self):
        if not hasattr(self.ns_local, "events"):
            self.ns_local.events = Events(self.app, self)
        return self.ns_local.events.get_remote_state()


options = Options('flower')

options.define("login_url", default=settings.LOGIN_URL, type=str,
               help="login url (settings.LOGIN_URL)")
options.define("inspect_timeout", default=1000, type=float,
               help="inspect timeout (in milliseconds)")
options.define("rpc_host", default='localhost', type=str,
               help="port used by rpc backend (int)")
options.define("rpc_port", default=DEFAULT_SERVER_PORT, type=int,
               help="port used by rpc backend (int)")
options.define("auth", default='', type=str,
               help="regexp of emails to grant access")
options.define("basic_auth", type=str, default=None, multiple=True,
               help="enable http basic authentication")
options.define("oauth2_key", type=str, default=None,
               help="OAuth2 key (requires --auth)")
options.define("oauth2_secret", type=str, default=None,
               help="OAuth2 secret (requires --auth)")
options.define("debug", default=settings.DEBUG,
               help="run in debug mode", type=bool)
options.define("oauth2_redirect_uri", type=str, default=None,
               help="OAuth2 redirect uri (requires --auth)")
options.define("max_workers", type=int, default=5000,
               help="maximum number of workers to keep in memory")
options.define("max_tasks", type=int, default=10000,
               help="maximum number of tasks to keep in memory")
options.define("db", type=str, default='flower',
               help="flower database file")
options.define("persistent", type=bool, default=False,
               help="enable persistent mode")
options.define("broker_api", type=str, default=None,
               help="inspect broker e.g. http://guest:guest@localhost:15672/api/")
options.define("ca_certs", type=str, default=None,
               help="SSL certificate authority (CA) file")
options.define("certfile", type=str, default=None,
               help="SSL certificate file")
options.define("keyfile", type=str, default=None,
               help="SSL key file")
options.define("xheaders", type=bool, default=False,
               help="enable support for the 'X-Real-Ip' and 'X-Scheme' headers.")
options.define("auto_refresh", default=True,
               help="refresh dashboards", type=bool)
options.define("cookie_secret", type=str, default=None,
               help="secure cookie secret")
options.define("enable_events", type=bool, default=True,
               help="periodically enable Celery events")
options.define("format_task", type=types.FunctionType, default=None,
               help="use custom task formatter")
options.define("natural_time", type=bool, default=False,
               help="show time in relative format")
options.define("tasks_columns", type=str,
               default="name,uuid,state,args,kwargs,result,received,started,runtime,worker",
               help="slugs of columns on /tasks/ page, delimited by comma")
options.define("auth_provider", default='flower.views.auth.GoogleAuth2LoginHandler',
               help="auth handler class")
options.define("inspect", default=False, help="inspect workers", type=bool)
