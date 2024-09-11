import base64
import os.path
import uuid

from django.contrib.auth.decorators import user_passes_test
from flower.options import options

from .. import __version__


def gen_cookie_secret():
    return base64.b64encode(uuid.uuid4().bytes + uuid.uuid4().bytes)


def bugreport(app=None):
    try:
        import celery
        import tornado
        import humanize

        app = app or celery.Celery()

        return 'flower   -> flower:%s tornado:%s humanize:%s%s' % (
            __version__,
            tornado.version,
            humanize.VERSION,
            app.bugreport()
        )
    except (ImportError, AttributeError):
        return 'Unknown Celery version'


def abs_path(path):
    path = os.path.expanduser(path)
    if not os.path.isabs(path):
        cwd = os.environ.get('PWD') or os.getcwd()
        path = os.path.join(cwd, path)
    return path


def prepend_url(url, prefix):
    return '/' + prefix.strip('/') + url


def login_required_admin(func_view):
    """Allows only administrator login"""
    return user_passes_test(lambda u: u.is_superuser,
                            login_url=options.login_url)(func_view)
