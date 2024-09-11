import sys
import json
import socket
import logging
import numbers

import requests

try:
    from urllib.parse import urlparse, urljoin, quote, unquote
except ImportError:
    from urlparse import urlparse, urljoin
    from urllib import quote, unquote


try:
    import redis
except ImportError:
    redis = None


logger = logging.getLogger(__name__)


class BrokerBase:
    def __init__(self, broker_url, *args, **kwargs):
        purl = urlparse(broker_url)
        self.host = purl.hostname
        self.port = purl.port
        self.vhost = purl.path[1:]

        username = purl.username
        password = purl.password

        self.username = unquote(username) if username else username
        self.password = unquote(password) if password else password

    def queues(self, names):
        raise NotImplementedError


class RabbitMQ(BrokerBase):
    def __init__(self, broker_url, http_api, io_loop=None, **kwargs):
        super(RabbitMQ, self).__init__(broker_url)

        self.host = self.host or 'localhost'
        self.port = self.port or 15672
        self.vhost = quote(self.vhost, '') or '/'
        self.username = self.username or 'guest'
        self.password = self.password or 'guest'

        if not http_api:
            http_api = "http://{username}:{password}@{host}:{port}/api/{vhost}".format(
                username=self.username, password=self.password,
                host=self.host, port=self.port, vhost=self.vhost
            )

        try:
            self.validate_http_api(http_api)
        except Exception as e:
            logger.error("Invalid broker api url:%s", http_api)

        self.http_api = http_api

    def queues(self, names):
        url = urljoin(self.http_api, 'queues/' + self.vhost)
        api_url = urlparse(self.http_api)
        username = unquote(api_url.username or '') or self.username
        password = unquote(api_url.password or '') or self.password

        try:
            response = requests.get(
                url,
                auth_username=username,
                auth_password=password,
                validate_cert=False
            )
        except (socket.error, requests.HTTPError) as e:
            logger.error("RabbitMQ management API call failed: %s", e)
            raise

        if response.status_code == 200:
            info = response.json()
            return [x for x in info if x['name'] in names]
        else:
            raise Exception('Status({0.status_code})'.format(response))

    @classmethod
    def validate_http_api(cls, http_api):
        url = urlparse(http_api)
        if url.scheme not in ('http', 'https'):
            raise ValueError("Invalid http api schema: %s" % url.scheme)


class RedisBase(BrokerBase):
    SEP = '\x06\x16'
    DEFAULT_PRIORITY_STEPS = [0, 3, 6, 9]

    def __init__(self, broker_url, *args, **kwargs):
        super(RedisBase, self).__init__(broker_url)

        if not redis:
            raise ImportError('redis library is required')

        broker_options = kwargs.get('broker_options')

        if broker_options and 'priority_steps' in broker_options:
            self.priority_steps = broker_options['priority_steps']
        else:
            self.priority_steps = self.DEFAULT_PRIORITY_STEPS

    def _q_for_pri(self, queue, pri):
        if pri not in self.priority_steps:
            raise ValueError('Priority not in priority steps')
        return '{0}{1}{2}'.format(*((queue, self.SEP, pri) if pri else (queue, '', '')))

    def queues(self, names):
        queue_stats = []
        for name in names:
            priority_names = [self._q_for_pri(name, pri) for pri in self.priority_steps]
            queue_stats.append({
                'name': name,
                'messages': sum([self.redis.llen(x) for x in priority_names])
            })
        return queue_stats


class Redis(RedisBase):

    def __init__(self, broker_url, *args, **kwargs):
        super(Redis, self).__init__(broker_url)
        self.host = self.host or 'localhost'
        self.port = self.port or 6379
        self.vhost = self._prepare_virtual_host(self.vhost)

        self.redis = redis.Redis(host=self.host, port=self.port,
                                 db=self.vhost, password=self.password)

    def _prepare_virtual_host(self, vhost):
        if not isinstance(vhost, numbers.Integral):
            if not vhost or vhost == '/':
                vhost = 0
            elif vhost.startswith('/'):
                vhost = vhost[1:]
            try:
                vhost = int(vhost)
            except ValueError:
                raise ValueError(
                    'Database is int between 0 and limit - 1, not {0}'.format(
                        vhost,
                    ))
        return vhost


class RedisSocket(RedisBase):

    def __init__(self, broker_url, *args, **kwargs):
        super(RedisSocket, self).__init__(broker_url)
        self.redis = redis.Redis(unix_socket_path='/' + self.vhost,
                                 password=self.password)


class Broker:
    def __new__(cls, broker_url, *args, **kwargs):
        scheme = urlparse(broker_url).scheme
        if scheme == 'amqp':
            return RabbitMQ(broker_url, *args, **kwargs)
        elif scheme == 'redis':
            return Redis(broker_url, *args, **kwargs)
        elif scheme == 'redis+socket':
            return RedisSocket(broker_url, *args, **kwargs)
        else:
            raise NotImplementedError

