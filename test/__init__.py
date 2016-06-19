import os
import pytest
import redis as redispy
import socket
import subprocess
import time


DEFAULT_COMPRESSION = 400
REDIS_SERVER = 'redis-server'
TDIGEST_SO = os.path.join(
  os.path.abspath(os.path.join(os.path.join(__file__, os.pardir),
                               os.pardir)),
  'tdigest.so')


class Redis(object):
  def __init__(self):
    self.port = None

  def start(self):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(('', 0))
    _, port = sock.getsockname()
    self.port = port

    # Let's hope someone doesn't take our port before we try to bind
    # Redis to it
    sock.close()

    rs = os.getenv('REDIS-SERVER', REDIS_SERVER)
    self.server = subprocess.Popen([rs, '--port', str(port)])
    self.client = redispy.StrictRedis(port=port)

    # Wait for Redis to start up
    time.sleep(1)

    # Load t-digest module
    self.client.execute_command('MODULE', 'LOAD', TDIGEST_SO)

  def stop(self):
    self.server.terminate()

  def tdigest_new(self, key, compression=None):
    cmd_args = ['TDIGEST.NEW', key]
    if compression:
      cmd_args.append(str(compression))
    return self.client.execute_command(*cmd_args)

  def tdigest_add(self, key, value, count, *args):
    cmd_args = ['TDIGEST.ADD', key, str(value), str(count)]
    cmd_args.extend(map(str, args))
    return self.client.execute_command(*cmd_args)

  def tdigest_merge(self, destkey, sourcekey, *args):
    cmd_args = ['TDIGEST.MERGE', destkey, sourcekey]
    cmd_args.extend(map(str, args))
    return self.client.execute_command(*cmd_args)

  def tdigest_cdf(self, key, value, *args):
    cmd_args = ['TDIGEST.CDF', key, str(value)]
    cmd_args.extend(map(str, args))
    return self.client.execute_command(*cmd_args)

  def tdigest_quantile(self, key, quantile, *args):
    cmd_args = ['TDIGEST.QUANTILE', key, str(quantile)]
    cmd_args.extend(map(str, args))
    return self.client.execute_command(*cmd_args)

  def tdigest_meta(self, key):
    cmd_args = ['TDIGEST.DEBUG', key]
    r = self.client.execute_command(*cmd_args)[0]
    r = map(int, r.split('(')[1].split(')')[0].split(','))
    # [compression, num_centroids, size]
    return r

  def info(self):
    return self.client.info()


@pytest.fixture
def flushdb(request):
  r = request.module.redis
  request.addfinalizer(r.client.flushdb)


@pytest.fixture(scope='module')
def redis(request):
  r = Redis()

  def client_shutdown():
    r.client.shutdown()

  request.addfinalizer(client_shutdown)
  request.addfinalizer(r.stop)

  # Attach it to the module so we can access it with test-scoped fixtures
  request.module.redis = r
  r.start()

  return r
