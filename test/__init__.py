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

  def start(self, redisargs=[]):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(('', 0))
    _, port = sock.getsockname()
    self.port = port

    # Let's hope someone doesn't take our port before we try to bind
    # Redis to it
    sock.close()

    rs = os.getenv('REDIS_SERVER', REDIS_SERVER)
    self.server = subprocess.Popen([rs, '--port', str(port), '--loadmodule', TDIGEST_SO] + redisargs)
    self.client = redispy.StrictRedis(port=port)

    # Wait for Redis to start up
    time.sleep(1)

  def stop(self):
    self.client.shutdown()
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

  def flushdb(self):
    self.client.flushdb()

  def reload_from_rdb(self):
    self.client.save()
    self.stop()
    self.start()

  def reload_from_aof(self):
    # if the RDB preamble is enabled, `BGREWRITEAOF` won't actually
    # call `TDigestTypeAofRewrite`
    self.client.config_set('aof-use-rdb-preamble', 'no')
    self.client.bgrewriteaof()
    # Wait for the background rewrite to complete
    time.sleep(2)

    self.stop()
    self.start(redisargs=['--appendonly', 'yes'])

@pytest.fixture
def flushdb(request):
  r = request.module.redis
  request.addfinalizer(r.flushdb)


@pytest.fixture(scope='module')
def redis(request):
  r = Redis()

  def persistence_cleanup():
    def cleanup(filename):
      try:
        os.remove(filename)
      except OSError:
        pass

    map(cleanup, ['dump.rdb', 'appendonly.aof'])

  request.addfinalizer(r.stop)
  request.addfinalizer(persistence_cleanup)

  # Attach it to the module so we can access it with test-scoped fixtures
  request.module.redis = r
  r.start()

  return r
