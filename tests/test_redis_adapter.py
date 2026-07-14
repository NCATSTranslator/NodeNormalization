import pytest, yaml
from node_normalizer.redis_adapter import ConnectionConfig, RedisInstance, RedisConnectionFactory, \
    RedisConnection, Resource
from pathlib import Path
from unittest.mock import patch

REDIS_CONF_PATH = Path(__file__).parent / 'resources' / 'redis-config.yaml'

@pytest.fixture
def config_dict():
    with open(REDIS_CONF_PATH) as stream:
        return yaml.safe_load(stream=stream)


def test_connection_config_is_dynamic(config_dict):
    connection = ConnectionConfig(config_dict)
    for key in config_dict:
        assert connection.__getattr__(key)
        assert isinstance(connection.__getattr__(key), RedisInstance)
    config_dict.pop('gene_protein_db')
    connection = ConnectionConfig(config_dict)
    for key in config_dict:
        assert connection.__getattr__(key)
        assert isinstance(connection.__getattr__(key), RedisInstance)
    try:
        connection.__getattr__('gene_protein_db')
    except KeyError:
        error_raise = True

    assert error_raise


def test_redis_instance_attributes(config_dict):
    connection = ConnectionConfig(config_dict)
    clusterd_db_name = "id_to_type_db"
    ssl_db_name = "gene_protein_db"
    ssl_and_cluster_db_name = "curie_to_bl_type_db"
    config_instance: RedisInstance = connection.__getattr__(clusterd_db_name)
    assert isinstance(config_instance, RedisInstance)
    assert config_instance.is_cluster
    assert not config_instance.ssl_enabled

    config_instance: RedisInstance = connection.__getattr__(ssl_db_name)
    assert isinstance(config_instance, RedisInstance)
    assert not config_instance.is_cluster
    assert config_instance.ssl_enabled

    config_instance: RedisInstance = connection.__getattr__(ssl_and_cluster_db_name)
    assert isinstance(config_instance, RedisInstance)
    assert config_instance.is_cluster
    assert config_instance.ssl_enabled



class RedisConnectionMock:
    instances_list = []
    def __init__(self):
        self.connector = None

    @classmethod
    async def create(cls, redis_instance: RedisInstance):
        self = RedisConnectionMock()
        RedisConnectionMock.instances_list.append(self)
        return self


@pytest.mark.asyncio
async def test_redis_connectors_created(config_dict):
    with patch('node_normalizer.redis_adapter.RedisConnection',RedisConnectionMock):
        redis_factory: RedisConnectionFactory = await RedisConnectionFactory.create_connection_pool(REDIS_CONF_PATH)
        connections = redis_factory.get_all_connections()
        config = redis_factory.get_config(REDIS_CONF_PATH)
        # here we are checking if configs are able to intialize the redisConncetion class with a RedisInstance class
        for key in config.connection_confg:
            assert RedisConnectionFactory.get_connection(key) in RedisConnectionMock.instances_list


class aioredisMock:

    def __init__(self, ):
        self.k_v = {
            "someKey": "someValue"
        }

    async def get(self, key, encoding):
        assert encoding == 'utf-8'
        return self.k_v.get(key)

    async def pipeline(self):
        return "pipeline"

    async def mget(self, *keys, encoding):
        assert encoding == 'utf-8'
        response = {}
        for k in keys:
            response[k] = self.k_v.get(k)
        return response

    async def lrange(self, key=0, start=0, stop=0, encoding='utf-8'):
        assert encoding == 'utf-8'
        return "called"

    def close(self):
        self.closed = True

    async def wait_closed(self):
        self.close()


    def __instancecheck__(self, *args, **kwargs):
        return True


@pytest.mark.asyncio
async def test_redis_aioredis_connection():

    mocked_connection = aioredisMock()
    redis_instance: RedisInstance = RedisInstance(ssl_enabled=True,
                  password='somepassword',
                  is_cluster=False,
                  db=0,
                  hosts=[Resource(host_name="127.0.0.1", port=6379)])
    async def create_redis_mock(*args, **kwargs):

        assert kwargs['password'] == redis_instance.password
        assert kwargs['db'] == redis_instance.db
        host = redis_instance.hosts[0].host_name
        port = redis_instance.hosts[0].port
        assert args == (f"redis://{host}:{port}",)
        assert kwargs['ssl'] == redis_instance.ssl_enabled
        return mocked_connection

    with patch('aioredis.create_redis_pool', create_redis_mock):


        with patch("aioredis.commands.Redis", mocked_connection):
            connection = await RedisConnection.create(redis_instance)
            assert connection.connector == mocked_connection
            # Test redis.get()
            result = await connection.get("someKey")
            assert result == mocked_connection.k_v.get("someKey")
            # test pipeline called
            assert await mocked_connection.pipeline() == await connection.pipeline()
            # test mget
            keys = ["someKey", "anotherKey"]
            assert await mocked_connection.mget(*keys, encoding='utf-8') == await connection.mget(*keys)
            # test lrange
            kwargs = {
                "key": 2,
                "start": 1,
                "stop": 2,
                "encoding": 'utf-8'
            }
            assert await mocked_connection.lrange(**kwargs) == await connection.lrange(**kwargs)

            # test close
            connection.close()
            assert mocked_connection.closed

            mocked_connection.closed = False

            # assert wait closed
            await connection.wait_closed()
            assert mocked_connection.closed == True


@pytest.mark.asyncio
async def test_redis_cluster_rejected():
    """is_cluster: true is no longer supported and must raise."""
    redis_instance: RedisInstance = RedisInstance(is_cluster=True,
                                                  db=0,
                                                  hosts=[Resource(host_name="127.0.0.1", port=6379)])
    with pytest.raises(ValueError):
        await RedisConnection.create(redis_instance)
