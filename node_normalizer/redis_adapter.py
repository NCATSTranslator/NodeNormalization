from dataclasses import dataclass, field
import aioredis
from typing import List, Dict


@dataclass
class Resource:
    host_name: str
    port: str = "6379"


@dataclass
class RedisInstance:
    ssl_enabled: bool = False
    password: str = ''
    is_cluster: bool = False
    hosts: List[Resource] = field(default_factory=list)
    host: Resource = None  # Use if is_cluster == False
    db: int = None  # if instance is not a cluster it supports multiple dbs

    def __post_init__(self):
        if len(self.hosts):
            self.hosts = [Resource(**host) if isinstance(host, dict) else host for host in self.hosts]
        if self.host and isinstance(self.host, dict):
            self.host = Resource(**self.host)


class ConnectionConfig:
    def __init__(self, config_dict):
        self.connection_confg = {}
        for k in config_dict:
            self.connection_confg[k] = RedisInstance(**config_dict[k])

    def __getattr__(self, item):
        return self.connection_confg[item]

    def get_connection_names(self):
        return list(self.connection_confg.keys())


class RedisConnection:
    """
    Abstraction layer for a single standalone Redis backend.

    Cluster mode was removed; see documentation/Redis.md for the history and
    what bringing it back would involve.
    """
    def __init__(self):
        self.connector = None

    @classmethod
    async def create(cls, redis_instance: RedisInstance):
        """
        Create redis connection.
        """
        if redis_instance.is_cluster:
            raise ValueError(
                "Redis cluster mode is no longer supported (is_cluster: true). "
                "See documentation/Redis.md for how it was implemented and how to bring it back."
            )

        # redis_instance contains the password, so this should definitely not be
        # printed except during debugging!
        self = RedisConnection()
        other_params = {}
        if redis_instance.password:
            other_params['password'] = redis_instance.password
        if redis_instance.ssl_enabled:
            other_params['ssl'] = redis_instance.ssl_enabled

        host: Resource = redis_instance.hosts[0]
        self.connector = await aioredis.create_redis_pool(f'redis://{host.host_name}:{host.port}',
                                                          db=redis_instance.db,
                                                          **other_params)
        return self

    async def mget(self, *keys, encoding='utf-8'):
        """
        Execute mget command.
        """
        return await self.connector.mget(*keys, encoding=encoding)

    async def get(self, key, encoding='utf-8'):
        """
        Execute redis get command.
        """
        return await self.connector.get(key, encoding=encoding)

    async def dbsize(self):
        """
        :return: The number of keys in this Redis database.
        """
        return await self.connector.dbsize()

    async def info(self, section):
        """
        :return: The info of this Redis database.
        """
        return await self.connector.info(section)

    async def used_memory_rss_human(self):
        """
        :return: The used memory in human units (e.g. 66.71G)
        """
        return (await self.info('memory')).get('memory').get('used_memory_rss_human')

    def close(self):
        """
        Close underlying connection.
        """
        self.connector.close()

    async def wait_closed(self):
        """
        Wait for closed underlying connection.
        """
        await self.connector.wait_closed()

    async def lrange(self, key, start, stop, encoding='utf-8'):
        """
        Execute Lrange command.
        """
        return await self.connector.lrange(key=key, start=start, stop=stop, encoding=encoding)

    def pipeline(self):
        return self.connector.pipeline()

    async def keys(self, pattern, encoding="utf-8"):
        """
        Execute keys command
        """
        return await self.connector.keys(pattern=pattern, encoding=encoding)

    @staticmethod
    async def execute_pipeline(pipeline):
        return await pipeline.execute()


class RedisConnectionFactory:
    """
    Class to create redis connections based on config
    """
    connections: Dict[str, RedisConnection] = {}

    def __init__(self):
        pass

    @staticmethod
    def get_config(file_name) -> ConnectionConfig:
        import yaml
        with open(file_name) as f:
            config = ConnectionConfig(yaml.load(f, yaml.FullLoader))
        return config

    @classmethod
    async def create_connection_pool(cls, config_file_path):
        config = RedisConnectionFactory.get_config(config_file_path)
        self = RedisConnectionFactory()
        if not RedisConnectionFactory.connections:
            RedisConnectionFactory.connections = {
                connection_name: await RedisConnection.create(config.__getattr__(connection_name))
                for connection_name in config.get_connection_names()
            }
        return self

    @staticmethod
    def get_connection(connection_id):
        return RedisConnectionFactory.connections[connection_id]

    @staticmethod
    def get_all_connections():
        return RedisConnectionFactory.connections
