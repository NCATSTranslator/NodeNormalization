from dataclasses import dataclass, field
import rediscluster
from rediscluster import RedisCluster
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
    is_cluster: bool = True
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
    Abstraction layer for redis interaction.
    Supporting both Clustered, standalone redis backends.
    """
    def __init__(self):
        self.connector = None

    @classmethod
    async def create(cls, redis_instance: RedisInstance):
        """
        Create redis connection.
        """
        # redis_instance contains the password, so this should definitely not be
        # printed except during debugging!
        # print(f"Creating connection to Redis instance {redis_instance} ...")
        self = RedisConnection()
        other_params = {}
        if redis_instance.password:
            other_params['password'] = redis_instance.password
        if redis_instance.ssl_enabled:
            other_params['ssl'] = redis_instance.ssl_enabled

        if redis_instance.is_cluster:
            host: Resource
            hosts = [{"host": host.host_name, "port": host.port} for host in redis_instance.hosts]
            if redis_instance.ssl_enabled:
                other_params['ssl_cert_reqs'] = False

            redis_connector = RedisCluster(startup_nodes=hosts,
                                           decode_responses=True,
                                           skip_full_coverage_check=True,
                                           **other_params)
        else:
            host: Resource = redis_instance.hosts[0]
            redis_connector = await aioredis.create_redis_pool(f'redis://{host.host_name}:{host.port}',
                                                               db=redis_instance.db,
                                                               **other_params)

        self.connector = redis_connector
        return self

    async def mget(self, *keys, encoding='utf-8'):
        """
        Execute mget command.
        """
        if isinstance(self.connector, RedisCluster):
            self.connector: RedisCluster
            return self.connector.mget(keys=keys)
        elif isinstance(self.connector, aioredis.commands.Redis):
            self.connector: aioredis.commands.Redis
            return await self.connector.mget(*keys, encoding=encoding)

    async def get(self, key, encoding='utf-8'):
        """
        Execute redis get command.
        """
        if isinstance(self.connector, RedisCluster):
            self.connector: RedisCluster
            return self.connector.get(name=key)
        elif isinstance(self.connector, aioredis.commands.Redis):
            self.connector: aioredis.commands.Redis
            return await self.connector.get(key, encoding=encoding)

    async def dbsize(self):
        """
        :return: The number of keys in this Redis database.
        """
        if isinstance(self.connector, RedisCluster):
            self.connector: RedisCluster
            return self.connector.dbsize()
        elif isinstance(self.connector, aioredis.commands.Redis):
            self.connector: aioredis.commands.Redis
            return await self.connector.dbsize()

    async def info(self, section):
        """
        :return: The info of this Redis database.
        """
        if isinstance(self.connector, RedisCluster):
            self.connector: RedisCluster
            return self.connector.info(section)
        elif isinstance(self.connector, aioredis.commands.Redis):
            self.connector: aioredis.commands.Redis
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
        if isinstance(self.connector, RedisCluster):
            self.connector: RedisCluster
            if self.connector.connection:
                self.connector.close()
        elif isinstance(self.connector, aioredis.commands.Redis):
            self.connector: aioredis.commands.Redis
            await self.connector.wait_closed()

    async def lrange(self, key, start, stop, encoding='utf-8'):
        """
        Execute Lrange command.
        """
        if isinstance(self.connector, RedisCluster):
            self.connector: RedisCluster
            return self.connector.lrange(name=key, start=start, end=stop)
        elif isinstance(self.connector, aioredis.commands.Redis):
            self.connector:  aioredis.commands.Redis
            return await self.connector.lrange(key=key, start=start, stop=stop, encoding=encoding)

    def pipeline(self):
        return self.connector.pipeline()

    async def keys(self, pattern, encoding="utf-8"):
        """
        Execute keys command
        :param str:
        :return:
        """
        if isinstance(self.connector, RedisCluster):
            self.connector: RedisCluster
            return self.connector.keys(pattern=pattern)
        elif isinstance(self.connector, aioredis.commands.Redis):
            self.connector: aioredis.commands.Redis
            return await self.connector.keys(pattern=pattern, encoding=encoding)
    @staticmethod
    def reset_pipeline(pipeline):
        if isinstance(pipeline, aioredis.commands.transaction.Pipeline):
            pipeline: aioredis.commands.transaction.Pipeline
            pipeline._pipeline = []
        elif isinstance(pipeline, rediscluster.pipeline.ClusterPipeline):
            pipeline: rediscluster.pipeline.ClusterPipeline
            pipeline.reset()

    @staticmethod
    async def execute_pipeline(pipeline):
        if isinstance(pipeline, aioredis.commands.transaction.Pipeline):
            pipeline: aioredis.commands.transaction.Pipeline
            return await pipeline.execute()
        elif isinstance(pipeline, rediscluster.pipeline.ClusterPipeline):
            pipeline: rediscluster.pipeline.ClusterPipeline
            return pipeline.execute()


class RedisConnectionFactory:
    """
    Class to create three redis connections based on config
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
