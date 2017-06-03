#!/usr/bin/env python
#coding: utf-8

import os
import sys
import redis
from rediscluster import StrictRedisCluster

PWD = os.path.dirname(os.path.realpath(__file__))
WORKDIR = os.path.join(PWD,'../')
sys.path.append(os.path.join(WORKDIR,'lib/'))
sys.path.append(os.path.join(WORKDIR,'conf/'))

import conf

from server_modules import *
from utils import *

import os
import conf

CLUSTER_NAME = 'ntest'
nc_verbose = int(getenv('T_VERBOSE', 5))
mbuf = int(getenv('T_MBUF', 512))
large = int(getenv('T_LARGE', 1000))

PWD = os.path.dirname(os.path.realpath(__file__))
WORKDIR = os.path.join(PWD,  '../')
redis_trib = os.path.join(WORKDIR, '_binaries/redis-trib.rb')

all_redis = [
        RedisClusterServer('127.0.0.1', 2100, '/tmp/r/redis-2100/', CLUSTER_NAME, 'redis-2100'),
        RedisClusterServer('127.0.0.1', 2101, '/tmp/r/redis-2101/', CLUSTER_NAME, 'redis-2101'),
        RedisClusterServer('127.0.0.1', 2102, '/tmp/r/redis-2102/', CLUSTER_NAME, 'redis-2102'),
        RedisClusterServer('127.0.0.1', 2103, '/tmp/r/redis-2103/', CLUSTER_NAME, 'redis-2103'),
        RedisClusterServer('127.0.0.1', 2104, '/tmp/r/redis-2104/', CLUSTER_NAME, 'redis-2104'),
        RedisClusterServer('127.0.0.1', 2105, '/tmp/r/redis-2105/', CLUSTER_NAME, 'redis-2105'),
        ]
startup_nodes =  [{"host": "127.0.0.1", "port": "2100"}, {"host": "127.0.0.1", "port": "2101"}, {"host": "127.0.0.1", "port": "2102"}, {"host": "127.0.0.1", "port": "2103"}, {"host": "127.0.0.1", "port": "2104"}, {"host": "127.0.0.1", "port": "2105"}]


nc = NutCracker('127.0.0.1', 4100, '/tmp/r/nutcracker-4100', CLUSTER_NAME,
                all_redis, mbuf=mbuf, verbose=nc_verbose)

def setup():
    print 'setup(mbuf=%s, verbose=%s)' %(mbuf, nc_verbose)
    cluster_servers = '' 
    for r in all_redis + [nc]:
        r.clean()
        r.deploy()
        r.stop()
        r.start()
        cluster_servers = cluster_servers + " %s:%d" % (r.host(), r.port()) 
    os.system("ruby " + redis_trib + " create --replicas 1 " + cluster_servers)

    rc = StrictRedisCluster(startup_nodes=startup_nodes)

def teardown():
    for r in all_redis + [nc]:
        assert(r._alive())
        r.stop()

default_kv = {'kkk-%s' % i : 'vvv-%s' % i for i in range(10)}

def getconn():
    for r in all_redis:
        c = redis.Redis(r.host(), r.port())
        c.flushdb()

    r = redis.Redis(nc.host(), nc.port())
    return r

