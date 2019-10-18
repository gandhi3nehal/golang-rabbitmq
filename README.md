# siddhi-compose

deployment of cassandra trigger

## services

three main servies now

```
1. elassandra
2. zookeeper
3. kafka
```

## configure .env

change `host.docker.local` field in `.env` file to local machines ip. Also
its possible to add a host entry to `/etc/hosts` file by overriding
`host.docker.local` with local machines ip.

```
#/etc/hosts file
10.4.1.104    host.docker.local
```

### deploy services

start services in following order

```
docker-compose up -d zookeeper
docker-compose up -d kafka
docker-compose up -d elassandra
```
