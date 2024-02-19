module.exports = {
    redisConfig: {
        port: Number(process.env.REDIS_PORT) || 6379,
    },
    influxDBConfig: {
        url: process.env.INFLUX_DB_URL || 'http://localhost:8086',
        token: process.env.INFLUX_DB_TOKEN || 'YpUl8QwkrtPSttB9Eu26j9d2xXhZ1JifZmrcu3okWuwyR0XRN7NCBMDeK9uKMNylqrZW11myxbUK9gO_ERF2eg==',
        org: process.env.INFLUX_DB_ORG || 'localorg',
        bucket: process.env.INFLUX_DB_BUCKET || 'localbucket',
    },
    networkInterface: process.env.NETWORK_INTERFACE || 'lo0',
};
