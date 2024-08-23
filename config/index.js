module.exports = {
    redisConfig: {
        port: Number(process.env.REDIS_PORT) || 6379,
    },
    influxDBConfig: {
        url: process.env.INFLUX_DB_URL || 'http://localhost:8086',
        token: process.env.INFLUX_DB_TOKEN || 'pN-lNl1SJ1ist5TTxd1vHFpuxZanhil0e8z9hvZfOEYQRa0GNUI-XPghIMkCex2oLvoMAJlRTkFLfGg1TXcTKQ==',
        org: process.env.INFLUX_DB_ORG || 'localorg',
        bucket: process.env.INFLUX_DB_BUCKET || 'localbucket',
    },
    networkInterface: process.env.NETWORK_INTERFACE || 'lo0',
    kafka: {
        topic: 'redis-queries',
        brokers: process.env.KAFKA_BOOTSTRAP_BROKERS ? process.env.KAFKA_BOOTSTRAP_BROKERS.split(',') : ['localhost:9092'],
        ssl: process.env.KAFKA_ACCESS_CERT && process.env.KAFKA_CA_CERT && process.env.KAFKA_ACCESS_KEY ? {
            cert: Buffer.from(process.env.KAFKA_ACCESS_CERT, 'base64').toString('utf8'),
            ca: Buffer.from(process.env.KAFKA_CA_CERT, 'base64').toString('utf8'),                 
            key: Buffer.from(process.env.KAFKA_ACCESS_KEY, 'base64').toString('utf8'),           
        }
        : undefined,
    }
};
