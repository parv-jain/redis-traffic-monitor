const { InfluxDB, Point } = require('@influxdata/influxdb-client');
const Influx = require('influx');
const { Kafka, PartitionAssigners, logLevel } = require('kafkajs')
const config = require('../config');
const { hostname } = require('node:os');

class MetricsConsumer {
    constructor({ logger }) {
        this.logger = logger;
        this.influxDBConfig = config.influxDBConfig;
        if (this.influxDBConfig.version === '2.x') {
            this.influxDB = new InfluxDB({ url: this.influxDBConfig.url, token: this.influxDBConfig.token });
        } else {
            this.influxDB = new Influx.InfluxDB({
                host: this.influxDBConfig['1.x'].host,
                port: this.influxDBConfig['1.x'].port,
                protocol: this.influxDBConfig['1.x'].protocol,
                database: this.influxDBConfig['1.x'].database,
                username: this.influxDBConfig['1.x'].username,
                password: this.influxDBConfig['1.x'].password,
            });
        }
        this.kafka = new Kafka({
            logLevel: logLevel.WARN,
            brokers: config.kafka.brokers,
            ssl: config.kafka.ssl,
            clientId: 'redis-metrics-consumer',
        });
        this.consumer = this.kafka.consumer({
            groupId: 'redis-metrics-consumer-group',
            partitionAssigners: [PartitionAssigners.roundRobin],
            sessionTimeout: 180000, // Timeout in milliseconds used to detect failures. The consumer sends periodic heartbeats to indicate its liveness to the broker. If no heartbeats are received by the broker before the expiration of this session timeout, then the broker will remove this consumer from the group and initiate a rebalance
            rebalanceTimeout: 60000, // The maximum time that the coordinator will wait for each member to rejoin when rebalancing the group
            heartbeatInterval: 3000, // The expected time in milliseconds between heartbeats to the consumer coordinator. Heartbeats are used to ensure that the consumer's session stays active. The value must be set lower than session timeout
            metadataMaxAge: 300000, // The period of time in milliseconds after which we force a refresh of metadata even if we haven't seen any partition leadership changes to proactively discover any new brokers or partitions
            allowAutoTopicCreation: true, // Allow topic creation when querying metadata for non-existent topics
            maxBytesPerPartition: 52429, // The maximum amount of data per-partition the server will return. This size must be at least as large as the maximum message size the server allows or else it is possible for the producer to send messages larger than the consumer can fetch. If that happens, the consumer can get stuck trying to fetch a large message on a certain partition
            minBytes: 1, // Minimum amount of data the server should return for a fetch request, otherwise wait up to maxWaitTimeInMs for more data to accumulate
            maxBytes: 524288, // Maximum amount of bytes to accumulate in the response.
            maxWaitTimeInMs: 30000, // The maximum amount of time in milliseconds the server will block before answering the fetch request if there isn’t sufficient data to immediately satisfy the requirement given by minBytes
            retry: {
                initialRetryTime: 300,
                retries: Number.MAX_SAFE_INTEGER,
                maxRetryTime: 30000,
                factor: 0.2,
                multiplier: 2,            
            },
            readUncommitted: false, // Configures the consumer isolation level. If false (default), the consumer will not return any transactional messages which were not committed
            maxInFlightRequests: undefined, // Max number of requests that may be in progress at any time. If falsey then no limit
            rackId: undefined, // Configure the "rack" in which the consumer resides to enable follower fetching. If falsey then fetch always from leader        
        });
    }

    async start() {
        await this.consumer.connect();
        await this.consumer.subscribe({ topics: [config.kafka.topic] });
        await this.consumer.run({
            autoCommit: true,
            partitionsConsumedConcurrently: 1,
            eachBatchAutoResolve: true,
            eachBatch: this.batchHandler.bind(this),
        });
    }

    async batchHandler({
        batch,
    }) {
        const metrics = [];
        for (let message of batch.messages) {
            if (message.value) {
                const parsedValue = JSON.parse(message.value);
                metrics.push(parsedValue);
            }
        }
        if (this.influxDBConfig.version === '2.x') {
            const writeApi = this.influxDB.getWriteApi(this.influxDBConfig.org, this.influxDBConfig.bucket)
            writeApi.useDefaultTags({location: hostname()});
            const dataPoints = [];
            metrics.forEach((metric) => {
                const fields = {
                    request: JSON.stringify(metric.request),
                    start_time: metric.start_time,
                    duration_in_ns: metric.duration_in_ns,
                    size_in_bytes: metric.size_in_bytes,
                };
                const point = new Point('redis_queries')
                    .tag('command', metric.command)
                    .tag('operation', JSON.stringify(metric.operation))
                    .tag('type', metric.type)
                    .tag('sender', metric.sender)
                    .tag('receiver', metric.receiver);
                point.fields = fields;
                dataPoints.push(point);
            });
            writeApi.writePoints(dataPoints);
            return writeApi.close()
                .then(async () => {
                    // this.logger.info({ metrics }, 'Metrics written to influxdb');
                    // Introduce an artificial delay to slow down processing
                    // await new Promise(resolve => setTimeout(resolve, 30000)); // 30s delay
                })
                .catch((err) => {
                    this.logger.error({ err }, 'Error in influx db write api write points')
                });
        } else {
            const points = metrics.map((metric) => {
                return {
                    measurement: 'redis_queries',
                    tags: {
                        location: hostname(),
                        command: metric.command,
                        operation: JSON.stringify(metric.operation),
                        type: metric.type,
                        sender: metric.sender,
                        receiver: metric.receiver,
                    }
                };
            });
            return this.influxDB.writePoints(points)
                .then(async () => {
                    // this.logger.info({ metrics }, 'Metrics written to influxdb');
                    // Introduce an artificial delay to slow down processing
                    // await new Promise(resolve => setTimeout(resolve, 30000)); // 30s delay
                })
                .catch((err) => {
                    this.logger.error({ err }, 'Error in influx db write api write points')
                });
        }
    }
}

module.exports = MetricsConsumer;
