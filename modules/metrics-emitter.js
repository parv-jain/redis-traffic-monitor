const EventEmitter = require('node:events');
const config = require('../config');
const { Kafka, Partitioners, CompressionTypes, logLevel } = require('kafkajs')

class MetricsEmitter extends EventEmitter {
    constructor({ logger }) {
        super();
        this.metrics = [];
        this.logger = logger;
        this.on('query', this.addMetric);
        this.kafka = new Kafka({
            logLevel: logLevel.WARN,
            brokers: config.kafka.brokers,
            ssl: config.kafka.ssl,
            clientId: 'redis-metrics-producer',
        });
        this.producer = this.kafka.producer({
            createPartitioner: Partitioners.DefaultPartitioner, // custom partitioner
            retry: {
                initialRetryTime: 300,
                retries: Number.MAX_SAFE_INTEGER,
                maxRetryTime: 30000,
                factor: 0.2,
                multiplier: 2,
            },
            metadataMaxAge: 300000, // The period of time in milliseconds after which we force a refresh of metadata even if we haven't seen any partition leadership changes to proactively discover any new brokers or partitions
            allowAutoTopicCreation: true, // Allow topic creation when querying metadata for non-existent topics
            transactionTimeout: 60000, // The maximum amount of time in ms that the transaction coordinator will wait for a transaction status update from the producer before proactively aborting the ongoing transaction. If this value is larger than the transaction.max.timeout.ms setting in the broker, the request will fail with a InvalidTransactionTimeout error
            idempotent: false, // Experimental. If enabled producer will ensure each message is written exactly once. Acks must be set to -1 ("all"). Retries will default to MAX_SAFE_INTEGER.
            maxInFlightRequests: undefined, // Max number of requests that may be in progress at any time. If falsey then no limit.
        });
        // this.monitorClassMemory();
    }

    async start() {
        await this.producer.connect();
        setInterval(() => {
            this.publishMetrics.bind(this)();
        }, 15 * 1000);
    }

    addMetric(data) {
        this.metrics.push(data);
    }

    publishMetrics() {
        const metrics = this.metrics;
        this.metrics = [];
        return this.producer.send({
            topic: config.kafka.topic,
            compression: CompressionTypes.GZIP,
            messages: metrics.map((metric) => { return { key: metric.command, value: JSON.stringify(metric) }}),
        })
            .then(() => {
                // this.logger.info({ metrics }, 'Metrics pushed to kafka');
            })
            .catch((err) => this.logger.error({ err }, 'Error in publishing data to kafka'));
    }

    monitorClassMemory() {
        setInterval(() => {
            this.logger.info({ metricsSize: this.metrics.length }, '[Metrics emitter] monitor');
        }, 60 * 1000)
    }
}

module.exports = MetricsEmitter;
