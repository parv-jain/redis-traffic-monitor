const EventEmitter = require('node:events');
const { InfluxDB, Point } = require('@influxdata/influxdb-client');
const { hostname } = require('node:os');
const config = require('../config');

class MetricsEmitter extends EventEmitter {
    constructor({ logger }) {
        super();
        this.metrics = [];
        this.logger = logger;
        this.influxDBConfig = config.influxDBConfig;
        this.influxDB = new InfluxDB({ url: this.influxDBConfig.url, token: this.influxDBConfig.token });
        this.on('query', this.addMetric);
        this.start();
        // this.monitorClassMemory();
    }

    start() {
        setInterval(() => {
            this.publishMetrics.bind(this)();
        }, 60 * 1000);
    }

    addMetric(data) {
        this.metrics.push(data);
    }

    publishMetrics() {
        const writeApi = this.influxDB.getWriteApi(this.influxDBConfig.org, this.influxDBConfig.bucket)
        writeApi.useDefaultTags({location: hostname()})
        const dataPoints = [];

        BigInt.prototype.toJSON = function() { return this.toString() }

        this.metrics.forEach((metric) => {
            const fields = {
                request: JSON.stringify(metric.request),
                startTime: metric.startTime,
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
        this.metrics = [];

        return writeApi.close()
            .catch((err) => {
                this.metrics = [];
                this.logger.error({ err }, 'Error in influx db write api write points')
            })
    }

    monitorClassMemory() {
        setInterval(() => {
            this.logger.info({ metricsSize: this.metrics.length }, '[Metrics emitter] monitor');
        }, 60 * 1000)
    }
}

module.exports = MetricsEmitter;
