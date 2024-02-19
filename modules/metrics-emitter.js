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
            const tags = {
                command: metric.command,
                sender: metric.sender,
            }
            const point = new Point('redis_queries')
                .tag('command', metric.command)
                .tag('sender', metric.sender);
            point.fields = fields;
            
            dataPoints.push(point);
        });
        writeApi.writePoints(dataPoints);
        this.metrics = [];

        return writeApi.close()
            .catch((err) => {
                this.logger.error(err, 'Error in influx db write api write points')
            })
    }
}

module.exports = MetricsEmitter;
