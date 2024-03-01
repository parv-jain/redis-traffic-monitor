const EventEmitter = require('node:events');
const MetricsEmitter = require('./metrics-emitter');
const RespParser = require('./resp-parser');

class QueryProcessor extends EventEmitter {
    constructor({ logger }) {
        super();
        this.queries = {};
        this.logger = logger;
        this.metricsEmitter = new MetricsEmitter({ logger: this.logger });
        this.respParser = new RespParser({ logger: this.logger });
    }

    start() {
        this.on('request', this.addQuery);
        this.on('response', this.processQueryResponse);
        setInterval(() => {
            if (Object.keys(this.queries).length > 1) {
                this.logger.info({ queries: this.queries }, '[QueryProcessor] Resetting queries')
                this.queries = {};
            }
            this.metricsEmitter.publishMetrics.bind(this.metricsEmitter)();
        }, 10 * 1000);
    }

    addQuery(query) {
        const { key, value } = query;
        const request = this.respParser.parseData(value);
        if (!request) {
            this.queries[key] = null;
        } else {
            this.queries[key] = {
                'request': request[0].join(' '),
                'command': request[0][0].toUpperCase(),
                'startTime': process.hrtime.bigint(),
                'duration_in_ns': 0,
                'size_in_bytes': 0,
            };
        }
    }

    processQueryResponse(result) {
        const { key, value } = result;
        const response = this.respParser.parseData(value);
        const query = this.queries[key];
        if (query === null) {
            delete this.queries[key];
        } else if (query) {
            const duration_in_ns = process.hrtime.bigint() - query['startTime'];
            query['duration_in_ns'] = duration_in_ns;
            query['size_in_bytes'] = Buffer.byteLength(value);
            this.metricsEmitter.emit('query', query);
            delete this.queries[key];
        }
    }
}

module.exports = QueryProcessor;
