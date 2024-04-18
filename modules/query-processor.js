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
        this.systemCommands = ['CLIENT', 'CLUSTER', 'INFO', 'READONLY', 'HELLO', 'PING'];
        // this.monitorClassMemory();
    }

    start() {
        this.on('request', this.addQuery);
        this.on('response', this.processQueryResponse);
    }

    addQuery(query) {
        const { key, value, sender, receiver } = query;
        const request = this.respParser.parseData(value);
        if (!request) {
            this.queries[key] = null;
        } else if (!request[0] || !request[0][0]) {
            this.logger.warn(request, 'Unknown request');
        } else {
            this.queries[key] = {
                'request': request[0].join(' '),
                'command': request[0][0].toUpperCase(),
                'operation': `${request[0][0].toUpperCase()} ${request[0][1]}`,
                'startTime': process.hrtime.bigint(),
                'duration_in_ns': 0,
                'size_in_bytes': 0,
                sender,
                receiver,
            };
            this.queries[key]['type'] = (this.systemCommands.includes(this.queries[key]['command'])) ? 'system' : 'user';
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

    monitorClassMemory() {
        setInterval(() => {
            this.logger.info({ queriesSize: Object.keys(this.queries).length }, '[Query processor] monitor');
        }, 60 * 1000)
    }
}

module.exports = QueryProcessor;
