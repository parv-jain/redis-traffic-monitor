const bunyan = require('bunyan');
const bunyanFormat = require('bunyan-format');

class Logger {
    constructor() {
        this.serializers = {
            req: this.reqSerializer,
            res: this.resSerializer,
            err: bunyan.stdSerializers.err,
        };
        this.streams = [{
            level: bunyan.levelFromName.info,
            stream: bunyanFormat({ outputMode: 'json', levelInString: true },  undefined),
        }];
    }

    stringify(value) {
        if (typeof value === 'object') {
            return JSON.stringify(value);
        }
        return String(value);
    }

    reqSerializer(req) {
        if (!req) {
            return req;
        }

        return {
            method: req.method,
            url: req.url,
            body: stringify(req.body),
            headers: req.headers,
            httpVersion: req.httpVersion,
            query: stringify(req.query),
        };
    }

    resSerializer(res) {
        if (!res) {
            return res;
        }

        return {
            statusCode: res.statusCode,
            headers: res.headers,
            body: res.body,
        };
    }

    createLogger() {
        return bunyan.createLogger({
            name: 'redis-traffic-monitor',
            serializers: this.serializers,
            streams: this.streams,
            tag: ['redis-monitoring'],
        });
    }
}

module.exports = Logger;
