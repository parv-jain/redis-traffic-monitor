const redisProto = require('redis-proto');

class RespParser {
    constructor({ logger }) {
        this.logger = logger;
    }

    parseData(data) {
        try {
            const decodedData = redisProto.decode(data);
            return decodedData;
        } catch (err) {
            // this.logger.error({
            //     err,
            //     // data: data && data.toString(),
            // }, '[respParser] [parseData] Error in decoding the data');
            return null;
        }
    }
}

module.exports = RespParser;
