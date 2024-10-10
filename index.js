const Logger = require('./modules/logger');
const PacketDecoder = require('./modules/packet-decoder');
const MetricsConsumer = require('./modules/metrics-consumer');
const config = require('./config');

(async () => {
    BigInt.prototype.toJSON = function() { return this.toString() } // Adding serializer for bigint - required in json.stringify for bigint values.
    const logger = new Logger().createLogger();
    const packetDecoder = new PacketDecoder({ logger });
    const metricsConsumer = new MetricsConsumer({ logger });
    await Promise.all([
        packetDecoder.start(),
        (config.pushMetricsToInfluxDb) ? metricsConsumer.start() : Promise.resolve(),
    ]);
})();