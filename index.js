const Logger = require('./modules/logger');
const PacketDecoder = require('./modules/packet-decoder');

const logger = new Logger().createLogger();

const packetDecoder = new PacketDecoder({ logger });
packetDecoder.start();