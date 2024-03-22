const pcap = require('pcap');
const config = require('../config');
const QueryProcessor = require('./query-processor');

class PacketDecoder {
    constructor({ logger }) {
        this.logger = logger;
        this.queryProcessor = new QueryProcessor({ logger: this.logger });
        this.pcapSession = pcap.createSession(config.networkInterface, {filter: `tcp and port ${config.redisConfig.port}`});
        this.segmentMapByNextSeqno = new Map();
        // this.monitorClassMemory();
    }

    start() {
        this.pcapSession.on('packet', this.segmentsReassembly.bind(this));
        this.pcapSession.on('error', this.handleError.bind(this));
        this.queryProcessor.start();
        this.logger.info('Started packet decoder');
    }

    segmentsReassembly(rawPacket) {
        try {
            const packet = pcap.decode.packet(rawPacket);
            const ipv4Packet = packet.payload.payload;
            if (ipv4Packet.protocol !== 6) return;
            const { payload, saddr, daddr } = ipv4Packet;
            const { ackno, seqno, dport, sport, data, dataLength } = payload;
            const { psh } = payload.flags;
            const isRedisRequest = dport === config.redisConfig.port;
            const isRedisResponse = sport === config.redisConfig.port;
            const sender = Array.isArray(saddr?.addr) ? saddr.addr.join('.') : '';
            const receiver = Array.isArray(daddr?.addr) ? daddr.addr.join('.') : '';

            if (data && (isRedisRequest || isRedisResponse)) {
                const expectedNextSeqno = seqno + dataLength;
                this.segmentMapByNextSeqno.set(expectedNextSeqno, {
                    seqno,
                    ackno,
                    data: Buffer.from(data), // Create a new Buffer object,
                    dataLength,
                });
                if (psh) {
                    // last segment
                    const chunks = [];
                    let current = expectedNextSeqno;
                    while(current) {
                        const segment = this.segmentMapByNextSeqno.get(current);
                        if (segment) {
                            chunks.unshift(segment);
                            this.segmentMapByNextSeqno.delete(current);
                            current = segment.seqno;
                        } else {
                            current = null;
                        }
                    }
                    const mergedData = Buffer.concat(chunks.map((chunk) => chunk.data));
                    if (chunks.length > 0) {
                        this.emitPacket(isRedisRequest, isRedisResponse, chunks[0].ackno, chunks[0].seqno, mergedData, sender, receiver);
                    }
                }
            }
        } catch (err) {
            this.logger.error(err, 'Error processing network packet');
        }
    }

    emitPacket(isRequest, isResponse, ackno, seqno, value, sender, receiver) {
        const key = (isRequest) ? ackno : seqno;
        if (isRequest) {
            this.queryProcessor.emit('request', { key, value, sender, receiver });
        } else if (isResponse) {
            this.queryProcessor.emit('response', { key, value, sender, receiver });
        }
    }

    handleError(err) {
        this.logger.error(err, 'Error in pcap session');
    }

    monitorClassMemory() {
        setInterval(() => {
            this.logger.info({ segmentsSize: this.segmentMapByNextSeqno.size }, '[Packet decoder] monitor');
        }, 60 * 1000)
    }
}

module.exports = PacketDecoder;
