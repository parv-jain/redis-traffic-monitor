const pcap = require('pcap');
const config = require('../config');
const QueryProcessor = require('./query-processor');

class PacketDecoder {
    constructor({ logger }) {
        this.queries = {};
        this.logger = logger;
        this.queryProcessor = new QueryProcessor({ logger: this.logger });
        this.pcapSession = pcap.createSession(config.networkInterface, {filter: 'tcp and port 6379'});
        this.fragmentsMapByIdentification = new Map();
        this.segmentMapByNextSeqno = new Map();
    }

    start() {
        this.pcapSession.on('packet', this.segmentsReassembly.bind(this));
        this.pcapSession.on('error', this.handleError.bind(this));
        this.queryProcessor.start();
        this.logger.info('Started packet decoder');
    }

    fragmentsReassembly(rawPacket) {
        try {
            const packet = pcap.decode.packet(rawPacket);
            const ipv4Packet = packet.payload.payload;
            if (ipv4Packet.protocol !== 6) return;
            const { payload, headerLength, length, flags, identification, fragmentOffset } = ipv4Packet;
            const { ackno, seqno, dport, sport, data } = payload;
            const { doNotFragment, moreFragments } = flags;
            const isRedisRequest = dport === config.redisConfig.port;
            const isRedisResponse = sport === config.redisConfig.port;
            if (data && (isRedisRequest || isRedisResponse)) {
                let isDatagramFragmented = !(moreFragments === false && fragmentOffset === 0);
                // if (doNotFragment) {
                //     this.logger.error({ headerLength, length, flags, identification, fragmentOffset }, 'Packet has doNotFragment set - it can\'t be fragmented');
                //     return;
                // }
                if (!isDatagramFragmented) {
                    this.emitPacket(isRedisRequest, isRedisResponse, ackno, seqno, data);
                    return;
                }

                const fragments = this.fragmentsMapByIdentification.get(identification) || {};
                fragments[fragmentOffset] = {
                    ackno,
                    seqno,
                    value: data,
                    fragmentOffset,
                    nextFragmentOffset: length - headerLength + fragmentOffset,
                };
                this.fragmentsMapByIdentification.set(identification, fragments);
                if (moreFragments === false) {
                    // this is last fragment
                    let mergedData = Buffer.alloc(0);
                    let expectedNextFragmentOffset = 0;
                    const fragmentOffsets = Object.keys(fragments).sort((a, b) => a - b);
                    fragmentOffsets.forEach((fragmentOffset) => {
                        if (fragmentOffset == expectedNextFragmentOffset) {
                            const fragment = fragments[fragmentOffset];
                            expectedNextFragmentOffset = fragment.nextFragmentOffset;
                            mergedData = Buffer.concat([mergedData, fragment.value]);
                        } else {
                            this.logger.error({ fragmentOffsets, fragmentOffset, expectedNextFragmentOffset }, 'Not able to find expected fragment offset');
                        }
                    });
                    this.emitPacket(isRedisRequest, isRedisResponse, ackno, seqno, mergedData); 
                    this.fragmentsMapByIdentification.delete(identification);
                }
            }
        } catch (err) {
            this.logger.error(err, 'Error processing network packet');
        }
    }

    segmentsReassembly(rawPacket) {
        try {
            const packet = pcap.decode.packet(rawPacket);
            const ipv4Packet = packet.payload.payload;
            if (ipv4Packet.protocol !== 6) return;
            const { payload } = ipv4Packet;
            const { ackno, seqno, dport, sport, data, dataLength } = payload;
            const { psh } = payload.flags;
            const isRedisRequest = dport === config.redisConfig.port;
            const isRedisResponse = sport === config.redisConfig.port;
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
                            this.segmentMapByNextSeqno.delete(expectedNextSeqno);
                            current = segment.seqno;
                        } else {
                            current = null;
                        }
                    }
                    const mergedData = Buffer.concat(chunks.map((chunk) => chunk.data));
                    const mergedSegment = chunks[0];
                    mergedSegment.data = mergedData;
                    this.emitPacket(isRedisRequest, isRedisResponse, mergedSegment.ackno, mergedSegment.seqno, mergedSegment.data);
                }
            }
        } catch (err) {
            this.logger.error(err, 'Error processing network packet');
        }
    }

    emitPacket(isRequest, isResponse, ackno, seqno, value) {
        const key = (isRequest) ? ackno : seqno;
        if (isRequest) {
            this.queryProcessor.emit('request', { key, value });
        } else if (isResponse) {
            this.queryProcessor.emit('response', { key, value });
        }
    }

    handleError(err) {
        this.logger.error(err, 'Error in pcap session');
    }
}

module.exports = PacketDecoder;
