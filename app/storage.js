import fs from "fs";

export const partitionIndices = new Map();

export function buildPartitionIndex(logFilePath) {
    const batches = [];
    let highWatermark = 0n;
    let currentByteOffset = 0;
    try {
        const buffer = fs.readFileSync(logFilePath);
        let offset = 0;
        while (offset + 12 <= buffer.length) {
            const baseOffset = buffer.readBigInt64BE(offset);
            const batchLength = buffer.readInt32BE(offset + 8);
            if (offset + 12 + batchLength > buffer.length) break;
            
            let lastOffsetDelta = 0;
            if (offset + 27 <= buffer.length) {
                lastOffsetDelta = buffer.readInt32BE(offset + 23);
            }
            const lastOffset = baseOffset + BigInt(lastOffsetDelta);
            
            batches.push({
                baseOffset,
                lastOffset,
                bytePos: offset,
                length: 12 + batchLength
            });
            highWatermark = lastOffset + 1n; // Next expected offset
            offset += 12 + batchLength;
        }
        currentByteOffset = offset;
    } catch (e) {
        // File might not exist
    }
    return { batches, highWatermark, currentByteOffset };
}

export function findBytePos(batches, fetchOffset) {
    let lo = 0, hi = batches.length - 1, result = -1;
    while (lo <= hi) {
        const mid = (lo + hi) >> 1;
        if (batches[mid].lastOffset >= fetchOffset) {
            result = mid;
            hi = mid - 1;
        } else {
            lo = mid + 1;
        }
    }
    return result === -1 ? -1 : batches[result].bytePos;
}

export function findTopicId(buffer, topicName) {
    if (!buffer) return null;
    
    // TopicRecord has type 2, version 0 -> 0x02, 0x00
    // Then topicName as compact string (length + 1)
    const nameLen = topicName.length + 1;
    let nameLenBytes = [];
    let value = nameLen;
    while (value >= 0x80) {
        nameLenBytes.push((value & 0x7f) | 0x80);
        value >>>= 7;
    }
    nameLenBytes.push(value);
    
    const prefix = Buffer.from([0x02, 0x00, ...nameLenBytes]);
    const topicBuffer = Buffer.from(topicName, 'utf8');
    const searchPattern = Buffer.concat([prefix, topicBuffer]);
    
    const idx = buffer.indexOf(searchPattern);
    if (idx !== -1) {
        const uuidOffset = idx + searchPattern.length;
        if (uuidOffset + 16 <= buffer.length) {
            return buffer.subarray(uuidOffset, uuidOffset + 16);
        }
    }
    return null;
}

export function findPartitions(buffer, topicId) {
    if (!buffer || !topicId) return [];
    let offset = 0;
    const partitions = [];
    while (true) {
        let idx = buffer.indexOf(topicId, offset);
        if (idx === -1) break;
        
        if (idx >= 6 && buffer[idx - 6] === 0x03 && (buffer[idx - 5] === 0x00 || buffer[idx - 5] === 0x01)) {
            const partitionId = buffer.readInt32BE(idx - 4);
            let cursor = idx + 16;
            
            function readUVarInt() {
                let val = 0;
                let shift = 0;
                while (true) {
                    const byte = buffer[cursor++];
                    val |= (byte & 0x7f) << shift;
                    if ((byte & 0x80) === 0) break;
                    shift += 7;
                }
                return val;
            }
            
            function readCompactArray() {
                const len = readUVarInt() - 1;
                if (len < 0) return [];
                const arr = [];
                for (let i = 0; i < len; i++) {
                    arr.push(buffer.readInt32BE(cursor));
                    cursor += 4;
                }
                return arr;
            }
            
            const replicas = readCompactArray();
            const isr = readCompactArray();
            const removingReplicas = readCompactArray();
            const addingReplicas = readCompactArray();
            
            const leader = buffer.readInt32BE(cursor); cursor += 4;
            const leaderEpoch = buffer.readInt32BE(cursor); cursor += 4;
            
            partitions.push({
                partitionId,
                replicas,
                isr,
                leader,
                leaderEpoch
            });
        }
        offset = idx + 1;
    }
    return partitions;
}

export function findTopicNameById(buffer, topicId) {
    if (!buffer || !topicId) return null;
    let offset = 0;
    while (true) {
        let idx = buffer.indexOf(Buffer.from([0x02, 0x00]), offset);
        if (idx === -1) break;
        
        let cursor = idx + 2;
        let nameLen = 0;
        let shift = 0;
        let valid = true;
        while (true) {
            if (cursor >= buffer.length) { valid = false; break; }
            const byte = buffer[cursor++];
            nameLen |= (byte & 0x7f) << shift;
            if ((byte & 0x80) === 0) break;
            shift += 7;
        }
        
        if (valid) {
            nameLen -= 1; // COMPACT_STRING length is N + 1
            if (nameLen > 0 && cursor + nameLen + 16 <= buffer.length) {
                const topicName = buffer.toString('utf8', cursor, cursor + nameLen);
                cursor += nameLen;
                const uuid = buffer.subarray(cursor, cursor + 16);
                if (uuid.equals(topicId)) {
                    return topicName;
                }
            }
        }
        offset = idx + 1;
    }
    return null;
}
