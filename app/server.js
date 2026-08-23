import net from "net";
import fs from "fs";
import { LOG_DIR, LOG_PATH, ACTIVE_SEGMENT } from "./constants.js";
import { BufferReader, BufferWriter } from "./protocol.js";
import { partitionIndices, buildPartitionIndex, findBytePos, findTopicId, findPartitions, findTopicNameById } from "./storage.js";

export const server = net.createServer((connection) => {
    let buffer = Buffer.alloc(0);

    connection.on("data", (data) => {
        buffer = Buffer.concat([buffer, data]);

        while (buffer.length >= 4) {
            const requestMessageSize = buffer.readInt32BE(0);
            const totalSize = 4 + requestMessageSize;

            if (buffer.length < totalSize) {
                break;
            }

            const requestData = buffer.subarray(0, totalSize);
            buffer = buffer.subarray(totalSize);

            const reader = new BufferReader(requestData);
            reader.offset = 4; // Skip message size

            const requestApiKey = reader.readInt16BE();
            const requestApiVersion = reader.readInt16BE();
            const correlationId = reader.readInt32BE();

            if (requestApiKey === 18) {
                // ApiVersions
                let errorCode = 0;
                if (requestApiVersion < 0 || requestApiVersion > 4) {
                    errorCode = 35; // UNSUPPORTED_VERSION
                }

                const writer = new BufferWriter();
                writer.writeInt32BE(correlationId);
                writer.writeInt16BE(errorCode);
                
                // api_keys array
                writer.writeUInt8(5); // length: 4 elements + 1
                
                // element 1: ApiVersions
                writer.writeInt16BE(18); // api_key
                writer.writeInt16BE(0); // min_version
                writer.writeInt16BE(4); // max_version
                writer.writeUInt8(0); // TAG_BUFFER

                // element 2: DescribeTopicPartitions
                writer.writeInt16BE(75); // api_key
                writer.writeInt16BE(0); // min_version
                writer.writeInt16BE(0); // max_version
                writer.writeUInt8(0); // TAG_BUFFER

                // element 3: Fetch
                writer.writeInt16BE(1); // api_key
                writer.writeInt16BE(0); // min_version
                writer.writeInt16BE(16); // max_version
                writer.writeUInt8(0); // TAG_BUFFER

                // element 4: Produce
                writer.writeInt16BE(0); // api_key
                writer.writeInt16BE(0); // min_version
                writer.writeInt16BE(11); // max_version
                writer.writeUInt8(0); // TAG_BUFFER
                
                writer.writeInt32BE(0); // throttle_time_ms
                writer.writeUInt8(0); // TAG_BUFFER

                const responseBody = writer.getBuffer();
                const response = Buffer.alloc(4 + responseBody.length);
                response.writeInt32BE(responseBody.length, 0);
                responseBody.copy(response, 4);
                connection.write(response);
            } else if (requestApiKey === 0) {
                // Produce
                const clientId = reader.readNullableString();
                reader.readUVarInt(); // header TAG_BUFFER
                
                const transactionalId = reader.readCompactString();
                const acks = reader.readInt16BE();
                const timeoutMs = reader.readInt32BE();
                
                const topicsCount = reader.readCompactArrayLength();
                let topicRequests = [];
                
                for (let t = 0; t < topicsCount; t++) {
                    const topicName = reader.readCompactString();
                    const partitionsCount = reader.readCompactArrayLength();
                    let partitionsRequests = [];
                    for (let i = 0; i < partitionsCount; i++) {
                        const partitionIndex = reader.readInt32BE();
                        const recordsLen = reader.readUVarInt() - 1;
                        let recordsBytes = Buffer.alloc(0);
                        if (recordsLen > 0) {
                            recordsBytes = reader.buffer.subarray(reader.offset, reader.offset + recordsLen);
                            reader.offset += recordsLen;
                        }
                        reader.readUVarInt(); // partition TAG_BUFFER
                        partitionsRequests.push({ index: partitionIndex, recordsBytes });
                    }
                    reader.readUVarInt(); // topic TAG_BUFFER
                    topicRequests.push({ topicName, partitionsRequests });
                }
                
                let clusterMetadata = null;
                try {
                    clusterMetadata = fs.readFileSync(LOG_PATH);
                } catch (e) {
                }
                
                const writer = new BufferWriter(1024);
                // Header v1
                writer.writeInt32BE(correlationId);
                writer.writeUInt8(0); // TAG_BUFFER
                
                // Body (Produce Response v11)
                writer.writeUInt8(topicRequests.length + 1); // responses array length
                
                for (const topicReq of topicRequests) {
                    writer.writeCompactString(topicReq.topicName);
                    
                    writer.writeUInt8(topicReq.partitionsRequests.length + 1); // partitions array length
                    
                    const topicId = findTopicId(clusterMetadata, topicReq.topicName);
                    let validPartitions = [];
                    if (topicId) {
                        validPartitions = findPartitions(clusterMetadata, topicId);
                    }
                    
                    for (const req of topicReq.partitionsRequests) {
                        let topicErrorCode = 3; // UNKNOWN_TOPIC_OR_PARTITION
                        
                        if (topicId) {
                            const partitionExists = validPartitions.some(p => p.partitionId === req.index);
                            if (partitionExists) {
                                topicErrorCode = 0; // NO_ERROR
                                
                                if (req.recordsBytes.length > 0) {
                                    const partitionDir = `${LOG_DIR}/${topicReq.topicName}-${req.index}`;
                                    fs.mkdirSync(partitionDir, { recursive: true });
                                    const partitionLogPath = `${partitionDir}/${ACTIVE_SEGMENT}`;
                                    
                                    let index = partitionIndices.get(partitionLogPath);
                                    if (!index) {
                                        index = buildPartitionIndex(partitionLogPath);
                                        partitionIndices.set(partitionLogPath, index);
                                    }
                                    
                                    let offset = 0;
                                    const buffer = req.recordsBytes;
                                    while (offset + 12 <= buffer.length) {
                                        const baseOffset = buffer.readBigInt64BE(offset);
                                        const batchLength = buffer.readInt32BE(offset + 8);
                                        if (offset + 12 + batchLength > buffer.length) break;
                                        
                                        let lastOffsetDelta = 0;
                                        if (offset + 27 <= buffer.length) {
                                            lastOffsetDelta = buffer.readInt32BE(offset + 23);
                                        }
                                        const lastOffset = baseOffset + BigInt(lastOffsetDelta);
                                        
                                        index.batches.push({
                                            baseOffset,
                                            lastOffset,
                                            bytePos: index.currentByteOffset + offset,
                                            length: 12 + batchLength
                                        });
                                        index.highWatermark = lastOffset + 1n;
                                        offset += 12 + batchLength;
                                    }
                                    index.currentByteOffset += offset;
                                    
                                    fs.appendFileSync(partitionLogPath, req.recordsBytes);
                                }
                            }
                        }
                        
                        const baseOffset = topicErrorCode === 0 ? 0n : -1n;
                        const logStartOffset = topicErrorCode === 0 ? 0n : -1n;
                        
                        writer.writeInt32BE(req.index);
                        writer.writeInt16BE(topicErrorCode); // error_code
                        writer.writeBigInt64BE(baseOffset); // base_offset
                        writer.writeBigInt64BE(-1n); // log_append_time_ms
                        writer.writeBigInt64BE(logStartOffset); // log_start_offset
                        
                        writer.writeUInt8(1); // record_errors (empty array)
                        writer.writeUInt8(0); // error_message (null string)
                        writer.writeUInt8(0); // partition TAG_BUFFER
                    }
                    
                    writer.writeUInt8(0); // topic TAG_BUFFER
                }
                
                writer.writeInt32BE(0); // throttle_time_ms
                writer.writeUInt8(0); // body TAG_BUFFER
                
                const responseBody = writer.getBuffer();
                const response = Buffer.alloc(4 + responseBody.length);
                response.writeInt32BE(responseBody.length, 0);
                responseBody.copy(response, 4);
                connection.write(response);
            } else if (requestApiKey === 75) {
                // DescribeTopicPartitions
                const clientId = reader.readNullableString();
                reader.readUVarInt(); // header TAG_BUFFER

                const topicCount = reader.readCompactArrayLength();
                let topicNames = [];
                for (let i = 0; i < topicCount; i++) {
                    topicNames.push(reader.readCompactString());
                    reader.readUVarInt(); // topic TAG_BUFFER
                }
                
                // Sort topics alphabetically as required by this stage
                topicNames.sort();

                const writer = new BufferWriter();
                // Header v1
                writer.writeInt32BE(correlationId);
                writer.writeUInt8(0); // TAG_BUFFER
                
                // Body
                writer.writeInt32BE(0); // throttle_time_ms
                writer.writeUInt8(topicNames.length + 1); // topics array length
                
                let clusterMetadata = null;
                try {
                    clusterMetadata = fs.readFileSync(LOG_PATH);
                } catch (e) {
                    // Ignore missing log file
                }

                for (const topicName of topicNames) {
                    const topicIdBuffer = findTopicId(clusterMetadata, topicName);
                    let partitions = [];
                    if (topicIdBuffer) {
                        partitions = findPartitions(clusterMetadata, topicIdBuffer);
                    }
                    
                    if (topicIdBuffer && partitions.length > 0) {
                        
                        writer.writeInt16BE(0); // error_code (0)
                        writer.writeCompactString(topicName);
                        writer.writeBuffer(topicIdBuffer);
                        writer.writeUInt8(0); // is_internal (false)
                        
                        // partitions array
                        writer.writeUInt8(partitions.length + 1); // length (N elements + 1)
                        
                        for (const partitionInfo of partitions) {
                            writer.writeInt16BE(0); // error_code
                            writer.writeInt32BE(partitionInfo.partitionId);
                            writer.writeInt32BE(partitionInfo.leader);
                            writer.writeInt32BE(partitionInfo.leaderEpoch);
                            
                            writer.writeUInt8(partitionInfo.replicas.length + 1);
                            for (const r of partitionInfo.replicas) writer.writeInt32BE(r);
                            
                            writer.writeUInt8(partitionInfo.isr.length + 1);
                            for (const r of partitionInfo.isr) writer.writeInt32BE(r);
                            
                            writer.writeUInt8(1); // eligible_leader_replicas
                            writer.writeUInt8(1); // last_known_elr
                            writer.writeUInt8(1); // offline_replicas
                            writer.writeUInt8(0); // partition TAG_BUFFER
                        }
                        
                    } else {
                        writer.writeInt16BE(3); // error_code (UNKNOWN_TOPIC_OR_PARTITION)
                        writer.writeCompactString(topicName);
                        
                        // topic_id: 16 bytes of 0s
                        writer.writeBuffer(Buffer.alloc(16, 0));
                        
                        writer.writeUInt8(0); // is_internal (false)
                        writer.writeUInt8(1); // partitions array length (0 elements + 1)
                    }

                    writer.writeInt32BE(0); // topic_authorized_operations
                    writer.writeUInt8(0); // topic TAG_BUFFER
                }
                
                writer.writeInt8(-1); // next_cursor (null)
                writer.writeUInt8(0); // TAG_BUFFER

                const responseBody = writer.getBuffer();
                const response = Buffer.alloc(4 + responseBody.length);
                response.writeInt32BE(responseBody.length, 0);
                responseBody.copy(response, 4);
                connection.write(response);
            } else if (requestApiKey === 1) {
                // Fetch
                const clientId = reader.readNullableString();
                reader.readUVarInt(); // header TAG_BUFFER
                
                // Fetch Request v16 Body
                reader.readInt32BE(); // max_wait_ms
                reader.readInt32BE(); // min_bytes
                reader.readInt32BE(); // max_bytes
                reader.readInt8();    // isolation_level
                reader.readInt32BE(); // session_id
                reader.readInt32BE(); // session_epoch
                
                const topicsCount = reader.readCompactArrayLength();
                let requestTopicId = Buffer.alloc(16, 0);
                let requestPartitionIndex = 0;
                let fetchOffset = 0n;
                let maxBytes = 0;
                if (topicsCount > 0) {
                    requestTopicId = Buffer.from(reader.buffer.subarray(reader.offset, reader.offset + 16));
                    reader.offset += 16;
                    
                    const partitionsCount = reader.readCompactArrayLength();
                    if (partitionsCount > 0) {
                        requestPartitionIndex = reader.readInt32BE(); // partition
                        reader.readInt32BE(); // current_leader_epoch
                        fetchOffset = reader.readBigInt64BE(); // fetch_offset
                        reader.readInt32BE(); // last_fetched_epoch
                        reader.readBigInt64BE(); // log_start_offset
                        maxBytes = reader.readInt32BE(); // partition_max_bytes
                    }
                }

                const writer = new BufferWriter();
                // Header v1
                writer.writeInt32BE(correlationId);
                writer.writeUInt8(0); // TAG_BUFFER
                
                // Body
                writer.writeInt32BE(0); // throttle_time_ms
                writer.writeInt16BE(0); // error_code
                writer.writeInt32BE(0); // session_id
                
                let clusterMetadata = null;
                try {
                    clusterMetadata = fs.readFileSync(LOG_PATH);
                } catch (e) {
                    // Ignore missing log file
                }
                
                let topicErrorCode = 100; // UNKNOWN_TOPIC_ID
                let partitionLogBytes = Buffer.alloc(0);
                let highWatermark = 0n;

                if (clusterMetadata && requestTopicId) {
                    const topicName = findTopicNameById(clusterMetadata, requestTopicId);
                    if (topicName) {
                        topicErrorCode = 0; // NO_ERROR
                        // Attempt to read partition log
                        const partitionLogPath = `${LOG_DIR}/${topicName}-${requestPartitionIndex}/${ACTIVE_SEGMENT}`;
                        try {
                            let index = partitionIndices.get(partitionLogPath);
                            if (!index) {
                                index = buildPartitionIndex(partitionLogPath);
                                partitionIndices.set(partitionLogPath, index);
                            }
                            highWatermark = index.highWatermark;
                            
                            let startBytePos = findBytePos(index.batches, fetchOffset);
                            
                            if (startBytePos !== -1) {
                                if (maxBytes === 0) {
                                    partitionLogBytes = Buffer.alloc(0);
                                } else {
                                    let accumulatedLength = 0;
                                    let batchCount = 0;
                                    
                                    for (let i = 0; i < index.batches.length; i++) {
                                        const batch = index.batches[i];
                                        if (batch.bytePos >= startBytePos) {
                                            if (batchCount === 0 || accumulatedLength + batch.length <= maxBytes) {
                                                accumulatedLength += batch.length;
                                                batchCount++;
                                            } else {
                                                break;
                                            }
                                        }
                                    }
                                    
                                    const fullBuffer = fs.readFileSync(partitionLogPath);
                                    let endBytePos = startBytePos + accumulatedLength;
                                    if (endBytePos > fullBuffer.length) {
                                        endBytePos = fullBuffer.length;
                                    }
                                    partitionLogBytes = fullBuffer.subarray(startBytePos, endBytePos);
                                }
                            }
                        } catch (e) {
                            // File not found, partition log bytes remain empty
                        }
                    }
                }

                if (topicsCount > 0) {
                    writer.writeUInt8(2); // responses array length (1 element + 1)
                    writer.writeBuffer(requestTopicId); // topic_id
                    
                    writer.writeUInt8(2); // partitions array length (1 element + 1)
                    writer.writeInt32BE(requestPartitionIndex); // partition_index
                    writer.writeInt16BE(topicErrorCode); // error_code
                    writer.writeBigInt64BE(highWatermark); // high_watermark
                    writer.writeInt32BE(0); writer.writeInt32BE(0); // last_stable_offset
                    writer.writeInt32BE(0); writer.writeInt32BE(0); // log_start_offset
                    writer.writeUInt8(1); // aborted_transactions (empty array)
                    writer.writeInt32BE(0); // preferred_read_replica
                    
                    // records (COMPACT_RECORDS)
                    if (partitionLogBytes.length > 0) {
                        writer.writeUVarInt(partitionLogBytes.length + 1);
                        writer.writeBuffer(partitionLogBytes);
                    } else {
                        writer.writeUInt8(0); // null
                    }
                    
                    writer.writeUInt8(0); // partition TAG_BUFFER
                    
                    writer.writeUInt8(0); // topic TAG_BUFFER
                } else {
                    writer.writeUInt8(1); // responses array length (0 elements + 1)
                }
                
                writer.writeUInt8(0); // body TAG_BUFFER
                
                const responseBody = writer.getBuffer();
                const response = Buffer.alloc(4 + responseBody.length);
                response.writeInt32BE(responseBody.length, 0);
                responseBody.copy(response, 4);
                connection.write(response);
            }
        }
    });
});
