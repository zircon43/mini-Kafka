export class BufferReader {
    constructor(buffer) {
        this.buffer = buffer;
        this.offset = 0;
    }
    readInt8() { const v = this.buffer.readInt8(this.offset); this.offset += 1; return v; }
    readInt16BE() { const v = this.buffer.readInt16BE(this.offset); this.offset += 2; return v; }
    readInt32BE() { const v = this.buffer.readInt32BE(this.offset); this.offset += 4; return v; }
    readBigInt64BE() { const v = this.buffer.readBigInt64BE(this.offset); this.offset += 8; return v; }
    readNullableString() {
        const len = this.readInt16BE();
        if (len === -1) return null;
        const str = this.buffer.toString('utf8', this.offset, this.offset + len);
        this.offset += len;
        return str;
    }
    readUVarInt() {
        let value = 0;
        let shift = 0;
        while (true) {
            const byte = this.buffer.readUInt8(this.offset++);
            value |= (byte & 0x7f) << shift;
            if ((byte & 0x80) === 0) break;
            shift += 7;
        }
        return value;
    }
    readCompactString() {
        const len = this.readUVarInt() - 1;
        if (len === -1) return null;
        const str = this.buffer.toString('utf8', this.offset, this.offset + len);
        this.offset += len;
        return str;
    }
    readCompactArrayLength() {
        return this.readUVarInt() - 1;
    }
}

export class BufferWriter {
    constructor(size = 1024) {
        this.buffer = Buffer.alloc(size);
        this.offset = 0;
    }
    writeInt8(v) { this.buffer.writeInt8(v, this.offset); this.offset += 1; }
    writeUInt8(v) { this.buffer.writeUInt8(v, this.offset); this.offset += 1; }
    writeInt16BE(v) { this.buffer.writeInt16BE(v, this.offset); this.offset += 2; }
    writeInt32BE(v) { this.buffer.writeInt32BE(v, this.offset); this.offset += 4; }
    writeBigInt64BE(v) { this.buffer.writeBigInt64BE(v, this.offset); this.offset += 8; }
    writeUVarInt(value) {
        while (value >= 0x80) {
            this.buffer.writeUInt8((value & 0x7f) | 0x80, this.offset++);
            value >>>= 7;
        }
        this.buffer.writeUInt8(value, this.offset++);
    }
    writeCompactString(str) {
        this.writeUVarInt(str.length + 1);
        this.buffer.write(str, this.offset, str.length, 'utf8');
        this.offset += str.length;
    }
    writeBuffer(buf) {
        buf.copy(this.buffer, this.offset);
        this.offset += buf.length;
    }
    getBuffer() {
        return this.buffer.subarray(0, this.offset);
    }
}
