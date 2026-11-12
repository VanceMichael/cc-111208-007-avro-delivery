import io
import json


def integer(stream):
    value = 0
    for shift in range(0, 70, 7):
        raw = stream.read(1)
        if not raw:
            raise ValueError('Truncated Avro long')
        current = raw[0]
        value |= (current & 127) << shift
        if not current & 128:
            return (value >> 1) ^ -(value & 1)
    raise ValueError('Avro long exceeds 64 bits')


def binary(stream):
    length = integer(stream)
    if length < 0 or length > 1048576:
        raise ValueError('Metadata size out of bounds')
    value = stream.read(length)
    if len(value) != length:
        raise ValueError('Truncated metadata value')
    return value


def inspect(payload):
    stream = io.BytesIO(payload)
    if stream.read(4) != b'Obj\x01':
        raise ValueError('Invalid OCF magic')
    metadata = {}
    count = integer(stream)
    while count:
        if abs(count) > 1024:
            raise ValueError('Too many metadata entries')
        size = integer(stream) if count < 0 else None
        start = stream.tell()
        for index in range(abs(count)):
            key = binary(stream).decode()
            if key in metadata:
                raise ValueError('Duplicate metadata key')
            metadata[key] = binary(stream)
        if size is not None and stream.tell() - start != size:
            raise ValueError('Metadata block length mismatch')
        count = integer(stream)
    sync = stream.read(16)
    if len(sync) != 16 or 'avro.schema' not in metadata:
        raise ValueError('Incomplete OCF header')
    return {'writer_schema': json.loads(metadata['avro.schema']),
            'codec': metadata.get('avro.codec', b'null').decode(),
            'sync': sync.hex(), 'data_offset': stream.tell()}
