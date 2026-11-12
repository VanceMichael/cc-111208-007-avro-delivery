"""逐块接收 Avro OCF 配送扫描文件。

状态存于 SQLite：sources 绑定源身份、头部摘要与已提交前缀摘要；blocks 记录
每块原文偏移与提交/阻塞状态；events 是已接收扫描；conflicts 留存同 id 异值。
每块在单个事务里同时写入业务记录与检查点，提交后立刻退出再恢复也不会错开。
"""
import hashlib
import json
from pathlib import Path
import sqlite3
import zlib

from .avro import (Corrupt, EvolutionError, LimitExceeded, Limits, canonical,
                   decode_block, normalize, resolve, schema_fingerprint)
from .parser import inspect

SCHEMA = """
CREATE TABLE IF NOT EXISTS sources (
  name TEXT PRIMARY KEY,
  status TEXT NOT NULL,
  header_digest TEXT NOT NULL,
  schema_fingerprint TEXT NOT NULL,
  codec TEXT NOT NULL,
  sync_hex TEXT NOT NULL,
  committed_end INTEGER NOT NULL,
  prefix_digest TEXT NOT NULL,
  detail TEXT
);
CREATE TABLE IF NOT EXISTS blocks (
  source TEXT NOT NULL,
  block_index INTEGER NOT NULL,
  start_offset INTEGER NOT NULL,
  end_offset INTEGER NOT NULL,
  record_count INTEGER NOT NULL,
  payload_digest TEXT NOT NULL,
  status TEXT NOT NULL,
  reason TEXT,
  PRIMARY KEY (source, block_index)
);
CREATE TABLE IF NOT EXISTS events (
  source TEXT NOT NULL,
  event_id TEXT NOT NULL,
  canonical TEXT NOT NULL,
  record_json TEXT NOT NULL,
  block_index INTEGER NOT NULL,
  record_index INTEGER NOT NULL,
  PRIMARY KEY (source, event_id)
);
CREATE TABLE IF NOT EXISTS conflicts (
  source TEXT NOT NULL,
  event_id TEXT NOT NULL,
  existing_canonical TEXT NOT NULL,
  incoming_canonical TEXT NOT NULL,
  block_index INTEGER NOT NULL,
  record_index INTEGER NOT NULL
);
"""

CODECS = ('null', 'deflate')


class Incomplete(ValueError):
    """尾块字节不足，属于等待而非错误。"""


def _read_long_at(data, position):
    value = 0
    for shift in range(0, 70, 7):
        if position >= len(data):
            raise Incomplete(f'block header truncated at offset {position}')
        current = data[position]
        position += 1
        value |= (current & 127) << shift
        if not current & 128:
            return (value >> 1) ^ -(value & 1), position
    raise Corrupt('block header varint exceeds 64 bits')


def split_block(data, offset):
    """切出单个数据块边界，返回 (记录数, 负载起点, 负载长度, 块尾偏移)。"""
    count, position = _read_long_at(data, offset)
    if count < 0:
        raise Corrupt(f'negative block count {count} at offset {offset}')
    length, position = _read_long_at(data, position)
    if length < 0:
        raise Corrupt(f'negative block payload length {length}')
    end = position + length + 16
    if end > len(data):
        have = len(data) - position
        raise Incomplete(f'tail block has {have} of {length + 16} payload+sync bytes')
    return count, position, length, end


def inflate(payload, codec, limits):
    """解压块负载并执行解压字节上限；压缩文件大小不能替代该校验。"""
    if codec == 'null':
        if len(payload) > limits.max_block_bytes:
            raise LimitExceeded(f'block payload is {len(payload)} bytes, limit is {limits.max_block_bytes}')
        return payload
    stream = zlib.decompressobj(-15)
    try:
        out = stream.decompress(payload, limits.max_block_bytes + 1)
    except zlib.error as error:
        raise Corrupt(f'invalid deflate stream: {error}') from error
    if stream.unconsumed_tail or len(out) > limits.max_block_bytes:
        raise LimitExceeded(f'decompressed block exceeds {limits.max_block_bytes} bytes')
    try:
        out += stream.flush()
    except zlib.error as error:
        raise Corrupt(f'invalid deflate stream: {error}') from error
    if len(out) > limits.max_block_bytes:
        raise LimitExceeded(f'decompressed block exceeds {limits.max_block_bytes} bytes')
    if not stream.eof:
        raise Corrupt('deflate stream ends before the block payload does')
    if stream.unused_data:
        raise Corrupt('deflate stream is shorter than the block payload')
    return out


def display(record):
    def default(value):
        if isinstance(value, (bytes, bytearray)):
            return {'bytes_hex': bytes(value).hex()}
        raise TypeError(type(value).__name__)
    return json.dumps(record, ensure_ascii=False, sort_keys=True, default=default)


class Store:
    def __init__(self, state):
        self.root = Path(state)
        self.root.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(self.root / 'ingest.sqlite3')
        self.db.row_factory = sqlite3.Row
        self.db.executescript(SCHEMA)

    def close(self):
        self.db.close()

    def source(self, name):
        return self.db.execute('SELECT * FROM sources WHERE name = ?', (name,)).fetchone()

    def quarantine(self, name, detail):
        with self.db:
            self.db.execute("UPDATE sources SET status = 'quarantined', detail = ? WHERE name = ?",
                            (detail, name))

    def block_index(self, name):
        return self.db.execute('SELECT COUNT(*) FROM blocks WHERE source = ?', (name,)).fetchone()[0]


def _check_reader(reader_node):
    if reader_node[0] != 'record':
        raise EvolutionError('reader schema must be a record of scan events')
    for name, sub, _ in reader_node[2]:
        if name == 'eventId':
            if sub != ('prim', 'string'):
                raise EvolutionError('reader field eventId must be a string for dedupe')
            return
    raise EvolutionError('reader schema must define a string field eventId for dedupe')


def ingest(path, state, reader_schema, name=None, limits=None, on_commit=None):
    """接收一个来源文件，返回 (摘要 dict, 退出码)。0 正常/等待，3 已隔离。"""
    limits = limits or Limits()
    data = Path(path).read_bytes()
    info = inspect(data)
    codec = info['codec']
    if codec not in CODECS:
        raise ValueError(f'unsupported codec {codec!r}; only null and deflate are accepted')
    writer = normalize(info['writer_schema'])
    reader = normalize(reader_schema)
    _check_reader(reader)
    plan = resolve(writer, reader)
    fingerprint = schema_fingerprint(info['writer_schema'])
    data_offset = info['data_offset']
    header_digest = hashlib.sha256(data[:data_offset]).hexdigest()
    sync = bytes.fromhex(info['sync'])
    name = name or Path(path).name

    store = Store(state)
    try:
        row = store.source(name)
        if row and row['status'] == 'quarantined':
            return _summary(store, name, fingerprint, 'quarantined', row['detail'], row['committed_end'],
                            len(data)), 3
        if row is None:
            with store.db:
                store.db.execute(
                    "INSERT INTO sources VALUES (?, 'active', ?, ?, ?, ?, ?, ?, NULL)",
                    (name, header_digest, fingerprint, codec, info['sync'], data_offset,
                     hashlib.sha256(data[:data_offset]).hexdigest()))
            offset = data_offset
        else:
            offset = row['committed_end']
            problem = None
            if row['header_digest'] != header_digest:
                problem = 'source header changed; the file was replaced, refusing to reuse offsets'
            elif len(data) < offset:
                problem = f'source shrank to {len(data)} bytes below committed prefix {offset}'
            elif hashlib.sha256(data[:offset]).hexdigest() != row['prefix_digest']:
                problem = 'committed prefix bytes were modified, refusing to reuse offsets'
            if problem:
                store.quarantine(name, problem)
                return _summary(store, name, fingerprint, 'quarantined', problem, offset, len(data)), 3

        hasher = hashlib.sha256(data[:offset])
        committed, blocked, conflicts_found = [], [], []
        duplicates = new_events = 0
        status, detail = 'complete', None
        while offset < len(data):
            try:
                count, payload_start, payload_length, end = split_block(data, offset)
            except Incomplete as waiting:
                status, detail = 'waiting', str(waiting)
                break
            except Corrupt as exc:
                problem = f'block header corrupt at offset {offset}: {exc}'
                store.quarantine(name, problem)
                return _summary(store, name, fingerprint, 'quarantined', problem, offset, len(data)), 3
            index = store.block_index(name)
            payload = data[payload_start:payload_start + payload_length]
            if data[end - 16:end] != sync:
                problem = f'block {index} sync marker mismatch at offset {end - 16}; not scanning forward'
                store.quarantine(name, problem)
                return _summary(store, name, fingerprint, 'quarantined', problem, offset, len(data)), 3
            payload_digest = hashlib.sha256(payload).hexdigest()
            try:
                records = decode_block(inflate(payload, codec, limits), writer, plan, count, limits)
            except LimitExceeded as exc:
                records, reason = None, f'limit: {exc}'
            except Corrupt as exc:
                problem = f'block {index} payload corrupt: {exc}'
                store.quarantine(name, problem)
                return _summary(store, name, fingerprint, 'quarantined', problem, offset, len(data)), 3
            conflicts = []
            if records is not None:
                canonicals = [canonical(reader, record) for record in records]
                event_ids = [record['eventId'] for record in records]
                seen = {}
                for record_index, (event_id, canon) in enumerate(zip(event_ids, canonicals)):
                    if event_id in seen and seen[event_id] != canon:
                        conflicts.append((event_id, seen[event_id], canon, record_index))
                    seen.setdefault(event_id, canon)
                    existing = store.db.execute(
                        'SELECT canonical FROM events WHERE source = ? AND event_id = ?',
                        (name, event_id)).fetchone()
                    if existing and existing[0] != canon:
                        conflicts.append((event_id, existing[0], canon, record_index))
                if conflicts:
                    reason = 'conflict: ' + ', '.join(sorted({item[0] for item in conflicts}))
            if records is None or conflicts:
                with store.db:
                    _record_block(store, name, index, offset, end, count, payload_digest,
                                  'blocked', reason)
                    for event_id, old, new, record_index in conflicts:
                        store.db.execute('INSERT INTO conflicts VALUES (?, ?, ?, ?, ?, ?)',
                                         (name, event_id, old, new, index, record_index))
                    _checkpoint(store, name, end, hasher, data, offset)
                blocked.append({'block': index, 'reason': reason, 'start': offset, 'end': end})
                conflicts_found.extend(event_id for event_id, *_ in conflicts)
            else:
                with store.db:
                    _record_block(store, name, index, offset, end, count, payload_digest,
                                  'committed', None)
                    for record_index, (event_id, canon, record) in enumerate(
                            zip(event_ids, canonicals, records)):
                        inserted = store.db.execute(
                            'INSERT OR IGNORE INTO events VALUES (?, ?, ?, ?, ?, ?)',
                            (name, event_id, canon, display(record), index, record_index))
                        if inserted.rowcount:
                            new_events += 1
                        else:
                            duplicates += 1
                    _checkpoint(store, name, end, hasher, data, offset)
                committed.append({'block': index, 'records': count, 'start': offset, 'end': end})
            offset = end
            if on_commit:
                on_commit(index)
        summary = _summary(store, name, fingerprint, status, detail, offset, len(data))
        summary.update({'committed_blocks': committed, 'blocked_blocks': blocked,
                        'new_events': new_events, 'duplicates': duplicates,
                        'conflicts': sorted(set(conflicts_found))})
        return summary, 0
    finally:
        store.close()


def _record_block(store, name, index, start, end, count, payload_digest, status, reason):
    store.db.execute('INSERT INTO blocks VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
                     (name, index, start, end, count, payload_digest, status, reason))


def _checkpoint(store, name, end, hasher, data, offset):
    hasher.update(data[offset:end])
    store.db.execute('UPDATE sources SET committed_end = ?, prefix_digest = ? WHERE name = ?',
                     (end, hasher.hexdigest(), name))


def _summary(store, name, fingerprint, status, detail, offset, size):
    row = store.source(name)
    return {'source': name, 'status': status, 'detail': detail,
            'schema_fingerprint': fingerprint,
            'committed_end': offset, 'remaining_bytes': size - offset,
            'total_events': store.db.execute(
                'SELECT COUNT(*) FROM events WHERE source = ?', (name,)).fetchone()[0],
            'source_status': row['status'] if row else 'unknown',
            'committed_blocks': [], 'blocked_blocks': [],
            'new_events': 0, 'duplicates': 0, 'conflicts': []}


def scans(state, source=None):
    """把已接收扫描、阻塞块、原文偏移、writer 指纹与来源身份关联查询。"""
    store = Store(state)
    try:
        params = ()
        where = ''
        if source:
            where = 'WHERE e.source = ?'
            params = (source,)
        events = [dict(row) for row in store.db.execute(
            f'''SELECT e.source, e.event_id, e.record_json, e.block_index, e.record_index,
                       b.start_offset, b.end_offset, b.status AS block_status,
                       s.schema_fingerprint, s.header_digest
                FROM events e
                JOIN blocks b ON b.source = e.source AND b.block_index = e.block_index
                JOIN sources s ON s.name = e.source
                {where} ORDER BY e.source, e.block_index, e.record_index''', params)]
        blocked = [dict(row) for row in store.db.execute(
            '''SELECT b.source, b.block_index, b.start_offset, b.end_offset, b.record_count,
                      b.reason, s.schema_fingerprint
               FROM blocks b JOIN sources s ON s.name = b.source
               WHERE b.status = 'blocked' ORDER BY b.source, b.block_index''')]
        conflicts = [dict(row) for row in store.db.execute(
            'SELECT * FROM conflicts ORDER BY source, block_index, record_index')]
        sources = [dict(row) for row in store.db.execute(
            'SELECT name, status, committed_end, prefix_digest, schema_fingerprint, codec, detail '
            'FROM sources ORDER BY name')]
        return {'events': events, 'blocked_blocks': blocked,
                'conflicts': conflicts, 'sources': sources}
    finally:
        store.close()
