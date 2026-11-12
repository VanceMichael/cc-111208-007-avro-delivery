"""合成 OCF 构造器与逐块接收的端到端回归测试。

所有数据都是本地合成字节，不连接真实物流系统。构造器只覆盖
docs/source-contract.md 的支持子集，用于生成字段重排、默认值、deflate、
负计数数组等场景的来源文件。
"""
import json
import os
from pathlib import Path
import shutil
import signal
import sqlite3
import subprocess
import sys
import tempfile
import unittest
import zlib

from deliveryocf.avro import Limits
from deliveryocf.ingest import ingest, scans
from scripts.materialize import main as materialize

ROOT = Path(__file__).resolve().parent.parent
SYNC = b'0123456789abcdef'
READER = {'type': 'record', 'name': 'Scan', 'fields': [
    {'name': 'eventId', 'type': 'string'},
    {'name': 'timestamp', 'type': 'long'},
    {'name': 'delivered', 'type': 'boolean'}]}


def encode_long(value):
    bits = (value << 1) ^ (value >> 63)
    out = bytearray()
    while bits & ~127:
        out.append((bits & 127) | 128)
        bits >>= 7
    out.append(bits)
    return bytes(out)


def encode_bytes(raw):
    return encode_long(len(raw)) + raw


class NegArray(list):
    """以负计数加字节长度形式编码的数组。"""


def encode_value(node, value):
    if isinstance(node, list):
        null_arm = 0 if node[0] == 'null' else 1
        if value is None:
            return encode_long(null_arm)
        return encode_long(1 - null_arm) + encode_value(node[1 - null_arm], value)
    if isinstance(node, dict):
        kind = node['type']
        if kind == 'record':
            return b''.join(encode_value(field['type'], value[field['name']])
                            for field in node['fields'])
        if kind == 'array':
            body = b''.join(encode_value(node['items'], item) for item in value)
            if isinstance(value, NegArray):
                return encode_long(-len(value)) + encode_long(len(body)) + body + encode_long(0)
            if value:
                return encode_long(len(value)) + body + encode_long(0)
            return encode_long(0)
        node = kind
    if node == 'null':
        return b''
    if node == 'boolean':
        return b'\x01' if value else b'\x00'
    if node in ('int', 'long'):
        return encode_long(value)
    if node == 'string':
        return encode_bytes(value.encode())
    if node == 'bytes':
        return encode_bytes(value)
    raise ValueError(f'unsupported test type {node!r}')


def build_header(schema, codec='null', negative_meta=False, sync=SYNC):
    entries = [(b'avro.schema', json.dumps(schema).encode())]
    if codec is not None:
        entries.append((b'avro.codec', codec.encode()))
    body = b''.join(encode_bytes(key) + encode_bytes(value) for key, value in entries)
    out = bytearray(b'Obj\x01')
    if negative_meta:
        out += encode_long(-len(entries)) + encode_long(len(body)) + body
    else:
        out += encode_long(len(entries)) + body
    return bytes(out) + encode_long(0) + sync


def build_block(schema, records, codec='null', sync=SYNC):
    raw = b''.join(encode_value(schema, record) for record in records)
    if codec == 'deflate':
        compressor = zlib.compressobj(6, zlib.DEFLATED, -15)
        payload = compressor.compress(raw) + compressor.flush()
    else:
        payload = raw
    return encode_long(len(records)) + encode_long(len(payload)) + payload + sync


def build_file(schema, blocks, codec='null', negative_meta=False, sync=SYNC):
    return build_header(schema, codec, negative_meta, sync) + b''.join(blocks)


def scan_record(event_id, timestamp, delivered):
    return {'eventId': event_id, 'timestamp': timestamp, 'delivered': delivered}


class IngestTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        materialize()

    def run_ingest(self, path, state, reader=READER, **kwargs):
        return ingest(str(path), str(state), reader, **kwargs)

    def test_growing_file_waits_then_commits_only_third_block(self):
        with tempfile.TemporaryDirectory() as directory:
            channel = Path(directory) / 'channel.avro'
            state = Path(directory) / 'state'
            shutil.copy(ROOT / 'fixtures/growing.avro', channel)
            first, code = self.run_ingest(channel, state)
            self.assertEqual(code, 0)
            self.assertEqual(first['status'], 'waiting')
            self.assertEqual([b['block'] for b in first['committed_blocks']], [0, 1])
            self.assertEqual(first['total_events'], 2)
            self.assertEqual(first['remaining_bytes'], 18)
            shutil.copy(ROOT / 'fixtures/scans.avro', channel)
            second, code = self.run_ingest(channel, state)
            self.assertEqual(code, 0)
            self.assertEqual(second['status'], 'complete')
            self.assertEqual([b['block'] for b in second['committed_blocks']], [2])
            self.assertEqual(second['new_events'], 1)
            self.assertEqual(second['total_events'], 3)
            self.assertEqual(second['remaining_bytes'], 0)
            third, _ = self.run_ingest(channel, state)
            self.assertEqual(third['committed_blocks'], [])
            self.assertEqual(third['duplicates'], 0)
            self.assertEqual(third['total_events'], 3)
            view = scans(str(state))
            self.assertEqual([row['event_id'] for row in view['events']], ['E1', 'E2', 'E3'])
            offsets = [(row['start_offset'], row['end_offset']) for row in view['events']]
            self.assertEqual(offsets, [(220, 243), (243, 266), (266, 289)])
            self.assertTrue(all(row['schema_fingerprint'] for row in view['events']))

    def test_field_reorder_defaults_and_int_promotion(self):
        writer = {'type': 'record', 'name': 'Scan', 'fields': [
            {'name': 'delivered', 'type': 'boolean'},
            {'name': 'channel', 'type': 'string'},
            {'name': 'timestamp', 'type': 'int'},
            {'name': 'eventId', 'type': 'string'}]}
        reader = {'type': 'record', 'name': 'Scan', 'fields': [
            {'name': 'eventId', 'type': 'string'},
            {'name': 'timestamp', 'type': 'long'},
            {'name': 'delivered', 'type': 'boolean'},
            {'name': 'note', 'type': 'string', 'default': 'unknown'}]}
        records = [{'delivered': True, 'channel': 'north', 'timestamp': 42, 'eventId': 'E9'}]
        payload = build_file(writer, [build_block(writer, records)], negative_meta=True)
        with tempfile.TemporaryDirectory() as directory:
            channel = Path(directory) / 'reordered.avro'
            channel.write_bytes(payload)
            summary, code = self.run_ingest(channel, Path(directory) / 'state', reader=reader)
            self.assertEqual(code, 0)
            self.assertEqual(summary['total_events'], 1)
            row = scans(str(Path(directory) / 'state'))['events'][0]
            self.assertEqual(json.loads(row['record_json']), {
                'eventId': 'E9', 'timestamp': 42, 'delivered': True, 'note': 'unknown'})

    def test_deflate_block_over_limit_is_blocked_not_published(self):
        big = [scan_record(f'B{index}', index, False) for index in range(200)]
        raw = b''.join(encode_value(READER, record) for record in big)
        compressor = zlib.compressobj(6, zlib.DEFLATED, -15)
        compressed = compressor.compress(raw) + compressor.flush()
        self.assertLess(len(compressed), 1024)
        self.assertGreater(len(raw), 1024)
        blocks = [build_block(READER, big, codec='deflate'),
                  build_block(READER, [scan_record('OK', 1, True)], codec='deflate')]
        with tempfile.TemporaryDirectory() as directory:
            channel = Path(directory) / 'deflate.avro'
            channel.write_bytes(build_file(READER, blocks, codec='deflate'))
            summary, code = self.run_ingest(channel, Path(directory) / 'state',
                                            limits=Limits(max_block_bytes=1024))
            self.assertEqual(code, 0)
            self.assertEqual([b['block'] for b in summary['blocked_blocks']], [0])
            self.assertTrue(summary['blocked_blocks'][0]['reason'].startswith('limit:'))
            self.assertEqual([b['block'] for b in summary['committed_blocks']], [1])
            view = scans(str(Path(directory) / 'state'))
            self.assertEqual([row['event_id'] for row in view['events']], ['OK'])
            self.assertEqual(view['blocked_blocks'][0]['record_count'], 200)

    def test_file_replacement_is_quarantined(self):
        other = {'type': 'record', 'name': 'Scan', 'fields': [
            {'name': 'eventId', 'type': 'string'},
            {'name': 'timestamp', 'type': 'long'},
            {'name': 'delivered', 'type': 'boolean'},
            {'name': 'extra', 'type': 'string'}]}
        with tempfile.TemporaryDirectory() as directory:
            channel = Path(directory) / 'channel.avro'
            state = Path(directory) / 'state'
            shutil.copy(ROOT / 'fixtures/scans.avro', channel)
            summary, code = self.run_ingest(channel, state)
            self.assertEqual(code, 0)
            self.assertEqual(summary['total_events'], 3)
            records = [{'eventId': 'X1', 'timestamp': 1, 'delivered': False, 'extra': 'x'}]
            channel.write_bytes(build_file(other, [build_block(other, records)]))
            summary, code = self.run_ingest(channel, state)
            self.assertEqual(code, 3)
            self.assertEqual(summary['status'], 'quarantined')
            self.assertIn('replaced', summary['detail'])
            again, code = self.run_ingest(channel, state)
            self.assertEqual(code, 3)
            self.assertEqual(again['status'], 'quarantined')
            view = scans(str(state))
            self.assertEqual(len(view['events']), 3)
            self.assertEqual(view['sources'][0]['status'], 'quarantined')

    def test_modified_committed_prefix_is_quarantined(self):
        with tempfile.TemporaryDirectory() as directory:
            channel = Path(directory) / 'channel.avro'
            state = Path(directory) / 'state'
            shutil.copy(ROOT / 'fixtures/growing.avro', channel)
            summary, _ = self.run_ingest(channel, state)
            self.assertEqual(summary['status'], 'waiting')
            data = bytearray(channel.read_bytes())
            data[224] ^= 0xFF  # 已提交第一块负载内的字节
            channel.write_bytes(bytes(data))
            summary, code = self.run_ingest(channel, state)
            self.assertEqual(code, 3)
            self.assertEqual(summary['status'], 'quarantined')
            self.assertIn('modified', summary['detail'])
            self.assertEqual(scans(str(state))['sources'][0]['status'], 'quarantined')

    def test_corrupt_sync_is_error_and_never_scans_forward(self):
        with tempfile.TemporaryDirectory() as directory:
            channel = Path(directory) / 'channel.avro'
            state = Path(directory) / 'state'
            data = bytearray((ROOT / 'fixtures/scans.avro').read_bytes())
            data[255] ^= 0xFF  # 第二块 sync 内，第三块字节保持完好
            channel.write_bytes(bytes(data))
            summary, code = self.run_ingest(channel, state)
            self.assertEqual(code, 3)
            self.assertEqual(summary['status'], 'quarantined')
            self.assertIn('sync', summary['detail'])
            view = scans(str(state))
            self.assertEqual([row['event_id'] for row in view['events']], ['E1'])
            database = sqlite3.connect(state / 'ingest.sqlite3')
            try:
                count = database.execute('SELECT COUNT(*) FROM blocks').fetchone()[0]
            finally:
                database.close()
            self.assertEqual(count, 1)

    def test_crash_after_block_commit_keeps_events_and_checkpoint_aligned(self):
        with tempfile.TemporaryDirectory() as directory:
            channel = Path(directory) / 'channel.avro'
            state = Path(directory) / 'state'
            shutil.copy(ROOT / 'fixtures/scans.avro', channel)
            env = dict(os.environ, DELIVERYOCF_PAUSE_AFTER_COMMIT_MS='1500')
            process = subprocess.Popen(
                [sys.executable, '-m', 'deliveryocf', 'ingest', str(channel),
                 '--state', str(state), '--reader', str(ROOT / 'fixtures/reader.json')],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, cwd=ROOT, env=env)
            line = process.stdout.readline()
            process.kill()
            process.wait()
            errors = process.stderr.read()
            process.stdout.close()
            process.stderr.close()
            self.assertEqual(line.strip(), 'committed block 0', f'child stderr: {errors}')
            self.assertEqual(process.returncode, -signal.SIGKILL)
            summary, code = self.run_ingest(channel, state)
            self.assertEqual(code, 0)
            self.assertEqual([b['block'] for b in summary['committed_blocks']], [1, 2])
            self.assertEqual(summary['total_events'], 3)
            database = sqlite3.connect(state / 'ingest.sqlite3')
            try:
                rows = database.execute(
                    'SELECT event_id, COUNT(*) FROM events GROUP BY event_id').fetchall()
                self.assertEqual(sorted(rows), [('E1', 1), ('E2', 1), ('E3', 1)])
                blocks = database.execute(
                    'SELECT block_index, start_offset, end_offset FROM blocks ORDER BY block_index'
                ).fetchall()
                self.assertEqual(blocks, [(0, 220, 243), (1, 243, 266), (2, 266, 289)])
                committed_end, prefix = database.execute(
                    'SELECT committed_end, prefix_digest FROM sources').fetchone()
                import hashlib
                self.assertEqual(committed_end, len(channel.read_bytes()))
                self.assertEqual(prefix, hashlib.sha256(channel.read_bytes()).hexdigest())
            finally:
                database.close()

    def test_duplicate_across_blocks_deduped_and_conflict_blocks_publication(self):
        blocks = [
            build_block(READER, [scan_record('E1', 1, True), scan_record('E2', 2, False)]),
            build_block(READER, [scan_record('E1', 1, True)]),
            build_block(READER, [scan_record('E2', 999, True), scan_record('E3', 3, True)]),
            build_block(READER, [scan_record('E4', 4, True)]),
        ]
        with tempfile.TemporaryDirectory() as directory:
            channel = Path(directory) / 'channel.avro'
            state = Path(directory) / 'state'
            channel.write_bytes(build_file(READER, blocks))
            summary, code = self.run_ingest(channel, state)
            self.assertEqual(code, 0)
            self.assertEqual([b['block'] for b in summary['committed_blocks']], [0, 1, 3])
            self.assertEqual([b['block'] for b in summary['blocked_blocks']], [2])
            self.assertEqual(summary['duplicates'], 1)
            self.assertEqual(summary['conflicts'], ['E2'])
            self.assertEqual(summary['total_events'], 3)
            view = scans(str(state))
            self.assertEqual([row['event_id'] for row in view['events']], ['E1', 'E2', 'E4'])
            self.assertEqual(view['conflicts'][0]['event_id'], 'E2')
            self.assertNotEqual(view['conflicts'][0]['existing_canonical'],
                                view['conflicts'][0]['incoming_canonical'])
            again, _ = self.run_ingest(channel, state)
            self.assertEqual(again['committed_blocks'], [])
            self.assertEqual(again['total_events'], 3)

    def test_array_limit_and_negative_count_byte_length(self):
        writer = {'type': 'record', 'name': 'Scan', 'fields': READER['fields'] + [
            {'name': 'tags', 'type': {'type': 'array', 'items': 'string'}}]}
        reader = dict(READER, fields=READER['fields'] + [
            {'name': 'tags', 'type': {'type': 'array', 'items': 'string'}}])
        with tempfile.TemporaryDirectory() as directory:
            # 负计数数组按字节长度核验通过，正常接收
            channel = Path(directory) / 'arrays.avro'
            state = Path(directory) / 'state1'
            record = dict(scan_record('A1', 1, True), tags=NegArray(['hub', 'night']))
            channel.write_bytes(build_file(writer, [build_block(writer, [record])]))
            summary, code = self.run_ingest(channel, state, reader=reader)
            self.assertEqual(code, 0)
            self.assertEqual(summary['total_events'], 1)
            row = scans(str(state))['events'][0]
            self.assertEqual(json.loads(row['record_json'])['tags'], ['hub', 'night'])
            # 元素数超限的数组整块阻塞
            channel = Path(directory) / 'wide.avro'
            state = Path(directory) / 'state2'
            record = dict(scan_record('A2', 2, True), tags=['a', 'b', 'c', 'd'])
            channel.write_bytes(build_file(writer, [build_block(writer, [record])]))
            summary, code = self.run_ingest(channel, state, reader=reader,
                                            limits=Limits(max_array_elements=3))
            self.assertEqual(code, 0)
            self.assertEqual(summary['blocked_blocks'][0]['block'], 0)
            self.assertIn('limit:', summary['blocked_blocks'][0]['reason'])
            self.assertEqual(summary['total_events'], 0)
            # 负计数附带的字节长度被核验，不符即损坏
            channel = Path(directory) / 'broken.avro'
            state = Path(directory) / 'state3'
            body = (encode_value('string', 'A3') + encode_long(3) + b'\x01' +
                    encode_long(-1) + encode_long(999) + encode_value('string', 'x') +
                    encode_long(0))
            block = encode_long(1) + encode_long(len(body)) + body + SYNC
            channel.write_bytes(build_header(writer) + block)
            summary, code = self.run_ingest(channel, state, reader=reader)
            self.assertEqual(code, 3)
            self.assertEqual(summary['status'], 'quarantined')
            self.assertIn('corrupt', summary['detail'])

    def test_bytes_and_nullable_union_round_trip(self):
        writer = {'type': 'record', 'name': 'Scan', 'fields': [
            {'name': 'eventId', 'type': 'string'},
            {'name': 'timestamp', 'type': 'long'},
            {'name': 'delivered', 'type': 'boolean'},
            {'name': 'receipt', 'type': 'bytes'},
            {'name': 'handler', 'type': ['null', 'string']}]}
        reader = {'type': 'record', 'name': 'Scan', 'fields': [
            {'name': 'eventId', 'type': 'string'},
            {'name': 'timestamp', 'type': 'long'},
            {'name': 'delivered', 'type': 'boolean'},
            {'name': 'receipt', 'type': 'bytes'},
            {'name': 'handler', 'type': ['null', 'string']},
            {'name': 'remark', 'type': ['null', 'string'], 'default': None}]}
        records = [
            {'eventId': 'U1', 'timestamp': 5, 'delivered': True,
             'receipt': b'\xde\xad\xbe\xef', 'handler': 'night-ops'},
            {'eventId': 'U2', 'timestamp': 6, 'delivered': False,
             'receipt': b'', 'handler': None},
        ]
        with tempfile.TemporaryDirectory() as directory:
            channel = Path(directory) / 'union.avro'
            state = Path(directory) / 'state'
            channel.write_bytes(build_file(writer, [build_block(writer, records)]))
            summary, code = self.run_ingest(channel, state, reader=reader)
            self.assertEqual(code, 0)
            self.assertEqual(summary['total_events'], 2)
            rows = scans(str(state))['events']
            first = json.loads(rows[0]['record_json'])
            self.assertEqual(first['receipt'], {'bytes_hex': 'deadbeef'})
            self.assertEqual(first['handler'], 'night-ops')
            self.assertIsNone(first['remark'])
            second = json.loads(rows[1]['record_json'])
            self.assertIsNone(second['handler'])

    def test_unsupported_evolution_is_explained_before_receiving(self):
        writer = {'type': 'record', 'name': 'Scan', 'fields': [
            {'name': 'eventId', 'type': 'string'},
            {'name': 'timestamp', 'type': 'string'},
            {'name': 'delivered', 'type': 'boolean'}]}
        with tempfile.TemporaryDirectory() as directory:
            channel = Path(directory) / 'evolved.avro'
            state = Path(directory) / 'state'
            channel.write_bytes(build_file(writer, [build_block(writer, [
                {'eventId': 'E1', 'timestamp': 'tomorrow', 'delivered': True}])]))
            with self.assertRaises(ValueError) as caught:
                self.run_ingest(channel, state)
            message = str(caught.exception)
            self.assertIn('timestamp', message)
            self.assertIn('string', message)
            self.assertIn('long', message)
            self.assertEqual(scans(str(state))['sources'], [])
            with self.assertRaises(ValueError) as caught:
                self.run_ingest(channel, Path(directory) / 'state2',
                                reader={'type': 'record', 'name': 'Scan', 'fields': [
                                    {'name': 'eventId', 'type': 'string'},
                                    {'name': 'timestamp', 'type': 'string'},
                                    {'name': 'delivered', 'type': 'boolean'},
                                    {'name': 'note', 'type': 'string', 'default': 7}]})
            self.assertIn('default', str(caught.exception))


if __name__ == '__main__':
    unittest.main()
