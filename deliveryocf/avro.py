"""Avro 子集的模式解析、按名消解与记录解码。

只实现 docs/source-contract.md 列出的子集：record、string、int、long、
bytes、boolean、双臂 nullable union、array；不支持递归模式、map/enum/fixed
等其余 Avro 特性。writer/reader 按字段名消解，允许 writer 多字段、reader
合法默认值以及 int 到 long 提升，其余演进在接收前以 EvolutionError 拒绝。
"""
import copy
import hashlib
import io
import json

PRIMITIVES = ('null', 'boolean', 'int', 'long', 'string', 'bytes')
MISSING = object()
MAX_DEPTH = 200


class EvolutionError(ValueError):
    """writer 到 reader 的演进不在支持子集内。"""


class Corrupt(ValueError):
    """字节内容违反格式约定，属于源损坏而非等待。"""


class LimitExceeded(ValueError):
    """解压块、单条记录或数组元素超过配置上限。"""


class Limits:
    def __init__(self, max_block_bytes=1048576, max_record_bytes=65536, max_array_elements=4096):
        self.max_block_bytes = max_block_bytes
        self.max_record_bytes = max_record_bytes
        self.max_array_elements = max_array_elements


def normalize(node, path='<schema>', depth=0):
    """把 JSON 模式归一成内部元组形式，拒绝子集外的类型。"""
    if depth > MAX_DEPTH:
        raise EvolutionError(f'{path}: schema nesting exceeds {MAX_DEPTH} levels')
    if isinstance(node, str):
        if node in PRIMITIVES:
            return ('prim', node)
        raise EvolutionError(f'{path}: named or recursive type {node!r} is not supported')
    if isinstance(node, list):
        if len(node) != 2:
            raise EvolutionError(f'{path}: only two-arm nullable unions are supported')
        arms = [normalize(arm, path, depth + 1) for arm in node]
        nulls = [index for index, arm in enumerate(arms) if arm == ('prim', 'null')]
        if len(nulls) != 1:
            raise EvolutionError(f'{path}: union must be a nullable union with exactly one null arm')
        return ('union', nulls[0], arms[1 - nulls[0]])
    if isinstance(node, dict):
        kind = node.get('type')
        if kind == 'record':
            fields = []
            seen = set()
            for field in node.get('fields', []):
                name = field.get('name')
                if not isinstance(name, str) or name in seen:
                    raise EvolutionError(f'{path}: record has duplicate or missing field name')
                seen.add(name)
                default = field['default'] if 'default' in field else MISSING
                fields.append((name, normalize(field.get('type'), f'{path}.{name}', depth + 1), default))
            return ('record', node.get('name'), tuple(fields))
        if kind == 'array':
            return ('array', normalize(node.get('items'), f'{path}[]', depth + 1))
        if isinstance(kind, str) and kind in PRIMITIVES:
            return ('prim', kind)
        if isinstance(kind, (dict, list)):
            return normalize(kind, path, depth + 1)
        raise EvolutionError(f'{path}: schema type {kind!r} is not in the supported subset')
    raise EvolutionError(f'{path}: invalid schema node')


def describe(node):
    if node[0] == 'prim':
        return node[1]
    if node[0] == 'record':
        return f'record {node[1]}'
    if node[0] == 'union':
        return f'nullable union of {describe(node[2])}'
    return f'array of {describe(node[1])}'


def valid_default(node, value, path):
    """校验 reader 默认值与字段类型匹配。"""
    kind = node[0]
    if kind == 'prim':
        name = node[1]
        ok = {'null': value is None,
              'boolean': isinstance(value, bool),
              'int': isinstance(value, int) and not isinstance(value, bool),
              'long': isinstance(value, int) and not isinstance(value, bool),
              'string': isinstance(value, str),
              'bytes': isinstance(value, str)}[name]
        if not ok:
            raise EvolutionError(f'{path}: reader default {value!r} does not match {name}')
        return
    if kind == 'union':
        # Avro 默认值对应联合的第一支；nullable union 第一支必须是 null。
        if node[1] != 0 or value is not None:
            raise EvolutionError(f'{path}: nullable union reader default must be null')
        return
    if kind == 'array':
        if not isinstance(value, list):
            raise EvolutionError(f'{path}: reader default must be a list')
        for index, item in enumerate(value):
            valid_default(node[1], item, f'{path}[{index}]')
        return
    if not isinstance(value, dict):
        raise EvolutionError(f'{path}: reader default must be an object')
    for name, sub, _ in node[2]:
        if name in value:
            valid_default(sub, value[name], f'{path}.{name}')


def resolve(writer, reader, path='<root>'):
    """生成解码计划；不支持的演进抛 EvolutionError 并指明字段路径。"""
    wkind, rkind = writer[0], reader[0]
    if wkind == 'prim' and rkind == 'prim':
        wname, rname = writer[1], reader[1]
        if wname == rname:
            return ('prim', rname)
        if wname == 'int' and rname == 'long':
            return ('prim', 'long')
        raise EvolutionError(f'{path}: writer {wname} cannot be read as reader {rname}')
    if wkind == 'record' and rkind == 'record':
        if writer[1] != reader[1]:
            raise EvolutionError(f'{path}: record name mismatch, writer {writer[1]!r} vs reader {reader[1]!r}')
        wfields = {name: sub for name, sub, _ in writer[2]}
        plan = []
        for rname, rsub, rdefault in reader[2]:
            field_path = f'{path}.{rname}'
            if rname in wfields:
                plan.append((rname, resolve(wfields[rname], rsub, field_path)))
            elif rdefault is not MISSING:
                valid_default(rsub, rdefault, field_path)
                plan.append((rname, ('default', rdefault)))
            else:
                raise EvolutionError(f'{field_path}: reader field has no writer counterpart and no default')
        order = tuple((wname, wsub) for wname, wsub, _ in writer[2])
        return ('record', order, tuple(plan))
    if wkind == 'union' and rkind == 'union':
        if writer[1] != reader[1]:
            raise EvolutionError(f'{path}: union arm order differs between writer and reader')
        return ('union', writer[1], resolve(writer[2], reader[2], f'{path}[]'))
    if wkind == 'array' and rkind == 'array':
        return ('array', resolve(writer[1], reader[1], f'{path}[]'))
    raise EvolutionError(f'{path}: unsupported evolution, writer {describe(writer)} vs reader {describe(reader)}')


def discard_plan(node):
    """为 reader 不需要的 writer 字段生成跳过计划。"""
    kind = node[0]
    if kind == 'prim':
        return node
    if kind == 'record':
        order = tuple((name, sub) for name, sub, _ in node[2])
        return ('record', order, tuple((name, discard_plan(sub)) for name, sub, _ in node[2]))
    if kind == 'union':
        return ('union', node[1], discard_plan(node[2]))
    return ('array', discard_plan(node[1]))


def _read_long(stream):
    value = 0
    for shift in range(0, 70, 7):
        raw = stream.read(1)
        if not raw:
            raise Corrupt('truncated zigzag long inside block payload')
        current = raw[0]
        value |= (current & 127) << shift
        if not current & 128:
            return (value >> 1) ^ -(value & 1)
    raise Corrupt('zigzag long exceeds 64 bits')


def _read_bytes(stream, remaining):
    length = _read_long(stream)
    if length < 0:
        raise Corrupt('negative bytes length')
    if length > remaining:
        raise Corrupt('bytes length exceeds remaining block payload')
    value = stream.read(length)
    if len(value) != length:
        raise Corrupt('truncated bytes inside block payload')
    return value


def _read_array(stream, node, plan, limits, remaining):
    items = []
    total = 0
    count = _read_long(stream)
    while count:
        total += abs(count)
        if total > limits.max_array_elements:
            raise LimitExceeded(f'array has at least {total} elements, limit is {limits.max_array_elements}')
        if count < 0:
            size = _read_long(stream)
            if size < 0:
                raise Corrupt('negative array block byte size')
            start = stream.tell()
        for _ in range(abs(count)):
            items.append(_read_value(stream, node, plan, limits, remaining))
        if count < 0 and stream.tell() - start != size:
            raise Corrupt('array block byte length mismatch')
        count = _read_long(stream)
    return items


def _read_value(stream, node, plan, limits, remaining):
    kind = plan[0]
    if kind == 'prim':
        name = plan[1]
        if name == 'null':
            return None
        if name == 'boolean':
            raw = stream.read(1)
            if not raw:
                raise Corrupt('truncated boolean inside block payload')
            if raw[0] not in (0, 1):
                raise Corrupt('boolean value is not 0 or 1')
            return bool(raw[0])
        if name in ('int', 'long'):
            return _read_long(stream)
        left = remaining()
        if name == 'string':
            try:
                return _read_bytes(stream, left).decode('utf-8')
            except UnicodeDecodeError as error:
                raise Corrupt(f'invalid utf-8 string: {error}') from error
        return _read_bytes(stream, left)
    if kind == 'default':
        return copy.deepcopy(plan[1])
    if kind == 'record':
        _, order, fields = plan
        plans = dict(fields)
        decoded = {}
        for wname, wsub in order:
            target = plans.get(wname)
            decoded[wname] = _read_value(stream, wsub, target if target else discard_plan(wsub),
                                         limits, remaining)
        return {rname: decoded[rname] if rname in decoded and plans.get(rname, (None,))[0] != 'default'
                else _read_value(stream, None, sub, limits, remaining) for rname, sub in fields}
    if kind == 'union':
        index = _read_long(stream)
        if index not in (0, 1):
            raise Corrupt('union branch index out of range')
        if index == plan[1]:
            return None
        return _read_value(stream, node[2], plan[2], limits, remaining)
    if kind == 'array':
        return _read_array(stream, node[1], plan[1], limits, remaining)
    raise Corrupt(f'unknown decode plan {kind!r}')


def decode_block(payload, writer, plan, count, limits):
    """把解压负载解码为 count 条 reader 形态记录，并核对记录数与负载边界。"""
    stream = io.BytesIO(payload)

    def remaining():
        return len(payload) - stream.tell()

    records = []
    for _ in range(count):
        start = stream.tell()
        records.append(_read_value(stream, writer, plan, limits, remaining))
        consumed = stream.tell() - start
        if consumed > limits.max_record_bytes:
            raise LimitExceeded(f'record uses {consumed} bytes, limit is {limits.max_record_bytes}')
    if stream.tell() != len(payload):
        raise Corrupt(f'block declares {count} records but {len(payload) - stream.tell()} payload bytes remain')
    return records


def _encode_long(value):
    out = bytearray()
    bits = (value << 1) ^ (value >> 63)
    while bits & ~127:
        out.append((bits & 127) | 128)
        bits >>= 7
    out.append(bits)
    return bytes(out)


def encode_value(node, value):
    """按 reader 模式做确定性编码，用于业务记录的规范判等。"""
    kind = node[0]
    if kind == 'prim':
        name = node[1]
        if name == 'null':
            return b''
        if name == 'boolean':
            return b'\x01' if value else b'\x00'
        if name in ('int', 'long'):
            return _encode_long(value)
        raw = value.encode('utf-8') if name == 'string' else bytes(value)
        return _encode_long(len(raw)) + raw
    if kind == 'record':
        return b''.join(encode_value(sub, value[name]) for name, sub, _ in node[2])
    if kind == 'union':
        if value is None:
            return _encode_long(node[1])
        return _encode_long(1 - node[1]) + encode_value(node[2], value)
    out = bytearray()
    if value:
        out += _encode_long(len(value))
        for item in value:
            out += encode_value(node[1], item)
    return bytes(out) + _encode_long(0)


def canonical(node, record):
    return encode_value(node, record).hex()


def schema_fingerprint(schema):
    """writer 模式的确定性指纹，只保留结构信息。"""
    def strip(node):
        if isinstance(node, str):
            return node
        if isinstance(node, list):
            return [strip(arm) for arm in node]
        keep = {}
        for key in ('type', 'name', 'items'):
            if key in node:
                keep[key] = strip(node[key])
        if 'fields' in node:
            keep['fields'] = [{'name': field.get('name'), 'type': strip(field.get('type'))}
                              for field in node['fields']]
        return keep
    text = json.dumps(strip(schema), sort_keys=True, separators=(',', ':'))
    return hashlib.sha256(text.encode()).hexdigest()
