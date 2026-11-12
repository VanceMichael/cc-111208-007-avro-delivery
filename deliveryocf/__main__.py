import argparse
import json
import os
from pathlib import Path
import sys
import time
from .avro import Limits
from .ingest import ingest, scans
from .parser import inspect
from .journal import Journal
from .transport import capture


def main():
    parser = argparse.ArgumentParser(description='配送扫描 Avro OCF 文件头查看器与逐块接收器。')
    sub = parser.add_subparsers(dest='command', required=True)
    view = sub.add_parser('header', help='诊断单个 OCF 文件头与元数据')
    view.add_argument('source')
    raw = sub.add_parser('capture', help='留存 loopback HTTP 原始响应')
    raw.add_argument('url')
    raw.add_argument('--state', default='var')
    raw.add_argument('--method', default='GET')
    raw.add_argument('--body')
    raw.add_argument('--header', action='append', default=[])
    recv = sub.add_parser('ingest', help='逐块接收本地 OCF 来源，断点续收')
    recv.add_argument('source')
    recv.add_argument('--state', default='var')
    recv.add_argument('--reader', required=True, help='reader 模式 JSON 配置')
    recv.add_argument('--name', help='来源身份名称，默认取文件名')
    recv.add_argument('--max-block-bytes', type=int, default=1048576, help='解压后单块字节上限')
    recv.add_argument('--max-record-bytes', type=int, default=65536, help='单条记录字节上限')
    recv.add_argument('--max-array-elements', type=int, default=4096, help='单数组元素数上限')
    query = sub.add_parser('scans', help='关联查询已接收扫描、阻塞块与来源身份')
    query.add_argument('--state', default='var')
    query.add_argument('--source')
    args = parser.parse_args()
    try:
        if args.command == 'capture':
            headers = dict(value.split(':', 1) for value in args.header)
            headers = {key.strip(): value.strip() for key, value in headers.items()}
            status, response_headers, payload = capture(args.url, args.method,
                Path(args.body).read_bytes() if args.body else None, headers)
            journal = Journal(args.state)
            try:
                result = journal.save(payload, args.url)
            finally:
                journal.close()
            result.update({'http_status': status, 'headers': response_headers})
        elif args.command == 'ingest':
            reader_schema = json.loads(Path(args.reader).read_text())
            limits = Limits(args.max_block_bytes, args.max_record_bytes, args.max_array_elements)
            pause_ms = int(os.environ.get('DELIVERYOCF_PAUSE_AFTER_COMMIT_MS', '0'))

            def on_commit(index):
                if pause_ms:
                    print(f'committed block {index}', flush=True)
                    time.sleep(pause_ms / 1000)

            result, code = ingest(args.source, args.state, reader_schema,
                                  name=args.name, limits=limits,
                                  on_commit=on_commit if pause_ms else None)
            print(json.dumps(result, ensure_ascii=True, sort_keys=True))
            if result['status'] == 'quarantined':
                print(result['detail'], file=sys.stderr)
            return code
        elif args.command == 'scans':
            result = scans(args.state, args.source)
        else:
            result = inspect(Path(args.source).read_bytes())
        print(json.dumps(result, ensure_ascii=True, sort_keys=True))
    except (ValueError, OSError) as error:
        print(str(error), file=sys.stderr)
        return 2
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
