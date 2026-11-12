import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
from .materialize import main as materialize

ROOT = Path(__file__).resolve().parent.parent


def run(*args):
    result = subprocess.run([sys.executable, '-m', 'deliveryocf', *args],
                            capture_output=True, text=True, cwd=ROOT)
    print('$ python -m deliveryocf', ' '.join(str(arg) for arg in args))
    print(result.stdout.strip())
    if result.returncode:
        print(result.stderr.strip(), file=sys.stderr)
    return result


def main():
    materialize()
    print('== 文件头诊断（原有能力，保留） ==')
    run('header', 'fixtures/scans.avro')
    with tempfile.TemporaryDirectory() as work:
        channel = Path(work) / 'channel-night.avro'
        state = Path(work) / 'state'
        reader = 'fixtures/reader.json'
        print()
        print('== 第一次接收：前两块到齐，末块缺尾 ==')
        shutil.copy(ROOT / 'fixtures/growing.avro', channel)
        first = run('ingest', channel, '--state', state, '--reader', reader)
        summary = json.loads(first.stdout)
        assert first.returncode == 0 and summary['status'] == 'waiting'
        assert [block['block'] for block in summary['committed_blocks']] == [0, 1]
        print()
        print('== 末块补齐后第二次接收：只新增第三块 ==')
        shutil.copy(ROOT / 'fixtures/scans.avro', channel)
        second = run('ingest', channel, '--state', state, '--reader', reader)
        summary = json.loads(second.stdout)
        assert second.returncode == 0 and summary['status'] == 'complete'
        assert [block['block'] for block in summary['committed_blocks']] == [2]
        assert summary['total_events'] == 3
        print()
        print('== 关联查询：扫描、块偏移、writer 指纹与来源身份 ==')
        run('scans', '--state', state)
    print()
    print('demo ok')


if __name__ == '__main__':
    main()
