import json
from pathlib import Path
import subprocess
import sys
import tempfile
from .materialize import main as materialize
from .source_server import running


def main():
    materialize()
    subprocess.run([sys.executable, '-m', 'deliveryocf', 'header', 'fixtures/scans.avro'], check=True)


if __name__ == '__main__':
    main()
