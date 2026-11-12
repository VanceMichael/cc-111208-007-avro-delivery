import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from deliveryocf.parser import inspect
from deliveryocf.journal import Journal
from deliveryocf.transport import capture
from scripts.materialize import main as materialize
from scripts.source_server import running


class BaselineTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        materialize()

    def test_capture_journal_is_content_addressed(self):
        with tempfile.TemporaryDirectory() as directory:
            journal = Journal(directory)
            try:
                first = journal.save(b'synthetic', 'source-1')
                second = journal.save(b'synthetic', 'source-1')
                self.assertEqual(first, second)
                self.assertEqual(Path(first['path']).read_bytes(), b'synthetic')
                self.assertEqual(journal.database.execute('SELECT COUNT(*) FROM captures').fetchone()[0], 1)
            finally:
                journal.close()

    def test_remote_transport_is_rejected_before_io(self):
        with self.assertRaises(ValueError):
            capture('https://example.invalid/private')

    def test_cli_help(self):
        result = subprocess.run([sys.executable, '-m', 'deliveryocf', '--help'], capture_output=True)
        self.assertEqual(result.returncode, 0)
        self.assertIn(b'header', result.stdout)

    def test_negative_count_metadata_header(self):
        result = inspect(Path('fixtures/scans.avro').read_bytes())
        self.assertEqual(result['codec'], 'null')
        self.assertEqual(result['writer_schema']['name'], 'Scan')
        self.assertEqual(len(result['sync']), 32)

    def test_partial_header_is_diagnostic(self):
        with self.assertRaises(ValueError):
            inspect(Path('fixtures/header.avro').read_bytes()[:-3])

    def test_metadata_byte_size_must_match(self):
        value = bytearray(Path('fixtures/header.avro').read_bytes())
        value[5] += 2
        with self.assertRaises(ValueError):
            inspect(bytes(value))


if __name__ == '__main__':
    unittest.main()
