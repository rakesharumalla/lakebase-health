"""scripts/report_acceptance.py helpers that criterion 3 and 6 depend on (found in the live run, Task 18)."""
import sys
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO / "scripts"))
import report_acceptance as ra  # noqa: E402


def test_a_table_row_is_compared_on_its_data_bytes_not_its_index_total():
    kv = {"table_name": "kv_store", "data_bytes": "6889472", "index_bytes": "306749440"}
    assert ra.inventory_size(kv) == 6889472


def test_a_table_without_indexes_still_reads_its_data_bytes():
    audit = {"table_name": "audit_events", "data_bytes": "770048", "index_bytes": "0"}
    assert ra.inventory_size(audit) == 770048


def test_an_index_row_reads_its_index_bytes():
    assert ra.inventory_size({"table_name": "orders", "index_name": "orders_pkey", "index_bytes": "45154304"}) == 45154304


def test_raw_get_finds_a_lowercase_header_by_any_case():
    class H(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header("content-disposition", 'attachment; filename="x.html"')
            self.end_headers()
            self.wfile.write(b"ok")

        def log_message(self, *a):
            pass

    srv = HTTPServer(("127.0.0.1", 0), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    try:
        status, body, headers = ra.raw_get(f"http://127.0.0.1:{srv.server_port}/", "t")
    finally:
        srv.shutdown()
    assert (status, body) == (200, "ok")
    assert headers.get("Content-Disposition") == 'attachment; filename="x.html"'
