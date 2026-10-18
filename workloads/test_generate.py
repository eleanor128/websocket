import csv
import json
import tempfile
import unittest
from pathlib import Path

from generate import generate


class TraceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.source = self.root / "session.csv"
        with self.source.open("w", encoding="utf-8", newline="") as stream:
            writer = csv.writer(stream)
            writer.writerow(["Time", "User", "Message"])
            writer.writerows([[0, "a", " 你好,\nworld "], [0, "b", "same time"],
                              [1, "c", "c"], [2, "d", "d"], [3, "a", "again"],
                              [4, "outsider", "filtered"], [5, "a", "window excluded"]])
        self.config = {"stage": "test", "window_start_seconds": "0", "window_end_seconds": "5",
                       "selected_users": 4, "total_clients": 8, "gateways": 4,
                       "duration_seconds": "10", "rates_messages_per_second": ["1", "2", "3", "4", "5"]}

    def test_reproducibility_and_delivery_contract(self):
        first, second = self.root / "one", self.root / "two"
        manifest = generate(self.source, self.config, first)
        generate(self.source, self.config, second)
        for path in first.iterdir():
            self.assertEqual(path.read_bytes(), (second / path.name).read_bytes())
        self.assertEqual(manifest["source_rows"], 5)
        clients = json.loads((first / "clients.json").read_text(encoding="utf-8"))
        for gateway in range(4):
            self.assertEqual(sum(c["gateway_id"] == f"G{gateway}" for c in clients["clients"]), 2)
        for summary in manifest["traces"]:
            events = [json.loads(line) for line in (first / summary["file"]).read_text(encoding="utf-8").splitlines()]
            self.assertEqual(len(events), len({e["message_id"] for e in events}))
            self.assertEqual(summary["expected_deliveries"], len(events) * 8)
            self.assertEqual(events[0]["payload"], " 你好,\nworld ")
            self.assertEqual(events[0]["payload_bytes"], len(" 你好,\nworld ".encode("utf-8")))
            self.assertEqual([e["source_row_id"] for e in events[:5]], [1, 2, 3, 4, 5])
            times = [e["scheduled_offset_ns"] for e in events]
            self.assertEqual(times, sorted(times))
            self.assertTrue(all(0 <= t < 10_000_000_000 for t in times))
            self.assertEqual(times[0], times[1])
            self.assertGreater(events[-1]["repeat_index"], 0)

    def test_reject_insufficient_users(self):
        self.config["selected_users"] = 8
        with self.assertRaisesRegex(ValueError, "only"):
            generate(self.source, self.config, self.root / "out")

    def test_reject_nonfinite_rate(self):
        self.config["rates_messages_per_second"][0] = "NaN"
        with self.assertRaises(ValueError):
            generate(self.source, self.config, self.root / "out")

    def test_reject_out_of_order_source(self):
        with self.source.open("a", encoding="utf-8") as stream:
            stream.write("1,a,bad order\n")
        with self.assertRaisesRegex(ValueError, "decreasing"):
            generate(self.source, self.config, self.root / "out")

    def test_refuse_overwrite(self):
        with self.assertRaisesRegex(ValueError, "already exists"):
            generate(self.source, self.config, self.root)


if __name__ == "__main__":
    unittest.main()
