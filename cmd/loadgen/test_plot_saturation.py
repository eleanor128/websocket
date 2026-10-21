import argparse
import json
from pathlib import Path
import tempfile
import unittest

import plot_saturation as plot


def summary(rate=10.133333333, unique=121600, expected=121600, writes=608, scheduled=608, p99=8.5, lag=0.9):
    return {
        "clients": 200,
        "duration_seconds": 60,
        "scheduled_messages": scheduled,
        "scheduled_messages_per_second": rate,
        "successful_writes": writes,
        "expected_deliveries": expected,
        "unique_deliveries": unique,
        "missing": expected - unique,
        "failed_or_unsent": scheduled - writes,
        "disconnects": 0,
        "unexpected_messages": 0,
        "on_time_fraction": unique / expected,
        "scheduled_latency_p50_ms": 2.0,
        "scheduled_latency_p95_ms": 5.0,
        "scheduled_latency_p99_ms": p99,
        "send_lag_p95_ms": lag,
    }


class SaturationPlotTests(unittest.TestCase):
    def test_parse_budget(self):
        self.assertIsNone(plot.parse_budget("unlimited")[1])
        self.assertEqual(plot.parse_budget("500kbit")[1], 500_000)
        self.assertEqual(plot.parse_budget("0.5mbit")[1], 500_000)
        with self.assertRaises(ValueError):
            plot.parse_budget("fast")

    def test_overlay_coefficients_preserve_three_cluster_copies(self):
        publications = {"G0": 25, "G1": 25, "G2": 25, "G3": 25}
        direct = plot.overlay_coefficients("direct", publications)
        star = plot.overlay_coefficients("star", publications)
        tree = plot.overlay_coefficients("tree", publications)
        self.assertEqual(direct, {gateway: 0.75 for gateway in plot.GATEWAYS})
        self.assertEqual(star, {"G0": 2.25, "G1": 0.25, "G2": 0.25, "G3": 0.25})
        self.assertEqual(tree, {"G0": 1.25, "G1": 1.25, "G2": 0.25, "G3": 0.25})
        self.assertAlmostEqual(sum(direct.values()), 3.0)
        self.assertAlmostEqual(sum(star.values()), 3.0)
        self.assertAlmostEqual(sum(tree.values()), 3.0)

    def test_discover_matrix_and_single_run(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            matrix_summary = root / "rate-10" / "run" / "summary.json"
            matrix_summary.parent.mkdir(parents=True)
            matrix_summary.write_text(json.dumps(summary()), encoding="utf-8")
            self.assertEqual(plot.discover_summaries(root), [matrix_summary])
            single = root / "single"
            single.mkdir()
            single_summary = single / "summary.json"
            single_summary.write_text(json.dumps(summary()), encoding="utf-8")
            self.assertEqual(plot.discover_summaries(single), [single_summary])

    def test_observed_state_is_conservative(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            path = root / "summary.json"
            spec = plot.SeriesSpec("star", "0.5mbit", 500_000, root)
            path.write_text(json.dumps(summary()), encoding="utf-8")
            clean = plot.load_run(spec, path)
            self.assertEqual(plot.observed_state(clean, 50), "clean")

            path.write_text(json.dumps(summary(p99=1200)), encoding="utf-8")
            pressure = plot.load_run(spec, path)
            self.assertEqual(plot.observed_state(pressure, 50), "pressure")

            path.write_text(json.dumps(summary(unique=121000)), encoding="utf-8")
            limited = plot.load_run(spec, path)
            self.assertEqual(plot.observed_state(limited, 50), "delivery-limited")

    def test_build_report_writes_auditable_artifacts(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            result = root / "star-matrix"
            for target, actual, messages in ((10, 10.133333333, 608), (20, 20.5, 1230)):
                path = result / f"rate-{target}" / "run" / "summary.json"
                path.parent.mkdir(parents=True)
                expected = messages * 200
                path.write_text(json.dumps(summary(actual, expected, expected, messages, messages)), encoding="utf-8")
            manifest = root / "manifest.json"
            manifest.write_text(json.dumps({"traces": [
                {"file": "rate-10.jsonl", "scheduled_window_messages_per_second": "10.133333333", "publications_by_gateway": {"G0": 149, "G1": 155, "G2": 173, "G3": 131}},
                {"file": "rate-20.jsonl", "scheduled_window_messages_per_second": "20.5", "publications_by_gateway": {"G0": 298, "G1": 318, "G2": 350, "G3": 264}},
            ]}), encoding="utf-8")
            output = root / "report"
            args = argparse.Namespace(
                series=[f"star|0.5mbit|{result}", f"direct|unlimited|{result}"], output=output, manifest=manifest,
                message_bytes=300, send_lag_warning_ms=50.0,
            )
            plot.build_report(args)
            expected_files = {
                "index.html", "saturation_observed.svg",
                "overlay_model.svg", "saturation_data.csv", "saturation_summary.json",
            }
            self.assertEqual({path.name for path in output.iterdir()}, expected_files)
            report = json.loads((output / "saturation_summary.json").read_text(encoding="utf-8"))
            self.assertEqual(len(report["runs"]), 4)
            self.assertEqual(len(report["overlay_model"]), 4)
            star_rows = [row for row in report["overlay_model"] if row["topology"] == "star"]
            self.assertEqual(len(star_rows), 2)
            self.assertTrue(all(row["bottleneck_gateway"] == "G0" for row in star_rows))
            self.assertIn("Observed saturation signals", (output / "saturation_observed.svg").read_text(encoding="utf-8"))
            html = (output / "index.html").read_text(encoding="utf-8")
            self.assertIn('id="overlay-editor"', html)
            self.assertIn('id="overlay-s-input"', html)
            self.assertIn("Download edited SVG", html)
            self.assertIn('data-base-demand="', html)
            self.assertNotIn('<img src="overlay_model.svg"', html)
            self.assertNotIn("<th>Offered</th>", html)


if __name__ == "__main__":
    unittest.main()
