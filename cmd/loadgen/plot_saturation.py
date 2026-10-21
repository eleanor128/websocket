"""Build a self-contained saturation report from load-generator result matrices.

The report deliberately separates observed load-response signals from the
proposal's analytical overlay-bandwidth estimate.  It uses only the Python
standard library so it can run anywhere the existing Python matrix runners run.
"""

from __future__ import annotations

import argparse
import csv
from dataclasses import asdict, dataclass
from html import escape
import json
import math
from pathlib import Path
import re
from statistics import fmean
from typing import Callable, Iterable


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_MANIFEST = ROOT / "workloads/generated/pilot-v1/manifest.json"
TOPOLOGIES = ("direct", "star", "tree")
GATEWAYS = ("G0", "G1", "G2", "G3")
COLORS = ("#E69F00", "#56B4E9", "#009E73", "#D55E00", "#0072B2", "#CC79A7", "#6B7280")
STATE_LABEL = {
    "clean": "No observed saturation",
    "pressure": "Latency / scheduler pressure",
    "delivery-limited": "Delivery limited",
}


@dataclass(frozen=True)
class SeriesSpec:
    topology: str
    budget_label: str
    budget_bps: float | None
    path: Path

    @property
    def label(self) -> str:
        return f"{self.topology} / {self.budget_label}"


@dataclass(frozen=True)
class RunRecord:
    topology: str
    budget_label: str
    budget_bps: float | None
    source: str
    target_rate: int
    offered_rate: float
    clients: int
    duration_seconds: float
    scheduled_messages: int
    successful_writes: int
    expected_deliveries: int
    unique_deliveries: int
    missing: int
    failed_or_unsent: int
    disconnects: int
    unexpected_messages: int
    on_time_fraction: float
    scheduled_latency_p50_ms: float
    scheduled_latency_p95_ms: float
    scheduled_latency_p99_ms: float
    send_lag_p95_ms: float

    @property
    def series_label(self) -> str:
        return f"{self.topology} / {self.budget_label}"

    @property
    def delivery_fraction(self) -> float:
        return self.unique_deliveries / self.expected_deliveries if self.expected_deliveries else 0.0

    @property
    def send_success_fraction(self) -> float:
        return self.successful_writes / self.scheduled_messages if self.scheduled_messages else 0.0

    @property
    def achieved_publication_rate(self) -> float:
        denominator = self.duration_seconds * self.clients
        return self.unique_deliveries / denominator if denominator else 0.0


def parse_budget(value: str) -> tuple[str, float | None]:
    label = value.strip()
    if label.lower() in {"unlimited", "none", "unshaped"}:
        return label, None
    match = re.fullmatch(r"([0-9]+(?:\.[0-9]+)?)\s*(bps|kbit|kbps|mbit|mbps|gbit|gbps)", label.lower())
    if not match:
        raise ValueError(f"invalid budget {value!r}; use unlimited, 500kbit, 0.5mbit, etc.")
    scale = {
        "bps": 1,
        "kbit": 1_000,
        "kbps": 1_000,
        "mbit": 1_000_000,
        "mbps": 1_000_000,
        "gbit": 1_000_000_000,
        "gbps": 1_000_000_000,
    }[match.group(2)]
    return label, float(match.group(1)) * scale


def parse_series(value: str) -> SeriesSpec:
    parts = value.split("|", 2)
    if len(parts) != 3:
        raise ValueError("series must be TOPOLOGY|BUDGET|PATH")
    topology = parts[0].strip().lower()
    if topology not in TOPOLOGIES:
        raise ValueError(f"invalid topology {topology!r}; expected one of {', '.join(TOPOLOGIES)}")
    budget_label, budget_bps = parse_budget(parts[1])
    path = Path(parts[2].strip()).resolve()
    if not path.exists():
        raise ValueError(f"result path does not exist: {path}")
    return SeriesSpec(topology, budget_label, budget_bps, path)


def discover_summaries(path: Path) -> list[Path]:
    if path.is_file():
        if path.name != "summary.json":
            raise ValueError(f"expected a summary.json file, got: {path}")
        return [path]
    direct = path / "summary.json"
    nested_run = path / "run" / "summary.json"
    if direct.exists():
        return [direct]
    if nested_run.exists():
        return [nested_run]
    matrix = sorted(path.glob("rate-*/run/summary.json"), key=infer_rate_from_path)
    if matrix:
        return matrix
    raise ValueError(
        f"no summaries found under {path}; expected summary.json, run/summary.json, "
        "or rate-*/run/summary.json"
    )


def infer_rate_from_path(path: Path) -> int:
    for part in reversed(path.parts):
        match = re.fullmatch(r"rate-(\d+)", part)
        if match:
            return int(match.group(1))
    return 0


def target_rate(summary_path: Path, summary: dict) -> int:
    inferred = infer_rate_from_path(summary_path)
    if inferred:
        return inferred
    actual = float(summary.get("scheduled_messages_per_second", 0.0))
    known = (10, 20, 40, 80, 160)
    if actual > 0:
        return min(known, key=lambda value: abs(actual - value))
    raise ValueError(f"cannot determine target rate from {summary_path}")


def number(summary: dict, key: str, default: float = 0.0) -> float:
    value = summary.get(key, default)
    if not isinstance(value, (int, float)):
        raise ValueError(f"summary field {key!r} must be numeric")
    return float(value)


def load_run(spec: SeriesSpec, summary_path: Path) -> RunRecord:
    try:
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"cannot read {summary_path}: {exc}") from exc
    required = (
        "clients", "duration_seconds", "scheduled_messages", "successful_writes",
        "expected_deliveries", "unique_deliveries",
    )
    missing_fields = [key for key in required if key not in summary]
    if missing_fields:
        raise ValueError(f"{summary_path} is missing fields: {', '.join(missing_fields)}")
    return RunRecord(
        topology=spec.topology,
        budget_label=spec.budget_label,
        budget_bps=spec.budget_bps,
        source=str(summary_path),
        target_rate=target_rate(summary_path, summary),
        offered_rate=number(summary, "scheduled_messages_per_second"),
        clients=int(number(summary, "clients")),
        duration_seconds=number(summary, "duration_seconds"),
        scheduled_messages=int(number(summary, "scheduled_messages")),
        successful_writes=int(number(summary, "successful_writes")),
        expected_deliveries=int(number(summary, "expected_deliveries")),
        unique_deliveries=int(number(summary, "unique_deliveries")),
        missing=int(number(summary, "missing")),
        failed_or_unsent=int(number(summary, "failed_or_unsent")),
        disconnects=int(number(summary, "disconnects")),
        unexpected_messages=int(number(summary, "unexpected_messages")),
        on_time_fraction=number(summary, "on_time_fraction"),
        scheduled_latency_p50_ms=number(summary, "scheduled_latency_p50_ms"),
        scheduled_latency_p95_ms=number(summary, "scheduled_latency_p95_ms"),
        scheduled_latency_p99_ms=number(summary, "scheduled_latency_p99_ms"),
        send_lag_p95_ms=number(summary, "send_lag_p95_ms"),
    )


def observed_state(run: RunRecord, send_lag_warning_ms: float) -> str:
    if (
        run.delivery_fraction < 0.999
        or run.send_success_fraction < 0.999
        or run.disconnects > 0
        or run.unexpected_messages > 0
    ):
        return "delivery-limited"
    if (
        run.on_time_fraction < 0.99
        or run.scheduled_latency_p99_ms > 1000.0
        or run.send_lag_p95_ms > send_lag_warning_ms
    ):
        return "pressure"
    return "clean"


def overlay_coefficients(topology: str, publications: dict[str, int]) -> dict[str, float]:
    total = sum(publications.get(gateway, 0) for gateway in GATEWAYS)
    if total <= 0:
        raise ValueError("publication counts must have a positive total")
    p = {gateway: publications.get(gateway, 0) / total for gateway in GATEWAYS}
    if topology == "direct":
        return {gateway: 3.0 * p[gateway] for gateway in GATEWAYS}
    if topology == "star":
        return {"G0": 2.0 + p["G0"], "G1": p["G1"], "G2": p["G2"], "G3": p["G3"]}
    if topology == "tree":
        return {"G0": 1.0 + p["G0"], "G1": 1.0 + p["G1"], "G2": p["G2"], "G3": p["G3"]}
    raise ValueError(f"unsupported topology: {topology}")


def load_manifest(path: Path) -> dict[int, dict]:
    manifest = json.loads(path.read_text(encoding="utf-8"))
    result = {}
    for entry in manifest.get("traces", []):
        match = re.fullmatch(r"rate-(\d+)\.jsonl", entry.get("file", ""))
        if match:
            result[int(match.group(1))] = entry
    if not result:
        raise ValueError(f"manifest has no rate traces: {path}")
    return result


def overlay_model(
    specs: Iterable[SeriesSpec], rates: Iterable[int], manifest: dict[int, dict], message_bytes: float
) -> list[dict]:
    rows = []
    unique_specs = {(spec.topology, spec.budget_label, spec.budget_bps) for spec in specs}
    unique_rates = sorted(set(rates))
    for topology, budget_label, budget_bps in sorted(unique_specs):
        for rate in unique_rates:
            entry = manifest.get(rate)
            if not entry:
                continue
            publications = entry["publications_by_gateway"]
            coefficients = overlay_coefficients(topology, publications)
            actual_rate = float(entry["scheduled_window_messages_per_second"])
            demand = {
                gateway: 8.0 * actual_rate * message_bytes * coefficients[gateway]
                for gateway in GATEWAYS
            }
            bottleneck = max(demand, key=demand.get)
            max_bps = demand[bottleneck]
            rows.append({
                "topology": topology,
                "budget_label": budget_label,
                "budget_bps": budget_bps,
                "target_rate": rate,
                "message_bytes": message_bytes,
                "coefficients": coefficients,
                "demand_bps_by_gateway": demand,
                "bottleneck_gateway": bottleneck,
                "max_demand_bps": max_bps,
                "estimated_utilization": max_bps / budget_bps if budget_bps else None,
            })
    return rows


def grouped(records: Iterable[RunRecord]) -> dict[str, dict[int, list[RunRecord]]]:
    result: dict[str, dict[int, list[RunRecord]]] = {}
    for record in records:
        result.setdefault(record.series_label, {}).setdefault(record.target_rate, []).append(record)
    return result


def mean_range(values: list[float]) -> tuple[float, float, float]:
    return fmean(values), min(values), max(values)


def nice_ticks(low: float, high: float, count: int = 5) -> list[float]:
    if not math.isfinite(low) or not math.isfinite(high):
        return [0.0, 1.0]
    if high <= low:
        padding = max(abs(high) * 0.1, 1.0)
        low, high = low - padding, high + padding
    raw = (high - low) / max(count - 1, 1)
    magnitude = 10 ** math.floor(math.log10(raw))
    step = min((1, 2, 5, 10), key=lambda value: abs(raw - value * magnitude)) * magnitude
    start = math.floor(low / step) * step
    end = math.ceil(high / step) * step
    ticks = []
    value = start
    while value <= end + step * 0.1 and len(ticks) < 20:
        ticks.append(value)
        value += step
    return ticks


def fmt_tick(value: float) -> str:
    if abs(value) >= 1000:
        return f"{value:,.0f}"
    if abs(value) >= 10:
        return f"{value:.0f}"
    return f"{value:.1f}".rstrip("0").rstrip(".")


class SVG:
    def __init__(self, width: int, height: int):
        self.width, self.height = width, height
        self.parts = [
            f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
            '<rect width="100%" height="100%" fill="#FFFFFF"/>',
            '<style>text{font-family:Arial,Helvetica,sans-serif;fill:#263238}.title{font-size:22px;font-weight:700}.panel-title{font-size:15px;font-weight:700}.axis{font-size:11px}.legend{font-size:11px}.note{font-size:10px;fill:#5F6B76}</style>',
        ]

    def add(self, value: str) -> None:
        self.parts.append(value)

    def text(self, x: float, y: float, value: str, css: str = "axis", anchor: str = "start", extra: str = "") -> None:
        self.add(f'<text x="{x:.1f}" y="{y:.1f}" class="{css}" text-anchor="{anchor}" {extra}>{escape(value)}</text>')

    def finish(self) -> str:
        return "\n".join(self.parts + ["</svg>", ""])


def draw_panel(
    svg: SVG,
    bounds: tuple[float, float, float, float],
    title: str,
    ylabel: str,
    data: dict[str, dict[int, list[RunRecord]]],
    metric: Callable[[RunRecord], float],
    colors: dict[str, str],
    fixed_y: tuple[float, float] | None = None,
    ideal_line: bool = False,
) -> None:
    x0, y0, width, height = bounds
    left, top, right, bottom = 64, 30, 18, 47
    px0, py0 = x0 + left, y0 + top
    pw, ph = width - left - right, height - top - bottom
    points = [(rate, metric(run)) for rates in data.values() for rate, runs in rates.items() for run in runs]
    if not points:
        return
    x_values = [point[0] for point in points]
    y_values = [point[1] for point in points]
    x_min, x_max = min(x_values), max(x_values)
    if x_min == x_max:
        x_min, x_max = 0, max(1, x_max * 1.1)
    if fixed_y:
        y_min, y_max = fixed_y
    else:
        y_min = min(0.0, min(y_values))
        y_max = max(y_values)
        y_max = y_max * 1.12 if y_max > 0 else 1.0
    y_ticks = nice_ticks(y_min, y_max)
    y_min, y_max = y_ticks[0], y_ticks[-1]
    sx = lambda value: px0 + (value - x_min) / (x_max - x_min) * pw
    sy = lambda value: py0 + ph - (value - y_min) / (y_max - y_min) * ph

    svg.text(x0, y0 + 16, title, "panel-title")
    for tick in y_ticks:
        y = sy(tick)
        svg.add(f'<line x1="{px0:.1f}" y1="{y:.1f}" x2="{px0 + pw:.1f}" y2="{y:.1f}" stroke="#E5E7EB" stroke-width="1"/>')
        svg.text(px0 - 8, y + 4, fmt_tick(tick), anchor="end")
    x_ticks = sorted(set(x_values))
    for tick in x_ticks:
        x = sx(tick)
        svg.add(f'<line x1="{x:.1f}" y1="{py0:.1f}" x2="{x:.1f}" y2="{py0 + ph:.1f}" stroke="#F3F4F6" stroke-width="1"/>')
        svg.text(x, py0 + ph + 18, str(tick), anchor="middle")
    svg.add(f'<line x1="{px0:.1f}" y1="{py0 + ph:.1f}" x2="{px0 + pw:.1f}" y2="{py0 + ph:.1f}" stroke="#455A64"/>')
    svg.add(f'<line x1="{px0:.1f}" y1="{py0:.1f}" x2="{px0:.1f}" y2="{py0 + ph:.1f}" stroke="#455A64"/>')
    svg.text(px0 + pw / 2, y0 + height - 6, "Target publication rate (msg/s)", anchor="middle")
    svg.text(x0 + 12, py0 + ph / 2, ylabel, anchor="middle", extra=f'transform="rotate(-90 {x0 + 12:.1f} {py0 + ph / 2:.1f})"')

    if ideal_line:
        lo, hi = max(x_min, y_min), min(x_max, y_max)
        if hi > lo:
            svg.add(f'<line x1="{sx(lo):.1f}" y1="{sy(lo):.1f}" x2="{sx(hi):.1f}" y2="{sy(hi):.1f}" stroke="#6B7280" stroke-width="1.5" stroke-dasharray="6 5"/>')

    for label, rates in data.items():
        color = colors[label]
        aggregates = []
        for rate, runs in sorted(rates.items()):
            mean, low, high = mean_range([metric(run) for run in runs])
            aggregates.append((rate, mean, low, high))
        path = " ".join(("M" if index == 0 else "L") + f" {sx(rate):.1f} {sy(mean):.1f}" for index, (rate, mean, _, _) in enumerate(aggregates))
        svg.add(f'<path d="{path}" fill="none" stroke="{color}" stroke-width="2.4"/>')
        for rate, mean, low, high in aggregates:
            x, y = sx(rate), sy(mean)
            if high > low:
                svg.add(f'<line x1="{x:.1f}" y1="{sy(low):.1f}" x2="{x:.1f}" y2="{sy(high):.1f}" stroke="{color}" stroke-width="1.2"/>')
                svg.add(f'<line x1="{x - 4:.1f}" y1="{sy(low):.1f}" x2="{x + 4:.1f}" y2="{sy(low):.1f}" stroke="{color}"/>')
                svg.add(f'<line x1="{x - 4:.1f}" y1="{sy(high):.1f}" x2="{x + 4:.1f}" y2="{sy(high):.1f}" stroke="{color}"/>')
            svg.add(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="4" fill="{color}" stroke="#FFFFFF" stroke-width="1"/>')


def observed_svg(records: list[RunRecord]) -> str:
    data = grouped(records)
    labels = sorted(data)
    colors = {label: COLORS[index % len(COLORS)] for index, label in enumerate(labels)}
    svg = SVG(1240, 900)
    svg.text(42, 38, "Observed saturation signals", "title")
    svg.text(42, 58, "Points are means; error bars show min-max across repeated runs.", "note")
    panel_bounds = ((30, 80, 580, 340), (630, 80, 580, 340), (30, 440, 580, 340), (630, 440, 580, 340))
    draw_panel(svg, panel_bounds[0], "A. Tail latency", "Scheduled P99 (ms)", data, lambda run: run.scheduled_latency_p99_ms, colors)
    minimum_delivery = min(run.delivery_fraction * 100 for run in records)
    delivery_floor = 95.0 if minimum_delivery >= 95.0 else 0.0
    draw_panel(svg, panel_bounds[1], "B. Delivery completeness", "Unique / expected (%)", data, lambda run: run.delivery_fraction * 100, colors, (delivery_floor, 100.0))
    draw_panel(svg, panel_bounds[2], "C. Achieved delivery throughput", "Publication-equivalent msg/s", data, lambda run: run.achieved_publication_rate, colors, ideal_line=True)
    draw_panel(svg, panel_bounds[3], "D. Generator schedule health", "Send lag P95 (ms)", data, lambda run: run.send_lag_p95_ms, colors)
    legend_y = 815
    for index, label in enumerate(labels):
        column = index % 3
        row = index // 3
        x, y = 50 + column * 390, legend_y + row * 22
        svg.add(f'<line x1="{x}" y1="{y}" x2="{x + 25}" y2="{y}" stroke="{colors[label]}" stroke-width="3"/>')
        svg.text(x + 34, y + 4, label, "legend")
    svg.text(1190, 878, "Generated from loadgen summary.json", "note", anchor="end")
    return svg.finish()


def overlay_svg(rows: list[dict], message_bytes: float) -> str:
    labels = sorted({f"{row['topology']} / {row['budget_label']}" for row in rows})
    rates = sorted({row["target_rate"] for row in rows})
    lookup = {(f"{row['topology']} / {row['budget_label']}", row["target_rate"]): row for row in rows}
    cell_w, cell_h = 140, 58
    width = 360 + cell_w * len(rates)
    height = 150 + cell_h * len(labels) + 110
    svg = SVG(width, height)
    svg.text(34, 38, "Proposal-model overlay demand", "title")
    svg.text(34, 60, f"Estimated max-node egress using B = 8 × lambda × S × a_i and S = {message_bytes:g} bytes; not measured traffic.", "note")
    start_x, start_y = 320, 108
    for column, rate in enumerate(rates):
        svg.text(start_x + column * cell_w + cell_w / 2, start_y - 18, f"rate {rate}", "panel-title", anchor="middle")
    for row_index, label in enumerate(labels):
        y = start_y + row_index * cell_h
        svg.text(start_x - 15, y + 35, label, "axis", anchor="end")
        for column, rate in enumerate(rates):
            x = start_x + column * cell_w
            item = lookup.get((label, rate))
            if not item:
                fill, value, detail = "#E5E7EB", "no data", ""
            elif item["estimated_utilization"] is None:
                fill = "#56B4E9"
                value = f"{item['max_demand_bps'] / 1_000_000:.3f} Mbps"
                detail = item["bottleneck_gateway"]
            else:
                utilization = item["estimated_utilization"]
                fill = "#009E73" if utilization < 0.8 else ("#E69F00" if utilization < 1.0 else "#D55E00")
                value = f"{utilization * 100:.0f}%"
                detail = f"{item['bottleneck_gateway']} max"
            svg.add(f'<rect x="{x + 2}" y="{y + 2}" width="{cell_w - 4}" height="{cell_h - 4}" rx="5" fill="{fill}" fill-opacity="0.86"/>')
            svg.text(x + cell_w / 2, y + 27, value, "axis", anchor="middle", extra='style="fill:white;font-weight:700"')
            svg.text(x + cell_w / 2, y + 43, detail, "note", anchor="middle", extra='style="fill:white"')
    legend_y = start_y + len(labels) * cell_h + 34
    # svg.text(35, legend_y, "Capped rows: green <80%, amber 80-99%, red >=100%. Unlimited rows show estimated max-node Mbps in blue.", "note")
    svg.text(35, legend_y + 22, "Configured budget is supplied metadata; this figure does not verify that traffic shaping was active.", "note")
    return svg.finish()


def write_csv(path: Path, records: list[RunRecord], send_lag_warning_ms: float) -> None:
    fields = [
        "topology", "budget_label", "budget_bps", "target_rate", "offered_rate", "clients",
        "scheduled_messages", "successful_writes", "send_success_fraction", "expected_deliveries",
        "unique_deliveries", "delivery_fraction", "achieved_publication_rate", "missing",
        "failed_or_unsent", "disconnects", "unexpected_messages", "on_time_fraction",
        "scheduled_latency_p50_ms", "scheduled_latency_p95_ms", "scheduled_latency_p99_ms",
        "send_lag_p95_ms", "observed_state", "source",
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for run in sorted(records, key=lambda item: (item.topology, item.budget_label, item.target_rate, item.source)):
            row = asdict(run)
            row.update({
                "send_success_fraction": run.send_success_fraction,
                "delivery_fraction": run.delivery_fraction,
                "achieved_publication_rate": run.achieved_publication_rate,
                "observed_state": observed_state(run, send_lag_warning_ms),
            })
            writer.writerow({field: row.get(field) for field in fields})


def overlay_editor(rows: list[dict], message_bytes: float) -> str:
    if not rows:
        return ""
    labels = sorted({f"{row['topology']} / {row['budget_label']}" for row in rows})
    rates = sorted({row["target_rate"] for row in rows})
    lookup = {(f"{row['topology']} / {row['budget_label']}", row["target_rate"]): row for row in rows}
    controls = []
    table_rows = []
    for index, label in enumerate(labels):
        sample = next(row for row in rows if f"{row['topology']} / {row['budget_label']}" == label)
        budget_mbps = "" if sample["budget_bps"] is None else f"{sample['budget_bps'] / 1_000_000:g}"
        controls.append(
            f'<fieldset><legend>Series {index + 1}</legend>'
            f'<label>Label<input class="series-label" data-series="s{index}" value="{escape(label)}"></label>'
            f'<label>Budget (Mbps)<input class="series-budget" data-series="s{index}" type="number" min="0" step="0.001" '
            f'value="{budget_mbps}" placeholder="unlimited"></label></fieldset>'
        )
        cells = []
        for rate in rates:
            item = lookup.get((label, rate))
            if item is None:
                cells.append('<td class="overlay-cell no-data">no data</td>')
            else:
                cells.append(
                    f'<td class="overlay-cell" data-series="s{index}" data-base-demand="{item["max_demand_bps"]:.12g}" '
                    f'data-gateway="{escape(item["bottleneck_gateway"])}"><strong></strong><small></small></td>'
                )
        table_rows.append(
            f'<tr><th scope="row" data-series-label="s{index}">{escape(label)}</th>{"".join(cells)}</tr>'
        )
    headers = "".join(f'<th scope="col">rate {rate}</th>' for rate in rates)
    return f"""
<section id="overlay-editor" data-base-s="{message_bytes:g}">
  <div class="overlay-controls">
    <label>Figure title<input id="overlay-title-input" value="Proposal-model overlay demand"></label>
    <label>S (serialized bytes)<input id="overlay-s-input" type="number" min="1" step="0.01" value="{message_bytes:g}"></label>
    {''.join(controls)}
    <button id="overlay-download" type="button">Download edited SVG</button>
  </div>
  <p id="overlay-error" class="input-error" role="alert" hidden></p>
  <h3 id="overlay-live-title">Proposal-model overlay demand</h3>
  <div class="overlay-table-wrap"><table class="overlay-grid" aria-label="Editable overlay budget model">
    <thead><tr><th scope="col">Topology / budget</th>{headers}</tr></thead>
    <tbody>{''.join(table_rows)}</tbody>
  </table></div>
  <p class="note">Green &lt;80%, amber 80–99%, red ≥100%. Unlimited rows show max-node Mbps. Values are analytical, not observed traffic.</p>
</section>
<script>
(() => {{
  const root = document.getElementById('overlay-editor');
  if (!root) return;
  const baseS = Number(root.dataset.baseS);
  const sInput = document.getElementById('overlay-s-input');
  const titleInput = document.getElementById('overlay-title-input');
  const title = document.getElementById('overlay-live-title');
  const error = document.getElementById('overlay-error');
  const budgets = () => Object.fromEntries([...root.querySelectorAll('.series-budget')].map(input => [input.dataset.series, input.value.trim() === '' ? null : Number(input.value) * 1e6]));
  const labels = () => Object.fromEntries([...root.querySelectorAll('.series-label')].map(input => [input.dataset.series, input.value.trim() || input.dataset.series]));
  function update() {{
    const size = Number(sInput.value);
    const byBudget = budgets();
    const byLabel = labels();
    const valid = Number.isFinite(size) && size > 0 && Object.values(byBudget).every(value => value === null || (Number.isFinite(value) && value > 0));
    error.hidden = valid;
    error.textContent = valid ? '' : 'S must be positive; budgets must be positive or blank.';
    if (!valid) return;
    title.textContent = titleInput.value.trim() || 'Overlay budget model';
    root.querySelectorAll('[data-series-label]').forEach(cell => {{ cell.textContent = byLabel[cell.dataset.seriesLabel]; }});
    root.querySelectorAll('.overlay-cell[data-base-demand]').forEach(cell => {{
      const demand = Number(cell.dataset.baseDemand) * size / baseS;
      const budget = byBudget[cell.dataset.series];
      const strong = cell.querySelector('strong');
      const small = cell.querySelector('small');
      cell.className = 'overlay-cell';
      if (budget === null) {{
        cell.classList.add('unlimited');
        strong.textContent = `${{(demand / 1e6).toFixed(3)}} Mbps`;
      }} else {{
        const utilization = demand / budget;
        cell.classList.add(utilization < 0.8 ? 'under' : (utilization < 1 ? 'warning' : 'over'));
        strong.textContent = `${{(utilization * 100).toFixed(0)}}%`;
      }}
      small.textContent = `${{(demand / 1e6).toFixed(3)}} Mbps · ${{cell.dataset.gateway}} max`;
      cell.setAttribute('aria-label', `${{byLabel[cell.dataset.series]}}, ${{strong.textContent}}, ${{small.textContent}}`);
    }});
  }}
  function svgEscape(value) {{ return value.replace(/[&<>\"]/g, character => ({{'&':'&amp;','<':'&lt;','>':'&gt;','\"':'&quot;'}}[character])); }}
  function downloadSVG() {{
    update();
    if (!error.hidden) return;
    const rows = [...root.querySelectorAll('.overlay-grid tbody tr')];
    const rateHeaders = [...root.querySelectorAll('.overlay-grid thead th')].slice(1).map(cell => cell.textContent);
    const cellW = 150, cellH = 62, left = 300, top = 112;
    const width = left + rateHeaders.length * cellW + 30;
    const height = top + rows.length * cellH + 90;
    const parts = [`<svg xmlns="http://www.w3.org/2000/svg" width="${{width}}" height="${{height}}" viewBox="0 0 ${{width}} ${{height}}">`, '<rect width="100%" height="100%" fill="white"/>', '<style>text{{font-family:Arial,sans-serif;fill:#263238}}.title{{font-size:22px;font-weight:700}}.label{{font-size:12px}}.value{{font-size:12px;font-weight:700;fill:white}}.detail{{font-size:10px;fill:white}}</style>'];
    parts.push(`<text x="30" y="38" class="title">${{svgEscape(title.textContent)}}</text>`);
    parts.push(`<text x="30" y="60" class="label">S = ${{svgEscape(sInput.value)}} serialized bytes</text>`);
    rateHeaders.forEach((rate, index) => parts.push(`<text x="${{left + index * cellW + cellW / 2}}" y="90" class="label" text-anchor="middle">${{svgEscape(rate)}}</text>`));
    rows.forEach((row, rowIndex) => {{
      const y = top + rowIndex * cellH;
      parts.push(`<text x="${{left - 12}}" y="${{y + 35}}" class="label" text-anchor="end">${{svgEscape(row.querySelector('th').textContent)}}</text>`);
      [...row.querySelectorAll('td')].forEach((cell, column) => {{
        const x = left + column * cellW;
        const colors = {{under:'#009E73',warning:'#E69F00',over:'#D55E00',unlimited:'#0072B2','no-data':'#9CA3AF'}};
        const state = [...cell.classList].find(name => colors[name]) || 'no-data';
        const primary = cell.querySelector('strong')?.textContent || 'no data';
        const detail = cell.querySelector('small')?.textContent || '';
        parts.push(`<rect x="${{x + 2}}" y="${{y + 2}}" width="${{cellW - 4}}" height="${{cellH - 4}}" rx="5" fill="${{colors[state]}}"/>`);
        parts.push(`<text x="${{x + cellW / 2}}" y="${{y + 27}}" class="value" text-anchor="middle">${{svgEscape(primary)}}</text>`);
        parts.push(`<text x="${{x + cellW / 2}}" y="${{y + 45}}" class="detail" text-anchor="middle">${{svgEscape(detail)}}</text>`);
      }});
    }});
    parts.push('</svg>');
    const link = document.createElement('a');
    link.href = URL.createObjectURL(new Blob([parts.join('\\n')], {{type:'image/svg+xml'}}));
    link.download = 'overlay_model_edited.svg';
    link.click();
    setTimeout(() => URL.revokeObjectURL(link.href), 1000);
  }}
  root.querySelectorAll('input').forEach(input => input.addEventListener('input', update));
  document.getElementById('overlay-download').addEventListener('click', downloadSVG);
  update();
}})();
</script>"""


def html_report(records: list[RunRecord], model_rows: list[dict], message_bytes: float | None, send_lag_warning_ms: float) -> str:
    rows = []
    for run in sorted(records, key=lambda item: (item.topology, item.budget_label, item.target_rate, item.source)):
        state = observed_state(run, send_lag_warning_ms)
        rows.append(
            "<tr>"
            f"<td>{escape(run.topology)}</td><td>{escape(run.budget_label)}</td><td>{run.target_rate}</td>"
            f"<td>{run.delivery_fraction:.3%}</td>"
            f"<td>{run.scheduled_latency_p99_ms:.3f}</td><td>{run.send_lag_p95_ms:.3f}</td>"
            f'<td><span class="state {state}">{escape(STATE_LABEL[state])}</span></td>'
            "</tr>"
        )
    overlay_section = '<h2>Overlay budget model</h2>' + overlay_editor(model_rows, message_bytes) if model_rows and message_bytes else ""
    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Saturation experiment report</title>
<style>
body{{font-family:Arial,Helvetica,sans-serif;margin:0;background:#f5f7f8;color:#263238}}main{{max-width:1280px;margin:auto;padding:28px}}
.card{{background:white;border-radius:10px;padding:20px;margin:18px 0;box-shadow:0 2px 12px #00000012}}img{{width:100%;height:auto}}
table{{border-collapse:collapse;width:100%;font-size:14px}}th,td{{padding:9px;border-bottom:1px solid #e5e7eb;text-align:right}}th:first-child,td:first-child,th:nth-child(2),td:nth-child(2){{text-align:left}}
.state{{color:white;border-radius:12px;padding:4px 8px;font-size:12px;white-space:nowrap}}.clean{{background:#009E73}}.pressure{{background:#E69F00}}.delivery-limited{{background:#D55E00}}
.note{{color:#5f6b76;line-height:1.5}}h1,h2{{margin-top:0}}h3{{margin-bottom:4px}}
.overlay-controls{{display:grid;grid-template-columns:repeat(auto-fit,minmax(190px,1fr));gap:12px;align-items:end;margin-bottom:16px}}
.overlay-controls label,.overlay-controls fieldset{{display:grid;gap:5px;font-size:13px}}.overlay-controls fieldset{{border:1px solid #d7dde2;padding:10px;margin:0}}
.overlay-controls legend{{font-weight:700}}.overlay-controls input{{box-sizing:border-box;width:100%;padding:8px;border:1px solid #aeb8c2;border-radius:5px;font:inherit}}
.overlay-controls button{{padding:9px 12px;border:0;border-radius:5px;background:#263238;color:white;font:inherit}}.input-error{{color:#b42318}}
.overlay-table-wrap{{overflow-x:auto}}.overlay-grid{{min-width:780px}}.overlay-grid th:first-child{{min-width:220px}}.overlay-grid td{{text-align:center;color:white;border:4px solid white;padding:9px}}
.overlay-grid td strong,.overlay-grid td small{{display:block}}.overlay-grid td small{{margin-top:4px}}.overlay-grid .under{{background:#009E73}}.overlay-grid .warning{{background:#E69F00}}.overlay-grid .over{{background:#D55E00}}.overlay-grid .unlimited{{background:#0072B2}}.overlay-grid .no-data{{background:#9CA3AF}}
</style></head><body><main>
<div class="card"><h1>Saturation experiment report</h1><p class="note">Observed curves come from loadgen summaries. The Run data table applies a conservative state classification; a single latency spike is not proof of bandwidth saturation.</p></div>
<div class="card"><img src="saturation_observed.svg" alt="Observed saturation charts"></div>
<div class="card">{overlay_section}</div>
<div class="card"><h2>Run data</h2><table><thead><tr><th>Topology</th><th>Budget</th><th>Target rate</th><th>Delivered</th><th>P99 ms</th><th>Send lag P95 ms</th><th>State</th></tr></thead><tbody>{''.join(rows)}</tbody></table></div>
</main></body></html>
"""


def build_report(args: argparse.Namespace) -> Path:
    specs = [parse_series(value) for value in args.series]
    records = [load_run(spec, path) for spec in specs for path in discover_summaries(spec.path)]
    if not records:
        raise ValueError("no complete load-generator runs were found")
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    (output / "saturation_observed.svg").write_text(observed_svg(records), encoding="utf-8")
    write_csv(output / "saturation_data.csv", records, args.send_lag_warning_ms)

    model_rows: list[dict] = []
    if args.message_bytes is not None:
        manifest = load_manifest(args.manifest.resolve())
        model_rows = overlay_model(specs, (record.target_rate for record in records), manifest, args.message_bytes)
        (output / "overlay_model.svg").write_text(overlay_svg(model_rows, args.message_bytes), encoding="utf-8")

    summary = {
        "schema_version": 1,
        "classification": {
            "delivery_limited": "delivery or send success below 99.9%, or disconnect/unexpected message",
            "pressure": f"on-time below 99%, P99 above 1000 ms, or send-lag P95 above {args.send_lag_warning_ms:g} ms",
            "clean": "none of the above; does not prove spare capacity",
        },
        "runs": [
            {
                **asdict(record),
                "delivery_fraction": record.delivery_fraction,
                "send_success_fraction": record.send_success_fraction,
                "achieved_publication_rate": record.achieved_publication_rate,
                "observed_state": observed_state(record, args.send_lag_warning_ms),
            }
            for record in records
        ],
        "overlay_model": model_rows,
        "limitations": [
            "Configured bandwidth is supplied metadata and is not proof that shaping was active.",
            "Overlay demand is analytical unless measured overlay-byte counters are added to the experiment.",
            "Use repeated controlled trials before claiming a topology saturation rate.",
        ],
    }
    (output / "saturation_summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    (output / "index.html").write_text(html_report(records, model_rows, args.message_bytes, args.send_lag_warning_ms), encoding="utf-8")
    return output


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--series", action="append", required=True, metavar="TOPOLOGY|BUDGET|PATH",
        help="result matrix or individual run; repeat for topologies, budgets, or repetitions",
    )
    parser.add_argument("--output", type=Path, required=True, help="new directory for HTML, SVG, CSV and JSON")
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST, help="pilot manifest used by the overlay model")
    parser.add_argument("--message-bytes", type=float, help="S in the proposal formula; enables analytical overlay model")
    parser.add_argument("--send-lag-warning-ms", type=float, default=50.0, help="amber pressure threshold (default: 50)")
    args = parser.parse_args()
    if args.message_bytes is not None and args.message_bytes <= 0:
        parser.error("--message-bytes must be positive")
    if args.send_lag_warning_ms <= 0:
        parser.error("--send-lag-warning-ms must be positive")
    try:
        output = build_report(args)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        parser.error(str(exc))
    print(f"Saturation report: {output / 'index.html'}")


if __name__ == "__main__":
    main()
