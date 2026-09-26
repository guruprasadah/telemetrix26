"""
Telemetrix - Grafana SimpleJSON Visualization & Inference Bridge
================================================================

Reads JSONL telematics data from stdin or by tailing a file, runs a
real-time inference engine that flags hazardous driving events and
data-quality faults, and exposes a SimpleJSON datasource on port 7000
that Grafana can consume directly.

ENDPOINTS (SimpleJSON protocol):
    GET  /                health check
    POST /search          list metric names (or vehicle IDs for templating)
    POST /query           time-series / table data
    POST /annotations     events (crash, harsh events, faults, ...)

USAGE
-----
    # 1) Pipe directly from the generator
    python telemetrix_generator.py --vehicles 5 --rate 1 \\
        | python telemetrix_visualizer.py --input stdin

    # 2) Tail a file the generator is writing to
    python telemetrix_generator.py --vehicles 10 --output file --file t.jsonl &
    python telemetrix_visualizer.py --input file --file t.jsonl

    # 3) Custom port / retention
    python telemetrix_visualizer.py --port 7001 --retention 7200

GRAFANA SETUP
-------------
    1. Install SimpleJSON datasource plugin (one-time):
         grafana-cli plugins install simpod-json-datasource
       (or use the marcusolsson-json-datasource fork if you prefer)
    2. Add a datasource of type "SimpleJSON", URL: http://localhost:7000
    3. Import the sample dashboard: telemetrix_dashboard.json
       (or build your own - every metric returns one series per vehicle)

METRICS EXPOSED
---------------
    Per-vehicle (one series per vehicle_id):
        speed_kmh, fuel_level_pct, battery_level_pct, odometer_km,
        rpm, gear, signal_quality, route_progress, ignition_on,
        engine_status_code (OFF=0, IDLE=1, ON=2, FAULT=3),
        diagnostic_codes_count, lat, lng, heading
    Fleet-wide aggregates:
        fleet.vehicles_online, fleet.messages_per_sec,
        fleet.alerts_active, fleet.avg_speed, fleet.avg_fuel
    Tables:
        events_table   (recent events, newest first)

INFERRED EVENTS (returned as annotations + table rows)
------------------------------------------------------
    Severity levels (color hint in tags):
      critical (red)    : crash, fuel_theft, unauthorized_use
      warning  (orange) : harsh_braking, harsh_acceleration
      caution  (yellow) : overspeed, route_deviation
      info     (green)  : idle, low_fuel, low_battery
      fault    (gray)   : duplicate, out_of_order, missing, invalid_data

TUNABLE THRESHOLDS (see constants below)
---------------------------------------
    HARSH_BRAKING_KMH_PER_S, HARSH_ACCEL_KMH_PER_S, OVERSPEED_KMH, ...
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import threading
import time
from collections import defaultdict, deque
from datetime import datetime, timezone
from http.server import HTTPServer, BaseHTTPRequestHandler
from typing import Any, Dict, List, Optional, Tuple


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DEFAULT_PORT = 7777
DEFAULT_RETENTION_SEC = 3600           # 1 hour of in-memory history
DEFAULT_MAX_EVENTS = 10000             # ring buffer for events
DEFAULT_TAIL_POLL_SEC = 0.05            # 50ms poll when tailing a file
DEFAULT_DUP_CACHE_SIZE = 20000         # message_ids kept for dedup

# Inference thresholds (tuned for ~1 Hz sampling)
HARSH_BRAKING_KMH_PER_S = 5.0          # decel > 12 km/h/s  (≈ 3.3 m/s²)
HARSH_ACCEL_KMH_PER_S = 5.0            # accel  > 12 km/h/s
OVERSPEED_KMH = 100.0
OVERSPEED_SUSTAIN_S = 5
IDLE_SUSTAIN_S = 30
FUEL_THEFT_DROP_PCT = 10.0              # >10% drop while stationary
LOW_FUEL_PCT = 15.0
LOW_BATTERY_PCT = 15.0
UNAUTHORIZED_SPEED_BAND = (75.0, 85.0)  # suspiciously constant cruising
UNAUTHORIZED_SUSTAIN_S = 30
ROUTE_DEV_MIN_JUMP_M = 300             # sudden location jump while moving
ROUTE_DEV_MAX_JUMP_KM = 5.0            # (>5 km = teleport, not deviation)
GAP_MISSING_FACTOR = 2.5               # gap > 2.5x expected interval => missing

# Cooldowns so we don't spam the same event type per vehicle
COOLDOWNS_MS = {
    "crash": 5_000,
    "harsh_braking": 5_000,
    "harsh_acceleration": 5_000,
    "overspeed": 30_000,
    "idle": 60_000,
    "route_deviation": 30_000,
    "unauthorized_use": 60_000,
    "fuel_theft": 5_000,
    "low_fuel": 300_000,
    "low_battery": 300_000,
    "duplicate": 1_000,
    "out_of_order": 1_000,
    "missing": 10_000,
    "invalid_data": 5_000,
}

ENGINE_STATUS_CODES = {"OFF": 0, "IDLE": 1, "ON": 2, "FAULT": 3}

# Metric catalogue: (name, description, unit, type)
METRICS: List[Tuple[str, str, str, str]] = [
    ("speed_kmh",              "Vehicle speed",                                 "km/h",  "timeseries"),
    ("fuel_level_pct",         "Fuel level (ICE vehicles)",                     "%",     "timeseries"),
    ("battery_level_pct",      "Battery level (EV vehicles)",                    "%",     "timeseries"),
    ("odometer_km",            "Odometer reading",                              "km",    "timeseries"),
    ("rpm",                    "Engine RPM",                                    "rpm",   "timeseries"),
    ("gear",                   "Current gear",                                  "",      "timeseries"),
    ("signal_quality",         "Cellular signal quality",                       "0-1",   "timeseries"),
    ("route_progress",        "Fraction of route completed",                    "0-1",   "timeseries"),
    ("ignition_on",            "Ignition state (0/1)",                          "",      "timeseries"),
    ("engine_status_code",     "Engine status (OFF=0,IDLE=1,ON=2,FAULT=3)",    "",      "timeseries"),
    ("diagnostic_codes_count", "Active DTC count",                              "",      "timeseries"),
    ("lat",                    "Latitude",                                      "deg",   "timeseries"),
    ("lng",                    "Longitude",                                     "deg",   "timeseries"),
    ("heading",                "Heading",                                       "deg",   "timeseries"),
    ("fleet.vehicles_online",  "Vehicles seen in last 30s",                     "",      "timeseries"),
    ("fleet.messages_per_sec", "Aggregate throughput",                          "msg/s", "timeseries"),
    ("fleet.alerts_active",    "Active alerts (rolling 5 min window)",          "",      "timeseries"),
    ("fleet.avg_speed",        "Fleet average speed",                          "km/h",  "timeseries"),
    ("fleet.avg_fuel",         "Fleet average fuel/battery %",                  "%",     "timeseries"),
    ("events_table",           "Recent events (table)",                         "",      "table"),
]


# ---------------------------------------------------------------------------
# Event severity catalogue
# ---------------------------------------------------------------------------

# type -> (severity, color)
SEVERITY: Dict[str, Tuple[str, str]] = {
    "crash":              ("critical", "#E02F44"),
    "harsh_braking":      ("warning",  "#FF780A"),
    "harsh_acceleration": ("warning",  "#FF780A"),
    "overspeed":          ("caution",  "#FADE2A"),
    "idle":               ("info",     "#37872D"),
    "route_deviation":    ("caution",  "#FADE2A"),
    "unauthorized_use":   ("critical", "#E02F44"),
    "fuel_theft":         ("critical", "#E02F44"),
    "low_fuel":           ("info",     "#37872D"),
    "low_battery":        ("info",     "#37872D"),
    "duplicate":          ("fault",    "#8F9296"),
    "out_of_order":       ("fault",    "#8F9296"),
    "missing":            ("fault",    "#8F9296"),
    "invalid_data":       ("fault",    "#8F9296"),
}

EVENT_TITLES: Dict[str, str] = {
    "crash":              "CRASH DETECTED",
    "harsh_braking":      "Harsh Braking",
    "harsh_acceleration": "Harsh Acceleration",
    "overspeed":          "Overspeed",
    "idle":               "Prolonged Idle",
    "route_deviation":    "Route Deviation",
    "unauthorized_use":   "Unauthorized Use",
    "fuel_theft":         "Fuel/Battery Theft",
    "low_fuel":           "Low Fuel",
    "low_battery":        "Low Battery",
    "duplicate":          "Duplicate Message",
    "out_of_order":       "Out-of-Order Message",
    "missing":            "Missing Messages",
    "invalid_data":       "Invalid Data",
}


# ---------------------------------------------------------------------------
# Telemetry store + inference engine
# ---------------------------------------------------------------------------

class TelemetryStore:
    """Thread-safe in-memory store + real-time event detector."""

    def __init__(self, retention_sec: int = DEFAULT_RETENTION_SEC):
        self.retention = retention_sec
        self.lock = threading.RLock()

        # series[vehicle_id][metric] = deque([(ts_ms, value), ...])
        self.series: Dict[str, Dict[str, deque]] = defaultdict(lambda: defaultdict(deque))
        # latest[vehicle_id] = last full message received
        self.latest: Dict[str, dict] = {}
        # previous state per vehicle (for delta detection)
        self.prev: Dict[str, dict] = {}
        # rolling event log
        self.events: deque = deque(maxlen=DEFAULT_MAX_EVENTS)
        # dedup cache for message_id
        self._seen_ids: deque = deque(maxlen=DEFAULT_DUP_CACHE_SIZE)
        self._seen_set: set = set()
        # rolling count of messages for mps calc (keyed on wall-clock)
        self._recent_msgs: deque = deque(maxlen=600)  # (wall_ms, 1)
        # active alerts per vehicle (rolling 5-min window)
        self.active_alerts: Dict[str, List[dict]] = defaultdict(list)
        # vehicle metadata for table joins
        self.vehicle_meta: Dict[str, dict] = {}

        # stats
        self.start_time = time.time()
        self.total_ingested = 0
        self.total_events = 0

    # ---- Public API ----------------------------------------------------
    def ingest(self, msg: dict):
        if not isinstance(msg, dict):
            return
        with self.lock:
            self._store_message(msg)
            for ev in self._detect_events(msg):
                self._emit_event(ev)
            self._prune_if_needed()

    def vehicles(self) -> List[str]:
        with self.lock:
            return sorted(self.latest.keys())

    def query_timeseries(self, target: str, from_ms: int, to_ms: int,
                         max_points: int = 1000) -> List[dict]:
        out: List[dict] = []
        with self.lock:
            if target.startswith("fleet."):
                dq = self.series.get("*", {}).get(target, [])
                pts = [(v, t) for (t, v) in dq if from_ms <= t <= to_ms]
                if pts:
                    out.append({
                        "target": target,
                        "datapoints": self._downsample(pts, max_points),
                    })
            else:
                for vid in sorted(self.series.keys()):
                    if vid == "*":
                        continue
                    dq = self.series[vid].get(target, [])
                    if not dq:
                        continue
                    pts = [(v, t) for (t, v) in dq if from_ms <= t <= to_ms]
                    if pts:
                        out.append({
                            "target": f"{vid}.{target}",
                            "datapoints": self._downsample(pts, max_points),
                        })
        return out

    def query_table(self, target: str, from_ms: int, to_ms: int) -> dict:
        if target != "events_table":
            return {"type": "table", "columns": [], "rows": []}
        cols = [
            {"text": "Time",     "type": "time"},
            {"text": "Severity"},
            {"text": "Type"},
            {"text": "Vehicle"},
            {"text": "Driver"},
            {"text": "Description"},
        ]
        rows = []
        with self.lock:
            for ev in self.events:
                if from_ms <= ev["time_ms"] <= to_ms:
                    meta = self.vehicle_meta.get(ev["vehicle_id"], {})
                    rows.append([
                        ev["time_ms"],
                        SEVERITY.get(ev["type"], ("info",))[0],
                        ev["type"],
                        ev["vehicle_id"],
                        meta.get("driver_id", "?"),
                        ev["text"],
                    ])
        rows.sort(key=lambda r: r[0], reverse=True)
        return {"type": "table", "columns": cols, "rows": rows}

    def query_annotations(self, from_ms: int, to_ms: int,
                          limit: int = 500) -> List[dict]:
        out: List[dict] = []
        with self.lock:
            for ev in self.events:
                if from_ms <= ev["time_ms"] <= to_ms:
                    sev, color = SEVERITY.get(ev["type"], ("info", "#37872D"))
                    out.append({
                        "annotation": {
                            "name": "Telemetrix Events",
                            "enabled": True,
                            "datasource": "Telemetrix",
                        },
                        "time": ev["time_ms"],
                        "title": f"{ev['title']} - {ev['vehicle_id']}",
                        "text": ev["text"],
                        "tags": [ev["type"], ev["vehicle_id"], sev],
                    })
                    if len(out) >= limit:
                        break
        out.sort(key=lambda x: x["time"])
        return out

    # ---- Internal: storage --------------------------------------------
    def _store_message(self, msg: dict):
        vid = msg.get("vehicle_id")
        if not vid:
            return
        ts_ms = self._parse_ts_ms(msg.get("vehicle_timestamp") or msg.get("timestamp"))
        self.latest[vid] = msg
        self.vehicle_meta[vid] = {
            "vehicle_type": msg.get("vehicle_type", "?"),
            "engine_type":  msg.get("engine_type", "?"),
            "driver_id":    msg.get("driver_id", "?"),
        }
        self._append(vid, "speed_kmh", ts_ms, msg.get("speed_kmh"))
        self._append(vid, "odometer_km", ts_ms, msg.get("odometer_km"))
        self._append(vid, "rpm", ts_ms, msg.get("rpm"))
        self._append(vid, "gear", ts_ms, msg.get("gear"))
        self._append(vid, "signal_quality", ts_ms, msg.get("signal_quality"))
        self._append(vid, "route_progress", ts_ms, msg.get("route_progress"))
        self._append(vid, "ignition_on", ts_ms,
                     1 if msg.get("ignition_on") else 0)
        eng = msg.get("engine_status", "OFF")
        self._append(vid, "engine_status_code", ts_ms,
                     ENGINE_STATUS_CODES.get(eng, 0))
        codes = msg.get("diagnostic_codes") or []
        if not isinstance(codes, list):
            codes = []
        self._append(vid, "diagnostic_codes_count", ts_ms, len(codes))
        loc = msg.get("location") or {}
        if isinstance(loc, dict):
            self._append(vid, "lat", ts_ms, loc.get("lat"))
            self._append(vid, "lng", ts_ms, loc.get("lng"))
            self._append(vid, "heading", ts_ms, loc.get("heading"))
        if msg.get("fuel_level_pct") is not None:
            self._append(vid, "fuel_level_pct", ts_ms, msg["fuel_level_pct"])
        if msg.get("battery_level_pct") is not None:
            self._append(vid, "battery_level_pct", ts_ms, msg["battery_level_pct"])

        self._update_fleet_metrics(ts_ms)
        self._recent_msgs.append((int(time.time() * 1000), 1))
        self.total_ingested += 1

        # Duplicate detection
        mid = msg.get("message_id")
        if mid:
            if mid in self._seen_set:
                self._emit_event(self._make_event(
                    "duplicate", vid, ts_ms,
                    f"Duplicate message_id {mid} from {vid}",
                ))
            else:
                self._seen_set.add(mid)
                self._seen_ids.append(mid)
                if len(self._seen_ids) >= DEFAULT_DUP_CACHE_SIZE:
                    old = self._seen_ids.popleft()
                    self._seen_set.discard(old)

    def _append(self, vid: str, metric: str, ts_ms: int, value: Any):
        if value is None:
            return
        if isinstance(value, bool):
            value = 1 if value else 0
        elif not isinstance(value, (int, float)):
            return
        if isinstance(value, float) and value != value:  # NaN check
            return
        self.series[vid][metric].append((ts_ms, value))

    def _update_fleet_metrics(self, ts_ms: int):
        cutoff = ts_ms - 30_000
        online = 0
        speeds: List[float] = []
        fuels: List[float] = []
        for vid, m in self.latest.items():
            m_ts = self._parse_ts_ms(m.get("vehicle_timestamp") or m.get("timestamp"))
            if m_ts and m_ts >= cutoff:
                online += 1
                spd = m.get("speed_kmh")
                if isinstance(spd, (int, float)):
                    speeds.append(spd)
                f = m.get("fuel_level_pct")
                if f is None:
                    f = m.get("battery_level_pct")
                if isinstance(f, (int, float)):
                    fuels.append(f)
        self._append("*", "fleet.vehicles_online", ts_ms, online)
        self._append("*", "fleet.messages_per_sec", ts_ms, self._compute_mps())
        self._append("*", "fleet.alerts_active", ts_ms,
                    sum(len(v) for v in self.active_alerts.values()))
        if speeds:
            self._append("*", "fleet.avg_speed", ts_ms, sum(speeds) / len(speeds))
        if fuels:
            self._append("*", "fleet.avg_fuel", ts_ms, sum(fuels) / len(fuels))

    def _compute_mps(self) -> float:
        now_ms = int(time.time() * 1000)
        cutoff = now_ms - 5000
        while self._recent_msgs and self._recent_msgs[0][0] < cutoff:
            self._recent_msgs.popleft()
        return round(len(self._recent_msgs) / 5.0, 2)

    # ---- Internal: inference ------------------------------------------
    def _detect_events(self, msg: dict) -> List[dict]:
        events: List[dict] = []
        vid = msg.get("vehicle_id")
        if not vid:
            return events
        ts_ms = self._parse_ts_ms(msg.get("vehicle_timestamp") or msg.get("timestamp"))
        prev = self.prev.get(vid, {})
        self.prev[vid] = msg

        # 1) Invalid / anomalous values (catches the 'invalid' fault)
        ev = self._check_invalid(msg, vid, ts_ms)
        if ev:
            events.append(ev)

        # 2) Missing gap detection
        if prev:
            prev_ts = self._parse_ts_ms(prev.get("vehicle_timestamp") or prev.get("timestamp"))
            if prev_ts and ts_ms > prev_ts:
                gap_s = (ts_ms - prev_ts) / 1000.0
                expected = 1.0  # generator default rate
                if gap_s > expected * GAP_MISSING_FACTOR:
                    if self._can_emit(vid, "missing", ts_ms):
                        events.append(self._make_event(
                            "missing", vid, ts_ms,
                            f"Gap of {gap_s:.1f}s in {vid} telemetry "
                            f"(expected ~{expected:.1f}s)",
                        ))

        # 3) Out-of-order timestamp
        if prev:
            prev_ts = self._parse_ts_ms(prev.get("vehicle_timestamp") or prev.get("timestamp"))
            if prev_ts and ts_ms < prev_ts:
                if self._can_emit(vid, "out_of_order", ts_ms):
                    events.append(self._make_event(
                        "out_of_order", vid, ts_ms,
                        f"{vid} emitted an out-of-order timestamp "
                        f"(prev={prev_ts}, now={ts_ms})",
                    ))

        eng = msg.get("engine_status")
        codes = msg.get("diagnostic_codes") or []
        crash_codes = {"B1100", "B1234", "C0300", "C1201"}

        # 4) Crash: engine FAULT or airbag/ABS DTC
        if eng == "FAULT" or (set(codes) & crash_codes):
            if self._can_emit(vid, "crash", ts_ms):
                events.append(self._make_event(
                    "crash", vid, ts_ms,
                    f"Crash signature on {vid}: engine_status={eng}, "
                    f"DTCs={[c for c in codes if c in crash_codes]}",
                ))

        speed = msg.get("speed_kmh")
        prev_speed = prev.get("speed_kmh")

        # 5) Harsh braking / acceleration (rate-of-change of speed)
        if (isinstance(speed, (int, float)) and
                isinstance(prev_speed, (int, float))):
            prev_ts = self._parse_ts_ms(prev.get("vehicle_timestamp") or prev.get("timestamp"))
            dt_s = max(0.1, (ts_ms - prev_ts) / 1000.0) if prev_ts else 1.0
            delta = speed - prev_speed
            rate = delta / dt_s
            if rate <= -HARSH_BRAKING_KMH_PER_S and prev_speed > 20:
                if self._can_emit(vid, "harsh_braking", ts_ms):
                    events.append(self._make_event(
                        "harsh_braking", vid, ts_ms,
                        f"{vid} decelerated {abs(delta):.1f} km/h in {dt_s:.1f}s "
                        f"({prev_speed:.0f} -> {speed:.0f} km/h, {rate:.1f} km/h/s)",
                    ))
            elif rate >= HARSH_ACCEL_KMH_PER_S and speed > 20:
                if self._can_emit(vid, "harsh_acceleration", ts_ms):
                    events.append(self._make_event(
                        "harsh_acceleration", vid, ts_ms,
                        f"{vid} accelerated {delta:.1f} km/h in {dt_s:.1f}s "
                        f"({prev_speed:.0f} -> {speed:.0f} km/h, {rate:.1f} km/h/s)",
                    ))

        # 6) Overspeed (sustained above threshold)
        if isinstance(speed, (int, float)) and speed > OVERSPEED_KMH:
            hist = list(self.series[vid].get("speed_kmh", []))
            need = int(OVERSPEED_SUSTAIN_S)
            if len(hist) >= need:
                window = hist[-need:]
                if all(v > OVERSPEED_KMH for _, v in window):
                    if self._can_emit(vid, "overspeed", ts_ms):
                        events.append(self._make_event(
                            "overspeed", vid, ts_ms,
                            f"{vid} sustained speed > {OVERSPEED_KMH} km/h "
                            f"for {OVERSPEED_SUSTAIN_S}s",
                        ))

        # 7) Prolonged idle
        if eng in ("IDLE", "ON") and isinstance(speed, (int, float)) and speed < 1:
            need = int(IDLE_SUSTAIN_S)
            hist = list(self.series[vid].get("speed_kmh", []))
            if len(hist) >= need:
                window = hist[-need:]
                if all(v < 1 for _, v in window):
                    if self._can_emit(vid, "idle", ts_ms):
                        events.append(self._make_event(
                            "idle", vid, ts_ms,
                            f"{vid} has been idling for > {IDLE_SUSTAIN_S}s",
                        ))

        # 8) Unauthorized use (suspiciously constant speed in narrow band)
        if (isinstance(speed, (int, float))
                and UNAUTHORIZED_SPEED_BAND[0] <= speed <= UNAUTHORIZED_SPEED_BAND[1]):
            need = int(UNAUTHORIZED_SUSTAIN_S)
            hist = list(self.series[vid].get("speed_kmh", []))
            if len(hist) >= need:
                window = hist[-need:]
                if all(UNAUTHORIZED_SPEED_BAND[0] <= v <= UNAUTHORIZED_SPEED_BAND[1]
                       for _, v in window):
                    if self._can_emit(vid, "unauthorized_use", ts_ms):
                        events.append(self._make_event(
                            "unauthorized_use", vid, ts_ms,
                            f"{vid} cruising at suspiciously constant "
                            f"{UNAUTHORIZED_SPEED_BAND[0]}-{UNAUTHORIZED_SPEED_BAND[1]} "
                            f"km/h for {UNAUTHORIZED_SUSTAIN_S}s",
                        ))

        # 9) Route deviation (sudden location jump while moving)
        loc = msg.get("location") or {}
        prev_loc = prev.get("location") or {}
        if isinstance(loc, dict) and isinstance(prev_loc, dict):
            lat, lng = loc.get("lat"), loc.get("lng")
            plat, plng = prev_loc.get("lat"), prev_loc.get("lng")
            if all(isinstance(x, (int, float)) for x in (lat, lng, plat, plng)):
                dlat_km = (lat - plat) * 111.0
                dlng_km = (lng - plng) * 111.0
                dist_km = (dlat_km ** 2 + dlng_km ** 2) ** 0.5
                if (dist_km * 1000 > ROUTE_DEV_MIN_JUMP_M
                        and dist_km < ROUTE_DEV_MAX_JUMP_KM):
                    if self._can_emit(vid, "route_deviation", ts_ms):
                        events.append(self._make_event(
                            "route_deviation", vid, ts_ms,
                            f"{vid} location jumped {dist_km*1000:.0f} m "
                            f"while moving",
                        ))

        # 10) Fuel / battery theft (sudden drop while stationary)
        fuel_now = msg.get("fuel_level_pct")
        if fuel_now is None:
            fuel_now = msg.get("battery_level_pct")
        fuel_prev = prev.get("fuel_level_pct")
        if fuel_prev is None:
            fuel_prev = prev.get("battery_level_pct")
        if (isinstance(fuel_now, (int, float))
                and isinstance(fuel_prev, (int, float))
                and isinstance(speed, (int, float)) and speed < 5):
            drop = fuel_prev - fuel_now
            if drop >= FUEL_THEFT_DROP_PCT:
                if self._can_emit(vid, "fuel_theft", ts_ms):
                    events.append(self._make_event(
                        "fuel_theft", vid, ts_ms,
                        f"{vid} lost {drop:.1f}% fuel/battery while stationary",
                    ))

        # 11) Low fuel / battery warnings
        if isinstance(fuel_now, (int, float)):
            if "fuel_level_pct" in msg and fuel_now < LOW_FUEL_PCT:
                if self._can_emit(vid, "low_fuel", ts_ms):
                    events.append(self._make_event(
                        "low_fuel", vid, ts_ms,
                        f"{vid} fuel low: {fuel_now:.1f}%",
                    ))
            elif "battery_level_pct" in msg and fuel_now < LOW_BATTERY_PCT:
                if self._can_emit(vid, "low_battery", ts_ms):
                    events.append(self._make_event(
                        "low_battery", vid, ts_ms,
                        f"{vid} battery low: {fuel_now:.1f}%",
                    ))

        return events

    def _check_invalid(self, msg: dict, vid: str, ts_ms: int) -> Optional[dict]:
        """Detect anomalous/invalid values (the 'invalid' fault)."""
        if not self._can_emit(vid, "invalid_data", ts_ms):
            return None
        speed = msg.get("speed_kmh")
        if isinstance(speed, (int, float)):
            if speed < 0 or speed > 300:
                return self._make_event("invalid_data", vid, ts_ms,
                                        f"{vid} invalid speed_kmh={speed}")
        loc = msg.get("location") or {}
        if isinstance(loc, dict):
            lat = loc.get("lat")
            lng = loc.get("lng")
            if lat is None:
                return self._make_event("invalid_data", vid, ts_ms,
                                        f"{vid} invalid lat=None")
            if isinstance(lat, (int, float)) and (lat < -90 or lat > 90):
                return self._make_event("invalid_data", vid, ts_ms,
                                        f"{vid} invalid lat={lat}")
            if isinstance(lng, (int, float)) and (lng < -180 or lng > 180):
                return self._make_event("invalid_data", vid, ts_ms,
                                        f"{vid} invalid lng={lng}")
        for k in ("fuel_level_pct", "battery_level_pct"):
            v = msg.get(k)
            if isinstance(v, (int, float)) and (v < 0 or v > 150):
                return self._make_event("invalid_data", vid, ts_ms,
                                        f"{vid} invalid {k}={v}")
        odo = msg.get("odometer_km")
        if isinstance(odo, (int, float)) and odo < 0:
            return self._make_event("invalid_data", vid, ts_ms,
                                    f"{vid} invalid odometer_km={odo}")
        eng = msg.get("engine_status")
        if eng not in ("ON", "OFF", "IDLE", "FAULT"):
            return self._make_event("invalid_data", vid, ts_ms,
                                    f"{vid} invalid engine_status={eng!r}")
        return None

    def _can_emit(self, vid: str, ev_type: str, ts_ms: int) -> bool:
        cd = COOLDOWNS_MS.get(ev_type, 5000)
        # Walk events in reverse - they're mostly in time order
        for ev in reversed(self.events):
            if ev["vehicle_id"] != vid or ev["type"] != ev_type:
                continue
            if ts_ms - ev["time_ms"] < cd:
                return False
            break
        return True

    def _make_event(self, ev_type: str, vid: str, ts_ms: int, text: str) -> dict:
        severity, _color = SEVERITY.get(ev_type, ("info", "#37872D"))
        return {
            "type": ev_type,
            "vehicle_id": vid,
            "time_ms": ts_ms,
            "title": EVENT_TITLES.get(ev_type, ev_type),
            "text": text,
            "tags": [ev_type, vid, severity],
        }

    def _emit_event(self, ev: dict):
        self.events.append(ev)
        self.total_events += 1
        lst = self.active_alerts[ev["vehicle_id"]]
        lst.append(ev)
        cutoff = ev["time_ms"] - 300_000
        self.active_alerts[ev["vehicle_id"]] = [
            e for e in lst if e["time_ms"] >= cutoff
        ]

    # ---- Internal: maintenance -----------------------------------------
    def _prune_if_needed(self):
        """Drop data older than retention. Called once per ingest."""
        cutoff = int(time.time() * 1000) - self.retention * 1000
        # Cheap check: prune only the head of each deque
        for vid in list(self.series.keys()):
            for metric in list(self.series[vid].keys()):
                dq = self.series[vid][metric]
                while dq and dq[0][0] < cutoff:
                    dq.popleft()

    @staticmethod
    def _parse_ts_ms(ts_str: Optional[str]) -> int:
        if not ts_str:
            return int(time.time() * 1000)
        try:
            s = ts_str.replace("Z", "+00:00")
            dt = datetime.fromisoformat(s)
            return int(dt.timestamp() * 1000)
        except Exception:
            try:
                return int(float(ts_str))
            except Exception:
                return int(time.time() * 1000)

    @staticmethod
    def _downsample(pts: List[Tuple[float, int]],
                    max_points: int) -> List[List[float]]:
        if len(pts) <= max_points:
            return [[v, t] for v, t in pts]
        step = len(pts) / max_points
        out: List[List[float]] = []
        i = 0.0
        while i < len(pts):
            out.append(list(pts[int(i)]))
            i += step
        return out


# ---------------------------------------------------------------------------
# HTTP handler - SimpleJSON protocol
# ---------------------------------------------------------------------------

class SimpleJSONHandler(BaseHTTPRequestHandler):
    store: TelemetryStore = None  # type: ignore[assignment]

    # Silence default noisy logging
    def log_message(self, format, *args):
        pass

    def _send_json(self, code: int, obj):
        body = json.dumps(obj).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "POST, GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers",
                         "Content-Type, Authorization")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_body(self) -> dict:
        length = int(self.headers.get("Content-Length", 0))
        if length == 0:
            return {}
        raw = self.rfile.read(length)
        try:
            return json.loads(raw.decode("utf-8"))
        except Exception:
            return {}

    def do_OPTIONS(self):
        self._send_json(200, {})

    def do_GET(self):
        if self.path in ("/", "/health"):
            self._send_json(200, {
                "status": "ok",
                "service": "telemetrix-visualizer",
                "vehicles": len(self.store.latest),
                "ingested": self.store.total_ingested,
                "events": self.store.total_events,
            })
        else:
            self._send_json(404, {"error": "not found"})

    def do_POST(self):
        if self.path == "/search":
            body = self._read_body()
            target = (body.get("target") or "").strip().lower()
            # Templating hook: when user queries "vehicles", return IDs
            if target.startswith("vehicle"):
                self._send_json(200, self.store.vehicles())
                return
            self._send_json(200, [m[0] for m in METRICS])
            return

        if self.path == "/query":
            body = self._read_body()
            targets = body.get("targets") or []
            range_obj = body.get("range") or {}
            from_ms = self._parse_iso_ms(range_obj.get("from")) or \
                      (int(time.time() * 1000) - 60_000)
            to_ms = self._parse_iso_ms(range_obj.get("to")) or \
                    int(time.time() * 1000)
            max_points = int(body.get("maxDataPoints") or 1000)
            result = []
            for t in targets:
                target = t.get("target") or ""
                ttype = (t.get("type") or "timeseries").lower()
                if not target:
                    continue
                if ttype == "table":
                    result.append(self.store.query_table(target, from_ms, to_ms))
                else:
                    result.extend(self.store.query_timeseries(
                        target, from_ms, to_ms, max_points))
            self._send_json(200, result)
            return

        if self.path == "/annotations":
            body = self._read_body()
            range_obj = body.get("range") or {}
            from_ms = self._parse_iso_ms(range_obj.get("from")) or \
                      (int(time.time() * 1000) - 3_600_000)
            to_ms = self._parse_iso_ms(range_obj.get("to")) or \
                    int(time.time() * 1000)
            self._send_json(200, self.store.query_annotations(from_ms, to_ms))
            return

        self._send_json(404, {"error": "unknown endpoint"})

    @staticmethod
    def _parse_iso_ms(s: Optional[str]) -> int:
        if not s:
            return 0
        try:
            s2 = s.replace("Z", "+00:00")
            dt = datetime.fromisoformat(s2)
            return int(dt.timestamp() * 1000)
        except Exception:
            try:
                return int(float(s))
            except Exception:
                return 0


# ---------------------------------------------------------------------------
# Ingestion threads
# ---------------------------------------------------------------------------

def ingest_stdin(store: TelemetryStore, stop_event: threading.Event):
    sys.stderr.write("[INFO] Reading JSONL from stdin...\n")
    sys.stderr.flush()
    while not stop_event.is_set():
        line = sys.stdin.readline()
        if not line:
            break
        line = line.strip()
        if not line:
            continue
        try:
            msg = json.loads(line)
            store.ingest(msg)
        except json.JSONDecodeError as e:
            sys.stderr.write(f"[WARN] Bad JSON: {e}: {line[:120]}\n")
    sys.stderr.write("[INFO] stdin EOF, ingest thread exiting\n")
    sys.stderr.flush()


def ingest_file_tail(store: TelemetryStore, file_path: str,
                     stop_event: threading.Event,
                     poll_interval: float = DEFAULT_TAIL_POLL_SEC):
    sys.stderr.write(f"[INFO] Tailing file: {file_path}\n")
    sys.stderr.flush()

    f = None
    while not stop_event.is_set():
        try:
            f = open(file_path, "r", encoding="utf-8")
            break
        except FileNotFoundError:
            sys.stderr.write(f"[WARN] File not found yet, waiting: {file_path}\n")
            time.sleep(0.5)

    if f is None:
        return

    # If file already has content, we still want to ingest the existing
    # lines so historical data shows up in Grafana. To skip historical
    # and only tail new lines, set --from-end.
    try:
        while not stop_event.is_set():
            pos = f.tell()
            line = f.readline()
            if not line:
                time.sleep(poll_interval)
                # Detect truncation / rotation
                try:
                    if os.path.getsize(file_path) < pos:
                        f.seek(0)
                except OSError:
                    pass
                continue
            line = line.strip()
            if not line:
                continue
            try:
                msg = json.loads(line)
                store.ingest(msg)
            except json.JSONDecodeError as e:
                sys.stderr.write(f"[WARN] Bad JSON: {e}: {line[:120]}\n")
    finally:
        f.close()
    sys.stderr.write("[INFO] file tail stopped\n")
    sys.stderr.flush()


def stats_printer(store: TelemetryStore, stop_event: threading.Event,
                  interval_s: float = 10.0):
    last = 0
    while not stop_event.is_set():
        time.sleep(interval_s)
        now = store.total_ingested
        rate = (now - last) / interval_s if interval_s else 0
        last = now
        sys.stderr.write(
            f"[STATS] ingested={store.total_ingested}  "
            f"events={store.total_events}  "
            f"vehicles={len(store.latest)}  "
            f"rate={rate:.1f} msg/s  "
            f"active_alerts={sum(len(v) for v in store.active_alerts.values())}\n"
        )
        sys.stderr.flush()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    p = argparse.ArgumentParser(
        prog="telemetrix_visualizer",
        description="Telemetrix - Grafana SimpleJSON visualization & inference bridge",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--input", choices=["stdin", "file"], default="stdin",
                   help="Source of JSONL data (default: stdin)")
    p.add_argument("--file", type=str, default="telemetry.jsonl",
                   help="File to tail when --input file (default: telemetry.jsonl)")
    p.add_argument("--port", type=int, default=DEFAULT_PORT,
                   help=f"HTTP port for SimpleJSON server (default: {DEFAULT_PORT})")
    p.add_argument("--host", default="0.0.0.0",
                   help="Bind address (default: 0.0.0.0)")
    p.add_argument("--retention", type=int, default=DEFAULT_RETENTION_SEC,
                   help=f"In-memory data retention in seconds (default: {DEFAULT_RETENTION_SEC})")
    p.add_argument("--stats-interval", type=float, default=10.0,
                   help="Print stats every N seconds to stderr (default: 10)")
    p.add_argument("--list-metrics", action="store_true",
                   help="Print available metrics and exit")
    p.add_argument("--list-events", action="store_true",
                   help="Print detected event types and exit")
    args = p.parse_args()

    if args.list_metrics:
        print("Available metrics:")
        for name, desc, unit, _ in METRICS:
            print(f"  {name:30s}  {desc}  [{unit}]")
        return 0

    if args.list_events:
        print("Detected event types:")
        for et, (sev, color) in SEVERITY.items():
            print(f"  {et:22s}  severity={sev:8s}  color={color}")
        return 0

    store = TelemetryStore(retention_sec=args.retention)
    SimpleJSONHandler.store = store

    stop_event = threading.Event()

    def handle_sigint(sig, frame):
        stop_event.set()
    signal.signal(signal.SIGINT, handle_sigint)

    # Start ingest thread
    if args.input == "stdin":
        t_ingest = threading.Thread(
            target=ingest_stdin, args=(store, stop_event), daemon=True)
    else:
        t_ingest = threading.Thread(
            target=ingest_file_tail,
            args=(store, args.file, stop_event), daemon=True)
    t_ingest.start()

    # Stats thread
    if args.stats_interval > 0:
        t_stats = threading.Thread(
            target=stats_printer,
            args=(store, stop_event, args.stats_interval), daemon=True)
        t_stats.start()

    # HTTP server (handle_request loop with short timeout so SIGINT works)
    httpd = HTTPServer((args.host, args.port), SimpleJSONHandler)
    httpd.timeout = 1.0

    sys.stderr.write("\n========================================\n")
    sys.stderr.write(" Telemetrix Visualizer\n")
    sys.stderr.write("========================================\n")
    sys.stderr.write(f" Listening:   http://{args.host}:{args.port}\n")
    sys.stderr.write(f" Input:       {args.input}" +
                     (f" ({args.file})" if args.input == "file" else "") + "\n")
    sys.stderr.write(f" Retention:   {args.retention}s in-memory\n\n")
    sys.stderr.write(" SimpleJSON endpoints:\n")
    sys.stderr.write("   GET  /             (health)\n")
    sys.stderr.write("   POST /search       (metric list / vehicle IDs)\n")
    sys.stderr.write("   POST /query        (timeseries + tables)\n")
    sys.stderr.write("   POST /annotations  (events for time range)\n\n")
    sys.stderr.write(" Grafana setup:\n")
    sys.stderr.write(f"   1. Install plugin: grafana-cli plugins install simpod-json-datasource\n")
    sys.stderr.write(f"   2. Add SimpleJSON datasource -> URL: http://localhost:{args.port}\n")
    sys.stderr.write(f"   3. Import dashboard: telemetrix_dashboard.json\n\n")
    sys.stderr.write(" Press Ctrl+C to stop.\n\n")
    sys.stderr.flush()

    try:
        while not stop_event.is_set():
            httpd.handle_request()
    except KeyboardInterrupt:
        stop_event.set()
    finally:
        sys.stderr.write("\n[INFO] Shutting down...\n")
        sys.stderr.flush()
        httpd.server_close()

    return 0


if __name__ == "__main__":
    sys.exit(main() or 0)