"""
Telemetrix - Grafana SimpleJson Data Provider
=============================================

A live JSONL-tail server that exposes connected-vehicle telemetry to Grafana
as a SimpleJson data source. It also runs an inline inference engine that
emits annotations (events) for data-quality issues and operational alerts.

DESIGN GOALS
------------
- Pipeline-friendly: read from one or more JSONL files OR from stdin so it can
  sit at the end of any `gen | process | provider` pipe.
- Schema-driven: auto-detects numeric fields from the data, so the moment a
  new field appears in the JSONL stream it shows up in /search.
- Resilient to real telematics mess: duplicates, late arrivals, gaps, bad
  values. The provider doesn't crash on any of these; it surfaces them as
  annotations instead.
- Inference layer: every line is run through a small rule engine that emits
  events (harsh brake, speeding, low fuel, diag code, dup msg_id, late data,
  impossible reading, etc.) — exactly the four failure modes the Telemetrix
  problem statement asks you to demonstrate live.

GRAFANA SETUP
-------------
1. Install the SimpleJson data source plugin:
   grafana-cli plugins install simpod-json-datasource
   (or via Administration > Plugins on Grafana 9+)
2. Add a new data source of type "JSON API" / "SimpleJson", set URL to
   http://<this-host>:7777
3. Build a dashboard. In each panel's query, type a target like:
       speed                       # all vehicles
       V001:speed                  # one vehicle
       fuel                        # any numeric field
4. For events, open Dashboard > Settings > Annotations, add a query against
   this same data source. Annotations are returned automatically.

USAGE
-----
  # tail an existing file
  python telemetrix_provider.py --file /path/to/telemetry.jsonl

  # multiple files (e.g. one per pipeline stage)
  python telemetrix_provider.py --file a.jsonl --file b.jsonl

  # pipe mode: read JSONL from stdin
  python sample_generator.py | python telemetrix_provider.py --stdin

  # both: tail files AND accept stdin
  python sample_generator.py | python telemetrix_provider.py --file live.jsonl --stdin
"""

from __future__ import annotations

import argparse
import collections
import json
import logging
import os
import sys
import threading
import time
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, Iterable, List, Optional, Tuple

from flask import Flask, jsonify, request

LOG = logging.getLogger("telemetrix")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DEFAULT_PORT = 7777
DEFAULT_HOST = "0.0.0.0"
DEFAULT_BUFFER = 100_000            # keep last N messages in memory per source
TAIL_POLL_INTERVAL = 0.25          # seconds between file reads

# Numeric fields auto-detected from data. The ones listed here are always
# offered in /search even before any data arrives, so a freshly-started
# dashboard shows sensible defaults. Names cover both the original minimal
# schema (speed, fuel_level ...) and the richer Telemetrix schema
# (speed_kmh, fuel_level_pct, battery_level_pct, location.lat, ...).
DEFAULT_NUMERIC_FIELDS = [
    # original minimal schema
    "speed", "fuel_level", "battery_level", "odometer",
    "lat", "long", "engine_rpm", "engine_temp",
    # rich schema (flat fields)
    "speed_kmh", "fuel_level_pct", "battery_level_pct",
    "odometer_km", "rpm", "gear", "signal_quality",
    "route_progress", "seq",
    # nested location.* flattened by Record._flatten
    "location.lat", "location.lng", "location.heading",
]

# Fields treated as the canonical timestamp. First match wins.
TS_FIELDS = ["vehicle_timestamp", "timestamp", "ts", "time", "@timestamp"]

# Fields treated as vehicle identifier.
VID_FIELDS = ["vehicle_id", "vid", "vin", "vehicleId"]

# Fields treated as message id (for duplicate detection).
MID_FIELDS = ["message_id", "msg_id", "id"]

# Fields treated as driver identifier (for unauthorized-use detection).
DRV_FIELDS = ["driver_id", "driverId", "drv"]

# Fields treated as engine type (ELECTRIC / ICE / HYBRID).
ETYPE_FIELDS = ["engine_type", "powertrain", "drivetrain"]

# Fields treated as vehicle type (SEDAN / TRUCK / SUV / VAN ...).
VTYPE_FIELDS = ["vehicle_type", "vehicleType", "vclass"]

# Fields treated as sequence number (per-vehicle monotonic counter).
SEQ_FIELDS = ["seq", "sequence", "seq_num", "seqno"]


# ---------------------------------------------------------------------------
# Field alias resolver - so the engine reads any of several field names
# ---------------------------------------------------------------------------
def _get_path(d: Dict[str, Any], path: str) -> Optional[Any]:
    """Read a dotted path (e.g. 'location.lat') from a (possibly nested) dict."""
    cur: Any = d
    for part in path.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return None
        cur = cur[part]
    return cur


def _field_value(rec: "Record", aliases: List[str]) -> Optional[Any]:
    """Try each alias in order; return the first non-None value found.

    Looks in: flattened numeric fields, then top-level raw, then dotted paths
    in the raw payload. This lets the inference engine work with both the
    original minimal schema and the richer Telemetrix schema interchangeably.
    """
    for name in aliases:
        if name in rec.fields and rec.fields[name] is not None:
            return rec.fields[name]
        if name in rec.raw and rec.raw[name] is not None:
            return rec.raw[name]
        v = _get_path(rec.raw, name)
        if v is not None:
            return v
    return None


# ---------------------------------------------------------------------------
# Telemetry record - normalises any JSONL payload into one shape
# ---------------------------------------------------------------------------
class Record:
    """Normalised view of one JSONL message.

    `fields` is a flat dict of all numeric leaves, with dotted keys for
    nested objects (e.g. `location.lat`, `location.lng`, `location.heading`).
    This means every numeric value anywhere in the JSONL is queryable via
    /search and /query without any per-schema configuration.
    """

    __slots__ = ("raw", "ts", "vid", "mid", "fields")

    def __init__(self, raw: Dict[str, Any]):
        self.raw = raw
        self.ts = self._extract_ts(raw)
        self.vid = self._extract_first(raw, VID_FIELDS)
        self.mid = self._extract_first(raw, MID_FIELDS)
        self.fields = dict(self._flatten(raw))

    @staticmethod
    def _flatten(d: Dict[str, Any], prefix: str = "") -> Iterable[Tuple[str, float]]:
        """Yield (dotted_key, numeric_value) for every numeric leaf in d.
        Booleans are skipped (not useful as time series)."""
        for k, v in d.items():
            full = f"{prefix}.{k}" if prefix else k
            if isinstance(v, dict):
                yield from Record._flatten(v, full)
            elif isinstance(v, bool):
                continue
            elif isinstance(v, (int, float)):
                yield full, v

    @staticmethod
    def _extract_first(d: Dict[str, Any], keys: List[str]) -> Optional[Any]:
        for k in keys:
            if k in d:
                return d[k]
        return None

    @staticmethod
    def _extract_ts(d: Dict[str, Any]) -> int:
        """Return epoch-ms. Falls back to wall clock if missing/bad."""
        for k in TS_FIELDS:
            if k in d:
                v = d[k]
                try:
                    # numeric epoch seconds or ms
                    if isinstance(v, (int, float)):
                        return int(v * 1000) if v < 1e12 else int(v)
                    if isinstance(v, str):
                        s = v.strip()
                        if s and s[-1] in ("Z", "z"):
                            s = s[:-1]
                        # try ISO 8601
                        try:
                            dt = datetime.fromisoformat(s)
                            if dt.tzinfo is None:
                                dt = dt.replace(tzinfo=timezone.utc)
                            return int(dt.timestamp() * 1000)
                        except ValueError:
                            pass
                        # try raw numeric string
                        try:
                            n = float(s)
                            return int(n * 1000) if n < 1e12 else int(n)
                        except ValueError:
                            pass
                except Exception:
                    pass
        return int(time.time() * 1000)

    def get(self, key: str) -> Optional[Any]:
        return self.raw.get(key)


# ---------------------------------------------------------------------------
# Live tail engine: reads JSONL from files and/or stdin, fills a ring buffer
# ---------------------------------------------------------------------------
class TailSource:
    """A single tailable input. For files: seeks to end, polls for new bytes,
    handles truncation/rotation. For stdin: reads line-by-line."""

    def __init__(self, name: str, path: Optional[str] = None, is_stdin: bool = False):
        self.name = name
        self.path = path
        self.is_stdin = is_stdin
        self._fh = None
        self._inode = None
        self._pos = 0
        self._stopped = False

    def start(self):
        if self.is_stdin:
            self._fh = sys.stdin
        else:
            # open file at end so we don't replay history on startup
            # UNLESS user wants to (handled in TailEngine)
            self._fh = open(self.path, "r", encoding="utf-8", errors="replace")
            try:
                self._inode = os.fstat(self._fh.fileno()).st_ino
                self._fh.seek(0, 2)  # seek to end
                self._pos = self._fh.tell()
            except OSError:
                pass

    def iter_lines(self) -> Iterable[str]:
        """Generator that yields new lines as they arrive."""
        if self.is_stdin:
            for line in self._fh:
                if self._stopped:
                    return
                yield line
            return

        while not self._stopped:
            line = self._fh.readline()
            if line:
                yield line
                continue
            # no new data; check rotation
            try:
                st = os.stat(self.path)
                cur_inode = st.st_ino
                if cur_inode != self._inode or st.st_size < self._pos:
                    # file rotated or truncated - reopen from start
                    LOG.info("source %s: rotation/truncation detected, reopening", self.name)
                    self._fh.close()
                    self._fh = open(self.path, "r", encoding="utf-8", errors="replace")
                    self._inode = cur_inode
                    self._pos = 0
                    continue
            except FileNotFoundError:
                LOG.warning("source %s: file gone, will retry", self.name)
                time.sleep(1.0)
                try:
                    self._fh = open(self.path, "r", encoding="utf-8", errors="replace")
                    self._inode = os.fstat(self._fh.fileno()).st_ino
                    self._pos = 0
                except FileNotFoundError:
                    continue
            time.sleep(TAIL_POLL_INTERVAL)

    def stop(self):
        self._stopped = True


class TailEngine:
    """Manages multiple TailSource workers and a shared ring buffer."""

    def __init__(self, buffer_size: int = DEFAULT_BUFFER):
        self.sources: List[TailSource] = []
        self.buffer_size = buffer_size
        # deque of (record, source_name)
        self.records: collections.deque = collections.deque(maxlen=buffer_size)
        # per-vehicle state for inference (latest known values)
        self.last_state: Dict[str, Dict[str, Any]] = {}
        # seen message ids for duplicate detection (sliding window)
        self.seen_mids: collections.deque = collections.deque(maxlen=50_000)
        self.seen_mids_set: set = set()
        # event queue for /annotations
        self.events: collections.deque = collections.deque(maxlen=10_000)
        # detected numeric field names (auto-populated)
        self.known_fields: set = set(DEFAULT_NUMERIC_FIELDS)
        # lock for safe concurrent access from flask threads
        self.lock = threading.RLock()
        self._threads: List[threading.Thread] = []

    def add_source(self, src: TailSource):
        self.sources.append(src)

    def start(self, replay_full: bool = False):
        for src in self.sources:
            src.start()
            if not src.is_stdin and not replay_full:
                # already seeked to end by start(); nothing to do
                pass
            t = threading.Thread(target=self._worker, args=(src,), daemon=True)
            t.start()
            self._threads.append(t)

    def stop(self):
        for s in self.sources:
            s.stop()

    # ----- ingest -----
    def _worker(self, src: TailSource):
        LOG.info("tailing source: %s", src.name)
        try:
            for line in src.iter_lines():
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError as e:
                    LOG.warning("source %s: bad JSON (%s): %s", src.name, e, line[:200])
                    continue
                if not isinstance(obj, dict):
                    continue
                rec = Record(obj)
                self._ingest(rec, src.name)
        except Exception as e:
            LOG.exception("source %s worker crashed: %s", src.name, e)

    def _ingest(self, rec: Record, src: str):
        with self.lock:
            self.records.append((rec, src))
            # register any new numeric fields
            for k in rec.fields:
                self.known_fields.add(k)
            # run inference engine
            new_events = InferenceEngine(self).evaluate(rec, src)
            for ev in new_events:
                self.events.append(ev)

    # ----- query helpers (called from flask handlers) -----
    def snapshot_records(self) -> List[Tuple[Record, str]]:
        with self.lock:
            return list(self.records)

    def snapshot_events(self) -> List[Dict[str, Any]]:
        with self.lock:
            return list(self.events)

    def fields(self) -> List[str]:
        with self.lock:
            return sorted(self.known_fields)


# ---------------------------------------------------------------------------
# Inference engine - turns raw records into annotations / events
# ---------------------------------------------------------------------------
class InferenceEngine:
    """Stateful rule engine. Evaluates each new record against the last known
    state for the same vehicle and emits events."""

    # rule parameters (tunable)
    SPEEDING_THRESHOLD = 80.0           # kmh, default for sedans
    TRUCK_SPEEDING_THRESHOLD = 70.0      # trucks have lower limit
    HARSH_BRAKE_DELTA = -15.0            # speed change over 5s
    HARSH_ACCEL_DELTA = 15.0
    LOW_FUEL = 20.0                      # ICE low-fuel warning %
    LOW_BATTERY = 15.0                  # EV low-battery warning %
    GAP_THRESHOLD_S = 30.0              # >30s silence = connectivity gap
    LATE_THRESHOLD_S = 60.0            # ts more than 60s behind last seen = late
    ODOMETER_JUMP_KM = 50.0            # impossible jump between two messages
    LOCATION_JUMP_KM = 10.0
    IMPOSSIBLE_SPEED_MAX = 200.0
    ENGINE_OFF_SPEED_MIN = 5.0         # engine off but still moving = anomaly
    IDLE_SPEED_THRESHOLD = 5.0         # below this, vehicle considered stopped
    IDLE_DURATION_S = 180.0            # 3 min of idling before alert
    IDLE_REEMIT_S = 60.0               # re-emit idle alert every 60s while still idle
    LOW_SIGNAL_THRESHOLD = 0.5         # signal_quality below this = bad connection
    ROUTE_BACKTRACK_THRESHOLD = 0.05   # route_progress dropped by >5% = deviation

    # Per-vehicle-type speeding thresholds (overrides SPEEDING_THRESHOLD).
    VTYPE_SPEEDING = {
        "TRUCK": 70.0,
        "BUS":   70.0,
        "VAN":   75.0,
        "SUV":   80.0,
        "SEDAN": 80.0,
        "MOTORBIKE": 70.0,
    }

    EVENT_TAGS = {
        # data quality
        "duplicate":        "data_quality",
        "late":             "data_quality",
        "gap":              "data_quality",
        "lost_messages":    "data_quality",
        "invalid":          "data_quality",
        "low_signal":       "data_quality",
        "ignition_mismatch": "data_quality",
        # operational
        "speeding":         "operational",
        "harsh_brake":      "operational",
        "harsh_accel":      "operational",
        "low_fuel":         "operational",
        "low_battery":      "operational",
        "engine_off_move":  "operational",
        "idle":             "operational",
        "route_deviation":  "operational",
        # security
        "driver_change":    "security",
        "unauthorized_use": "security",
        # diagnostic
        "diag_code":        "diagnostic",
        # anomaly
        "odometer_jump":    "anomaly",
        "location_jump":    "anomaly",
    }

    def __init__(self, engine: TailEngine):
        self.engine = engine

    def evaluate(self, rec: Record, src: str) -> List[Dict[str, Any]]:
        events: List[Dict[str, Any]] = []
        vid = rec.vid or "__global__"
        prev = self.engine.last_state.get(vid)

        # ---- resolved fields via alias resolver (works for both schemas) ----
        speed     = _field_value(rec, ["speed", "speed_kmh"])
        fuel      = _field_value(rec, ["fuel_level_pct", "fuel_level"])
        battery   = _field_value(rec, ["battery_level_pct", "battery_level"])
        odometer  = _field_value(rec, ["odometer_km", "odometer"])
        rpm       = _field_value(rec, ["rpm", "engine_rpm"])
        lat       = _field_value(rec, ["lat", "location.lat"])
        lng       = _field_value(rec, ["long", "lng", "location.lng"])
        signal    = _field_value(rec, ["signal_quality"])
        route     = _field_value(rec, ["route_progress"])
        seq       = _field_value(rec, SEQ_FIELDS)
        driver_id = _field_value(rec, DRV_FIELDS)
        vtype     = _field_value(rec, VTYPE_FIELDS)
        etype     = _field_value(rec, ETYPE_FIELDS)
        eng       = _field_value(rec, ["engine_status", "engine_on"])
        ign       = _field_value(rec, ["ignition_on"])

        # ---- 1. duplicate message id ----
        if rec.mid is not None:
            if rec.mid in self.engine.seen_mids_set:
                events.append(self._event(rec, "duplicate",
                    f"Duplicate message_id {rec.mid} from {vid}"))
            else:
                self.engine.seen_mids.append(rec.mid)
                self.engine.seen_mids_set.add(rec.mid)
                # bound the set size
                if len(self.engine.seen_mids_set) > 50_000:
                    old = self.engine.seen_mids.popleft()
                    self.engine.seen_mids_set.discard(old)

        # ---- 2. late / out-of-order (by ts) ----
        if prev and rec.ts < prev["ts"] - int(self.LATE_THRESHOLD_S * 1000):
            lag = (prev["ts"] - rec.ts) / 1000.0
            events.append(self._event(rec, "late",
                f"Out-of-order: {vid} ts {lag:.1f}s behind last seen"))

        # ---- 3. time gap detection ----
        if prev and rec.ts > prev["ts"] + int(self.GAP_THRESHOLD_S * 1000):
            gap = (rec.ts - prev["ts"]) / 1000.0
            events.append(self._event(rec, "gap",
                f"Connectivity gap: {vid} silent for {gap:.1f}s"))

        # ---- 4. seq-based lost message detection (more reliable than time) ----
        if prev and seq is not None and prev.get("last_seq") is not None:
            prev_seq = prev["last_seq"]
            if isinstance(seq, (int, float)) and isinstance(prev_seq, (int, float)):
                delta = int(seq) - int(prev_seq)
                if delta > 1:
                    lost = delta - 1
                    events.append(self._event(rec, "lost_messages",
                        f"{vid} seq jumped {int(prev_seq)} -> {int(seq)} ({lost} lost)"))
                # delta == 0 with different mid = duplicate-ish replay (already caught above)
                # delta < 0 = sequence regression = strong out-of-order signal
                if delta < 0:
                    events.append(self._event(rec, "lost_messages",
                        f"{vid} seq regressed {int(prev_seq)} -> {int(seq)}"))

        # ---- 5. low signal quality ----
        if signal is not None and isinstance(signal, (int, float)) and signal < self.LOW_SIGNAL_THRESHOLD:
            events.append(self._event(rec, "low_signal",
                f"{vid} signal_quality {signal:.2f}"))

        # ---- 6. invalid readings ----
        # engine-type aware: EVs null fuel is normal, ICE null battery is normal.
        is_ev = isinstance(etype, str) and etype.upper() in ("ELECTRIC", "EV", "BEV")
        is_ice = isinstance(etype, str) and etype.upper() in ("ICE", "DIESEL", "PETROL", "GASOLINE")
        if speed is not None and isinstance(speed, (int, float)) and (speed < 0 or speed > self.IMPOSSIBLE_SPEED_MAX):
            events.append(self._event(rec, "invalid",
                f"Impossible speed {speed} on {vid}"))
        # check fuel only when not null (null is normal for EVs)
        if fuel is not None and isinstance(fuel, (int, float)) and (fuel < 0 or fuel > 100):
            events.append(self._event(rec, "invalid",
                f"Impossible fuel_level_pct {fuel} on {vid}"))
        if battery is not None and isinstance(battery, (int, float)) and (battery < 0 or battery > 100):
            events.append(self._event(rec, "invalid",
                f"Impossible battery_level_pct {battery} on {vid}"))
        # cross-check: ICE should have fuel, EV should have battery
        if is_ice and fuel is None and battery is not None:
            events.append(self._event(rec, "invalid",
                f"{vid} engine_type=ICE but fuel_level_pct is null"))
        if is_ev and battery is None and fuel is not None:
            events.append(self._event(rec, "invalid",
                f"{vid} engine_type=ELECTRIC but battery_level_pct is null"))

        # ---- 7. speeding (vehicle-type aware) ----
        if speed is not None and isinstance(speed, (int, float)):
            thr = self.SPEEDING_THRESHOLD
            if isinstance(vtype, str):
                thr = self.VTYPE_SPEEDING.get(vtype.upper(), self.SPEEDING_THRESHOLD)
            if speed > thr:
                events.append(self._event(rec, "speeding",
                    f"{vid} ({vtype or 'unknown'}) speeding at {speed:.1f} kmh (limit {thr:.0f})"))

        # ---- 8. harsh brake / accel (uses prev speed from any alias) ----
        if prev and speed is not None:
            prev_speed = prev.get("last_speed")
            if prev_speed is not None:
                dt_s = (rec.ts - prev["ts"]) / 1000.0
                if 0 < dt_s <= 30:
                    dv = speed - prev_speed
                    dv5 = dv * (5.0 / dt_s) if dt_s > 0 else 0
                    if dv5 <= self.HARSH_BRAKE_DELTA:
                        events.append(self._event(rec, "harsh_brake",
                            f"{vid} harsh braking Δspeed={dv5:.1f}/5s"))
                    elif dv5 >= self.HARSH_ACCEL_DELTA:
                        events.append(self._event(rec, "harsh_accel",
                            f"{vid} harsh accel Δspeed={dv5:.1f}/5s"))

        # ---- 9. low fuel / battery (engine-type aware) ----
        if fuel is not None and isinstance(fuel, (int, float)) and fuel < self.LOW_FUEL and not is_ev:
            events.append(self._event(rec, "low_fuel",
                f"{vid} low fuel {fuel:.1f}%"))
        if battery is not None and isinstance(battery, (int, float)) and battery < self.LOW_BATTERY and (is_ev or not is_ice):
            events.append(self._event(rec, "low_battery",
                f"{vid} low battery {battery:.1f}%"))

        # ---- 10. engine off vs moving contradiction ----
        eng_off = False
        if isinstance(eng, bool):
            eng_off = not eng
        elif isinstance(eng, str):
            eng_off = eng.upper() in ("OFF", "0", "STOPPED", "FALSE")
        elif isinstance(eng, (int, float)):
            eng_off = eng == 0
        if eng_off and speed is not None and isinstance(speed, (int, float)) and speed > self.ENGINE_OFF_SPEED_MIN:
            events.append(self._event(rec, "engine_off_move",
                f"{vid} engine off but moving at {speed:.1f} kmh"))

        # ---- 11. ignition vs engine status mismatch ----
        if ign is not None and eng is not None:
            ign_on = ign if isinstance(ign, bool) else (
                ign.upper() in ("ON", "1", "TRUE") if isinstance(eng, str) else bool(ign)
            )
            if eng_off and ign_on:
                events.append(self._event(rec, "ignition_mismatch",
                    f"{vid} ignition_on but engine_status off"))
            elif not eng_off and not ign_on:
                events.append(self._event(rec, "ignition_mismatch",
                    f"{vid} engine_status on but ignition off"))

        # ---- 12. idle detection (stateful) ----
        # Vehicle is "idle" if speed below threshold and engine on. After
        # IDLE_DURATION_S of continuous idling, emit an idle event. Re-emit
        # every IDLE_REEMIT_S so the operator sees ongoing waste.
        moving = speed is not None and isinstance(speed, (int, float)) and speed > self.IDLE_SPEED_THRESHOLD
        if not eng_off and not moving:
            idle_since = prev.get("idle_since") if prev else None
            last_alert = prev.get("last_idle_alert") if prev else None
            if idle_since is None:
                idle_since = rec.ts
            idle_dur_s = (rec.ts - idle_since) / 1000.0
            if idle_dur_s >= self.IDLE_DURATION_S:
                if last_alert is None or (rec.ts - last_alert) / 1000.0 >= self.IDLE_REEMIT_S:
                    events.append(self._event(rec, "idle",
                        f"{vid} idling for {idle_dur_s/60:.1f} min"))
                    # remember alert ts in the new state we'll write below
                    self._pending_idle_alert_ts = rec.ts
        # else: not idling, reset idle state (handled by clearing on update)

        # ---- 13. diagnostic codes ----
        diag = rec.raw.get("diagnostic_codes") or rec.raw.get("dtc") or _get_path(rec.raw, "vehicle.dtc")
        if diag:
            codes = diag if isinstance(diag, list) else [diag]
            for c in codes:
                if c:
                    events.append(self._event(rec, "diag_code",
                        f"{vid} DTC {c}"))

        # ---- 14. odometer jump ----
        if prev and odometer is not None and prev.get("last_odometer") is not None:
            dv = odometer - prev["last_odometer"]
            if dv > self.ODOMETER_JUMP_KM:
                events.append(self._event(rec, "odometer_jump",
                    f"{vid} odometer jumped {dv:.1f}km"))

        # ---- 15. location jump (impossible GPS) ----
        if prev and lat is not None and lng is not None and prev.get("last_lat") is not None:
            d = haversine_km(prev["last_lat"], prev["last_lng"], lat, lng)
            dt_s = (rec.ts - prev["ts"]) / 1000.0
            if dt_s > 0 and d > self.LOCATION_JUMP_KM and d / max(dt_s / 3600.0, 1e-6) > 500:
                events.append(self._event(rec, "location_jump",
                    f"{vid} GPS jump {d:.1f}km in {dt_s:.1f}s"))

        # ---- 16. driver change (security: unauthorized use) ----
        if prev and driver_id is not None and prev.get("last_driver") is not None:
            if driver_id != prev["last_driver"]:
                events.append(self._event(rec, "driver_change",
                    f"{vid} driver changed {prev['last_driver']} -> {driver_id}"))

        # ---- 17. route deviation (route_progress going backwards) ----
        if prev and route is not None and prev.get("last_route") is not None:
            delta = route - prev["last_route"]
            if delta < -self.ROUTE_BACKTRACK_THRESHOLD:
                events.append(self._event(rec, "route_deviation",
                    f"{vid} route_progress dropped {delta:.3f} ({(delta*100):.1f}%)"))

        # ---- update per-vehicle state ----
        new_state: Dict[str, Any] = {
            "ts": rec.ts,
            "fields": dict(rec.fields),
            "raw": dict(rec.raw),
            "last_speed": speed,
            "last_odometer": odometer,
            "last_lat": lat,
            "last_lng": lng,
            "last_seq": seq,
            "last_driver": driver_id,
            "last_route": route,
            # idle state machine
            "idle_since": (rec.ts if (not eng_off and not moving and not prev)
                           else (None if (eng_off or moving)
                                 else (prev.get("idle_since") if prev else None))),
            "last_idle_alert": (getattr(self, "_pending_idle_alert_ts", None)
                                 if (not eng_off and not moving)
                                 else None),
        }
        # clean up the per-call attribute we used as scratch
        if hasattr(self, "_pending_idle_alert_ts"):
            del self._pending_idle_alert_ts
        self.engine.last_state[vid] = new_state
        return events

    @staticmethod
    def _event(rec: Record, etype: str, text: str) -> Dict[str, Any]:
        return {
            "time": rec.ts,
            "type": etype,
            "title": etype,
            "text": text,
            "tags": [InferenceEngine.EVENT_TAGS.get(etype, "general")],
            "vehicle_id": rec.vid,
        }


def haversine_km(lat1, lon1, lat2, lon2) -> float:
    from math import radians, sin, cos, asin, sqrt
    R = 6371.0
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat/2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon/2)**2
    return 2 * R * asin(sqrt(a))


# ---------------------------------------------------------------------------
# Target parser - "speed", "V001:speed", "speed{vehicle_id=V001}"
# ---------------------------------------------------------------------------
def parse_target(target: str) -> Tuple[str, Optional[str]]:
    """Return (field, vehicle_id_or_None)."""
    if not target:
        return "", None
    t = target.strip()
    # support "VID:field"
    if ":" in t:
        parts = t.split(":", 1)
        if parts[0] and parts[1]:
            return parts[1].strip(), parts[0].strip()
    # support "field{vehicle_id=V001}"
    if "{" in t and "}" in t:
        field = t.split("{", 1)[0].strip()
        filt = t[t.index("{")+1:t.index("}")]
        for kv in filt.split(","):
            if "=" in kv:
                k, v = kv.split("=", 1)
                k = k.strip()
                v = v.strip().strip('"')
                if k in VID_FIELDS:
                    return field, v
        return field, None
    return t, None


# ---------------------------------------------------------------------------
# Flask app - Grafana SimpleJson protocol
# ---------------------------------------------------------------------------
def build_app(engine: TailEngine) -> Flask:
    app = Flask(__name__)

    @app.route("/", methods=["GET"])
    def health():
        return jsonify({"status": "ok", "sources": [s.name for s in engine.sources]})

    @app.route("/health", methods=["GET"])
    def healthz():
        return jsonify({"status": "ok"})

    @app.route("/search", methods=["POST"])
    def search():
        """Return list of available targets. Includes every discovered numeric
        field, plus per-vehicle variants for any vehicle seen so far."""
        result = []
        vids = set()
        recs = engine.snapshot_records()
        for rec, _ in recs:
            if rec.vid:
                vids.add(rec.vid)
        for f in engine.fields():
            result.append(f)
            for v in sorted(vids):
                result.append(f"{v}:{f}")
        return jsonify(result)

    @app.route("/query", methods=["POST"])
    def query():
        """Return time-series datapoints for each requested target."""
        req = request.get_json(force=True, silent=True) or {}
        rng = req.get("range", {})
        try:
            t_from = _parse_grafana_time(rng.get("from"))
            t_to = _parse_grafana_time(rng.get("to"))
        except Exception:
            t_from = int((time.time() - 3600) * 1000)
            t_to = int(time.time() * 1000)
        # "maxDataPoints" - we downsample to avoid flooding grafana
        max_points = int(req.get("maxDataPoints") or 1000) or 1000

        targets = req.get("targets", [])
        recs = engine.snapshot_records()
        # sort by ts ascending so we can dedupe + downsample cleanly
        recs.sort(key=lambda x: x[0].ts)

        response = []
        for tgt in targets:
            target = tgt.get("target") if isinstance(tgt, dict) else str(tgt)
            if not target:
                continue
            field, want_vid = parse_target(target)
            dps = []
            for rec, _ in recs:
                if rec.ts < t_from or rec.ts > t_to:
                    continue
                if want_vid and rec.vid != want_vid:
                    continue
                if field not in rec.fields:
                    continue
                v = rec.fields[field]
                if isinstance(v, bool):
                    continue
                dps.append([float(v), rec.ts])
            # downsample
            if len(dps) > max_points:
                step = max(1, len(dps) // max_points)
                dps = dps[::step]
            response.append({"target": target, "datapoints": dps})
        return jsonify(response)

    @app.route("/annotations", methods=["POST"])
    def annotations():
        req = request.get_json(force=True, silent=True) or {}
        rng = req.get("range", {})
        try:
            t_from = _parse_grafana_time(rng.get("from"))
            t_to = _parse_grafana_time(rng.get("to"))
        except Exception:
            t_from = int((time.time() - 3600) * 1000)
            t_to = int(time.time() * 1000)

        # optional filter: Grafana passes annotation query as a "query" string
        # in the form "tag" or "type:speeding". Empty = all.
        q = (req.get("annotation") or {}).get("query", "") if isinstance(req.get("annotation"), dict) else ""
        q = q or req.get("query", "")
        q = (q or "").strip()

        result = []
        for ev in engine.snapshot_events():
            if ev["time"] < t_from or ev["time"] > t_to:
                continue
            if q:
                if q.startswith("type:"):
                    if ev["type"] != q[5:]:
                        continue
                elif q.startswith("tag:"):
                    if q[4:] not in ev.get("tags", []):
                        continue
                elif q not in ev["type"] and q not in ev.get("text", ""):
                    continue
            result.append({
                "time": ev["time"],
                "title": f"[{ev['type']}] {ev.get('vehicle_id') or ''}",
                "text": ev["text"],
                "tags": ev.get("tags", []),
            })
        return jsonify(result)

    @app.route("/tags", methods=["POST"])
    def tags():
        # optional SimpleJson extension: return list of known event tags
        all_tags = set()
        for ev in engine.snapshot_events():
            for t in ev.get("tags", []):
                all_tags.add(t)
        return jsonify([{"text": t} for t in sorted(all_tags)])

    return app


def _parse_grafana_time(v) -> int:
    """Grafana sends time either as ISO string or epoch ms (number-as-string)."""
    if v is None:
        return int((time.time() - 3600) * 1000)
    if isinstance(v, (int, float)):
        return int(v)
    s = str(v).strip()
    if s and s[-1] in ("Z", "z"):
        s = s[:-1]
    # numeric?
    try:
        n = float(s)
        return int(n * 1000) if n < 1e12 else int(n)
    except ValueError:
        pass
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
    except ValueError:
        return int((time.time() - 3600) * 1000)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main():
    p = argparse.ArgumentParser(
        description="Telemetrix Grafana SimpleJson provider - live JSONL tail",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--file", "-f", action="append", default=[], metavar="PATH",
                   help="JSONL file to tail. Can be given multiple times.")
    p.add_argument("--stdin", action="store_true",
                   help="Also read JSONL from stdin (pipe mode).")
    p.add_argument("--host", default=DEFAULT_HOST)
    p.add_argument("--port", type=int, default=DEFAULT_PORT)
    p.add_argument("--buffer", type=int, default=DEFAULT_BUFFER,
                   help="In-memory ring buffer size (number of records).")
    p.add_argument("--replay", action="store_true",
                   help="On startup, replay the full file from the start instead of tailing from end.")
    p.add_argument("--log-level", default="INFO",
                   choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    args = p.parse_args()

    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )

    engine = TailEngine(buffer_size=args.buffer)

    if not args.file and not args.stdin:
        # default: read from stdin so the tool is pipe-friendly by default
        LOG.info("no --file given; defaulting to stdin (pipe mode). Pass --file to tail a file.")
        args.stdin = True

    for f in args.file:
        if not os.path.exists(f):
            LOG.warning("file does not exist yet, will wait: %s", f)
        engine.add_source(TailSource(name=f, path=f, is_stdin=False))

    if args.stdin:
        engine.add_source(TailSource(name="stdin", is_stdin=True))

    # For file sources in --replay mode, override the start() seek-to-end behaviour
    if args.replay:
        for s in engine.sources:
            if not s.is_stdin:
                def _patched_start(_self=s):
                    _self._fh = open(_self.path, "r", encoding="utf-8", errors="replace")
                    _self._inode = os.fstat(_self._fh.fileno()).st_ino
                    _self._pos = 0
                s.start = _patched_start  # type: ignore

    engine.start(replay_full=args.replay)

    app = build_app(engine)
    LOG.info("Telemetrix provider listening on http://%s:%d", args.host, args.port)
    LOG.info("Grafana data source URL: http://<this-host>:%d", args.port)
    try:
        app.run(host=args.host, port=args.port, threaded=True, use_reloader=False)
    finally:
        engine.stop()
