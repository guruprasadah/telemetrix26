"""
Telemetrix — Connected Vehicle Telematics Data Generator
========================================================
Invente'26 Hackathon

A single-file, stdlib-only synthetic telematics generator for a fleet of
connected vehicles. Emits JSONL (one JSON object per line) suitable for
streaming pipelines (Kafka, Kinesis, file tail, pipe, websocket, etc.).

Each message contains at minimum:
    message_id, vehicle_id, location{lat,lng}, engine_status,
    fuel_level_pct | battery_level_pct, speed_kmh, odometer_km,
    diagnostic_codes[], vehicle_timestamp

Plus optional realism fields: heading, rpm, gear, driver_id, ignition_on,
signal_quality, vehicle_type, engine_type, route_progress.

DATA QUALITY FAULTS (on-demand, judges can trigger these live):
    duplicate       — re-emit a message with the same message_id (modem resend)
    out_of_order    — hold back ~30% of messages, release later shuffled
    missing         — silently drop ~30% of messages (gaps in time series)
    invalid         — corrupt ~30% of messages (negative fuel, NaN lat,
                      impossible speed, future timestamp, invalid engine
                      status, zero odometer, etc.)

PRODUCT SCENARIOS (on-demand):
    crash               — airbag + ABS fault, vehicle stationary, FAULT status
    harsh_braking       — sudden target-speed drop to 0
    harsh_acceleration  — sudden target-speed jump to 100
    overspeed           — forced 120 km/h target for 60s
    idle                — forced idle for 60s (engine on, speed 0)
    route_deviation     — drift off planned route for 60s
    unauthorized_use    — force vehicle to keep moving at 80 km/h
    fuel_theft          — sudden 20% fuel drop while stationary

USAGE EXAMPLES
--------------

1. Pipe JSONL to stdout (5 vehicles at 1 Hz per vehicle):
       python telemetrix_generator.py --vehicles 5 --rate 1

2. Write to a file for 60 seconds:
       python telemetrix_generator.py --vehicles 10 --rate 2 \\
           --duration 60 --output file --file telemetry.jsonl

3. Interactive demo mode (judges type commands at runtime):
       python telemetrix_generator.py --vehicles 5 --interactive

4. Start with a fault already active:
       python telemetrix_generator.py --vehicles 5 --inject duplicate

5. Trigger a crash on vehicle VHC-003 at startup:
       python telemetrix_generator.py --vehicles 5 --scenario crash:VHC-003

INTERACTIVE COMMANDS (when --interactive is set)
------------------------------------------------
    inject <fault>                          start a fault
    stop  <fault>                            stop a fault
    trigger <scenario>[:<vehicle_id>]        trigger a scenario
    list vehicles                            list vehicle IDs
    list faults                              list active faults
    status                                   show generator status
    help                                     show command help
    quit                                     stop the generator

CLI FLAGS
---------
    --vehicles N           number of vehicles (default 5)
    --rate R               messages per second per vehicle (default 1.0)
    --duration S           stop after S seconds (0 = unlimited)
    --output {stdout,file,both}  (default stdout)
    --file PATH            output file path when output=file/both
    --seed N               RNG seed for reproducibility
    --inject <fault>       start with fault active (repeatable; 'all' = all 4)
    --scenario <s[:vid]>   trigger scenario at startup (repeatable)
    --interactive          enable stdin command control
    --stats-interval S     print stats to stderr every S seconds (default 10)
    --tag-corrupt          tag corrupted messages with _corrupted field (debug)
    --list-faults          list known faults and exit
    --list-scenarios       list known scenarios and exit

DESIGN NOTES
------------
- Single file, stdlib only (no pip install required).
- Threaded interactive controller reads stdin without blocking the stream.
- Out-of-order fault drains its held-back buffer even after the fault stops,
  so the pipeline sees the delayed messages eventually.
- Reproducible with --seed for demo consistency.
- Vehicles loop along predefined routes around Bangalore (Asia/Calcutta TZ).
- Mixed fleet: sedans / vans / trucks (ICE) and EVs (battery).
"""

from __future__ import annotations

import argparse
import json
import math
import random
import signal
import sys
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Dict, List, Optional


# ---------------------------------------------------------------------------
# Constants & reference data
# ---------------------------------------------------------------------------

# Bangalore region — all routes live inside this bounding box
BANGALORE_CENTER = (12.9716, 77.5946)

# Each route is a list of (lat, lng) waypoints. Vehicles loop along their route.
ROUTES: List[List[tuple]] = [
    # Airport -> Whitefield tech corridor
    [(12.9716, 77.5946), (12.9550, 77.6520), (12.9698, 77.7500), (12.9698, 77.7495)],
    # Indiranagar -> Koramangala -> HSR Layout
    [(12.9719, 77.6412), (12.9352, 77.6245), (12.9279, 77.6271), (12.9116, 77.6473)],
    # MG Road -> Electronic City
    [(12.9756, 77.6060), (12.9050, 77.6030), (12.8450, 77.6600)],
    # Hebbal -> Yelahanka -> Airport
    [(13.0350, 77.5970), (13.0550, 77.5950), (13.1000, 77.5800), (13.1986, 77.7066)],
    # Jayanagar -> Banashankari -> JP Nagar
    [(12.9250, 77.5940), (12.9160, 77.5740), (12.9180, 77.5450), (12.9080, 77.5850)],
    # Marathahalli -> Sarjapur -> Whitefield loop
    [(12.9569, 77.7011), (12.9350, 77.6250), (12.9698, 77.7500), (12.9569, 77.7011)],
]

# OBD-II diagnostic trouble codes (DTCs) — real P/C/B/U codes
OBD_CODES: Dict[str, str] = {
    "P0300": "Random/Multiple Cylinder Misfire Detected",
    "P0420": "Catalyst System Efficiency Below Threshold (Bank 1)",
    "P0171": "System Too Lean (Bank 1)",
    "P0128": "Coolant Thermostat Below Operating Temperature",
    "P0455": "Evaporative Emission System Leak Detected (large)",
    "P0442": "Evaporative Emission System Leak Detected (small)",
    "P0500": "Vehicle Speed Sensor Malfunction",
    "P0110": "Intake Air Temperature Circuit Malfunction",
    "P0339": "Crankshaft Position Sensor Circuit Intermittent",
    "P0234": "Turbocharger/Supercharger Overboost Condition",
    "P0401": "Exhaust Gas Recirculation Insufficient Flow",
    "P0606": "Powertrain Control Module Processor Fault",
    "P0102": "Mass Air Flow Circuit Low Input",
    "P0135": "O2 Sensor Heater Circuit Malfunction (Bank 1, Sensor 1)",
    "C0300": "Anti-Lock Brake System (ABS) Fault",
    "C1201": "Brake System Malfunction",
    "B1100": "Airbag Deployment Fault",
    "B1234": "Driver Side Airbag Circuit Open",
    "U0100": "Loss of Communication with ECM/PCM",
    "U0121": "Loss of Communication with ABS Control Module",
}

# Vehicle archetypes — mixed fleet
VEHICLE_TYPES = [
    {"id_prefix": "SEDAN",  "engine": "ICE",      "max_speed": 180, "fuel_burn_idle": 0.003, "fuel_burn_per_kmh": 0.0008},
    {"id_prefix": "SEDAN",  "engine": "ELECTRIC", "max_speed": 160, "fuel_burn_idle": 0.002, "fuel_burn_per_kmh": 0.0006},
    {"id_prefix": "VAN",    "engine": "ICE",      "max_speed": 130, "fuel_burn_idle": 0.005, "fuel_burn_per_kmh": 0.0012},
    {"id_prefix": "TRUCK",  "engine": "ICE",      "max_speed": 100, "fuel_burn_idle": 0.008, "fuel_burn_per_kmh": 0.0020},
    {"id_prefix": "TRUCK",  "engine": "ELECTRIC", "max_speed":  90, "fuel_burn_idle": 0.004, "fuel_burn_per_kmh": 0.0012},
]

KNOWN_FAULTS = ["duplicate", "out_of_order", "missing", "invalid"]
KNOWN_SCENARIOS = [
    "crash", "harsh_braking", "harsh_acceleration", "overspeed",
    "idle", "route_deviation", "unauthorized_use", "fuel_theft",
]


# ---------------------------------------------------------------------------
# Vehicle simulation
# ---------------------------------------------------------------------------

class EngineStatus(str, Enum):
    ON = "ON"
    OFF = "OFF"
    IDLE = "IDLE"
    FAULT = "FAULT"


@dataclass
class VehicleConfig:
    vehicle_id: str
    vehicle_type: str
    engine_type: str          # "ICE" or "ELECTRIC"
    route: List[tuple]
    driver_id: str
    initial_energy_pct: float = 80.0
    initial_odometer_km: float = 0.0
    max_speed: float = 120.0
    fuel_burn_idle: float = 0.003
    fuel_burn_per_kmh: float = 0.0008


@dataclass
class VehicleState:
    cfg: VehicleConfig
    lat: float
    lng: float
    heading: float = 0.0
    speed_kmh: float = 0.0
    target_speed_kmh: float = 50.0
    fuel_pct: Optional[float] = None       # ICE only
    battery_pct: Optional[float] = None    # EV only
    odometer_km: float = 0.0
    engine_status: EngineStatus = EngineStatus.OFF
    rpm: int = 0
    gear: int = 0
    diagnostic_codes: List[str] = field(default_factory=list)
    route_progress: float = 0.0           # 0..1 along route
    ignition_on: bool = False
    signal_quality: float = 1.0
    # Scenario state
    crash_until: float = 0.0
    forced_idle_until: float = 0.0
    forced_overspeed_until: float = 0.0
    forced_off_route_until: float = 0.0
    unauthorized_until: float = 0.0
    fuel_theft_drop: float = 0.0
    harsh_brake_until: float = 0.0
    harsh_accel_until: float = 0.0
    harsh_brake_pending: bool = False
    harsh_accel_pending: bool = False
    last_message_seq: int = 0


def haversine_km(p1: tuple, p2: tuple) -> float:
    """Great-circle distance between two (lat, lng) points, in km."""
    R = 6371.0
    lat1, lng1 = math.radians(p1[0]), math.radians(p1[1])
    lat2, lng2 = math.radians(p2[0]), math.radians(p2[1])
    dlat = lat2 - lat1
    dlng = lng2 - lng1
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlng / 2) ** 2
    return R * 2.0 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def route_length_km(route: List[tuple]) -> float:
    return sum(haversine_km(route[i], route[i + 1]) for i in range(len(route) - 1))


def interpolate_route(route: List[tuple], progress: float) -> tuple:
    """Return (lat, lng, heading_deg) at given progress (0..1) along route."""
    if len(route) < 2:
        return route[0][0], route[0][1], 0.0
    n = len(route) - 1
    p = (progress % 1.0) * n
    i = min(int(p), n - 1)
    t = p - i
    p1, p2 = route[i], route[i + 1]
    lat = p1[0] + (p2[0] - p1[0]) * t
    lng = p1[1] + (p2[1] - p1[1]) * t
    # Bearing from p1 to p2 (degrees)
    dlng = math.radians(p2[1] - p1[1])
    lat1r, lat2r = math.radians(p1[0]), math.radians(p2[0])
    x = math.sin(dlng) * math.cos(lat2r)
    y = math.cos(lat1r) * math.sin(lat2r) - math.sin(lat1r) * math.cos(lat2r) * math.cos(dlng)
    heading = (math.degrees(math.atan2(x, y)) + 360.0) % 360.0
    return lat, lng, heading


class Vehicle:
    """A single connected vehicle with physics, energy model and scenarios."""

    def __init__(self, cfg: VehicleConfig, rng: random.Random):
        self.cfg = cfg
        self.rng = rng
        self.state = VehicleState(
            cfg=cfg,
            lat=cfg.route[0][0],
            lng=cfg.route[0][1],
            fuel_pct=(cfg.initial_energy_pct if cfg.engine_type == "ICE" else None),
            battery_pct=(cfg.initial_energy_pct if cfg.engine_type == "ELECTRIC" else None),
            odometer_km=cfg.initial_odometer_km,
        )
        self._start_engine()

    def _start_engine(self):
        s = self.state
        s.engine_status = EngineStatus.ON
        s.ignition_on = True
        s.target_speed_kmh = self.rng.uniform(30, 70)

    # ---- Main physics step ----------------------------------------------
    def step(self, dt: float):
        s = self.state
        now = time.time()

        # Crash scenario — vehicle stationary with engine fault
        if s.crash_until > 0:
            if now < s.crash_until:
                s.speed_kmh = 0.0
                s.engine_status = EngineStatus.FAULT
                s.rpm = 0
                s.gear = 0
                return
            else:
                # Crash is over, clear codes and restart engine
                s.crash_until = 0.0
                for code in ("B1100", "B1234", "C0300", "C1201"):
                    if code in s.diagnostic_codes:
                        s.diagnostic_codes.remove(code)
                s.engine_status = EngineStatus.ON
                s.ignition_on = True

        # Scenario contract (shared with the inference engine):
        # overspeed >100 km/h for 5s; idle <1 km/h for 30s; unauthorized
        # 75-85 km/h for 30s; harsh accel/braking >=5 km/h/s; fuel theft
        # >=10% drop while <5 km/h; route deviation >300m.
        if now < s.forced_idle_until:
            target = 0.0
            s.engine_status = EngineStatus.IDLE
        elif now < s.forced_overspeed_until:
            target = max(110.0, min(120.0, s.cfg.max_speed))
            s.engine_status = EngineStatus.ON
        elif now < s.unauthorized_until:
            target = 80.0
            s.engine_status = EngineStatus.ON
        else:
            target = s.target_speed_kmh
            s.engine_status = EngineStatus.ON

        delta = target - s.speed_kmh
        rate = 5.0
        if now < s.harsh_brake_until:
            if s.harsh_brake_pending:
                s.harsh_brake_pending = False
                return
            rate = 8.0
        elif now < s.harsh_accel_until:
            if s.harsh_accel_pending:
                s.harsh_accel_pending = False
                return
            rate = 8.0
        max_delta = rate * dt
        s.speed_kmh += max(-max_delta, min(max_delta, delta))
        s.speed_kmh = max(0.0, min(s.speed_kmh, s.cfg.max_speed))

        # Move along route if moving and engine on
        if s.speed_kmh > 0 and s.engine_status in (EngineStatus.ON, EngineStatus.IDLE):
            dist_km = (s.speed_kmh * dt) / 3600.0
            s.odometer_km += dist_km
            total_len = route_length_km(s.cfg.route)
            if total_len > 0:
                s.route_progress = (s.route_progress + dist_km / total_len) % 1.0
            lat, lng, hdg = interpolate_route(s.cfg.route, s.route_progress)
            # Deterministic route-deviation offset: ~500m north of route.
            if now < s.forced_off_route_until:
                lat += 0.0045
            s.lat, s.lng, s.heading = lat, lng, hdg

        # RPM / gear
        if s.engine_status == EngineStatus.ON and s.speed_kmh > 0:
            s.gear = min(6, max(1, int(s.speed_kmh / 20) + 1))
            s.rpm = max(700, min(7000, int(800 + s.speed_kmh * 35 + self.rng.randint(-100, 100))))
        elif s.engine_status == EngineStatus.IDLE:
            s.gear = 0
            s.rpm = int(800 + self.rng.randint(-50, 50))
        else:
            s.gear = 0
            s.rpm = 0

        # Energy consumption
        if s.cfg.engine_type == "ICE" and s.fuel_pct is not None:
            if s.fuel_pct > 0:
                burn = s.cfg.fuel_burn_idle * dt
                if s.speed_kmh > 0:
                    burn += s.cfg.fuel_burn_per_kmh * s.speed_kmh * dt
                s.fuel_pct = max(0.0, s.fuel_pct - burn)
                if s.fuel_theft_drop > 0:
                    s.fuel_pct = max(0.0, s.fuel_pct - s.fuel_theft_drop)
                    s.fuel_theft_drop = 0.0
                if s.fuel_pct <= 0:
                    s.engine_status = EngineStatus.OFF
                    s.ignition_on = False
                    s.speed_kmh = 0.0
        elif s.cfg.engine_type == "ELECTRIC" and s.battery_pct is not None:
            if s.battery_pct > 0:
                burn = s.cfg.fuel_burn_idle * dt
                if s.speed_kmh > 0:
                    burn += s.cfg.fuel_burn_per_kmh * s.speed_kmh * dt
                s.battery_pct = max(0.0, s.battery_pct - burn)
                if s.fuel_theft_drop > 0:
                    s.battery_pct = max(0.0, s.battery_pct - s.fuel_theft_drop)
                    s.fuel_theft_drop = 0.0
                if s.battery_pct <= 0:
                    s.engine_status = EngineStatus.OFF
                    s.ignition_on = False
                    s.speed_kmh = 0.0

        # Random target speed changes (traffic, lights, turns)
        if self.rng.random() < 0.05 and s.engine_status == EngineStatus.ON:
            s.target_speed_kmh = self.rng.uniform(20, min(80, s.cfg.max_speed))

        # Signal quality drifts
        s.signal_quality = max(0.30, min(1.0, s.signal_quality + self.rng.uniform(-0.05, 0.05)))

        # Spontaneous low-probability DTC
        if self.rng.random() < 0.0008 and not s.diagnostic_codes:
            code = self.rng.choice(list(OBD_CODES.keys()))
            s.diagnostic_codes.append(code)

    # ---- Scenario triggers ---------------------------------------------
    def trigger_crash(self, duration_s: float = 30.0):
        s = self.state
        s.crash_until = time.time() + duration_s
        s.speed_kmh = 0.0
        s.engine_status = EngineStatus.FAULT
        for code in ("B1100", "C0300"):
            if code not in s.diagnostic_codes:
                s.diagnostic_codes.append(code)

    def trigger_harsh_braking(self):
        s = self.state
        # Guarantee the detector's previous-speed >20 precondition.
        s.speed_kmh = max(40.0, s.speed_kmh)
        s.target_speed_kmh = 0.0
        s.harsh_brake_until = time.time() + 4.0
        s.harsh_brake_pending = True

    def trigger_harsh_acceleration(self):
        s = self.state
        s.speed_kmh = min(max(10.0, s.speed_kmh), 20.0)
        s.target_speed_kmh = min(100.0, s.cfg.max_speed)
        s.harsh_accel_until = time.time() + 4.0
        s.harsh_accel_pending = True

    def trigger_overspeed(self, duration_s: float = 60.0):
        self.state.forced_overspeed_until = time.time() + duration_s

    def trigger_idle(self, duration_s: float = 60.0):
        self.state.forced_idle_until = time.time() + duration_s

    def trigger_route_deviation(self, duration_s: float = 60.0):
        self.state.forced_off_route_until = time.time() + duration_s

    def trigger_unauthorized_use(self, duration_s: float = 60.0):
        s = self.state
        s.unauthorized_until = time.time() + duration_s
        s.target_speed_kmh = 80.0

    def trigger_fuel_theft(self, drop_pct: float = 20.0):
        s = self.state
        s.target_speed_kmh = 0.0
        s.forced_idle_until = max(s.forced_idle_until, time.time() + 2.0)
        s.speed_kmh = 0.0
        s.fuel_theft_drop = max(10.0, drop_pct)

    # ---- Build outgoing message ---------------------------------------
    def build_message(self) -> dict:
        s = self.state
        s.last_message_seq += 1
        ts = datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
        return {
            "message_id": f"msg_{uuid.uuid4().hex[:24]}",
            "vehicle_id": s.cfg.vehicle_id,
            "timestamp": ts,                       # generator emit time
            "vehicle_timestamp": ts,                # vehicle-side timestamp (may lag due to OOO)
            "location": {
                "lat": round(s.lat, 6),
                "lng": round(s.lng, 6),
                "heading": round(s.heading, 1),
            },
            "engine_status": s.engine_status.value,
            "fuel_level_pct": (round(s.fuel_pct, 2) if s.cfg.engine_type == "ICE" else None),
            "battery_level_pct": (round(s.battery_pct, 2) if s.cfg.engine_type == "ELECTRIC" else None),
            "speed_kmh": round(s.speed_kmh, 2),
            "odometer_km": round(s.odometer_km, 2),
            "diagnostic_codes": list(s.diagnostic_codes),
            # extra realism fields — useful for product features
            "rpm": s.rpm,
            "gear": s.gear,
            "driver_id": s.cfg.driver_id,
            "ignition_on": s.ignition_on,
            "signal_quality": round(s.signal_quality, 3),
            "vehicle_type": s.cfg.vehicle_type,
            "engine_type": s.cfg.engine_type,
            "route_progress": round(s.route_progress, 4),
            # Contract field consumed directly by inference.
            "route_deviation_m": (500.0 if time.time() < s.forced_off_route_until else 0.0),
            "telemetry_rate_hz": getattr(self, "telemetry_rate_hz", 1.0),
            "seq": s.last_message_seq,
        }


# ---------------------------------------------------------------------------
# Fault injector
# ---------------------------------------------------------------------------

class FaultInjector:
    """Applies data quality faults to outgoing message batches."""

    def __init__(self, rng: random.Random, tag_corrupt: bool = False):
        self.rng = rng
        self.tag_corrupt = tag_corrupt
        self.active_faults: set = set()
        self._pending_ooo: List[dict] = []

    def activate(self, fault: str):
        if fault in KNOWN_FAULTS:
            self.active_faults.add(fault)

    def deactivate(self, fault: str):
        self.active_faults.discard(fault)

    def process(self, messages: List[dict]) -> List[dict]:
        out: List[dict] = list(messages)

        # 1. Missing — silently drop ~30%
        if "missing" in self.active_faults:
            out = [m for m in out if self.rng.random() >= 0.3]

        # 2. Duplicate — re-emit ~20% with the SAME message_id (modem resend)
        if "duplicate" in self.active_faults:
            new_out = []
            for m in out:
                new_out.append(m)
                if self.rng.random() < 0.2:
                    new_out.append(dict(m))
            out = new_out

        # 3. Invalid — corrupt ~30% of messages with anomalous readings
        if "invalid" in self.active_faults:
            for m in out:
                if self.rng.random() < 0.3:
                    self._corrupt(m)

        # 4. Out-of-order — hold back ~30%, release previously held shuffled
        if "out_of_order" in self.active_faults or self._pending_ooo:
            if "out_of_order" in self.active_faults:
                new_out = []
                for m in out:
                    if self.rng.random() < 0.3:
                        self._pending_ooo.append(m)
                    else:
                        new_out.append(m)
                out = new_out
            # Drain half of pending buffer even if the fault is now off,
            # so the pipeline eventually sees the delayed messages.
            if self._pending_ooo:
                self.rng.shuffle(self._pending_ooo)
                n_release = max(1, len(self._pending_ooo) // 2)
                out.extend(self._pending_ooo[:n_release])
                self._pending_ooo = self._pending_ooo[n_release:]

        return out

    def _corrupt(self, m: dict):
        """Inject one of several realistic anomalous readings."""
        kind = self.rng.choice([
            "negative_fuel", "impossible_speed", "null_lat", "bad_lng",
            "future_timestamp", "past_timestamp", "zero_odometer",
            "negative_odometer", "invalid_engine_status", "huge_fuel",
            "negative_speed", "lat_out_of_range",
        ])
        if kind == "negative_fuel":
            if m.get("fuel_level_pct") is not None:
                m["fuel_level_pct"] = -5.0
            elif m.get("battery_level_pct") is not None:
                m["battery_level_pct"] = -10.0
        elif kind == "huge_fuel":
            if m.get("fuel_level_pct") is not None:
                m["fuel_level_pct"] = 250.0
            elif m.get("battery_level_pct") is not None:
                m["battery_level_pct"] = 250.0
        elif kind == "impossible_speed":
            m["speed_kmh"] = self.rng.choice([350.0, 999.0, 1234.5])
        elif kind == "negative_speed":
            m["speed_kmh"] = self.rng.choice([-30.0, -100.0])
        elif kind == "null_lat":
            m["location"]["lat"] = None
        elif kind == "bad_lng":
            m["location"]["lng"] = 200.0
        elif kind == "lat_out_of_range":
            m["location"]["lat"] = self.rng.choice([95.0, -85.0])
        elif kind == "future_timestamp":
            ts = (datetime.now(timezone.utc) + timedelta(hours=2)).isoformat(timespec="milliseconds").replace("+00:00", "Z")
            m["vehicle_timestamp"] = ts
        elif kind == "past_timestamp":
            ts = (datetime.now(timezone.utc) - timedelta(days=3)).isoformat(timespec="milliseconds").replace("+00:00", "Z")
            m["vehicle_timestamp"] = ts
        elif kind == "zero_odometer":
            m["odometer_km"] = 0.0
        elif kind == "negative_odometer":
            m["odometer_km"] = -100.0
        elif kind == "invalid_engine_status":
            m["engine_status"] = self.rng.choice(["INVALID", "RUNNING", "", "ON_FIRE"])

        if self.tag_corrupt:
            m["_corrupted"] = kind


# ---------------------------------------------------------------------------
# Scenario engine
# ---------------------------------------------------------------------------

class ScenarioEngine:
    def __init__(self, vehicles: Dict[str, Vehicle]):
        self.vehicles = vehicles

    def trigger(self, scenario: str, vehicle_id: Optional[str] = None) -> bool:
        scenario = scenario.lower()
        if scenario not in KNOWN_SCENARIOS:
            return False
        if vehicle_id and vehicle_id in self.vehicles:
            targets = [self.vehicles[vehicle_id]]
        elif vehicle_id:
            return False  # requested a specific vehicle that doesn't exist
        else:
            targets = list(self.vehicles.values())
        for v in targets:
            if   scenario == "crash":                v.trigger_crash()
            elif scenario == "harsh_braking":        v.trigger_harsh_braking()
            elif scenario == "harsh_acceleration":   v.trigger_harsh_acceleration()
            elif scenario == "overspeed":            v.trigger_overspeed()
            elif scenario == "idle":                 v.trigger_idle()
            elif scenario == "route_deviation":      v.trigger_route_deviation()
            elif scenario == "unauthorized_use":    v.trigger_unauthorized_use()
            elif scenario == "fuel_theft":           v.trigger_fuel_theft()
        return True


# ---------------------------------------------------------------------------
# Output sink
# ---------------------------------------------------------------------------

class OutputSink:
    def __init__(self, mode: str, file_path: Optional[str] = None):
        self.mode = mode
        self._fp = None
        if mode in ("file", "both") and file_path:
            self._fp = open(file_path, "w", buffering=1)

    def write(self, message: dict):
        line = json.dumps(message, separators=(",", ":"), ensure_ascii=False)
        if self.mode in ("stdout", "both"):
            sys.stdout.write(line + "\n")
            sys.stdout.flush()
        if self.mode in ("file", "both") and self._fp:
            self._fp.write(line + "\n")
            self._fp.flush()

    def close(self):
        if self._fp:
            self._fp.close()
            self._fp = None


# ---------------------------------------------------------------------------
# Interactive controller (background thread reading stdin)
# ---------------------------------------------------------------------------

class InteractiveController(threading.Thread):
    HELP = """
Interactive commands:
  inject <fault>                          start a fault (duplicate|out_of_order|missing|invalid)
  stop  <fault>                            stop a fault
  trigger <scenario>[:<vehicle_id>]        trigger a scenario
  list vehicles                            list vehicle IDs
  list faults                              list active faults
  list scenarios                           list known scenarios
  status                                   show generator status
  help                                     show this help
  quit                                     stop the generator
""".strip()

    def __init__(self, fi: FaultInjector, se: ScenarioEngine,
                 vehicles: Dict[str, Vehicle], stats: dict,
                 stop_event: threading.Event):
        super().__init__(daemon=True)
        self.fi = fi
        self.se = se
        self.vehicles = vehicles
        self.stats = stats
        self.stop_event = stop_event

    def run(self):
        sys.stderr.write("\n" + self.HELP + "\n\n")
        sys.stderr.flush()
        while not self.stop_event.is_set():
            try:
                line = input("telemetrix> ")
            except (EOFError, KeyboardInterrupt):
                self.stop_event.set()
                break
            line = line.strip()
            if not line:
                continue
            parts = line.split()
            cmd = parts[0].lower()
            try:
                if cmd in ("quit", "exit", "q"):
                    self.stop_event.set()
                    break
                elif cmd == "help":
                    sys.stderr.write(self.HELP + "\n")
                elif cmd == "inject" and len(parts) >= 2:
                    fault = parts[1].lower()
                    if fault in KNOWN_FAULTS:
                        self.fi.activate(fault)
                        self.stats["faults_active"] = sorted(self.fi.active_faults)
                        sys.stderr.write(f"[OK] Activated fault: {fault}\n")
                    else:
                        sys.stderr.write(f"[ERR] Unknown fault: {fault}. Known: {KNOWN_FAULTS}\n")
                elif cmd == "stop" and len(parts) >= 2:
                    fault = parts[1].lower()
                    self.fi.deactivate(fault)
                    self.stats["faults_active"] = sorted(self.fi.active_faults)
                    sys.stderr.write(f"[OK] Stopped fault: {fault}\n")
                elif cmd == "trigger" and len(parts) >= 2:
                    spec = parts[1]
                    scenario, vid = (spec.split(":", 1) + [None])[:2]
                    ok = self.se.trigger(scenario, vid)
                    if ok:
                        self.stats["scenarios_triggered"] += 1
                        sys.stderr.write(f"[OK] Triggered scenario: {scenario} on {vid or 'all vehicles'}\n")
                    else:
                        sys.stderr.write(f"[ERR] Unknown scenario or vehicle: {spec}\n")
                elif cmd == "list" and len(parts) >= 2:
                    what = parts[1].lower()
                    if what == "vehicles":
                        sys.stderr.write("Vehicles:\n")
                        for vid, v in self.vehicles.items():
                            sys.stderr.write(f"  {vid}  type={v.cfg.vehicle_type}  engine={v.cfg.engine_type}  driver={v.cfg.driver_id}\n")
                    elif what == "faults":
                        sys.stderr.write(f"Active faults: {sorted(self.fi.active_faults) or '(none)'}\n")
                        sys.stderr.write(f"Known faults: {KNOWN_FAULTS}\n")
                    elif what == "scenarios":
                        sys.stderr.write(f"Known scenarios: {KNOWN_SCENARIOS}\n")
                elif cmd == "status":
                    sys.stderr.write(f"Vehicles:           {len(self.vehicles)}\n")
                    sys.stderr.write(f"Active faults:      {sorted(self.fi.active_faults) or '(none)'}\n")
                    sys.stderr.write(f"Messages emitted:   {self.stats['messages_emitted']}\n")
                    sys.stderr.write(f"Faults activated:   {self.stats['faults_active']}\n")
                    sys.stderr.write(f"Scenarios triggered:{self.stats['scenarios_triggered']}\n")
                else:
                    sys.stderr.write("[ERR] Unknown command. Type 'help'.\n")
            except Exception as e:
                sys.stderr.write(f"[ERR] {e}\n")
            sys.stderr.flush()


# ---------------------------------------------------------------------------
# Fleet builder
# ---------------------------------------------------------------------------

def build_fleet(num_vehicles: int, rng: random.Random) -> Dict[str, Vehicle]:
    vehicles: Dict[str, Vehicle] = {}
    for i in range(num_vehicles):
        vtype = rng.choice(VEHICLE_TYPES)
        vid = f"VHC-{i + 1:03d}"
        route = rng.choice(ROUTES)
        cfg = VehicleConfig(
            vehicle_id=vid,
            vehicle_type=vtype["id_prefix"],
            engine_type=vtype["engine"],
            route=route,
            driver_id=f"DRV-{rng.randint(1, 50):03d}",
            initial_energy_pct=rng.uniform(60, 100),
            initial_odometer_km=rng.uniform(1000, 200000),
            max_speed=vtype["max_speed"],
            fuel_burn_idle=vtype["fuel_burn_idle"],
            fuel_burn_per_kmh=vtype["fuel_burn_per_kmh"],
        )
        vehicles[vid] = Vehicle(cfg, rng)
    return vehicles


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    p = argparse.ArgumentParser(
        prog="telemetrix_generator",
        description="Telemetrix — Connected Vehicle Telematics Data Generator (Invente'26)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--vehicles", type=int, default=5, help="Number of vehicles (default: 5)")
    p.add_argument("--rate", type=float, default=1.0, help="Messages per second per vehicle (default: 1.0)")
    p.add_argument("--duration", type=float, default=0.0, help="Stop after N seconds (0 = unlimited)")
    p.add_argument("--output", choices=["stdout", "file", "both"], default="stdout",
                   help="Output destination (default: stdout)")
    p.add_argument("--file", type=str, default="telemetry.jsonl", help="File path when output=file/both")
    p.add_argument("--seed", type=int, default=None, help="RNG seed for reproducibility")
    p.add_argument("--inject", action="append", default=[],
                   choices=KNOWN_FAULTS + ["all"],
                   help="Start with fault(s) active (repeatable, or 'all')")
    p.add_argument("--scenario", action="append", default=[],
                   help="Trigger scenario at startup: scenario[:vehicle_id] (repeatable)")
    p.add_argument("--interactive", action="store_true", help="Enable stdin command control for live demos")
    p.add_argument("--stats-interval", type=float, default=10.0,
                   help="Print stats to stderr every N seconds (0 = off, default: 10)")
    p.add_argument("--tag-corrupt", action="store_true",
                   help="Tag corrupted messages with _corrupted field (debug only)")
    p.add_argument("--list-faults", action="store_true", help="List known faults and exit")
    p.add_argument("--list-scenarios", action="store_true", help="List known scenarios and exit")
    args = p.parse_args()

    if args.list_faults:
        print("\n".join(KNOWN_FAULTS))
        return 0
    if args.list_scenarios:
        print("\n".join(KNOWN_SCENARIOS))
        return 0

    rng = random.Random(args.seed)

    vehicles = build_fleet(args.vehicles, rng)
    for v in vehicles.values():
        v.telemetry_rate_hz = args.rate if args.rate > 0 else 1.0
    sys.stderr.write(f"[INFO] Built fleet of {args.vehicles} vehicles\n")
    for vid, v in vehicles.items():
        sys.stderr.write(f"  {vid}: type={v.cfg.vehicle_type:7s} engine={v.cfg.engine_type:8s} "
                         f"driver={v.cfg.driver_id}  fuel={v.state.fuel_pct if v.state.fuel_pct is not None else v.state.battery_pct:.1f}%\n")
    sys.stderr.flush()

    fi = FaultInjector(rng, tag_corrupt=args.tag_corrupt)
    if "all" in args.inject:
        for f in KNOWN_FAULTS:
            fi.activate(f)
    else:
        for f in args.inject:
            fi.activate(f)

    se = ScenarioEngine(vehicles)
    for sc in args.scenario:
        if ":" in sc:
            sname, vid = sc.split(":", 1)
        else:
            sname, vid = sc, None
        if not se.trigger(sname, vid):
            sys.stderr.write(f"[WARN] Could not trigger scenario '{sc}'\n")
    sys.stderr.flush()

    sink = OutputSink(args.output, args.file)

    stop_event = threading.Event()

    def handle_sigint(sig, frame):
        stop_event.set()
    signal.signal(signal.SIGINT, handle_sigint)

    stats = {"messages_emitted": 0, "faults_active": sorted(fi.active_faults), "scenarios_triggered": 0}

    if args.interactive:
        controller = InteractiveController(fi, se, vehicles, stats, stop_event)
        controller.start()

    sys.stderr.write(f"[INFO] Streaming data... "
                     f"(rate={args.rate} Hz/vehicle, vehicles={args.vehicles}, "
                     f"output={args.output}, interactive={args.interactive})\n")
    sys.stderr.write(f"[INFO] Press Ctrl+C to stop.\n\n")
    sys.stderr.flush()

    interval = 1.0 / args.rate if args.rate > 0 else 1.0
    start_time = time.time()
    last_stats = start_time
    total_emitted = 0

    try:
        while not stop_event.is_set():
            now = time.time()
            if args.duration > 0 and (now - start_time) >= args.duration:
                break

            batch = []
            for v in vehicles.values():
                v.step(interval)
                batch.append(v.build_message())

            batch = fi.process(batch)

            for m in batch:
                sink.write(m)
                total_emitted += 1
            stats["messages_emitted"] = total_emitted
            stats["faults_active"] = sorted(fi.active_faults)

            if args.stats_interval > 0 and (now - last_stats) >= args.stats_interval:
                sys.stderr.write(
                    f"[STATS] t={now - start_time:6.1f}s  emitted={total_emitted:8d}  "
                    f"faults={sorted(fi.active_faults) or '(none)'}\n"
                )
                sys.stderr.flush()
                last_stats = now

            elapsed = time.time() - now
            sleep_time = max(0.0, interval - elapsed)
            if sleep_time > 0:
                time.sleep(sleep_time)
    finally:
        stop_event.set()
        sink.close()
        sys.stderr.write(f"\n[INFO] Stopped. Total messages emitted: {total_emitted}\n")
        sys.stderr.flush()

    return 0
