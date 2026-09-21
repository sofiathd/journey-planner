# +
from __future__ import annotations

import heapq
import math
import time
from contextlib import closing
from typing import Optional

import pandas as pd

# Chronological order of weekdays for GTFS calendar management
DAY_ORDER = ("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday")


def prepare_graph(
    *,
    conn,
    sharedns: str,
    userns: str,
    uuids: Optional[list[str]] = None,
    max_walk_m: float = 500,
    rebuild: bool = False,
    verbose: bool = True,
):
    """
    Orchestrates the creation/loading of the three fundamental tables for our graph:
    1. Stops: Filtered by the bounding geometry of the specified administrative regions.
    2. Transfers (Stop-to-Stop): Pre-calculated walking edges constrained by max_walk_m.
    3. Stop Times: Valid timetable entries mapped to the filtered stops.

    This function offloads heavy geospatial computations (ST_Contains, ST_Distance) to the 
    Trino engine to minimize local memory footprint and execution time.
    """

    def log(msg: str):
        if verbose:
            print(msg)

    t0 = time.time()

    if rebuild:
        log("Preparing topological graph tables in Trino...")
        
        # Retrieve the most recent publication date to ensure timetable freshness
        max_pub_date = pd.read_sql(
            f"SELECT MAX(pub_date) AS pub_date FROM {sharedns}.sbb_stops",
            conn,
        ).iloc[0, 0]
        log(f"Reference timetable publication date: {max_pub_date}")

        # Dynamically construct spatial filters if region UUIDs are provided
        if uuids:
            uuids_list = ", ".join(f"CAST('{u}' AS UUID)" for u in uuids)
            geo_join = f"CROSS JOIN {sharedns}.geo geo"
            spatial_filter = f"""
              AND geo.uuid IN ({uuids_list})
              AND ST_Contains(
                  ST_GeomFromBinary(geo.wkb_geometry),
                  ST_Point(CAST(sbb_stops.stop_lon AS DOUBLE), CAST(sbb_stops.stop_lat AS DOUBLE))
              )
            """
        else:
            geo_join = ""
            spatial_filter = ""

        # Isolate and deduplicate physical stops within the targeted area
        with closing(conn.cursor()) as cur:
            cur.execute(f"""
            CREATE OR REPLACE TABLE {userns}.stops AS
            WITH prepared_stops AS (
                SELECT
                    CAST(split_part(sbb_stops.stop_id, ':', 1) AS BIGINT) AS base_id,
                    sbb_stops.stop_name,
                    CAST(sbb_stops.stop_lat AS DOUBLE) AS lat,
                    CAST(sbb_stops.stop_lon AS DOUBLE) AS lon
                FROM {sharedns}.sbb_stops sbb_stops
                {geo_join}
                WHERE sbb_stops.pub_date = DATE '{max_pub_date}'
                  AND sbb_stops.stop_id NOT LIKE 'Parent%'
                  AND sbb_stops.parent_station IS NOT NULL
                  {spatial_filter}
            )
            SELECT
                base_id AS stop_id,
                MIN(stop_name) AS stop_name,
                AVG(lat) AS stop_lat,
                AVG(lon) AS stop_lon
            FROM prepared_stops
            GROUP BY base_id
            """)

        # Compute the pedestrian adjacency matrix using spherical distance
        with closing(conn.cursor()) as cur:
            cur.execute(f"""
            CREATE OR REPLACE TABLE {userns}.stop_to_stop AS
            WITH stops_geo AS (
                SELECT
                    stop_id,
                    to_spherical_geography(ST_Point(stop_lon, stop_lat)) AS geog
                FROM {userns}.stops
            )
            SELECT
                a.stop_id AS a_stop_id,
                b.stop_id AS b_stop_id,
                ST_Distance(a.geog, b.geog) AS distance
            FROM stops_geo a
            JOIN stops_geo b
              ON a.stop_id < b.stop_id
            WHERE ST_Distance(a.geog, b.geog) < {float(max_walk_m)}
            """)

        # Extract scheduled stop times and pivot the operating days calendar matrix
        with closing(conn.cursor()) as cur:
            cur.execute(f"""
            CREATE OR REPLACE TABLE {userns}.stop_times AS
            WITH relevant_trips AS (
                SELECT DISTINCT st.trip_id
                FROM {sharedns}.sbb_stop_times st
                WHERE st.pub_date = DATE '{max_pub_date}'
                  AND CAST(split_part(st.stop_id, ':', 1) AS BIGINT) IN (
                      SELECT stop_id FROM {userns}.stops
                  )
            )
            SELECT
                st.trip_id,
                st.stop_id AS raw_stop_id,
                CAST(split_part(st.stop_id, ':', 1) AS BIGINT) AS base_stop_id,

                -- Convert time strings to absolute seconds since midnight for optimized arithmetic
                CAST(split_part(st.departure_time, ':', 1) AS INTEGER) * 3600 +
                CAST(split_part(st.departure_time, ':', 2) AS INTEGER) * 60 +
                CAST(split_part(st.departure_time, ':', 3) AS INTEGER) AS departure_time,

                CAST(split_part(st.arrival_time, ':', 1) AS INTEGER) * 3600 +
                CAST(split_part(st.arrival_time, ':', 2) AS INTEGER) * 60 +
                CAST(split_part(st.arrival_time, ':', 3) AS INTEGER) AS arrival_time,

                st.stop_sequence,

                -- Extract boolean indicators from the service calendar
                CAST(c.monday AS TINYINT) AS monday,
                CAST(c.tuesday AS TINYINT) AS tuesday,
                CAST(c.wednesday AS TINYINT) AS wednesday,
                CAST(c.thursday AS TINYINT) AS thursday,
                CAST(c.friday AS TINYINT) AS friday,
                CAST(c.saturday AS TINYINT) AS saturday,
                CAST(c.sunday AS TINYINT) AS sunday
            FROM {sharedns}.sbb_stop_times st
            JOIN {sharedns}.sbb_trips t
              ON st.trip_id = t.trip_id AND st.pub_date = t.pub_date
            JOIN {sharedns}.sbb_calendar c
              ON t.service_id = c.service_id AND t.pub_date = c.pub_date
            WHERE st.trip_id IN (SELECT trip_id FROM relevant_trips)
              AND st.pub_date = DATE '{max_pub_date}'
              AND CAST(split_part(st.stop_id, ':', 1) AS BIGINT) IS NOT NULL
            """)
    else:
        log("Loading pre-computed graph structures from Trino namespace...")

    stops_df = pd.read_sql(f"SELECT * FROM {userns}.stops", conn)
    transfers_df = pd.read_sql(f"SELECT * FROM {userns}.stop_to_stop", conn)
    stop_times_df = pd.read_sql(f"SELECT * FROM {userns}.stop_times", conn)

    log(
        f"Topological graph loaded: {len(stops_df)} stops, "
        f"{len(transfers_df)} walking edges, {len(stop_times_df)} scheduled stops "
        f"({time.time() - t0:.1f}s)."
    )
    return stops_df, transfers_df, stop_times_df


class JourneyPlanner:
    """
    Stochastic route calculator based on a backward-search approach.

    Algorithmic principle: Multi-criteria Dijkstra/A* exploration conducted backwards 
    from the final destination and target arrival time. In an inverted search space, maximizing 
    the departure time identifies the latest possible departure route that still 
    satisfies the prescribed stochastic confidence threshold (Q%).
    """

    def __init__(
        self, 
        delay_model=None, 
        use_weather: bool = False, 
        max_walk_distance_m: float = 500, 
        min_transfer_time_sec: float = 120, 
        walking_speed_m_per_min: int = 50
    ):
        self.delay_model = delay_model
        
        # Regulatory constraints for the transport system
        self.MAX_WALK_DISTANCE_M = float(max_walk_distance_m)
        self.MIN_TRANSFER_TIME_SEC = float(min_transfer_time_sec)
        self.WALKING_SPEED_M_PER_MIN = float(walking_speed_m_per_min)

        self.stops_df = None
        self.transfers_df = None
        self.stop_times_df = None

        # Indexing structures for O(1) lookups
        self._trips = {}
        self._stop_to_trips = {}
        self._trip_stop_index = {}
        self._walk_edges = {}
        self._walk_cache = {}

    @staticmethod
    def _time_to_seconds(time_str: str):
        """Converts a time format (HH:MM or HH:MM:SS) into absolute seconds since midnight."""
        parts = str(time_str).split(":")
        
        if len(parts) not in (2, 3):
            raise ValueError("Expected format is HH:MM or HH:MM:SS")
            
        h, m = int(parts[0]), int(parts[1])
        s = int(parts[2]) if len(parts) == 3 else 0
        
        return h * 3600 + m * 60 + s

    @staticmethod
    def _format_seconds(seconds: int):
        """Re-formats internal integer seconds back into a readable HH:MM:SS string."""
        seconds = int(seconds)
        day_shift, sec = divmod(seconds, 24 * 3600)
        h, rem = divmod(sec, 3600)
        m, s = divmod(rem, 60)
        txt = f"{h:02d}:{m:02d}:{s:02d}"
        if day_shift > 0:
            return f"{txt} (+{day_shift}d)"
        if day_shift < 0:
            return f"{txt} ({day_shift}d)"
        return txt

    def _transfer_seconds(self, distance_m: float):
        """
        Calculates the time of a walking transfer based on the following imposed logic:
        Base connection time (2 mins) + 1 minute per started 50m block.
        """
        walking_time_mins = math.ceil(float(distance_m) / self.WALKING_SPEED_M_PER_MIN)
        return int(self.MIN_TRANSFER_TIME_SEC + walking_time_mins * 60)

    @staticmethod
    def _normalize_weather_bucket(weather):
        """Standardizes and categorizes raw weather strings into binary states."""
        if weather is None:
            return None
        w = str(weather).strip().lower()
        if w in {"normal", "clear", "sunny", "dry", "cloudy", "fair"}:
            return "normal"
        if w in {"adverse", "rain", "rainy", "snow", "snowy", "storm", "stormy", "wet"}:
            return "adverse"
        return w

    @classmethod
    def _previous_day(cls, day: str):
        """Returns the preceding calendar day (useful for handling late-night services)."""
        i = DAY_ORDER.index(day)
        return DAY_ORDER[(i - 1) % len(DAY_ORDER)]

    def _query_contexts(self, time_str: str, day: str, cutoff: str = "03:00"):
        """
        Manages day roll-overs for journeys occurring in the early hours of the morning,
        linking them to the previous day's operational schedule context.
        """
        
        day_key = day.lower()
        if day_key not in DAY_ORDER:
            raise ValueError(f"Invalid day '{day}'. Accepted values: {', '.join(DAY_ORDER)}")
            
        t0 = self._time_to_seconds(time_str)
        contexts = [(day_key, 0)]
        
        if t0 < self._time_to_seconds(cutoff):
            contexts.append((self._previous_day(day_key), -24 * 3600))
            
        return t0, contexts

    def prepare_from_dataframes(self, stops_df, transfers_df, stop_times_df):
        """
        Binds GTFS data source tables to the engine and constructs the 
        internal graph structures required for routing queries.
        """
        self.stops_df = stops_df
        self.transfers_df = transfers_df
        self.stop_times_df = stop_times_df
        self._build_indices()
        print(
            f"Search engine index initialized: {len(self.stops_df)} stops, "
            f"{len(self.transfers_df)} walking edges, {len(self._trips)} unique trips."
        )

    def _build_indices(self):
        """
        Generates highly optimized native Python dictionaries to eliminate the overhead 
        of querying Pandas structures inside the critical graph exploration loop.
        """
        required = {"trip_id", "raw_stop_id", "base_stop_id", "arrival_time", "departure_time", "stop_sequence"}
        missing = sorted(required - set(self.stop_times_df.columns))
        if missing:
            raise ValueError(f"stop_times_df is missing mandatory columns: {missing}")

        self._trips.clear()
        self._stop_to_trips.clear()
        self._trip_stop_index.clear()
        self._walk_edges.clear()
        self._walk_cache.clear()

        st_df = self.stop_times_df.dropna(subset=list(required))
        day_cols = [d for d in DAY_ORDER if d in st_df.columns]

        # Structure the sequential topology of trips
        for row in st_df.itertuples(index=False):
            rowd = row._asdict()
            tid = rowd["trip_id"]
            stop_id = int(rowd["base_stop_id"])

            stop = {
                "raw_stop_id": rowd["raw_stop_id"],
                "stop_id": stop_id,
                "arr": int(rowd["arrival_time"]),
                "dep": int(rowd["departure_time"]),
                "seq": int(rowd["stop_sequence"]),
            }
            for day in day_cols:
                stop[day] = int(rowd.get(day, 1))

            self._trips.setdefault(tid, []).append(stop)
            self._stop_to_trips.setdefault(stop_id, set()).add(tid)

        # Index the sequential positions to accelerate upstream/downstream stop lookups
        for tid, stops in self._trips.items():
            stops.sort(key=lambda x: x["seq"])
            idx = {}
            for i, stop in enumerate(stops):
                idx.setdefault(stop["stop_id"], []).append(i)
            self._trip_stop_index[tid] = idx

        # Populate bidirectional walking edges
        for row in self.transfers_df.itertuples(index=False):
            rowd = row._asdict()
            a = int(rowd["a_stop_id"])
            b = int(rowd["b_stop_id"])
            dist = float(rowd["distance"])
            walk_time = self._transfer_seconds(dist)
            self._walk_edges.setdefault(a, []).append((b, walk_time, dist))
            self._walk_edges.setdefault(b, []).append((a, walk_time, dist))

    def _prepare_walk_index(self, max_walk_m: float):
        """Dynamically filters and caches walking edges based on user's maximum distance preference."""
        max_walk_m = float(max_walk_m)
        if max_walk_m in self._walk_cache:
            return self._walk_cache[max_walk_m]
        walk_ok = {
            sid: [(nb, t_s, d) for nb, t_s, d in neighbours if d <= max_walk_m]
            for sid, neighbours in self._walk_edges.items()
        }
        walk_ok = {sid: edges for sid, edges in walk_ok.items() if edges}
        self._walk_cache[max_walk_m] = walk_ok
        return walk_ok

    def _connection_probability(self, trip_id, stop_id, margin_seconds, weather=None) -> float:
        """Proxy to the delay model to determine risk of missing a transfer."""
        if margin_seconds < 0:
            return 0.0
        if self.delay_model is None:
            return 1.0
        
        return float(self.delay_model.get_connection_probability(
            tt_trip_id=trip_id,
            bpuic=int(stop_id),
            margin_seconds=float(margin_seconds),
            weather=self._normalize_weather_bucket(weather),
        ))

    def route(
        self,
        *,
        start_id: int,
        end_id: int,
        arrival_time: str,
        day: str = "monday",
        min_confidence: float = 0.90,
        weather=None,
        max_walk_m: float = 500,
        max_expansions: int = 50_000,
        max_routes: Optional[int] = 5,
    ):
        """
        Core backward-search routing algorithm.
        Starts at the destination at the target arrival time and steps backward through time.
        We maximize the timestamp, which translates to finding the *latest* possible departure.
        """
        if not self._trips:
            raise RuntimeError("Data not prepared. Call prepare_from_dataframes() first.")
        if not (0 < float(min_confidence) <= 1):
            raise ValueError("min_confidence must be in range (0, 1].")
        if max_routes is not None and max_routes <= 0:
            raise ValueError("max_routes must be a positive integer or None.")
        if max_walk_m < 0 or max_walk_m > self.MAX_WALK_DISTANCE_M:
            raise ValueError(f"max_walk_m must be between 0 and {self.MAX_WALK_DISTANCE_M}m.")

        start_id = int(start_id)
        end_id = int(end_id)
        if start_id not in self._stop_to_trips and start_id not in self._walk_edges:
            raise ValueError(f"Start stop ID {start_id} not found in the network.")
        if end_id not in self._stop_to_trips and end_id not in self._walk_edges:
            raise ValueError(f"Destination stop ID {end_id} not found in the network.")

        target_t, service_contexts = self._query_contexts(arrival_time, day)
        walk_ok = self._prepare_walk_index(max_walk_m)

        # Graph traversal tracking structures
        labels = {}
        labels_by_stop = {}
        heap = []
        next_id = 0

        def is_dominated(stop_id, t, conf, total_walk):
            """
            Pareto dominance check.
            If a path to this stop exists that departs later (or simultaneously),
            possesses higher (or equal) confidence, and requires less (or equal) total walking,
            the current branch is strictly inferior and can be safely pruned.
            """
            return any(
                labels[lid]["time"] >= t and 
                labels[lid]["confidence"] >= conf and
                labels[lid]["total_walk"] <= total_walk
                for lid in labels_by_stop.get(stop_id, [])
            )

        def add_label(stop_id, t, conf, parent, edge_events, edge_walk_m):
            """Registers a newly discovered path state into the search priority queue."""
            nonlocal next_id
            
            # 1. Calculate global cumulative walking distance
            parent_walk = labels[parent]["total_walk"] if parent is not None else 0.0
            new_total_walk = parent_walk + float(edge_walk_m or 0.0)
            
            # 2. Strict global constraint check
            if new_total_walk > float(max_walk_m):
                return None

            if conf < min_confidence or is_dominated(stop_id, t, conf, new_total_walk):
                return None

            lid = next_id
            next_id += 1
            labels[lid] = {
                "stop": int(stop_id),
                "time": int(t),
                "confidence": float(conf),
                "parent": parent,
                "edge_events": edge_events or [],
                "edge_walk_m": float(edge_walk_m or 0.0),
                "total_walk": new_total_walk,
            }

            # Update the Pareto dominance tracker
            labels_by_stop[stop_id] = [
                old_id for old_id in labels_by_stop.get(stop_id, [])
                if not (t >= labels[old_id]["time"] and 
                        conf >= labels[old_id]["confidence"] and 
                        new_total_walk <= labels[old_id]["total_walk"])
            ] + [lid]

            # Priority Queue relies on max departure time (inverted via negative sign)
            heapq.heappush(heap, (-int(t), -float(conf), lid))
            return lid

        def reconstruct(candidate_id):
            """
            Backtracks through the parent pointers to rebuild the journey events,
            and applies a Forward Replay to eliminate artificial wait times generated by backward search.
            """
            events = []
            cur = candidate_id
            while cur is not None:
                lab = labels[cur]
                events.extend(lab["edge_events"])
                cur = lab["parent"]
            
            # Forward Replay pass for exact arrival time calculation
            current_actual_time = labels[candidate_id]["time"]
            
            for ev in events:
                ev_time, src, tid, dst = ev
                
                if tid is not None:
                    # Public transport: sync actual time to the GTFS scheduled arrival
                    current_actual_time = ev_time
                else:
                    # Walking transfer: process immediately, ignoring backward-search phantom wait buffers
                    walk_duration = self.MIN_TRANSFER_TIME_SEC
                    for nb_sid, dur, dist in self._walk_edges.get(src, []):
                        if nb_sid == dst:
                            walk_duration = dur
                            break
                    current_actual_time += walk_duration
                    
            actual_arrival_time = current_actual_time

            return {
                "departure_time": labels[candidate_id]["time"],
                "arrival_deadline": target_t,
                "arrival_time": actual_arrival_time,
                "confidence": labels[candidate_id]["confidence"],
                "total_walk_m": labels[candidate_id]["total_walk"],
                "route": events,
            }

        # Seed the algorithm from the destination
        root_id = add_label(end_id, target_t, 1.0, None, [], 0.0)
        results = []
        expansions = 0

        # Core Node Expansion Loop
        while heap and expansions < int(max_expansions):
            _, _, label_id = heapq.heappop(heap)
            lab = labels[label_id]
            sid = lab["stop"]
            cur_t = lab["time"]
            cur_conf = lab["confidence"]
            expansions += 1

            # Termination state: Reached the requested origin stop
            if sid == start_id and label_id != root_id:
                results.append(reconstruct(label_id))
                if max_routes is not None and len(results) >= max_routes:
                    break
                continue

            # Expansion 1: Evaluate pedestrian transfers to neighboring nodes
            for prev_sid, walk_duration, dist in walk_ok.get(sid, []):
                prev_t = cur_t - walk_duration
                if prev_t >= -24 * 3600:
                    add_label(
                        stop_id=prev_sid,
                        t=prev_t,
                        conf=cur_conf,
                        parent=label_id,
                        edge_events=[(prev_t, int(prev_sid), None, int(sid))],
                        edge_walk_m=dist,
                    )

            # Expansion 2: Evaluate upstream public transport connections
            for tid in self._stop_to_trips.get(sid, ()):
                trip_stops = self._trips[tid]
                for idx in self._trip_stop_index[tid].get(sid, []):
                    if idx == 0:
                        continue # Cannot board if we are already at the route's initial origin
                    arr_stop = trip_stops[idx]

                    for service_day, offset in service_contexts:
                        if arr_stop.get(service_day, 0) == 0:
                            continue

                        scheduled_arr = arr_stop["arr"] + offset
                        margin = cur_t - scheduled_arr
                        
                        # Temporal constraint: Vehicle must arrive prior to our required departure
                        if margin < 0:
                            continue

                        # Propagate statistical delay risk
                        p_segment = self._connection_probability(tid, sid, margin, weather=weather)
                        new_conf = cur_conf * p_segment
                        if new_conf < min_confidence:
                            continue

                        # Back-propagate boarding states to all preceding stops on this specific vehicle's route
                        for prev_idx in range(idx - 1, -1, -1):
                            prev_stop = trip_stops[prev_idx]
                            dep_prev = prev_stop["dep"] + offset
                            if dep_prev > scheduled_arr or dep_prev < -24 * 3600:
                                continue

                            prev_sid = int(prev_stop["stop_id"])
                            add_label(
                                stop_id=prev_sid,
                                t=dep_prev,
                                conf=new_conf,
                                parent=label_id,
                                edge_events=[
                                    (dep_prev, prev_sid, tid, None),
                                    (scheduled_arr, None, tid, int(sid)),
                                ],
                                edge_walk_m=0.0,
                            )

        if expansions >= int(max_expansions):
            print(f"Search aborted after max_expansions={max_expansions}. Increase limit if optimal routes are missing.")
        return results

    @classmethod
    def print_routes(cls, routes, limit: Optional[int] = None):
        """Helper utility to display the reconstructed itinerary in a human-readable format."""
        routes = routes if limit is None else routes[:limit]
        print(f"Found {len(routes)} route(s).")
        for idx, route in enumerate(routes, start=1):
            print(f"\n=== ROUTE {idx} ===")
            print(f"Latest departure: {cls._format_seconds(route['departure_time'])}")
            print(f"Arrival time: {cls._format_seconds(route['arrival_time'])}")
            print(f"Arrival deadline: {cls._format_seconds(route['arrival_deadline'])}")
            print(f"Confidence: {route['confidence'] * 100:.1f}%")
            print(f"Walking distance: {route['total_walk_m']:.1f} m")
            for event in route["route"]:
                ev_time = cls._format_seconds(event[0])
                ev_from, ev_trip, ev_to = event[1], event[2], event[3]
                if ev_trip is None:
                    print(f"  [{ev_time}] Walk from Stop {ev_from} to Stop {ev_to}")
                elif ev_from is None:
                    print(f"  [{ev_time}] Arrive at Stop {ev_to} via Trip {ev_trip}")
                elif ev_to is None:
                    print(f"  [{ev_time}] Board Trip {ev_trip} at Stop {ev_from}")
