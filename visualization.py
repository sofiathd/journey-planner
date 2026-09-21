"""
The following code generates an interactive map visualization for our journey planner.
"""

# +
import time
import datetime
import plotly.graph_objects as go
import ipywidgets as widgets
from ipywidgets import interact_manual, fixed
from IPython.display import display

# Color used for our visualisationThe following code generates an interactive map visualization for our journey planner.
_LINE_COLOR  = "#1E3A8A"   # Blue color
_WALK_COLOR  = "#94A3B8"   # Grey color
_START_COLOR = "#34D399"   # Green color
_END_COLOR   = "#FB7185"   # Red color 


# +
def _build_route_traces(route, stops_info, planner):
    """
    Given a single route dictionary, this function then return :
    - traces : a list of go.Scattermapbox objects.
    - all_lat
    - all_lon
    - dep_str
    Basically convert the computed route into Mapbox/Plotly elements
    """
    traces = []
    all_lat, all_lon = [], []
    current_board_stop = None

    dep_str = str(datetime.timedelta(seconds=route['departure_time']))
    arr_str = str(datetime.timedelta(seconds=route['arrival_time']))

    for idx, ev in enumerate(route['route']):
        ev_time, src, trip_id, dst = ev[0], ev[1], ev[2], ev[3]
        segment_lat, segment_lon, segment_names = [], [], []

        # --- WALKING SEGMENT ---
        if trip_id is None:
            info_src, info_dst = stops_info[src], stops_info[dst]
            segment_lat = [info_src['stop_lat'], info_dst['stop_lat']]
            segment_lon = [info_src['stop_lon'], info_dst['stop_lon']]

            walk_duration = planner.MIN_TRANSFER_TIME_SEC
            walk_dist = 0.0

            if src != dst:
                for nb_sid, duration, dist in planner._walk_edges.get(src, []):
                    if nb_sid == dst:
                        walk_duration = duration
                        walk_dist = dist
                        break

            arrival_at_dst = ev_time + walk_duration
            next_dep_time = None

            for next_ev in route['route'][idx + 1:]:
                if next_ev[2] is not None and next_ev[1] == dst:
                    next_dep_time = next_ev[0]
                    break

            if next_dep_time is not None:
                wait_seconds = next_dep_time - arrival_at_dst
                wait_str = f"{int(wait_seconds // 60)} min {int(wait_seconds % 60)} s"
            else:
                wait_str = "0 min (Final Destination)"

            hover_text = (
                f"<b>Type:</b> Walk / Transfer<br>"
                f"<b>From:</b> {info_src['stop_name']}<br>"
                f"<b>To:</b> {info_dst['stop_name']}<br>"
                f"<b>Distance:</b> {walk_dist:.1f} m<br>"
                f"<b>Walking Time:</b> {int(walk_duration // 60)} min<br>"
                f"<b>Platform Wait Time:</b> {wait_str}"
            )

            traces.append(go.Scattermapbox(
                mode="lines",
                lon=segment_lon, lat=segment_lat,
                line={'width': 3, 'color': _WALK_COLOR},
                hoverinfo='skip',
                name="Walk", visible=False,
            ))

            n_anchors = 10
            step_lat = (segment_lat[1] - segment_lat[0]) / (n_anchors - 1)
            step_lon = (segment_lon[1] - segment_lon[0]) / (n_anchors - 1)
            hover_lats = [segment_lat[0] + step_lat * i for i in range(1, n_anchors - 1)]
            hover_lons = [segment_lon[0] + step_lon * i for i in range(1, n_anchors - 1)]

            traces.append(go.Scattermapbox(
                mode="markers",
                lon=hover_lons, lat=hover_lats,
                marker={'size': 20, 'color': _WALK_COLOR, 'opacity': 0.001},
                text=[hover_text] * len(hover_lats),
                hoverinfo='text',
                name="Walk info", visible=False,
            ))

            all_lat.extend(segment_lat)
            all_lon.extend(segment_lon)

        # --- BOARDING VEHICLE ---
        elif dst is None:
            current_board_stop = src

        # --- ALIGHTING VEHICLE ---
        elif src is None and current_board_stop is not None:
            real_src = current_board_stop
            real_dst = dst
            src_name = stops_info[real_src]['stop_name']
            dst_name = stops_info[real_dst]['stop_name']

            trip_stops = planner._trips[trip_id]
            s_indices = planner._trip_stop_index[trip_id][real_src]
            e_indices = planner._trip_stop_index[trip_id][real_dst]

            valid_pair = [(s, e) for s in s_indices for e in e_indices if s < e]
            if valid_pair:
                s_idx, e_idx = valid_pair[0]
                for i in range(s_idx, e_idx + 1):
                    stop_id = trip_stops[i]['stop_id']
                    info = stops_info.get(stop_id)
                    if info is None:
                        continue  # intermediate stop not in stops_df, skip silently
                    segment_lat.append(info['stop_lat'])
                    segment_lon.append(info['stop_lon'])

                    t_arr = str(datetime.timedelta(seconds=trip_stops[i]['arr']))
                    hover_text = (
                        f"<b>Transit Stop:</b> {info['stop_name']}<br>"
                        f"<b>Scheduled Time:</b> {t_arr}<br>"
                        f"<b>Trip ID:</b> {trip_id}<br>"
                        f"<b>Boarded at:</b> {src_name}<br>"
                        f"<b>Alighting at:</b> {dst_name}"
                    )
                    segment_names.append(hover_text)

            traces.append(go.Scattermapbox(
                mode="lines+markers", lon=segment_lon, lat=segment_lat,
                marker={'size': 8, 'color': _LINE_COLOR},
                line={'width': 4, 'color': _LINE_COLOR},
                text=segment_names, hoverinfo='text', name="Transit Ride",
                visible=False
            ))

            # --- Anchors  ---
            anchor_lats, anchor_lons, anchor_texts = [], [], []
            n_sub = 8  
            for i in range(len(segment_lat) - 1):
                lat0, lat1 = segment_lat[i], segment_lat[i + 1]
                lon0, lon1 = segment_lon[i], segment_lon[i + 1]
    
                leg_text = segment_names[i + 1] if (i + 1) < len(segment_names) else segment_names[i]
                for k in range(1, n_sub):
                    f = k / n_sub
                    anchor_lats.append(lat0 + (lat1 - lat0) * f)
                    anchor_lons.append(lon0 + (lon1 - lon0) * f)
                    anchor_texts.append(leg_text)

            if anchor_lats:
                traces.append(go.Scattermapbox(
                    mode="markers",
                    lon=anchor_lons, lat=anchor_lats,
                    marker={'size': 20, 'color': _LINE_COLOR, 'opacity': 0.001},
                    text=anchor_texts, hoverinfo='text',
                    name="Transit info", visible=False,
                ))

            all_lat.extend(segment_lat)
            all_lon.extend(segment_lon)
            current_board_stop = None

    # --- START & END MARKERS ---
    if route['route']:
        first_stop = route['route'][0][1]
        last_stop = route['route'][-1][3]
        if first_stop is None:
            first_stop = route['route'][1][1]
        if last_stop is None:
            last_stop = route['route'][-2][3]

        traces.append(go.Scattermapbox(
            mode="markers",
            lon=[stops_info[first_stop]['stop_lon']],
            lat=[stops_info[first_stop]['stop_lat']],
            marker={'size': 16, 'color': _START_COLOR, 'symbol': 'circle'},
            text=[f"<b>Start:</b> {stops_info[first_stop]['stop_name']}<br><b>Departure:</b> {dep_str}"],
            hoverinfo='text', name="Start", visible=False
        ))
        traces.append(go.Scattermapbox(
            mode="markers",
            lon=[stops_info[last_stop]['stop_lon']],
            lat=[stops_info[last_stop]['stop_lat']],
            marker={'size': 16, 'color': _END_COLOR, 'symbol': 'circle'},
            text=[f"<b>End:</b> {stops_info[last_stop]['stop_name']}<br><b>Arrival:</b> {arr_str}"],
            hoverinfo='text', name="Destination", visible=False
        ))

    return traces, all_lat, all_lon, dep_str


def _build_route_summary(route, stops_info, route_idx, n_routes, planner):
    """
    This function creates the HTML itinerary panel shown next to the map, including :
    - the start point
    - arrival point
    - walking segments
    - public transport segments
    - their associated times.
    """
    def fmt(sec):
        return str(datetime.timedelta(seconds=int(sec)))
 
    dep_str = fmt(route['departure_time'])
    arr_str = fmt(route['arrival_time'])
 
    # Start / end stop names
    first_stop = route['route'][0][1]
    last_stop = route['route'][-1][3]
 
    if first_stop is None:
        first_stop = route['route'][1][1]
    if last_stop is None:
        last_stop = route['route'][-2][3]
 
    start_name = stops_info[first_stop]['stop_name']
    end_name = stops_info[last_stop]['stop_name']
 
    legs_html = []
    current_board_stop = None
    board_time = None
 
    # Keeps track of the current time along the itinerary.
    # Useful to infer the start time of walking segments.
    last_known_time = route['departure_time']
 
    events = route['route']
 
    for i, ev in enumerate(events):
        ev_time, src, trip_id, dst = ev[0], ev[1], ev[2], ev[3]
 
        # --- WALK / TRANSFER ---
        if trip_id is None:
            # Pure platform transfer: do not display it, but still update time.
            if src == dst:
                last_known_time = ev_time
                continue
 
            walk_start_time = last_known_time
 
            # Default walking duration if no specific walking edge is found.
            walk_duration = planner.MIN_TRANSFER_TIME_SEC
            walk_dist = 0.0
 
            if src != dst:
                for nb_sid, duration, dist in planner._walk_edges.get(src, []):
                    if nb_sid == dst:
                        walk_duration = duration
                        walk_dist = dist
                        break
 
            # Walking end time = start time + walking duration in seconds.
            walk_end_time = walk_start_time + walk_duration
 
            legs_html.append(
                "<div style='margin:7px 0;'>"
                f"<span style='opacity:.75'>{fmt(walk_start_time)} &rarr; {fmt(walk_end_time)}</span>"
                "&nbsp;&nbsp;🚶Walk<br>"
                f"<span style='opacity:.9'>{stops_info[src]['stop_name']} "
                f"&rarr; {stops_info[dst]['stop_name']}</span><br>"
                f"<span style='opacity:.65'>Walking time: {int(walk_duration // 60)} min"
                f"{f' · {walk_dist:.0f} m' if walk_dist > 0 else ''}</span>"
                "</div>"
            )
 
            last_known_time = walk_end_time
 
        # --- BOARDING VEHICLE ---
        # Event shape: (board_time, src, trip_id, None)
        elif dst is None:
            current_board_stop = src
            board_time = ev_time
            # The clock advances to the actual departure time of the ride
            # (there may have been a wait on the platform before it).
            last_known_time = ev_time
 
        # --- ALIGHTING VEHICLE ---
        # Event shape: (alight_time, None, trip_id, dst)
        elif src is None and current_board_stop is not None:
            real_src = current_board_stop
            real_dst = dst
            ride_start = board_time if board_time is not None else last_known_time
            ride_end = ev_time
 
            src_name = stops_info[real_src]['stop_name']
            dst_name = stops_info[real_dst]['stop_name']
 
            # Number of intermediate stops on this ride, computed the same way
            # as in _build_route_traces (only data already used elsewhere).
            n_stops = None
            try:
                trip_stops = planner._trips[trip_id]
                s_indices = planner._trip_stop_index[trip_id][real_src]
                e_indices = planner._trip_stop_index[trip_id][real_dst]
                valid_pair = [(s, e) for s in s_indices for e in e_indices if s < e]
                if valid_pair:
                    s_idx, e_idx = valid_pair[0]
                    n_stops = e_idx - s_idx  # number of legs between board and alight
            except Exception:
                n_stops = None
 
            detail = ""
            if n_stops is not None:
                detail += f" · {n_stops} stop{'s' if n_stops != 1 else ''}"
 
            legs_html.append(
                "<div style='margin:7px 0;'>"
                f"<span style='opacity:.75'>{fmt(ride_start)} &rarr; {fmt(ride_end)}</span>"
                "&nbsp;&nbsp;🚆 Transit<br>"
                f"<span style='opacity:.9'>{src_name} &rarr; {dst_name}</span><br>"
                f"<span style='opacity:.65'>{detail}</span>"
                "</div>"
            )
 
            last_known_time = ride_end
            current_board_stop = None
            board_time = None
 
    legs_block = "".join(legs_html) if legs_html else (
        "<div style='opacity:.75'>Direct trip (no intermediate legs)</div>"
    )
 
    divider = (
        "<div style='border-top:1px solid rgba(255,255,255,.25); "
        "margin:9px 0;'></div>"
    )
 
    return f"""
    <div style="display:flex; justify-content:flex-start;">
      <div style="
          display:inline-block; width:max-content; max-width:330px;
          background:{_LINE_COLOR}; color:#ffffff; border-radius:12px;
          padding:16px 20px; box-sizing:border-box;
          font-family:-apple-system,Segoe UI,Roboto,sans-serif;
          font-size:14px; line-height:1.35;
          box-shadow:0 4px 14px rgba(0,0,0,.18);">
 
          <div style="font-size:16px; font-weight:700;">Route {route_idx + 1} / {n_routes}</div>
          <div style="opacity:.8;">Itinerary summary</div>
 
          {divider}
 
          <div style="margin:7px 0;">
              <span style="color:{_START_COLOR}; font-weight:700;">&#9679; Start</span><br>
              <span style="opacity:.9">{dep_str} &middot; {start_name}</span>
          </div>
 
          {divider}
 
          {legs_block}
 
          {divider}
 
          <div style="margin:7px 0;">
              <span style="color:{_END_COLOR}; font-weight:700;">&#9679; Arrive</span><br>
              <span style="opacity:.9">{arr_str} &middot; {end_name}</span>
          </div>
      </div>
    </div>
    """
 
 

def _zoom_for_bbox(all_lat, all_lon, default_lon=6.6328, default_lat=46.5218):
    """
    This function computes the best map center and zoom level so that the full route fits nicely in the visualization.
    """
    if not all_lat or not all_lon:
        return 12.0, default_lon, default_lat
    max_diff = max(max(all_lat) - min(all_lat), max(all_lon) - min(all_lon))
    if max_diff < 0.01:
        zoom = 14.0
    elif max_diff < 0.03:
        zoom = 13.0
    elif max_diff < 0.06:
        zoom = 12.0
    elif max_diff < 0.12:
        zoom = 11.0
    else:
        zoom = 10.0
    center_lon = sum(all_lon) / len(all_lon)
    center_lat = sum(all_lat) / len(all_lat)
    return zoom, center_lon, center_lat



# -


def visualize_robust_route(planner, start_id, end_id, day, hour, minute, min_confidence_pct, weather_condition):
    """
    The following function is the main visualization wrapper for the journey planner: 
    - it checks the user inputs
    - converts the selected arrival time
    - confidence threshold
    - weather condition into planner arguments
    --> then calls `planner.route(...)` to compute all feasible robust routes.
    
    Once the routes are found, it prepares one interactive map view per route, including:
    - route geometry
    - title
    - confidence
    - arrival constraint
    - zoom level
    - itinerary summary
    
    It then displays everything with Plotly and `ipywidgets`, 
    """
    if start_id == end_id:
        print("Error: Start and End stops must be different.")
        return

    def fmt(sec):
        return str(datetime.timedelta(seconds=int(sec)))

    arrival_time_str = f"{hour:02d}:{minute:02d}:00"
    min_confidence = min_confidence_pct / 100.0

    weather_arg = "adverse" if weather_condition == "Adverse (Rain/Snow)" else None

    stop_names = planner.stops_df.set_index('stop_id')['stop_name'].to_dict()
    start_name = stop_names.get(start_id, start_id)
    end_name = stop_names.get(end_id, end_id)

    print(f"Searching routes from {start_name} ({start_id}) to {end_name} ({end_id})...")

    start_compute = time.time()

    routes = planner.route(
        start_id=start_id,
        end_id=end_id,
        arrival_time=arrival_time_str,
        day=day,
        min_confidence=min_confidence,
        weather=weather_arg
    )

    compute_time = time.time() - start_compute
    print(f"Computation took {compute_time:.2f} seconds.")

    if not routes:
        print(f"\nNo routes found satisfying a {min_confidence_pct}% confidence level by {arrival_time_str}.")
        return

    n_routes = len(routes)
    stops_info = planner.stops_df.set_index('stop_id').to_dict('index')

    # Pre-build one standalone Figure per route
    route_figures = []

    for r_idx, route in enumerate(routes):
        traces, r_lat, r_lon, dep_str = _build_route_traces(route, stops_info, planner)
        zoom, c_lon, c_lat = _zoom_for_bbox(r_lat, r_lon)

        conf = route.get('confidence', min_confidence)
        conf_pct = int(conf * 100) if conf <= 1 else int(conf)

        arr_str = fmt(route['arrival_time'])

        # Make all traces in this route visible
        for t in traces:
            t.visible = True

        route_figures.append({
            'traces': traces,
            'zoom': zoom,
            'c_lon': c_lon,
            'c_lat': c_lat,
            'title': (
                f"Route {r_idx + 1}/{n_routes}<br>"
                f"<span style='font-size:14px;'>"
                f"Departure {dep_str}  |  "
                f"Arrival {arr_str} <= {arrival_time_str}  |  "
                f"Confidence = {conf_pct}% >= {min_confidence_pct}%"
                f"</span>"
            ),
            'summary': _build_route_summary(route, stops_info, r_idx, n_routes, planner),
            })

    # Build a single FigureWidget, initially showing route 0
    current = {'idx': 0}

    def _make_layout(rf):
        return dict(
            mapbox={
                'style': "carto-positron",
                'center': {'lon': rf['c_lon'], 'lat': rf['c_lat']},
                'zoom': rf['zoom'],
            },
            title=dict(text=rf['title'], x=0.5, xanchor='center'),
            margin={"l": 0, "r": 0, "t": 50, "b": 0},
            height=620,
            showlegend=False,
        )

    rf0 = route_figures[0]
    fig = go.FigureWidget(data=rf0['traces'], layout=_make_layout(rf0))

    # Right-side summary panel (sized to its content, vertically centered)
    panel = widgets.HTML(
        value=rf0['summary'],
        layout=widgets.Layout(width="auto", flex="0 0 auto"),
    )

    def _show_route(idx):
        """Swap all traces, update layout and refresh the summary panel."""
        rf = route_figures[idx]
        with fig.batch_update():
            # Replace traces entirely
            fig.data = []  # clear
        # FigureWidget requires adding traces one by one after clearing
        for t in rf['traces']:
            fig.add_trace(t)
        fig.update_layout(_make_layout(rf))
        panel.value = rf['summary']
        current['idx'] = idx
        label.value = f"Route {idx + 1} / {n_routes}"
        btn_prev.disabled = (idx == 0)
        btn_next.disabled = (idx == n_routes - 1)

    # ipywidgets nav bar < [Route X / N]  >  (tl-blue buttons)
    btn_prev = widgets.Button(
        description="<  Prev",
        layout=widgets.Layout(width="100px"),
        disabled=True,   # starts on route 0
    )
    btn_next = widgets.Button(
        description="Next  >",
        layout=widgets.Layout(width="100px"),
        disabled=(n_routes == 1),
    )
    for _b in (btn_prev, btn_next):
        _b.style.button_color = _LINE_COLOR
        try:
            _b.style.text_color = "#ffffff"   # ipywidgets >= 8
        except Exception:
            pass

    label = widgets.Label(
        value=f"Route 1 / {n_routes}",
        layout=widgets.Layout(width="100px", display="flex", justify_content="center"),
        style={'font_weight': 'bold'},
    )

    def on_prev(_):
        if current['idx'] > 0:
            _show_route(current['idx'] - 1)

    def on_next(_):
        if current['idx'] < n_routes - 1:
            _show_route(current['idx'] + 1)

    btn_prev.on_click(on_prev)
    btn_next.on_click(on_next)

    nav_bar = widgets.HBox(
        [btn_prev, label, btn_next],
        layout=widgets.Layout(
            display="flex",
            justify_content="center",
            align_items="center",
            margin="6px 0 0 0",
            gap="16px",
        ),
    )

    # Left column: nav bar centered over the map, then the map itself.
    left_col = widgets.VBox(
        [nav_bar, fig],
        layout=widgets.Layout(align_items="center"),
    )

    # Map (left) + summary panel (right), with a white gap between them,
    # panel vertically centered against the map.
    body = widgets.HBox(
        [left_col, panel],
        layout=widgets.Layout(
            display="flex",
            align_items="center",
            gap="36px",
        ),
    )

    display(body)


def show_dashboard(planner):
    """
    Builds and displays the interactive user interface using the provided planner instance.
    """
    stop_options = sorted(
        [(row['stop_name'], row['stop_id']) for _, row in planner.stops_df.iterrows()]
    )
    default_start = next((s[1] for s in stop_options if "Renens VD" in s[0]), stop_options[0][1])
    default_end = next((s[1] for s in stop_options if "Lausanne-Flon" in s[0]), stop_options[1][1])

    is_weather_aware = getattr(planner.delay_model, 'is_weather_aware', False)
    
    im = interact_manual(
        visualize_robust_route,
        planner=fixed(planner),
        start_id=widgets.Dropdown(options=stop_options, value=default_start, description='Start:'),
        end_id=widgets.Dropdown(options=stop_options, value=default_end, description='Destination:'),
        day=widgets.Dropdown(
            options=['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'],
            value='Monday', description='Day:'
        ),
        hour=widgets.IntSlider(min=0, max=23, step=1, value=8, description='Target Hr:'),
        minute=widgets.IntSlider(min=0, max=59, step=1, value=30, description='Target Min:'),
        min_confidence_pct=widgets.IntSlider(min=50, max=100, step=5, value=90, description='Conf. Q (%):'),

        weather_condition=widgets.Dropdown(
            options=['Normal', 'Adverse (Rain/Snow)'],
            value='Normal', 
            description='Weather:',
            disabled=not is_weather_aware
        ),
    )

    im.widget.children[-1].description = "Find & Draw Routes"
    im.widget.children[-1].button_style = "primary"

