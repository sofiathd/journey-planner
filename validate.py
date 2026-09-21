import pyspark.sql.functions as F


arr_actual_minutes = (
    F.hour("arr_actual") * 60 +
    F.minute("arr_actual")
)

"""
Validate a route by comparing the arrival time at a stop with the start time of the next step.
"""
def valide_route(route_data, istdaten_df, walk_model, debug=False):
    before = 0
    after = 0
    route = route_data["route"]
    deadline = route_data["arrival_deadline"]

    for i in range(0, len(route)):
        if debug:
            print(i)
        time, id_start, trip_id, id_end = route[i]
        if trip_id is None or id_end is None:
            continue

        target_hour = time // 3600
        target_minute = (time % 3600) // 60
        
        # next step is not walk
        if (i < len(route)-2) and route[i+1][2] is not None:
            time_next = route[i+1][0] - 120 # 2 minutes to change train

            result = (
                istdaten_df
                .filter(F.col("bpuic") == id_end)
                .filter(
                    (F.hour("arr_time") == target_hour) &
                    (F.minute("arr_time") == target_minute)
                )
                .agg(
                    F.sum(
                        F.when(arr_actual_minutes <
                               time_next, 1).otherwise(0)
                    ).alias("before"),

                    F.sum(
                        F.when(arr_actual_minutes >=
                               time_next, 1).otherwise(0)
                    ).alias("after")
                )
                .select("before", "after")
                .first()
            )

            if result["before"] is not None:
                before += result["before"]
                after += result["after"]

            if debug:
                print(before, after)

        # next step is walk
        if (i < len(route)-2) and route[i+1][2] is None:
            time_next = route[i+2][0]
            time_next -= get_time(walk_model[id_end], route[i+2][1]) 

            result = (
                istdaten_df
                .filter(F.col("bpuic") == id_end)
                .filter(
                    (F.hour("arr_time") == target_hour) &
                    (F.minute("arr_time") == target_minute)
                )
                .agg(
                    F.sum(
                        F.when(arr_actual_minutes <
                               time_next, 1).otherwise(0)
                    ).alias("before"),

                    F.sum(
                        F.when(arr_actual_minutes >=
                               time_next, 1).otherwise(0)
                    ).alias("after")
                )
                .select("before", "after")
                .first()
            )

            if result["before"] is not None:
                before += result["before"]
                after += result["after"]

            if debug:
                print(before, after)

        # before last step and last step is walk
        if (i == (len(route) - 2) and route[i+1][2] is None):
            deadline -= get_time(walk_model[id_end], route[i+1][3])

            result = (
                istdaten_df
                .filter(F.col("bpuic") == id_end)
                .filter(
                    (F.hour("arr_time") == target_hour) &
                    (F.minute("arr_time") == target_minute)
                )
                .agg(
                    F.sum(
                        F.when(arr_actual_minutes <
                               deadline, 1).otherwise(0)
                    ).alias("before"),

                    F.sum(
                        F.when(arr_actual_minutes >=
                               deadline, 1).otherwise(0)
                    ).alias("after")
                )
                .select("before", "after")
                .first()
            )
            if result["before"] is not None:
                before += result["before"]
                after += result["after"]

            if debug:
                print(before, after)
            return before, after
            
        if (i == (len(route) - 1)):  # last step
            result = (
                istdaten_df
                .filter(F.col("bpuic") == id_end)
                .filter(
                    (F.hour("arr_time") == target_hour) &
                    (F.minute("arr_time") == target_minute)
                )
                .agg(
                    F.sum(
                        F.when(arr_actual_minutes <
                               deadline, 1).otherwise(0)
                    ).alias("before"),

                    F.sum(
                        F.when(arr_actual_minutes >=
                               deadline, 1).otherwise(0)
                    ).alias("after")
                )
                .select("before", "after")
                .first()
            )
            if result["before"] is not None:
                before += result["before"]
                after += result["after"]

            if debug:
                print(before, after)
            return before,  after

"""
Return the time to walk from a stop to another stop.
"""
def get_time(list_stop, stop):
    for data in list_stop:
        if data[0] == stop:
            return data[1]
