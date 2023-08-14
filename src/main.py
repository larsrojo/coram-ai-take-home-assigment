import sqlalchemy as sa

def database_connection() -> sa.Connection:
    engine = sa.create_engine("postgresql://postgres:postgres@localhost:5432/postgres")
    conn = engine.connect()
    conn.execute(
        sa.text(
            "CREATE TABLE IF NOT EXISTS detections "
            "(id SERIAL PRIMARY KEY, time TIMESTAMP WITH TIME ZONE, type VARCHAR)"
        )
    )

    return conn

# TO-DO
def ingest_data(conn: sa.Connection, timestamp: str, detection_type: str):
    conn.execute(
        sa.text("INSERT INTO detections (time, type) VALUES (:timestamp, :detection_type)"),
        {
            "timestamp": timestamp,
            "detection_type": detection_type
        }
    )

# TO-DO
def aggregate_detections(conn: sa.Connection) -> dict[str, list[tuple[str, str]]]:

    # return {
    #     "people": [
    #         ("2023-08-10T10:00:00", "2023-08-10T10:02:00"),
    #         ("2023-08-10T10:04:00", "2023-08-10T10:05:00"),
    #     ],
    #     "vehicles": [
    #         ("2023-08-10T10:00:00", "2023-08-10T10:02:00"),
    #         ("2023-08-10T10:05:00", "2023-08-10T10:07:00"),
    #     ],
    # }

    query = sa.text(
        """
        SELECT type,
               MIN(time) AS start_time,
               MAX(time) AS end_time
        FROM (
            SELECT type,
                   time,
                   LAG(time) OVER (PARTITION BY type ORDER BY time) AS prev_time,
                   EXTRACT(EPOCH FROM (time - LAG(time) OVER (PARTITION BY type ORDER BY time))) AS time_diff
            FROM detections
        ) AS subquery
        WHERE time_diff IS NULL OR time_diff > 60
        GROUP BY type, time_diff
        """
    )
    result = conn.execute(query)

    aggregate_results = {
        "people": [],
        "vehicles": []
    }

    for row in result:
        # print(row)
        # detection_type = row["type"]
        detection_type = row[0]
        # start_time = row["start_time"]
        start_time = row[1]
        # end_time = row["end_time"]
        end_time = row[2]
        
        if detection_type in ("pedestrian", "bicycle"):
            aggregate_results["people"].append((start_time, end_time))
        elif detection_type in ("car", "truck", "van"):
            aggregate_results["vehicles"].append((start_time, end_time))

    return aggregate_results

# TO-DO
def main():
    conn = database_connection()

    consecutive_pedestrian_count = 0

    # Simulate real-time detections every 30 seconds
    detections = [
        ("2023-08-10T18:30:30", "pedestrian"),
        ("2023-08-10T18:31:00", "pedestrian"),
        ("2023-08-10T18:31:00", "car"),
        ("2023-08-10T18:31:30", "pedestrian"),
        ("2023-08-10T18:35:00", "pedestrian"),
        ("2023-08-10T18:35:30", "pedestrian"),
        ("2023-08-10T18:36:00", "pedestrian"),
        ("2023-08-10T18:37:00", "pedestrian"),
        ("2023-08-10T18:37:30", "pedestrian"),
    ]

    for timestamp, detection_type in detections:
        ingest_data(conn, timestamp, detection_type)
        
        if detection_type == "pedestrian":
            consecutive_pedestrian_count += 1
        else:
            consecutive_pedestrian_count = 0

        if consecutive_pedestrian_count == 5:
            print(f"{timestamp}: Pedestrian unusual activity detected")

    aggregate_results = aggregate_detections(conn)
    
    print('Detected people: ')
    for result in aggregate_results['people']:
        print(f"From {result[0]} to {result[1]}")
    print('Detected vehicles: ')
    for result in aggregate_results['vehicles']:
        print(f"From {result[0]} to {result[1]}")


if __name__ == "__main__":
    main()
