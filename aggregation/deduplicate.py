import logging

from calculate_tolerance import calculate_tolerance

SAMPLE_SIZE = 1000


def deduplicate(records: list) -> list:
    try:

        print("Getting metrics' tolerance...")

        tolerance = calculate_tolerance(records[:SAMPLE_SIZE])
        print(tolerance)

        print("Deduplicating...")

        previous_reading = {}
        deduplicated_records = []
        for row in records:
            label = row[3]
            value = row[4]

            if label not in previous_reading and value is not None:
                previous_reading[label] = value
                deduplicated_records.append(row)
            else:
                prev_value = previous_reading[label]
                if value is not None:
                    if ((value < prev_value - tolerance[label]) or (value > prev_value + tolerance[label])):
                        previous_reading[label] = value
                        deduplicated_records.append(row)

        print(f"Number of records before deduplication: {len(records)}")
        print(
            f"Number of records after deduplication: {len(deduplicated_records)}")
        print(
            f"Improvement: {(len(records) - len(deduplicated_records))/len(records) * 100}%\n")

        return deduplicated_records

    except Exception as err:
        logging.error(f"Deduplication error : {err}")
