import logging

from utils.calculate_tolerance import calculate_tolerance


def deduplicate(records: list) -> list:

    previous_reading = {}
    deduplicated_records = []

    try:
        tolerance = calculate_tolerance(records)

        for row in records:
            label = row[3]
            value = row[4]

            if value is not None:
                if label not in previous_reading:
                    previous_reading[label] = value
                    deduplicated_records.append(row)
                else:
                    prev_value = previous_reading[label]
                    if ((value < prev_value - tolerance[label])
                            or (value > prev_value + tolerance[label])):
                        previous_reading[label] = value
                        deduplicated_records.append(row)

        return deduplicated_records

    except Exception as err:
        logging.error(f"Deduplication error : {err}")
