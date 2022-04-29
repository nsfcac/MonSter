import logging

from utils.calculate_tolerance import calculate_tolerance


def deduplicate(records: list) -> list:

    previous_reading = {}
    deduplicated_records = []

    tolerance = calculate_tolerance(records)

    try:
        for record in records:
            nodeid = record[1]
            label = record[3]
            value = record[4]

            if value is None or value < 0:
                deduplicated_records.append(record)
            else:
                if nodeid not in previous_reading:
                    previous_reading[nodeid] = {}

                if label not in previous_reading[nodeid]:
                    previous_reading[nodeid][label] = value
                    deduplicated_records.append(record)
                else:
                    prev_value = previous_reading[nodeid][label]
                    if ((value < prev_value - tolerance[label])
                            or (value > prev_value + tolerance[label])):
                        previous_reading[nodeid][label] = value
                        deduplicated_records.append(record)

        return deduplicated_records

    except Exception as err:
        logging.error(f"Deduplication error : {err}")
