import logging

from datetime import timedelta

from utils.calculate_tolerance import calculate_tolerance


def deduplicate(records: list) -> list:
    """Deduplicates records list.

    :param list records: original metrics.
    :return list: deduplicated list from original metrics.
    """
    previous_reading = {}
    deduplicated_records = []

    tolerances = calculate_tolerance(records)

    start_index = 0
    start_time = records[0][0]
    finish_time = start_time.replace(microsecond=0, second=0, minute=0) + timedelta(hours=1)

    try:
        for index, record in enumerate(records):
            curr_time = record[0]
            if curr_time > finish_time:
                for record in records[start_index:index]:
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
                            floor_tolerance = prev_value - prev_value * tolerances[nodeid][label]
                            ceiling_tolerance = prev_value + prev_value * tolerances[nodeid][label]
                            if value < floor_tolerance or value > ceiling_tolerance:
                                previous_reading[nodeid][label] = value
                                deduplicated_records.append(record)
                start_index = index
                finish_time = curr_time.replace(microsecond=0, second=0, minute=0) + timedelta(hours=1)
    except Exception as err:
        logging.error(f"deduplicate error : {err}")
        
    return deduplicated_records
