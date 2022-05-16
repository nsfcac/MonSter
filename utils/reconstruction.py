import logging

from datetime import timedelta


def reconstruction(records: list) -> list:

    previous_reading = {}
    reconstructed_records = []

    try:
        for record in records:
            timestamp, nodeid, source, label, avg_value, min_value, max_value = record

            if ((avg_value is None or avg_value < 0) or (min_value is None or min_value < 0)
                    or (max_value is None or max_value < 0)):
                reconstructed_records.append(record)
                continue

            if nodeid not in previous_reading:
                previous_reading[nodeid] = {}

            if label not in previous_reading[nodeid]:
                previous_reading[nodeid][label] = timestamp
                reconstructed_records.append(record)
            else:
                previous_timestamp = previous_reading[nodeid][label]
                reconstructed_timestamp = previous_timestamp + \
                    timedelta(minutes=10)

                while reconstructed_timestamp < timestamp:
                    reconstructed_record = [
                        reconstructed_timestamp,
                        nodeid,
                        source,
                        label,
                        avg_value,
                        min_value,
                        max_value
                    ]
                    reconstructed_records.append(reconstructed_record)
                    reconstructed_timestamp += timedelta(minutes=10)

                reconstructed_records.append(record)
                previous_reading[nodeid][label] = timestamp

    except Exception as err:
        logging.error(f"Reconstruction error : {err}")

    return reconstructed_records
