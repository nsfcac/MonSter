import logging


def deduplicate(records: list) -> any:
    try:

        print("Deduplicating...")

        current_reading = {}
        deduplicated_records = []

        for row in records:
            label = row[3]
            value = row[4]

            if label not in current_reading:
                current_reading[label] = value
                deduplicated_records.append(row)
            else:
                if current_reading[label] != value:
                    current_reading[label] = value
                    deduplicated_records.append(row)

    except Exception as err:
        logging.error(f"Deduplication error : {err}")
