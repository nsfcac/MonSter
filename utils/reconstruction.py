import logging

from datetime import datetime, timedelta


def reconstruction(records: list, end_date: datetime, time_gap: int = 10) -> list:

    node_label_records = {}
    reconstructed = []

    try:
        for record in records:
            nodeid = record[1]
            label = record[3]
            if nodeid not in node_label_records:
                node_label_records[nodeid] = {}

            if label not in node_label_records[nodeid]:
                node_label_records[nodeid][label] = []

            node_label_records[nodeid][label].append(record)

        for label_records in node_label_records.values():
            for records in label_records.values():
                reconstructed.append(records[0])
                for i in range(1, len(records)):
                    curr_record = records[i]
                    curr_time = curr_record[0].replace(second=0, microsecond=0)
                    prev_record = records[i - 1]
                    prev_time = prev_record[0].replace(second=0, microsecond=0)

                    recon_time = prev_time + \
                        timedelta(minutes=time_gap)

                    while recon_time < curr_time:
                        recon_record = (recon_time, *prev_record[1:])
                        reconstructed.append(recon_record)
                        recon_time += timedelta(minutes=time_gap)

                    reconstructed.append(curr_record)

                    if i == (len(records) - 1):
                        recon_time = curr_time + \
                            timedelta(minutes=time_gap)
                        while recon_time < end_date:
                            recon_record = (
                                recon_time, *curr_record[1:])
                            reconstructed.append(recon_record)
                            recon_time += timedelta(minutes=time_gap)

    except Exception as err:
        logging.error(f"Reconstruction error : {err}")

    return reconstructed
