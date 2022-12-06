import logging
import multiprocessing

from datetime import datetime, timedelta
from itertools import repeat

logger = logging.getLogger("reconstruction")


def partition_records(records: list) -> dict:
    """Partitions records based on node-label combination.

    :param list records: list with records
    :return dict: records in dict separated by node and label
    """

    partitioned_records = {}

    for record in records:
        node = record[1]
        label = record[3]
        if node not in partitioned_records:
            partitioned_records[node] = {}
        if label not in partitioned_records[node]:
            partitioned_records[node][label] = []

        partitioned_records[node][label].append(record)

    return partitioned_records


def reconstruct(partitioned_records: dict, start_time: datetime,
                end_time: datetime, gap: int = 60) -> list:
    """Reconstructs records list (deduplicated, aggregated).

    :param dict partitioned_records: reduced records
    :param datetime start_time: reconstruction start time
    :param datetime end_time: reconstruction end time
    :param int gap: expected interval between records
    :return list: reconstructed records.
    """

    reconstructed = []

    for label_records in partitioned_records.values():
        for records in label_records.values():
            first_record = records[0]
            first_timestamp = first_record[0].replace(second=0, microsecond=0)
            recon_timestamp = start_time.replace(second=0, microsecond=0)

            while recon_timestamp < first_timestamp:
                recon_record = (recon_timestamp, *first_record[1:])
                reconstructed.append(recon_record)
                recon_timestamp += timedelta(seconds=gap)

            reconstructed.append(first_record)

            for i in range(1, len(records)):
                curr_record = records[i]
                curr_timestamp = curr_record[0].replace(second=0,
                                                        microsecond=0)
                prev_record = records[i - 1]
                prev_timestamp = prev_record[0].replace(second=0,
                                                        microsecond=0)

                recon_timestamp = prev_timestamp + timedelta(seconds=gap)

                while recon_timestamp < curr_timestamp:
                    recon_record = (recon_timestamp, *prev_record[1:])
                    reconstructed.append(recon_record)
                    recon_timestamp += timedelta(seconds=gap)

                reconstructed.append(curr_record)

                if i == (len(records) - 1):
                    recon_timestamp = curr_timestamp + timedelta(seconds=gap)
                    while recon_timestamp < end_time:
                        recon_record = (recon_timestamp, *curr_record[1:])
                        reconstructed.append(recon_record)
                        recon_timestamp += timedelta(seconds=gap)

    return reconstructed


def reconstruct_parallel(partitioned_records: dict, start_time: datetime,
                         end_time: datetime, gap: int = 60) -> list:
    """Reconstructs records list (deduplicated, aggregated)

    :param list partitioned_records: reduced records
    :param datetime start_time: reconstruction start time
    :param datetime end_time: reconstruction end time
    :param int gap: expected interval between records
    :return list: reconstructed records
    """

    reconstructed = []

    try:
        cores = multiprocessing.cpu_count()
        nodes = [*partitioned_records.keys()]

        nodes_count = len(nodes)
        nodes_per_core = nodes_count // cores
        surplus = nodes_count % cores

        groups = []
        for i in range(cores):
            if (surplus != 0 and i == (cores - 1)):
                nodes_subset = nodes[i * nodes_per_core:]
            else:
                nodes_subset = nodes[
                  i * nodes_per_core: (i + 1) * nodes_per_core]

            records_subset = {node: partitioned_records[node]
                              for node in nodes_subset
                              if node in partitioned_records}
            groups.append(records_subset)

        args = zip(groups, repeat(start_time), repeat(end_time), repeat(gap))

        with multiprocessing.Pool() as pool:
            recon_records = pool.starmap(reconstruct, args)

        reconstructed = [item for sublist in recon_records for item in sublist]

    except Exception as err:
        logger.error("%s", err)

    return reconstructed
