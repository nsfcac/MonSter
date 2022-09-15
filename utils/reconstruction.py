from itertools import repeat
import logging
import multiprocessing
from datetime import datetime, timedelta

logger = logging.getLogger("reconstruction")


def reconstruct(records: list, start_time: datetime, end_time: datetime, gap: int = 60) -> list:
    """Reconstructs records list (deduplicated, aggregated).
    :param list records: reduced records.
    :param datetime start_time: reconstruction start time.
    :param datetime end_time: reconstruction end time.
    :param int gap: expected interval between records, defaults to 60 seconds.
    :return list: reconstructed records.
    """
    clustered_records = {}
    reconstructed = []

    try:
        for record in records:
            node = record[1]
            label = record[3]
            if node not in clustered_records:
                clustered_records[node] = {}
            if label not in clustered_records[node]:
                clustered_records[node][label] = []

            clustered_records[node][label].append(record)

        for label_records in clustered_records.values():
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
                    curr_timestamp = curr_record[0].replace(second=0, microsecond=0)
                    prev_record = records[i - 1]
                    prev_timestamp = prev_record[0].replace(second=0, microsecond=0)

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
                            
    except Exception as err:
        logger.error("%s", err)

    return reconstructed


def recon(clustered_records: dict, start_time: datetime, end_time: datetime, gap: int = 60):
    """Reconstructs records list (deduplicated, aggregated).
    :param list clustered_records: reduced records.
    :param datetime start_time: reconstruction start time.
    :param datetime end_time: reconstruction end time.
    :param int gap: expected interval between records, defaults to 60 seconds.
    :return list: reconstructed records.
    """    
    
    reconstructed = []
    
    for label_records in clustered_records.values():
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
                curr_timestamp = curr_record[0].replace(second=0, microsecond=0)
                prev_record = records[i - 1]
                prev_timestamp = prev_record[0].replace(second=0, microsecond=0)

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
                    

def reconstruct_parallel(records: list, start_time: datetime, end_time: datetime, gap: int = 60) -> list:
    """Reconstructs records list (deduplicated, aggregated).
    :param list records: reduced records.
    :param datetime start_time: reconstruction start time.
    :param datetime end_time: reconstruction end time.
    :param int gap: expected interval between records, defaults to 60 seconds.
    :return list: reconstructed records.
    """
    clustered_records = {}
    reconstructed = []
    
    try:    
        for record in records:
            node = record[1]
            label = record[3]
            if node not in clustered_records:
                clustered_records[node] = {}
            if label not in clustered_records[node]:
                clustered_records[node][label] = []

            clustered_records[node][label].append(record)
            
        timer_start = datetime.now()
                    
        cores = multiprocessing.cpu_count()
        nodes = [*clustered_records.keys()]
        
        nodes_count = len(nodes)
        nodes_per_core = nodes_count // cores
        surplus = nodes_count % cores
        
        groups = []
        for i in range(cores):
            if(surplus != 0 and i == (cores - 1)):
                nodes_subset = nodes[i * nodes_per_core:]
            else:
                nodes_subset = nodes[i * nodes_per_core: (i + 1) * nodes_per_core]
                
            records_subset = {node: clustered_records[node] for node in nodes_subset if node in clustered_records}
            groups.append(records_subset)
            
        args = zip(groups, repeat(start_time), repeat(end_time), repeat(gap))
        
        timer_end = datetime.now() - timer_start
        print(f"Partition runtime: {timer_end.total_seconds()}")
        
        timer_start = datetime.now()
        with multiprocessing.Pool() as pool:
            records = pool.starmap(recon, args)
            
        timer_end = datetime.now() - timer_start
        print(f"Multiprocessing runtime: {timer_end.total_seconds()}")
            
        timer_start = datetime.now()
        reconstructed = [item for sublist in records for item in sublist]
        timer_end = datetime.now() - timer_start
        print(f"Flattening runtime: {timer_end.total_seconds()}")
                
    except Exception as err:
        logger.error("%s", err)

    return reconstructed
