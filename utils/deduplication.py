import logging
import pandas as pd

from datetime import timedelta

from utils.tolerance import calculate_tolerance

logger = logging.getLogger("deduplicate")


def deduplicate(records: list, formula: str = "none", bucket: int = 1) -> list:
    """Deduplicates records list.

    :param list records: records from table
    :param str formula: formula for tolerance calculation
    :param int bucket: determines size of bucket in hours
    :return list: deduplicated records from table
    """

    previous = {}
    deduplicated = []

    try:
        # Deduplicating without tolerance & bucketing
        if formula == "none":
            for record in records:
                node = record[1]
                label = record[3]
                value = record[4]
                if value is None or value < 0:
                    deduplicated.append(record)
                    continue
                if node not in previous:
                    previous[node] = {}
                if label not in previous[node]:
                    previous[node][label] = value
                    deduplicated.append(record)
                else:
                    prev_value = previous[node][label]
                    if value != prev_value:
                        deduplicated.append(record)
                        previous[node][label] = value
        else:
            records_df = pd.DataFrame(records)
            bucket_start = records_df[0][0].replace(minute=0, second=0,
                                                    microsecond=0)
            bucket_finish = bucket_start + timedelta(hours=bucket)

            while True:
                # Creating bucket
                records_bucket = records_df[(records_df[0] >= bucket_start) &
                                            (records_df[0] < bucket_finish)]

                # Exit bucket condition
                if len(records_bucket) <= 0:
                    break

                # Calculates bucket tolerance
                tolerances = calculate_tolerance(records_bucket,
                                                 formula=formula)

                # Deduplicates bucket with respective tolerance
                for record in records_bucket.itertuples():
                    node = record[2]
                    label = record[4]
                    value = record[5]

                    if value is None or value < 0:
                        deduplicated.append(record[1:])
                        continue
                    if node not in previous:
                        previous[node] = {}
                    if label not in previous[node]:
                        previous[node][label] = value
                        deduplicated.append(record[1:])
                    else:
                        prev_value = previous[node][label]
                        if formula == "cv":
                            floor = (prev_value - prev_value
                                     * tolerances[node][label])
                            ceiling = (prev_value + prev_value
                                       * tolerances[node][label])
                        else:
                            floor = prev_value - tolerances[node][label]
                            ceiling = prev_value + tolerances[node][label]
                        if value < floor or value > ceiling:
                            deduplicated.append(record[1:])
                            previous[node][label] = value

                bucket_start = bucket_finish
                bucket_finish += timedelta(hours=bucket)

    except Exception as err:
        logger.error("%s", err)

    return deduplicated
