import logging
import math
import statistics

from pandas import DataFrame
from typing import Union

logger = logging.getLogger("tolerance")


def calculate_tolerance(records: Union[list, DataFrame],
                        formula: str = "cv") -> dict:
    """Calculates tolerances from given records list.

    :param Union[list, DataFrame] records: records from table
    :param str formula: formula for tolerance calculation
    :return dict: tolerances for each metric type
    """

    tolerances = {}

    try:
        if isinstance(records, list):
            for record in records:
                node = record[1]
                label = record[3]
                value = record[4]
                if node not in tolerances:
                    tolerances[node] = {}
                if label not in tolerances[node]:
                    tolerances[node][label] = []
                if value is not None and value > 0:
                    tolerances[node][label].append(value)
        else:
            # Using Pandas DataFrame for bucketing
            for record in records.itertuples():
                node = record[2]
                label = record[4]
                value = record[5]
                if node not in tolerances:
                    tolerances[node] = {}
                if label not in tolerances[node]:
                    tolerances[node][label] = []
                if value is not None and value > 0:
                    tolerances[node][label].append(value)

        for node, labels_metrics in tolerances.items():
            for label, metrics in labels_metrics.items():
                if len(metrics) > 1:
                    if formula == "cv":
                        # Coefficient of Variation formula
                        stddev = statistics.stdev(metrics)
                        mean = statistics.mean(metrics)
                        tolerance = stddev / mean
                    elif formula == "stddev":
                        # Square root of standard deviation formula
                        tolerance = int(math.sqrt(statistics.stdev(metrics)))
                    elif formula == "mean":
                        # Square root of mean formula
                        tolerance = int(math.sqrt(statistics.mean(metrics)))
                elif len(metrics) == 1:
                    tolerance = int(math.sqrt(metrics[0]))
                else:
                    tolerance = 0

                tolerances[node][label] = tolerance

    except Exception as err:
        logger.error("%s", err)

    return tolerances
