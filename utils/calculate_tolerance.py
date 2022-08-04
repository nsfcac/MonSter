import logging
import math
import statistics


def calculate_tolerance(records: list) -> dict:
    """Calculates tolerances from given records list using Coefficient of Variation.

    :param list records: original metrics.
    :return dict: tolerances for each metric type.
    """
    tolerances = {}

    try:
        for record in records:
            node_id = record[1]
            label = record[3]
            value = record[4]
            if node_id not in tolerances:
                tolerances[node_id] = {}
            if label not in tolerances[node_id]:
                tolerances[node_id][label] = []
            if value is not None and value > 0:
                tolerances[node_id][label].append(value)
                
        for node_id, labels_metrics in tolerances.items():
            for label, metrics in labels_metrics.items():
                if len(metrics) > 1:
                    stddev = statistics.stdev(metrics)
                    mean = statistics.mean(metrics)
                    tolerance = stddev / mean
                else:
                    tolerance = int(math.sqrt(metrics[0]))
                    
                tolerances[node_id][label] = tolerance
    except Exception as err:
        logging.error(f"calculate_tolerance error : {err}")

    return tolerances
