import logging
import math
import statistics


def calculate_tolerance(records: list) -> dict:

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
                    tolerance = int(math.sqrt(statistics.stdev(metrics)))
                else:
                    tolerance = int(math.sqrt(metrics[0]))

                tolerances[node_id][label] = tolerance

    except Exception as err:
        logging.error(
            f"Tolerance calculation error : {err}")

    return tolerances
