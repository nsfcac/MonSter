import logging
import math
import statistics


def calculate_tolerance(records: list) -> dict:

    labels_metrics = {}
    tolerance = {}

    try:
        for record in records:
            label = record[3]
            if label not in labels_metrics:
                labels_metrics[label] = []

        for record in records:
            curr_label = record[3]
            curr_value = record[4]
            if curr_value is not None and curr_value > 0:
                labels_metrics[curr_label].append(curr_value)

        for label in labels_metrics.keys():
            tolerance[label] = int(
                math.sqrt(statistics.stdev(labels_metrics[label])))

        return tolerance

    except Exception as err:
        logging.error(
            f"Tolerance calculation error : {err}")
