import logging
import math
import statistics


def calculate_tolerance(records_sample: list) -> dict:

    labels = []
    tolerance = {}

    try:
        for i in range(len(records_sample)):
            label = records_sample[i][3]
            if label not in labels:
                labels.append(label)

        for label in labels:
            metric_sample = []
            for i in range(len(records_sample)):
                curr_label = records_sample[i][3]
                curr_value = records_sample[i][4]
                if curr_label == label and curr_value is not None and curr_value > 0:
                    metric_sample.append(curr_value)

            tolerance[label] = int(
                math.sqrt(statistics.mean(metric_sample)))

        return tolerance

    except Exception as err:
        logging.error(
            f"Tolerance calculation error : {err}")
