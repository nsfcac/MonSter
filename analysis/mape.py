from cmath import isnan
import numpy as np


def compute_mapes(table, rec1, rec2):

    labels = {record[3] for record in rec1}
    print(labels)

    data = []
    for label in labels:
        for node_id in range(1, 468):
            y = []
            for record in rec1:
                if record[1] == node_id and record[3] == label and record[4] is not None:
                    y.append(record[4])

            y_hat = []
            for record in rec2:
                if record[1] == node_id and record[3] == label and record[4] is not None:
                    y_hat.append(record[4])

            length = len(y) if len(y) < len(y_hat) else len(y_hat)

            res = mape(np.array(y[:length]), np.array(y_hat[:length]))
            if not isnan(res):
                data.append(res)

        print(
            f"\nAverage MAPE for all nodes from {table} with fqdd = {label}: {np.average(data)}")


def mape(y, y_hat):
    """
    Mean Absolute Percentage Error (MAPE)
    """
    return np.mean(np.abs((y - y_hat) / y))
