from cmath import isnan
import numpy as np


def compute_mapes(rec1, rec2):

    data = {}
    labels = {record[3] for record in rec1}

    for label in labels:
        label_data = []
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

            res = mape(y[:length], y_hat[:length])

            if not isnan(res):
                label_data.append(res)

        data[label] = np.average(label_data)

    return data


def mape(y, y_hat):
    """
    Mean Absolute Percentage Error (MAPE)
    """
    return np.nanmean(np.abs((np.array(y) - np.array(y_hat)) / np.array(y)))
