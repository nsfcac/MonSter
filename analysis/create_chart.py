import matplotlib.pyplot as plt


def create_chart(records1: list, records2: list) -> None:

    records_x = []
    records_y = []
    for record in records1:
        if record[3] == "FAN_4" and record[1] == 20:
            records_x.append(record[0])
            records_y.append(record[4])

    records2_x = []
    records2_y = []
    for record in records2:
        if record[3] == "FAN_4" and record[1] == 20:
            records2_x.append(record[0])
            records2_y.append(record[4])

    fig = plt.figure()
    plt.style.use('seaborn')
    plt.plot_date(records_x, records_y, '-', label='Aggregated')
    plt.plot_date(records2_x, records2_y, '-', label='Reconstructed')
    fig.autofmt_xdate()
    plt.legend(loc=2)
    plt.savefig(
        './analysis/results/aggregated_vs_reconstructed_fan4_20.png')
