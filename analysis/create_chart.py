import matplotlib.pyplot as plt


def create_chart(records: list, deduplicated_records: list) -> None:

    records_x = []
    records_y = []
    for record in records:
        if record[3] == "CPU1 Temp" and record[1] == 21 and record[4] > 0:
            records_x.append(record[0])
            records_y.append(record[4])

    deduplicated_records_x = []
    deduplicated_records_y = []
    for deduplicated_record in deduplicated_records:
        if deduplicated_record[3] == "CPU1 Temp" and deduplicated_record[1] == 21 and deduplicated_record[4] > 0:
            deduplicated_records_x.append(deduplicated_record[0])
            deduplicated_records_y.append(deduplicated_record[4])

    fig = plt.figure()
    plt.plot_date(records_x, records_y, 'g-', label='Aggregated')
    plt.plot_date(deduplicated_records_x,
                  deduplicated_records_y, 'b-', label='Aggregated & Deduplicated', linestyle='dashed')
    fig.autofmt_xdate()
    plt.legend()
    plt.savefig('regular_vs_deduplicated_temperaturereading.png')
