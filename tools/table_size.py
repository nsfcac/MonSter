import csv
lines = []
with open ('./data/table_size.txt') as f:
    lines = f.readlines()
    # print(lines)

tablename_list = []
tablesize_list = []
table_size_record = {}
for line in lines:
    tablename = line.split(':')[1].replace(' ', '')
    try:
        tablesize = line.split('"')[-2]
        size_numb = int(tablesize.split(' ')[0])
        size_unit = tablesize.split(' ')[1]
        if size_unit == 'MB':
            size_numb = size_numb / 1024
        tablesize = float("{0:.2f}".format(size_numb))
        tablename_list.append(tablename)
        tablesize_list.append(tablesize)
        table_size_record.update({
            tablename: tablesize
        })
    except:
        pass


ordered = dict(sorted(table_size_record.items(), key=lambda item: item[1], reverse=True))
print(sum(tablesize_list))
# print(tablename_list)
# print(tablesize_list)

# with open('./data/table_size_slurm_csv.csv', mode='w') as f:
#     csv_writer = csv.writer(f)
#     csv_writer.writerow(ordered.keys())
#     csv_writer.writerow(ordered.values())