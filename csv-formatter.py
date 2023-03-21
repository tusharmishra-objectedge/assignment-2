import csv


def change_csv_delimiter(input_file_path, output_file_path):
    with open(input_file_path, "r") as input_file, open(
        output_file_path, "w", newline=""
    ) as output_file:
        reader = csv.reader(input_file)
        writer = csv.writer(output_file, delimiter="|")
        for row in reader:
            writer.writerow(row)


input_file_path = "one_batch.csv"
output_file_path = "output.csv"
change_csv_delimiter(input_file_path, output_file_path)
