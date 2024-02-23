import csv


class ReadCSV:

    def read_csv(self, file_path):
        with open(file_path, 'r', newline='', encoding='utf-8') as file:
            csv_reader = csv.DictReader(file)
            data_list = []
            for row in csv_reader:
                data_list.append(row)
        return data_list


# c = ReadCSV()
# print(c.read_csv('test/api_rusdram_users.csv'))
