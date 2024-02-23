import elasticsearch.exceptions
import elasticsearch
from elasticsearch import Elasticsearch
from config import ELASTIC_PASSWORD, ELASTIC_USERNAME
from read_csv_file import ReadCSV
from read_sql_dump import ReadDump
from sql_db import WorkWithDB


class WorkWithElastic:
    ELASTIC_HOST = "localhost"
    ELASTIC_PORT = 9200
    ELASTIC_SERVICE = f"https://{ELASTIC_HOST}:{ELASTIC_PORT}"
    es = Elasticsearch(hosts=ELASTIC_SERVICE, basic_auth=(ELASTIC_USERNAME, ELASTIC_PASSWORD), verify_certs=False,
                       ssl_show_warn=False)

    def add_data(self, index_name, data):
        count = 0
        for i in range(len(data)):
            if count == 1000:
                print("1000 документів додана!")
                count = 0
            self.es.index(index=index_name, body=data[i])
            count += 1
        return f"Дані додано до index - {index_name}!"

    def get_data(self, index_name):
        try:
            search_result = self.es.search(index=index_name, size=10000)
            hits = search_result['hits']['hits']
            return hits
        except elasticsearch.NotFoundError:
            return f"Index - {index_name} відсутній."

    def delete_index(self, index_name):
        try:
            self.es.indices.delete(index=index_name)
            return f"Index - {index_name} був видалений."
        except elasticsearch.NotFoundError:
            return f"Index - {index_name} відсутній."


index_name_csv = 'data_from_csv'
index_name_sql_1 = 'data_from_sql_dump_1'
index_name_sql_2 = 'data_from_sql_dump_2'


work = WorkWithElastic()


def work_with_csv():
    reader = ReadCSV()
    csv_data = reader.read_csv('test/api_rusdram_users.csv')
    work.add_data(index_name_csv, csv_data)
    result = work.get_data(index_name_csv)
    return result


def work_with_dump_1():
    dump = ReadDump("test/tui.ua.sql")
    data_from_dump = dump.read_dump()
    worker = WorkWithDB()
    worker.add_data_to_db(data_from_dump)
    data_from_mysql = worker.get_data_from_db("user_data")
    worker.end_connection()
    work.add_data(index_name_sql_1, data_from_mysql)
    result = work.get_data(index_name_sql_1)
    return result


def work_with_dump_2():
    dump = ReadDump("test/demonforums.net usertable.sql")
    data_from_dump = dump.read_dump("cp1251")
    worker = WorkWithDB()
    worker.drop_table()
    worker.create_table()
    worker.add_data_to_db(data_from_dump)
    data_from_mysql = worker.get_data_from_db("mybb_users")
    for i in data_from_mysql:
        for k, v in i.items():
            if v != type(str):
                i[k] = str(v)
    # print(data_from_mysql[0])
    worker.end_connection()
    work.add_data(index_name_sql_2, data_from_mysql)
    result = work.get_data(index_name_sql_2)
    return result


def main():
    print(work_with_csv())
    print(work_with_dump_1())
    print(work_with_dump_2())
    # print(work.delete_index(index_name_csv))
    # print(work.delete_index(index_name_sql_1))
    # print(work.delete_index(index_name_sql_2))


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("You are stopped the program!")
