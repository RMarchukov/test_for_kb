import mysql.connector
from config import MYSQL_DATABASE, MYSQL_PASSWORD, MYSQL_USER
from mysql.connector import Error
from read_sql_dump import ReadDump


dump = ReadDump("test/demonforums.net usertable.sql")
params = dump.read_dump_for_params("cp1251")
# print(params)


class WorkWithDB:
    config = {
        'user': MYSQL_USER,
        'password': MYSQL_PASSWORD,
        'host': 'localhost',
        'database': MYSQL_DATABASE,
    }

    def __init__(self):
        self.connection = mysql.connector.connect(**self.config)
        self.cursor = self.connect_with_db()

    def connect_with_db(self):
        try:
            cursor = self.connection.cursor(dictionary=True)
            return cursor
        except mysql.connector.Error as err:
            print(f"Помилка: {err}")

    def add_data_to_db(self, data):
        try:
            for _ in self.cursor.execute(data, multi=True):
                pass
            self.connection.commit()
            return f"Дані додано!"
        except mysql.connector.Error as err:
            print(f"Помилка: {err}")

    def get_data_from_db(self, table_name):
        self.cursor.execute(f"SELECT * FROM {table_name}")
        result = self.cursor.fetchall()
        return result

    def end_connection(self):
        self.cursor.close()
        self.connection.close()

    def drop_table(self):
        drop_table_query = "DROP TABLE IF EXISTS mybb_users;"
        self.cursor.execute(drop_table_query)
        self.connection.commit()
        return "БД видалена!"

    def create_table(self):
        try:
            new_list = ["uid INT AUTO_INCREMENT PRIMARY KEY"]
            for i in params[1:]:
                if i == "regip" or i == "lastip":
                    new = f"{i} VARBINARY(255)"
                elif i in ["signature", "notepad", "usernotes", "last_useragent"]:
                    new = f"{i} TEXT"
                else:
                    new = f"{i} varchar(150)"
                new_list.append(new)
            res = ",\n".join(new_list)
            create_table_query = f"CREATE TABLE mybb_users ({res});"
            self.cursor.execute(create_table_query)
            self.connection.commit()
            return "БД створена!"
        except Error as err:
            print(f"Помилка: {err}")
