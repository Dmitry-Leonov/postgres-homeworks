"""Скрипт для заполнения данными таблиц в БД Postgres."""
import csv
import psycopg2


def get_filling_table(name_file_csv, name_table):
    # connect to db
    conn = psycopg2.connect(host='localhost', database='north', user='postgres', password='040872')
    with open(name_file_csv, 'r') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        # Пропускаем первую строку, чтобы не заносить в базу наименование полей из файла
        next(csv_reader)
        try:
            with conn:
                with conn.cursor() as cur:
                    for row in csv_reader:
                        if name_table == 'customers':
                            cur.execute("INSERT INTO customers VALUES (%s, %s, %s)",
                                        (row[0], row[1], row[2]))
                        elif name_table == 'employees':
                            cur.execute("INSERT INTO employees VALUES (%s, %s, %s, %s, %s, %s)",
                                        (row[0], row[1], row[2], row[3], row[4], row[5]))
                        elif name_table == 'orders':
                            cur.execute("INSERT INTO orders VALUES (%s, %s, %s, %s, %s)",
                                        (row[0], row[1], row[2], row[3], row[4]))
        finally:
            conn.close()


if __name__ == '__main__':
    get_filling_table('north_data\customers_data.csv', 'customers')
    get_filling_table('north_data\employees_data.csv', 'employees')
    get_filling_table('north_data\orders_data.csv', 'orders')
