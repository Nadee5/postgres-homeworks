"""Скрипт для заполнения данными таблиц в БД Postgres."""
import csv

import psycopg2

connection = psycopg2.connect(host='localhost', database='north', user='postgres', password='12345')
connection.autocommit = True
try:
    with connection as conn:
        with conn.cursor() as cur:
            with open('north_data/customers_data.csv', encoding='utf-8') as csv_file:
                reader = csv.DictReader(csv_file)
                for row in reader:
                    customer_id = row['customer_id']
                    company_name = row['company_name']
                    contact_name = row['contact_name']
                    cur.execute('INSERT INTO customers VALUES (%s, %s, %s)', (customer_id, company_name, contact_name))

            with open('north_data/employees_data.csv', encoding='utf-8') as csv_file:
                reader = csv.DictReader(csv_file)
                for row in reader:
                    employee_id = row['employee_id']
                    first_name = row['first_name']
                    last_name = row['last_name']
                    title = row['title']
                    birth_date = row['birth_date']
                    notes = row['notes']
                    cur.execute('INSERT INTO employees VALUES (%s, %s, %s, %s, %s, %s)', (employee_id, first_name,
                                                                                          last_name, title,
                                                                                          birth_date, notes))

            with open('north_data/orders_data.csv', encoding='utf-8') as csv_file:
                reader = csv.DictReader(csv_file)
                for row in reader:
                    order_id = row['order_id']
                    customer_id = row['customer_id']
                    employee_id = row['employee_id']
                    order_date = row['order_date']
                    ship_city = row['ship_city']
                    cur.execute('INSERT INTO orders VALUES (%s, %s, %s, %s, %s)', (order_id, customer_id, employee_id,
                                                                                   order_date, ship_city))
finally:
    conn.close()

