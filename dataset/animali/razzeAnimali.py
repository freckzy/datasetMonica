import csv
import mysql.connector

config = {
    'user': 'root',
    'password': 'root',
    'host': 'localhost',
    'database': 'dataset',
    'raise_on_warnings': True
}

conn = mysql.connector.connect(**config)
cursor = conn.cursor()

create_table_query = '''
    CREATE TABLE IF NOT EXISTS razze (
        id INT AUTO_INCREMENT PRIMARY KEY,
        nome VARCHAR(255) NOT NULL
    )
'''
cursor.execute(create_table_query)

razze_animali = []
with open('razze_animali.CSV', newline='', encoding='utf-8') as csvfile:
    reader = csv.reader(csvfile)
    next(reader)  
    for row in reader:
        razze_animali.append(row[0])

insert_query = 'INSERT INTO razze (nome) VALUES (%s)'
razze_data = [(razza,) for razza in razze_animali]

cursor.executemany(insert_query, razze_data)

conn.commit()
conn.close()

print("Le razze sono state inserite nel database con successo.")
