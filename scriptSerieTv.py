import csv
import mysql.connector

def extract_numeric_value(value):
    try:
        return float(value)
    except (ValueError, TypeError):
        return None

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
    CREATE TABLE IF NOT EXISTS series_data (
        id INT AUTO_INCREMENT PRIMARY KEY,
        series_title VARCHAR(255),
        release_year VARCHAR(50),
        runtime VARCHAR(20),
        genre VARCHAR(255),
        rating VARCHAR(50),
        cast VARCHAR(255),
        synopsis TEXT
    )
'''
cursor.execute(create_table_query)

file_path = 'TV Series.csv'

with open(file_path, newline='', encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile)
    
    for row in reader:
        values = [
            row.get('Series Title', None), 
            row.get('Release Year', 0),  
            row.get('Runtime', None),
            row.get('Genre', None),
            row.get('Rating', None),
            row.get('Cast', None),
            row.get('Synopsis', None)
        ]

        values = [None if value == 'NaN' else value for value in values]

        insert_query = '''
            INSERT INTO series_data 
            (series_title, release_year, runtime, genre, rating, cast, synopsis) 
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        '''

        cursor.execute(insert_query, values)

conn.commit()
conn.close()

print("Dati inseriti nel database con successo.")
