import csv
import mysql.connector

db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': 'root',
    'database': 'dataset'
}

conn = mysql.connector.connect(**db_config)
cursor = conn.cursor()

create_table_query = '''
CREATE TABLE IF NOT EXISTS film (
    id INT PRIMARY KEY,
    title VARCHAR(255),
    overview TEXT,
    original_language VARCHAR(10),
    vote_count INT,
    vote_average FLOAT
);
'''
cursor.execute(create_table_query)
conn.commit()

csv_file_path = 'TMDb_updated.csv' 
with open(csv_file_path, 'r', encoding='utf-8') as csv_file:
    csv_reader = csv.DictReader(csv_file)
    
    for row in csv_reader:
        insert_query = '''
        INSERT INTO film (id, title, overview, original_language, vote_count, vote_average)
        VALUES (%s, %s, %s, %s, %s, %s);
        '''
        values = (
            int(row['id']),
            row['title'],
            row['overview'],
            row['original_language'],
            int(row['vote_count']),
            float(row['vote_average'])
        )
        
        cursor.execute(insert_query, values)
        conn.commit()

conn.close()

print("Dati inseriti nel database MySQL con successo.")
