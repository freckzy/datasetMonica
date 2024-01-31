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
    CREATE TABLE IF NOT EXISTS anime_data (
        id INT AUTO_INCREMENT PRIMARY KEY,
        anime_id INT,
        title VARCHAR(255),
        mean FLOAT,
        genres VARCHAR(255),
        studios VARCHAR(255),
        synopsis TEXT,
        media_type VARCHAR(255),
        num_episodes INT
    )
'''
cursor.execute(create_table_query)

file_path = 'anime_titles.csv'

with open(file_path, newline='', encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        values = [
            int(row['anime_id']), row['title'], float(row['mean']), row['genres'],
            row['studios'], row['synopsis'], row['media_type'], int(row['num_episodes'])
        ]

        values = [None if value == 'nan' else value for value in values]

        insert_query = '''
            INSERT INTO anime_data 
            (anime_id, title, mean, genres, studios, synopsis, media_type, num_episodes) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        '''

        cursor.execute(insert_query, values)

conn.commit()
conn.close()

print("Dati inseriti nel database con successo.")
