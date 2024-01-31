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
    CREATE TABLE IF NOT EXISTS books (
        id INT AUTO_INCREMENT PRIMARY KEY,
        ISBN VARCHAR(20),
        book_title VARCHAR(500),
        book_author VARCHAR(255),
        year_of_publication VARCHAR(255),
        publisher VARCHAR(255),
        image_url_s VARCHAR(255),
        image_url_m VARCHAR(255),
        image_url_l VARCHAR(255)
    )
'''
cursor.execute(create_table_query)

file_path = 'Books.csv'

with open(file_path, newline='', encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        values = [
            row['ISBN'], row['Book-Title'], row['Book-Author'],
            row['Year-Of-Publication'], row['Publisher'],
            row['Image-URL-S'], row['Image-URL-M'], row['Image-URL-L']
        ]

        values = [None if value == 'NaN' else value for value in values]

        insert_query = '''
            INSERT INTO books 
            (ISBN, book_title, book_author, year_of_publication, publisher, 
            image_url_s, image_url_m, image_url_l) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        '''

        cursor.execute(insert_query, values)

conn.commit()
conn.close()

print("Dati inseriti nel database con successo.")
