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

create_table_query = """
CREATE TABLE IF NOT EXISTS alimenti (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255),
    category VARCHAR(255),
    food_code VARCHAR(255),
    scientific_name VARCHAR(255),
    english_name VARCHAR(255)
)
"""
cursor.execute(create_table_query)

csv_file_path = 'crea_food_composition_tables.csv'

with open(csv_file_path, 'r', newline='', encoding='utf-8') as csv_file:
    csv_reader = csv.DictReader(csv_file)
    
    for row in csv_reader:
        name = row['name']
        category = row['category']
        food_code = row['food_code']
        scientific_name = row['scientific_name']
        english_name = row['english_name']
        
        insert_query = """
        INSERT INTO alimenti (name, category, food_code, scientific_name, english_name)
        VALUES (%s, %s, %s, %s, %s)
        """
        data = (name, category, food_code, scientific_name, english_name)
        cursor.execute(insert_query, data)

conn.commit()

cursor.close()
conn.close()
