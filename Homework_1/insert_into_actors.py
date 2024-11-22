import psycopg2
from psycopg2 import sql

DB_CONFIG = {
    'dbname': 'postgres',
    'user': 'postgres',
    'password': 'postgres',
    'host': 'localhost',
    'port': '5432'
}

SQL_FILE_PATH = "Homework_1/cumulative_table_query_actors.sql"

def read_sql_file(file_path):
    try:
        with open(file_path, 'r') as file:
            return file.read()
    except FileNotFoundError:
        print(f"SQL file not found: {file_path}")
        raise


def load_incrementally(start_year, end_year, sql_template):
    try:
        connection = psycopg2.connect(**DB_CONFIG)
        cursor = connection.cursor()

        for year in range(start_year, end_year + 1):
            print(f"Processing year: {year}")
            
            query = sql_template.replace("{prev_year}", str(year - 1)).replace("{current_year}", str(year))

            cursor.execute(query)
            connection.commit()
            print(f"Year {year} processed successfully.")

    except Exception as e:
        print(f"An error occurred: {e}")
        if connection:
            connection.rollback()
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

if __name__ == "__main__":
    try:
        sql_template = read_sql_file(SQL_FILE_PATH)
        load_incrementally(1969, 2024, sql_template)
    except Exception as e:
        print(f"Failed to load incrementally: {e}")
