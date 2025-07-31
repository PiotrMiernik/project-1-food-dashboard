import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from dotenv import load_dotenv
from src.helpers.db_utils import get_db_connection

def execute_sql_file(sql_file_path):
    with open(sql_file_path, 'r') as file:
        sql_commands = file.read()

    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute(sql_commands)
        conn.commit()
        print("Schema created successfully.")
    except Exception as e:
        print(f"Error creating schema: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    # Load environment variables from .env file
    load_dotenv()

    sql_path = "src/datawarehouse/food_dw.sql"
    execute_sql_file(sql_path)
