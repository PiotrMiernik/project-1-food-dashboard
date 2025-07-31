import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from src.helpers.db_utils import get_db_connection

def alter_column_type():
    """
    Alters the column type of 'value' in the 'fact_metrics' table to DECIMAL(14, 2).
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("ALTER TABLE fact_metrics ALTER COLUMN value TYPE DECIMAL(14, 2);")
            conn.commit()
            print("Column 'value' in 'fact_metrics' updated to DECIMAL(14, 2).")
    except Exception as e:
        print("Error altering column type:", e)
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    alter_column_type()
