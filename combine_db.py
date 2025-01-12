import sqlite3
import os

class DatabaseMerger:
    def __init__(self, db1_path, db2_path, output_db_path, chunk_size=1000):
        self.db1_path = db1_path
        self.db2_path = db2_path
        self.output_db_path = output_db_path
        self.chunk_size = chunk_size

    def merge(self):
        if os.path.exists(self.output_db_path):
            os.remove(self.output_db_path)

        conn1 = sqlite3.connect(self.db1_path)
        conn2 = sqlite3.connect(self.db2_path)
        conn_output = sqlite3.connect(self.output_db_path)

        try:
            cursor1 = conn1.cursor()
            cursor2 = conn2.cursor()
            cursor_output = conn_output.cursor()

            # Get table names from both databases
            tables_db1 = self._get_table_names(cursor1)
            tables_db2 = self._get_table_names(cursor2)

            # Combine all unique tables
            all_tables = tables_db1.union(tables_db2)

            for table in all_tables:
                print(f"Merging table: {table}")

                # Get unified column information
                columns_info = self._get_unified_columns_info(cursor1, cursor2, table)
                if not columns_info:
                    print(f"Skipping table '{table}' as it has no columns.")
                    continue

                # Create the unified table in the output database
                self._create_table(cursor_output, table, columns_info)

                # Merge data from both databases
                self._merge_table_data(cursor1, cursor2, cursor_output, table, columns_info)

            conn_output.commit()
            print(f"Merging complete. Output database: {self.output_db_path}")

        finally:
            conn1.close()
            conn2.close()
            conn_output.close()

    def _get_table_names(self, cursor):
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        return {row[0] for row in cursor.fetchall()}

    def _get_unified_columns_info(self, cursor1, cursor2, table):
        columns_db1 = self._get_table_columns(cursor1, table)
        columns_db2 = self._get_table_columns(cursor2, table)

        all_columns = set(columns_db1).union(columns_db2)
        return [(col, col in columns_db1, col in columns_db2) for col in sorted(all_columns)]

    def _get_table_columns(self, cursor, table):
        try:
            cursor.execute(f"PRAGMA table_info({table});")
            return [row[1] for row in cursor.fetchall()]
        except sqlite3.OperationalError:
            return []

    def _create_table(self, cursor, table, columns_info):
        column_defs = [f"{col} TEXT" for col, _, _ in columns_info]  # Default to TEXT type
        create_table_sql = f"CREATE TABLE IF NOT EXISTS {table} ({', '.join(column_defs)});"
        cursor.execute(create_table_sql)

    def _merge_table_data(self, cursor1, cursor2, cursor_output, table, columns_info):
        all_columns = [col for col, _, _ in columns_info]
        placeholders = ", ".join("?" for _ in all_columns)

        def fetch_and_insert(cursor, db_columns):
            if not db_columns:
                return
            cursor.execute(f"SELECT * FROM {table}")
            db_column_set = set(db_columns)
            column_map = [db_columns.index(col) if col in db_column_set else None for col in all_columns]

            while rows := cursor.fetchmany(self.chunk_size):
                # Map rows to unified column format
                data = [
                    [row[idx] if idx is not None else None for idx in column_map]
                    for row in rows
                ]
                cursor_output.executemany(f"INSERT INTO {table} VALUES ({placeholders})", data)

        # Insert data from both databases
        fetch_and_insert(cursor1, self._get_table_columns(cursor1, table))
        fetch_and_insert(cursor2, self._get_table_columns(cursor2, table))

    def remove_duplicates_from_merged_db(self):
        if not os.path.exists(self.output_db_path):
            print("Output database does not exist.")
            return

        conn_output = sqlite3.connect(self.output_db_path)
        try:
            cursor_output = conn_output.cursor()
            tables = self._get_table_names(cursor_output)

            for table in tables:
                print(f"Removing duplicates from table: {table}")

                # Get the column names for the table
                cursor_output.execute(f"PRAGMA table_info({table});")
                columns_info = cursor_output.fetchall()
                columns = [col[1] for col in columns_info]  # Column names are in the second position
                column_list = ", ".join(columns)

                # Create a temporary table without duplicates
                temp_table = f"{table}_temp"
                cursor_output.execute(f"DROP TABLE IF EXISTS {temp_table};")  # Ensure no leftover temp table
                cursor_output.execute(f"""
                    CREATE TABLE {temp_table} AS
                    SELECT DISTINCT {column_list}
                    FROM {table};
                """)

                # Replace the original table with the deduplicated table
                cursor_output.execute(f"DROP TABLE {table};")
                cursor_output.execute(f"ALTER TABLE {temp_table} RENAME TO {table};")

                print(f"Duplicates removed from table: {table}")

            conn_output.commit()
            print("Duplicate removal complete.")
        finally:
            conn_output.close()

if __name__ == "__main__":
    db1 = "NDW1.db"
    db2 = "NDW2.db"
    output_db = "NDW.db"

    merger = DatabaseMerger(db1, db2, output_db)
    merger.merge()

    # Call the duplicate removal function after merging
    merger.remove_duplicates_from_merged_db()