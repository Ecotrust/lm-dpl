"""
Database Schema Manager for the Landmapper data pipeline.

Provides an interface for creating and managing the database schema
from *.sql files and SQL strings.
"""

import os
import psycopg2
from typing import Dict, Any

from ..utils.config import get_config


class DatabaseManager:
    """Context manager for managing the database schema from an SQL script."""

    def __init__(self, db_credentials: Dict[str, Any]):
        """
        Initialize the DatabaseManager with database connection details.

        Parameters
        ----------
        db_credentials : dict
            Database connection info.
        """
        self.db_credentials = db_credentials
        self.conn = None

    def __enter__(self):
        """Establish a connection to the PostgreSQL database."""
        try:
            print("Connecting to the database...")
            self.conn = psycopg2.connect(**self.db_credentials)
            print("Connection successful.")
        except psycopg2.OperationalError as e:
            print(f"Could not connect to the database: {e}")
            raise
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Close the database connection if it is open."""
        if self.conn:
            self.conn.close()
            print("Database connection closed.")

    def execute(self, sql: str) -> None:
        """Execute a single SQL command."""
        if not self.conn:
            print("No database connection.")
            return
        try:
            with self.conn.cursor() as cur:
                print(f"Executing SQL: {sql[:80]}...")
                cur.execute(sql)
            self.conn.commit()
            print("SQL executed successfully.")
        except Exception as e:
            print(f"Error executing SQL: {e}")
            self.conn.rollback()
            raise

    def execute_from_file(self, sql_script_path: str) -> None:
        """Execute SQL script from a file."""
        if not self.conn:
            print("No database connection.")
            return
        try:
            with open(sql_script_path, "r") as f:
                sql = "".join(line for line in f if not line.strip().startswith("--"))
            statements = [stmt.strip() for stmt in sql.split(";") if stmt.strip()]
            print(f"Found {len(statements)} SQL statements to execute.")
            with self.conn.cursor() as cur:
                for stmt in statements:
                    print(f"Executing: {stmt[:80]}...")
                    cur.execute(stmt)
            self.conn.commit()
            print("Database schema created successfully.")
        except Exception as e:
            print(f"Error executing schema script: {e}")
            self.conn.rollback()
            raise


def main():
    """Main function to set up the database schema."""
    dsn = get_config().postgres_dsn_dict

    sql_script_path = os.path.join(
        os.path.dirname(__file__), "sql", "pg_stage_schema.sql"
    )

    db_manager = None
    with DatabaseManager(dsn) as db_manager:
        db_manager.execute_from_file(sql_script_path)


if __name__ == "__main__":
    main()
