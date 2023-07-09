import sqlite3
import logging
from pathlib import Path
from collections.abc import Iterable, Generator, Callable
import pandas as pd
from contextlib import contextmanager
import json

logger = logging.getLogger(__name__)

def dict_factory(cursor, row):
    fields = [column[0] for column in cursor.description]

    to_return = dict()
    for field, value in zip(fields, row):
        if field.endswith('_json'):
            to_return[field] = json.loads(value)
        else:
            to_return[field] = value

    return to_return

class Database():
    # TODO: Move SQL statements to a separate file
    def __init__(self, db_path: Path | str, overwrite_db: bool = False) -> None:
        logger.info(f'Initializing database connection, db_path={db_path}')
        
        init_db = False
        if db_path == ':memory:' or (not Path(db_path).exists()):
            init_db = True

        if overwrite_db:
            logger.warning(f'Removing existing database at "{db_path}"')
            Path(db_path).unlink()
            init_db = True
            
        self._conn = sqlite3.connect(db_path, isolation_level=None)
        self._conn.row_factory = dict_factory
        if init_db:
            self.initialize()

    @contextmanager
    def query(self) -> Generator[sqlite3.Cursor, None, None]:
        cursor = self._conn.cursor()
        logger.debug("Cursor created.")
        cursor.execute("BEGIN;")
        logger.debug("Transaction begin.")
        try:
            yield cursor
        except sqlite3.Error as e:
        # except Exception as e:
            logger.error(f"Transaction failed: {e}", exc_info=True)
            self.rollback(cursor)
            raise
        else:
            logger.debug(f"Transaction committed successfully.")
            cursor.execute("COMMIT;")
        finally:
            if self._conn.in_transaction:
                logger.warning("Open transaction detcted while cleaing up, rolling back...")
                self.rollback(cursor)
            logger.debug("Closing cursor.")
            cursor.close()

    def rollback(self, cursor: sqlite3.Cursor):
        try:
            logger.debug(f"Trying to rowback transaction...")
            cursor.execute("ROLLBACK;")
        except sqlite3.OperationalError as e:
            logger.warning(f"Rowback failed: {e}. The transaction may has already been rolled back automatically by the error response.", exc_info=True)
        else:
            logger.debug(f"Transaction rolled back successfully.")

    def initialize(self) -> None:
        curs = self._conn.cursor()
        # cursor.executescript implicitly commit any pending transactions, cannot execute "BEGIN TRANSACTION" here.
        curs.executescript("""--sql
            -- PRAGMA foreign_keys is a no-op within a transaction; foreign key constraint enforcement may only be enabled or disabled when there is no pending BEGIN or SAVEPOINT.
            PRAGMA foreign_keys = on;
            BEGIN;
            CREATE TABLE duplicates (
                id INTEGER PRIMARY KEY,
                type TEXT NOT NULL
            );

            CREATE TABLE files (
                id INTEGER PRIMARY KEY,
                path TEXT NOT NULL UNIQUE,
                size INTEGER NOT NULL CHECK( size >= 0 ),
                parent_dir INTEGER NOT NULL,
                partial_hash TEXT,
                complete_hash TEXT,
                duplicate_id INTEGER,
                FOREIGN KEY(duplicate_id) REFERENCES duplicates(id),
                FOREIGN KEY(parent_dir) REFERENCES dirs(id)
            );

            CREATE TABLE photos (
                id INTEGER PRIMARY KEY,
                file_id INTEGER,
                image_hash TEXT NOT NULL,
                data_json TEXT,
                FOREIGN KEY(file_id) REFERENCES files(id)
            );

            CREATE TABLE dirs (
                id INTEGER PRIMARY KEY,
                path TEXT NOT NULL UNIQUE,
                parent_dir INTEGER,
                depth INTEGER CHECK( depth >= 0 ),
                hash TEXT,
                duplicate_id INTEGER,
                FOREIGN KEY(parent_dir) REFERENCES dirs(id)
                FOREIGN KEY(duplicate_id) REFERENCES duplicates(id)
            );

            CREATE INDEX idx_files_dir_id ON files (parent_dir);
            CREATE INDEX idx_files_hash ON files (partial_hash);
            CREATE INDEX idx_files_duplicate_id ON files (duplicate_id);
            CREATE INDEX idx_files_complete_hash ON files (complete_hash);
            CREATE INDEX idx_photos_file_id ON photos (file_id);
            CREATE INDEX idx_photos_image_hash ON photos (image_hash);
            CREATE INDEX idx_dirs_hash ON dirs (hash);
            CREATE INDEX idx_dirs_duplicate_id ON dirs (duplicate_id);

            COMMIT;
        """)
        curs.close()
        logger.info("Database tables initialized")

    def close(self) -> None:
        self._conn.close()

    def dumpTables(self, tables: list[str]) -> None:
        query = """--sql
            SELECT name FROM sqlite_schema
            WHERE type='table'
            ORDER BY name;
        """
        curs = self._conn.cursor()
        curs.execute(query)
        self.tables = [row['name'] for row in curs.fetchall()]
        
        if 'all' in tables:
            for table in self.tables:
                self._dumpTable(table)
        else:
            for table in tables:
                if table not in self.tables:
                    raise ValueError(f"Invalid table name: {table}")
                self._dumpTable(table)

        curs.close()

    def _dumpTable(self, table: str) -> None:
        self._conn.row_factory = sqlite3.Row
        print("\n----- " + f'Dumping table "{table}"' " -----\n" )
        print(pd.read_sql_query(f"SELECT * FROM {table};", self._conn, index_col="id"))
        print("\n----- " + f'End of table "{table}"' " -----\n" )
        self._conn.row_factory = dict_factory
