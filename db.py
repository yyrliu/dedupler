import sqlite3
import logging
from pathlib import Path
from collections.abc import Iterable, Generator
import pandas as pd
from contextlib import contextmanager

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

class PartialHashCollision(Exception):
    def __init__(self, current_path: str, id: str, path: str, dir_id: str, has_hash_complete: bool):
        exception_msg = (
            f'Partial hash collision detected! Between files:\n'
            f'"{current_path}",\n'
            f'"{path}"\n'
        )
        super().__init__(exception_msg)
        self.id = id
        self.path = path
        self.dir_id = dir_id
        self.has_hash_complete = has_hash_complete

class NoRootDirException(Exception):
    def __init__(self):
        super().__init__("Root directory is not set. Please call setRootDir() to set it before inserting dirs or files.")

class Database():
    # TODO: Move SQL statements to a separate file
    def __init__(self, db_path: Path | str) -> None:
        logger.info(f"Initializing database connection, db_path={db_path}")
        self._conn = sqlite3.connect(db_path, isolation_level=None)
        self._curs = self._conn.cursor()
        self._rootDirID = None

    @property
    def rootDirID(self) -> int:
        return self._rootDirID

    def _sqlStartTransaction(self) -> None:
        self._curs.execute("BEGIN;")

    def _sqlCommitTransaction(self) -> None:
        self._curs.execute("COMMIT;")

    def _sqlRollbackTransaction(self) -> None:
        try:
            logger.info(f"Trying to rowback transaction...")
            self._curs.execute("ROLLBACK;")
            logger.info(f"Transaction rolled back.")
        except sqlite3.OperationalError as e:
            logger.info(f"Rowback failed: {e}. The transaction may has already been rolled back automatically by the error response.", exc_info=True)
        
    def _sqlExecute(self, sql: str, *args) -> list[tuple] | None:
        self._curs.execute(sql, *args)
        res = self._curs.fetchall()
        return res or None

    def _sqlExecuteMany(self, sql: str, *args) -> list[tuple] | None:
        self._curs.executemany(sql, *args)
        res = self._curs.fetchall()
        return res or None

    def _sqlExecuteScript(self, script: str) -> None:
        self._curs.executescript(script)

    def _sqlGetOne(self, sql: str, *args) -> tuple | None:
        res = self._sqlExecute(sql, *args)
        if res and len(res) > 1:
            raise Exception("More than one result returned.")
        return res[0] if res else None

    def _sqlGetFirst(self, sql: str, *args) -> tuple | None:
        res = self._sqlExecute(sql, *args)
        return res[0] if res else None

    def _sqlInsertDir(self, path: str, parent_id: int, dup_id: int) -> None:
        self._sqlExecute("""--sql
                INSERT INTO dirs (path, parent_id, duplicate_id) VALUES (?, ?, ?)
            """, (path, parent_id, dup_id))

    def _sqlInsertFile(self, path: str, size: int, dir_id: int) -> None:
        self._sqlExecute("""--sql
                INSERT INTO files (path, size, dir_id) VALUES (?, ?, ?)
            """, (path, size, dir_id))

    def _sqlUpdateDir(self, id: int, hash: str | None = None, dup_id: int | None = None) -> None:
        self._sqlExecute("""--sql
            UPDATE dirs
            SET hash = COALESCE(?, hash),
                duplicate_id = COALESCE(?, duplicate_id)
            WHERE id = ?
        """, (hash, dup_id, id))
            
    def _sqlDirRemoveDupID(self, ids: Iterable[int]) -> None:
        ids = [(id, ) for id in ids]
        self._sqlExecuteMany("""--sql
                UPDATE dirs
                SET duplicate_id = NULL
                WHERE id = ?
            """, ids)
        
    def _seqGetDulpFile(
            self,
            size: int,
            hash: str | None = None,
            hash_complete: str | None = None
        ) -> tuple[int, str | None, int, str | None] | None:
        res = self._sqlGetFirst("""--sql
                SELECT id, path, dir_id, hash_complete, duplicate_id
                FROM files
                WHERE size = ? AND (hash = ? OR hash_complete = ?)
                LIMIT 1
            """, (size, hash, hash_complete))
        return res

    def _lastRowID(self) -> int | None:
        return self._curs.lastrowid

    def _dropAll(self) -> None:
        self._sqlExecuteScript("""--sql
            DROP TABLE IF EXISTS files;
            DROP TABLE IF EXISTS dirs;
            DROP TABLE IF EXISTS duplicates;
        """)

    def _sqlInsertDuplicate(self, type) -> int | None:
        self._sqlExecute("""--sql
                INSERT INTO duplicates (type) VALUES (?)
            """, (type, ))
        return self._lastRowID()

    def _sqlRemoveDuplicate(self, id) -> None:
        self._sqlExecute("""--sql
                DELETE FROM duplicates WHERE id = ?;
            """, (id, ))

    def _sqlGetDirsFromDupID(self, dup_id: Iterable[int]) -> tuple[int]:
        res = self._sqlExecute("""--sql
                SELECT id FROM dirs WHERE duplicate_id = ?
            """, (dup_id, ))
        return [id for (id, *_) in res]
    
    def _sqlFindDupDirFromHash(self, hash: str) -> tuple[int, int] | None:
        res = self._sqlGetFirst("""--sql
                SELECT id, duplicate_id FROM dirs WHERE hash = ?
            """, (hash, ))
        return res
    
    def _sqlUpdateFile(
            self,
            id: int, hash: str | None = None,
            hash_complete: str | None = None,
            dup_id: int | None = None
        ) -> None:
        self._sqlExecute("""--sql
            UPDATE files
            SET hash = COALESCE(?, hash),
                hash_complete = COALESCE(?, hash_complete),
                duplicate_id = COALESCE(?, duplicate_id)
            WHERE id = ?
        """, (hash, hash_complete, dup_id, id))

    @contextmanager
    def _sqlTransaction(self) -> None:
        self._sqlStartTransaction()
        try:
            yield
        except sqlite3.Error as e:
            logger.error(f"Transaction failed: {e}", exc_info=True)
            self._sqlRollbackTransaction()
            raise
        else:
            self._sqlCommitTransaction()

    def initialize(self) -> None:
        # cursor.executescript implicitly commit any pending transactions, cannot apply context manager startTransaction() here.
        self._dropAll()
        self._sqlExecuteScript("""--sql
            
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
                dir_id INTEGER NOT NULL,
                hash TEXT,
                hash_complete TEXT,
                duplicate_id INTEGER,
                FOREIGN KEY(duplicate_id) REFERENCES duplicates(id),
                FOREIGN KEY(dir_id) REFERENCES dirs(id)
            );

            CREATE TABLE dirs (
                id INTEGER PRIMARY KEY,
                path TEXT NOT NULL UNIQUE,
                parent_id INTEGER,
                hash TEXT,
                duplicate_id INTEGER,
                FOREIGN KEY(parent_id) REFERENCES dirs(id)
                FOREIGN KEY(duplicate_id) REFERENCES duplicates(id)
            );

            CREATE INDEX idx_files_dir_id ON files (dir_id);
            CREATE INDEX idx_files_hash ON files (hash);
            CREATE INDEX idx_files_duplicate_id ON files (duplicate_id);
            CREATE INDEX idx_files_hash_complete ON files (hash_complete);
            CREATE INDEX idx_dirs_hash ON dirs (hash);
            CREATE INDEX idx_dirs_duplicate_id ON dirs (duplicate_id);

            COMMIT;
        """)
        
    def commit(self) -> None:
        self._sqlCommitTransaction()

    def dumpTable(self, table: str) -> None:
        '''Not SQL injection safe!'''

        print("\n----- " + f'Dumping table "{table}"' " -----\n" )
        print(pd.read_sql_query(f"SELECT * FROM {table};", self._conn, index_col="id"))
        print("\n----- " + f'End of table "{table}"' " -----\n" )

    def setRootDir(self, root_path: str) -> None:
        self._sqlInsertDir(root_path, None, None)
        self._rootDirID = self._lastRowID()
        logger.info(f"Root dir set to {root_path}")

    def insertDir(self, path: str, parent_id: int, dup_id: int | None = None) -> int | None:
        if not isinstance(parent_id, int):
            raise NoRootDirException()
        
        self._sqlInsertDir(path, parent_id, dup_id)
        return self._lastRowID()

    def insertFile(self, path: str, size: int, dir_id: int) -> None:
        if not isinstance(dir_id, int):
            raise NoRootDirException()
        
        self._sqlInsertFile(path, size, dir_id)
        return self._lastRowID()

    def updateDirHash(self, id: int, hash: str) -> None:

        old_hash, old_dup_id = self._sqlGetOne("""--sql
                SELECT hash, duplicate_id FROM dirs WHERE id = ?
            """, (id, ))

        # If hash has not changed, do nothing
        if old_hash == hash:
            return

        with self._sqlTransaction():
            # If duplicate record exists for old hash, remove it
            if old_dup_id:
                res = self._sqlGetDirsFromDupID(old_dup_id)
                # If there's only 2 dirs with same old_dup_id, remove the entry in duplicates table
                if len(res) == 2:
                    self._sqlDirRemoveDupID(res)
                    self._sqlRemoveDuplicate(old_dup_id)
                
                else:
                    self._sqlDirRemoveDupID((id, ))

            # Check if there is a duplicate folder
            dir_id, dup_id = self._sqlFindDupDirFromHash(hash) or (None, None)

            # Freshly detected duplicate folder
            if dir_id and (not dup_id):
                dup_id = self._sqlInsertDuplicate("dir")
                self._sqlUpdateDir(dir_id, None, dup_id)

            self._sqlUpdateDir(id, hash, dup_id)

    def updateFileHash(self, id: int, hash: str, hash_complete: str | None = None) -> None:
        # Get file path and size 
        path, size = self._sqlGetOne("""--sql
            SELECT path, size FROM files WHERE id = ?
        """, (id, ))

        # If file size < 1024, hash_complete will be set to the same value as hash
        if size < 1024:
            hash_complete = hash

        # For file bigger than 1024, first scan (partial hash)
        if not hash_complete:
            res = self._seqGetDulpFile(size, hash=hash)

            # If there is a match, throw exception to request a full hash
            if res:
                res_id, res_path, res_dir_id, res_has_hash_complete, *_ = res
                raise PartialHashCollision(path, res_id, res_path, res_dir_id, bool(res_has_hash_complete))

            # Simple file hash update if no match is found
            else:
                self._sqlUpdateFile(id, hash=hash)
                return

        # For file smaller than 1024, first scan (partial hash) or file bigger than 1024, second scan (full hash)
        res = self._seqGetDulpFile(size, hash_complete=hash_complete)
        file_id, *_ ,dup_id = res or (None, None)

        # Freshly detected duplicate, insert new row in "duplicates"
        # TODO: Add hash_complete column to duplicates
        with self._sqlTransaction():
            if file_id and (not dup_id):
                dup_id = self._sqlInsertDuplicate("file")            
                self._sqlUpdateFile(file_id, dup_id=dup_id)
    
            self._sqlUpdateFile(id, hash=hash, hash_complete=hash_complete, dup_id=dup_id)

    def updateFileCompleteHash(self, id: int, hash_complete: str) -> None:
        self._sqlExecute("""--sql
                UPDATE files SET hash_complete = ? WHERE id = ?
            """, (hash_complete, id))

    def getDirHash(self, id: int) -> str:
        res = self._sqlGetFirst("""--sql
                SELECT hash FROM dirs WHERE id = ?
            """, (id, ))

        hash, *_ = res
        return hash

    def getDirParentID(self, id: int) -> int:
        res = self._sqlGetFirst("""--sql
                SELECT parent_id FROM dirs WHERE id = ?
            """, (id, ))

        id, *_ = res
        return id

    def getChildrenHashes(self, id: int) -> list[str]:
        res = self._sqlExecute("""--sql
                SELECT id, hash, hash_complete FROM files WHERE dir_id = ?
                UNION ALL
                SELECT id, hash, NULL FROM dirs WHERE parent_id = ?
                ORDER BY id
            """, (id, id))

        hashes = []
        for entry in res:
            _, hash, hash_complete = entry
            hashes.append(hash_complete or hash or '')

        return hashes
    
    # Generator that returns id of all directors in the database starting from the ones with no children
    def getDirs(self) -> Generator[tuple, None, None]:
    # def getDirs(self) -> Generator[int]:
        res = self._sqlExecute("""--sql
            WITH RECURSIVE cte (id, parent_id, depth, path) AS (
                SELECT id, parent_id, 1, path
                FROM dirs WHERE parent_id IS NULL
                UNION ALL
                SELECT dirs.id, dirs.parent_id, cte.depth + 1 AS depth, dirs.path 
                FROM dirs JOIN cte ON dirs.parent_id = cte.id
                ORDER BY depth DESC
            )
            SELECT * FROM cte ORDER BY depth DESC;
        """)

        for entry in res:
            yield entry
    
    # Create a generator that returns all the files in the database
    def getFilesInDir(self, id: int) -> Generator[tuple, None, None]:
        res = self._sqlExecute("""--sql
                SELECT id, path, size, dir_id FROM files WHERE dir_id = ?
            """, (id, ))
        
        if not res:
            return
        
        for entry in res:
            yield entry
    
    def close(self) -> None:
        self._curs.close()
        self._conn.close()
