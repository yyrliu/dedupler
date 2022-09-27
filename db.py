import sqlite3
from typing import Iterable
import pandas as pd
from contextlib import contextmanager

class PartialHashCollisionException(Exception):
    def __init__(self, message, id, path, dir_id, has_hash_complete):
        # Call the base class constructor with the parameters it needs
        super().__init__(message)
        self.id = id
        self.path = path
        self.dir_id = dir_id
        self.has_hash_complete = has_hash_complete

class Database():

    def __init__(self, db_path) -> None:
        self.conn = sqlite3.connect(db_path, isolation_level=None)
        self.curs = self.conn.cursor()

    def _sqlStartTransaction(self) -> None:
        self.curs.execute("BEGIN TRANSACTION;")

    def _sqlCommitTransaction(self) -> None:
        self.curs.execute("COMMIT TRANSACTION;")

    def _sqlRollbackTransaction(self) -> None:
        self.curs.execute("ROLLBACK TRANSACTION;")

    def _sqlExecute(self, sql: str, *args) -> list[tuple] | None:
        self.curs.execute(sql, *args)
        res = self.curs.fetchall()
        return res or None

    def _sqlExecuteMany(self, sql: str, *args) -> list[tuple] | None:
        self.curs.executemany(sql, *args)
        res = self.curs.fetchall()
        return res or None

    def _sqlExecuteScript(self, script: str) -> None:
        self.curs.executescript(script)

    def _sqlGetFirst(self, sql: str, *args) -> tuple | None:
        res = self._sqlExecute(sql, *args)
        return res[0] if res else None

    def _sqlInsertDir(self, path: str, parent_id: int, dup_id: int) -> None:
        self._sqlExecute("""--sql
                INSERT INTO dirs (path, parent_id, duplicate_id) VALUES (?, ?, ?)
            """, (path, parent_id, dup_id))

    def _sqlInsertFile(self, path: str, size: int, dir_id: int, hash: str, hash_complete: str | None = None, dup_id: int | None = None) -> None:
        self._sqlExecute("""--sql
                INSERT INTO files (path, size, dir_id, hash, hash_complete, duplicate_id) VALUES (?, ?, ?, ?, ?, ?)
            """, (path, size, dir_id, hash, hash_complete, dup_id))

    def _sqlUpdateDir(self, id, hash, dup_id=None) -> None:
        self._sqlExecute("""--sql
                UPDATE dirs
                SET hash = ?, duplicate_id = ?
                WHERE id = ?
            """, (hash, dup_id, id))

    def _sqlDirRemoveDupID(self, ids: Iterable[int]) -> None:
        ids = [(id, ) for id in ids]
        self._sqlExecuteMany("""--sql
                UPDATE dirs
                SET duplicate_id = NULL
                WHERE id = ?
            """, ids)

    def _lastRowID(self) -> int | None:
        return self.curs.lastrowid

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

    def _sqlGetDirsFromDupID(self, dup_id: Iterable[int]) -> list[int]:
        res = self._sqlExecute("""--sql
                SELECT id FROM dirs WHERE duplicate_id = ?
            """, (dup_id, ))
        return [id for (id, *_) in res]

    @contextmanager
    def startTransaction(self) -> None:
        self._sqlStartTransaction()
        try:
            yield
        except sqlite3.Error as e:
            try:
                self._sqlRollbackTransaction()
            except sqlite3.OperationalError:
                pass
            raise e
        else:
            self._sqlCommitTransaction()

    def initialize(self, root_path: str = "/") -> None:
        # cursor.executescript implicitly commit any pending transactions, cannot apply context manager startTransaction() here.
        self._dropAll()
        self._sqlExecuteScript("""--sql
            PRAGMA foreign_keys = ON;

            CREATE TABLE duplicates (
                id INTEGER PRIMARY KEY,
                type TEXT NOT NULL
            );

            CREATE TABLE files (
                id INTEGER PRIMARY KEY,
                path TEXT NOT NULL UNIQUE,
                size INTEGER NOT NULL,
                dir_id INTEGER NOT NULL,
                hash TEXT NOT NULL,
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
        """)

        self.rootDirID = self.insertDir(root_path, None)
        
    def commit(self) -> None:
        self._sqlCommitTransaction()

    def dumpTable(self, table: str) -> None:
        '''Not SQL injection safe!'''

        print("\n----- " + f'Dumping table "{table}"' " -----\n" )
        print(pd.read_sql_query(f"SELECT * FROM {table}", self.conn))
        print("\n----- " + f'End of table "{table}"' " -----\n" )

    def insertDir(self, path: str, parent_id: int, dup_id: int | None = None) -> int | None:
        self._sqlInsertDir(path, parent_id, dup_id)
        return self._lastRowID()

    def insertFile(self, path: str, size: int, dir_id: int, hash: str, hash_complete: str | None = None) -> None:
        # If file size < 1024, hash_complete will be set to the same value as hash
        if size < 1024:
            hash_complete = hash

        # For file bigger than 1024, first scan (partial hash)
        if not hash_complete:
            res = self._sqlGetFirst("""--sql
                SELECT id, path, dir_id, hash_complete
                FROM files
                WHERE hash = ? AND size = ?
                LIMIT 1
            """, (hash, size))

            # If there is a match, throw exception to request a full hash
            if res:
                res_id, res_path, res_dir_id, res_has_hash_complete = res
                exception_str = (
                    f'Partial hash collision detected! Between files:\n'
                    f'"{res_path}",\n'
                    f'"{path}"\n'
                )
                raise PartialHashCollisionException(exception_str, res_id, res_path, res_dir_id, bool(res_has_hash_complete))

            # Insert file if no match is found
            else:
                self._sqlInsertFile(path, size, dir_id, hash)
                return

        # For file smaller than 1024, first scan (partial hash) or file bigger than 1024, second scan (full hash)
        res = self._sqlGetFirst("""--sql
                SELECT id, duplicate_id
                FROM files
                WHERE hash_complete = ? AND size = ?
                LIMIT 1
            """, (hash_complete, size))

        file_id, dup_id = res or (None, None)

        # Freshly detected duplicate, insert new row in "duplicates"
        # TODO: Add hash_complete column to duplicates
        with self.startTransaction():
            if file_id and (not dup_id):
                dup_id = self._sqlInsertDuplicate("file")

                self._sqlExecute("""--sql
                    UPDATE files
                    SET duplicate_id = ?
                    WHERE id = ?
                """, (dup_id, file_id))

            self._sqlInsertFile(path, size, dir_id, hash, hash_complete, dup_id)

    def updateDirHash(self, id: int, hash: str) -> None:
        res = self._sqlGetFirst("""--sql
                SELECT hash, duplicate_id
                FROM dirs
                WHERE id = ?
            """, (id, ))

        old_hash, old_dup_id = res

        # If hash has not changed, do nothing
        if old_hash == hash:
            return

        with self.startTransaction():
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
            res = self._sqlGetFirst("""--sql
                    SELECT id, duplicate_id
                    FROM dirs
                    WHERE hash = ?
                    LIMIT 1
                """, (hash, ))

            dir_id, dup_id = res or (None, None)

            # Freshly detected duplicate folder
            if dir_id and (not dup_id):
                dup_id = self._sqlInsertDuplicate("dir")

                self._sqlExecute("""--sql
                    UPDATE dirs
                    SET duplicate_id = ?
                    WHERE id = ?
                """, (dup_id, dir_id))

            self._sqlUpdateDir(id, hash, dup_id)

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

    def close(self) -> None:
        self.curs.close()
        self.conn.close()
