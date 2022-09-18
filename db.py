import sqlite3
import pandas as pd

class PartialHashCollisionException(Exception):
    def __init__(self, message, id, path, has_hash_complete):            
        # Call the base class constructor with the parameters it needs
        super().__init__(message)
        self.id = id
        self.path = path
        self.has_hash_complete = has_hash_complete

class Database():

    def __init__(self, db_path):
        self.conn = sqlite3.connect(db_path)
        self.curs = self.conn.cursor()

    def _sqlExecute(self, *args):
        self.curs.execute(*args)
        res = self.curs.fetchall()
        return res or None

    def _sqlExecuteMany(self, *args):
        self.curs.executemany(*args)
        res = self.curs.fetchall()
        return res or None

    def _sqlExecuteScript(self, script):
        self.curs.executescript(script)

    def _sqlGetFirst(self, *args):
        res = self._sqlExecute(*args)
        return res[0] if res else None

    def _sqlInsertFile(self, path, size, hash, hash_complete=None, dup_id=None):
        self._sqlExecute("""--sql
                INSERT INTO files (path, size, hash, hash_complete, duplicate_id) VALUES (?, ?, ?, ?, ?)
            """, (path, size, hash, hash_complete, dup_id))

    def _lastRowID(self):
        return self.curs.lastrowid

    def _dropAll(self):
        self._sqlExecuteScript("""--sql
            DROP TABLE IF EXISTS files;
            DROP TABLE IF EXISTS dirs;
            DROP TABLE IF EXISTS duplicates;
        """)

    def _sqlInsertDuplicate(self, type):
        self._sqlExecute("""--sql
                INSERT INTO duplicates (type) VALUES (?)
            """, (type, ))
        return self._lastRowID()

    def initialize(self):
        self._dropAll()
        self._sqlExecuteScript("""--sql
            PRAGMA foreign_keys = ON;

            CREATE TABLE duplicates (
                id INTEGER PRIMARY KEY,
                type TEXT NOT NULL
            );

            CREATE TABLE files (
                id INTEGER PRIMARY KEY,
                path TEXT NOT NULL,
                size INTEGER NOT NULL,
                hash TEXT NOT NULL,
                hash_complete TEXT,
                duplicate_id INTEGER,
                FOREIGN KEY(duplicate_id) REFERENCES duplicates(id)
            );

            CREATE TABLE dirs (
                id INTEGER PRIMARY KEY,
                path TEXT NOT NULL,
                hash TEXT NOT NULL,
                duplicate_id INTEGER,
                FOREIGN KEY(duplicate_id) REFERENCES duplicates(id)
            );

            CREATE INDEX idx_files_hash ON files (hash);
            CREATE INDEX idx_files_duplicate_id ON files (duplicate_id);
            CREATE INDEX idx_dirs_hash ON dirs (hash);
            CREATE INDEX idx_dirs_duplicate_id ON dirs (duplicate_id);
        """)
        
    def commit(self):
        self.conn.commit()

    def dumpTable(self, table):
        '''Not SQL injection safe!'''

        print("\n----- " + f'Dumping table "{table}"' " -----\n" )
        print(pd.read_sql_query(f"SELECT * FROM {table}", self.conn))
        print("\n----- " + f'End of table "{table}"' " -----\n" )

    def insertFile(self, path, size, hash, hash_complete=None):
        # If file size < 1024, hash_complete will be set to the same value as hash
        if size < 1024:
            hash_complete = hash

        # For file bigger than 1024, first scan (partial hash)
        if not hash_complete:
            res = self._sqlGetFirst("""--sql
                SELECT id, path, hash_complete
                FROM files AS f
                WHERE f.hash = ? AND f.size = ?
                LIMIT 1
            """, (hash, size))

            # If there is a match, throw exception to request a full hash
            if res:
                res_id, res_path, res_has_hash_complete = res
                raise PartialHashCollisionException(f'Partial hash collision detected! Please do full file hash on "{res_path}".', res_id, res_path, bool(res_has_hash_complete))

            # Insert file if no match is found
            else:
                return self._sqlInsertFile(path, size, hash)

        # For file smaller than 1024, first scan (partial hash) or file bigger than 1024, second scan (full hash)
        res = self._sqlGetFirst("""--sql
                SELECT id, duplicate_id
                FROM files AS f
                WHERE f.hash_complete = ? AND f.size = ?
                LIMIT 1
            """, (hash_complete, size))

        file_id, dup_id = res or (None, None)

        # Freshly detected duplicate, insert new row in "duplicates"
        # TODO: Add hash_complete column to duplicates
        if file_id and (not dup_id) :
            dup_id = self._sqlInsertDuplicate("file")

            self._sqlExecute("""--sql
                UPDATE files
                SET duplicate_id = ?
                WHERE id = ?
            """, (dup_id, file_id))

        self._sqlInsertFile(path, size, hash, hash_complete, dup_id)

    def updateFileCompleteHash(self, id, hash_complete):
        self._sqlExecute("""--sql
                UPDATE files SET hash_complete=? WHERE id=?
            """, (hash_complete, id))

def main():
    db = Database(':memory:')

    db.initialize()

    db._sqlExecuteMany("""--sql
        INSERT INTO files (path, size , hash) VALUES (?, ?, ?)
    """, (
        ("path/a/b", 100, "hash1"),
        ("path/a/b", 100, "hash2"),
        ("path/a/c", 200, "hash3")
    ))

    db._sqlExecuteMany("""--sql
        INSERT INTO dirs (path, hash) VALUES (?, ?)
    """, (
        ("path/a/c", "hash1"),
        ("path/a/b", "hash1")
    ))

    db.commit()

    db.dumpTable("files")
    db.dumpTable("dirs")
    db.dumpTable("duplicates")

    print("--- Mock data created ---")

    db.insertFile("path/a/d", 100, "hash4")
    db.insertFile("path/a/d", 100, "hash2")
    db.insertFile("path/a/d", 200, "hash2")
    db.insertFile("path/a/d", 200, "hash5")
    db.insertFile("path/a/e", 100, "hash2")
    db.insertFile("path/a/e", 100, "hash4")

    db.commit()
    db.dumpTable("files")
    db.dumpTable("duplicates")

if __name__ == "__main__":
    main()
