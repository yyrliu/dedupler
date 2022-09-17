import sqlite3
import pandas as pd

class PartialHashCollisionException(Exception):
    def __init__(self, message, id, path, has_md5_complete):            
        # Call the base class constructor with the parameters it needs
        super().__init__(message)
        self.id = id
        self.path = path
        self.has_md5_complete = has_md5_complete

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

    def _sqlInsertFile(self, path, size, md5, md5_complete=None, dup_id=None):
        self._sqlExecute("""--sql
                INSERT INTO files (path, size, md5, md5_complete, duplicate_id) VALUES (?, ?, ?, ?, ?)
            """, (path, size, md5, md5_complete, dup_id))

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
                md5 TEXT NOT NULL,
                md5_complete TEXT,
                duplicate_id INTEGER,
                FOREIGN KEY(duplicate_id) REFERENCES duplicates(id)
            );

            CREATE TABLE dirs (
                id INTEGER PRIMARY KEY,
                path TEXT NOT NULL,
                md5 TEXT NOT NULL,
                duplicate_id INTEGER,
                FOREIGN KEY(duplicate_id) REFERENCES duplicates(id)
            );

            CREATE INDEX idx_files_md5 ON files (md5);
            CREATE INDEX idx_files_duplicate_id ON files (duplicate_id);
            CREATE INDEX idx_dirs_md5 ON dirs (md5);
            CREATE INDEX idx_dirs_duplicate_id ON dirs (duplicate_id);
        """)
        
    def commit(self):
        self.conn.commit()

    def dumpTable(self, table):
        '''Not SQL injection safe!'''

        print("\n----- " + f'Dumping table "{table}"' " -----\n" )
        print(pd.read_sql_query(f"SELECT * FROM {table}", self.conn))
        print("\n----- " + f'End of table "{table}"' " -----\n" )

    def insertFile(self, path, size, md5, md5_complete=None):
        # If file size < 1024, md5_complete will be set to the same value as md5
        if size < 1024:
            md5_complete = md5

        # For file bigger than 1024, first scan (partial hash)
        if not md5_complete:
            res = self._sqlGetFirst("""--sql
                SELECT id, path, md5_complete
                FROM files AS f
                WHERE f.md5 = ? AND f.size = ?
                LIMIT 1
            """, (md5, size))

            # If there is a match, throw exception to request a full hash
            if res:
                res_id, res_path, res_has_md5_complete = res
                raise PartialHashCollisionException(f'Partial hash collision detected! Please do full file hash on "{res_path}".', res_id, res_path, bool(res_has_md5_complete))

            # Insert file if no match is found
            else:
                return self._sqlInsertFile(path, size, md5)

        # For file smaller than 1024, first scan (partial hash) or file bigger than 1024, second scan (full hash)
        res = self._sqlGetFirst("""--sql
                SELECT id, duplicate_id
                FROM files AS f
                WHERE f.md5_complete = ? AND f.size = ?
                LIMIT 1
            """, (md5_complete, size))

        file_id, dup_id = res or (None, None)

        # Freshly detected duplicate, insert new row in "duplicates"
        # TODO: Add md5_complete column to duplicates
        if file_id and (not dup_id) :
            dup_id = self._sqlInsertDuplicate("file")

            self._sqlExecute("""--sql
                UPDATE files
                SET duplicate_id = ?
                WHERE id = ?
            """, (dup_id, file_id))

        self._sqlInsertFile(path, size, md5, md5_complete, dup_id)

    def updateFileCompleteHash(self, id, md5_complete):
        self._sqlExecute("""--sql
                UPDATE files SET md5_complete=? WHERE id=?
            """, (md5_complete, id))

def main():
    db = Database(':memory:')

    db.initialize()

    db._sqlExecuteMany("""--sql
        INSERT INTO files (path, size , md5) VALUES (?, ?, ?)
    """, (
        ("path/a/b", 100, "md51"),
        ("path/a/b", 100, "md52"),
        ("path/a/c", 200, "md53")
    ))

    db._sqlExecuteMany("""--sql
        INSERT INTO dirs (path, md5) VALUES (?, ?)
    """, (
        ("path/a/c", "md51"),
        ("path/a/b", "md51")
    ))

    db.commit()

    db.dumpTable("files")
    db.dumpTable("dirs")
    db.dumpTable("duplicates")

    print("--- Mock data created ---")

    db.insertFile("path/a/d", 100, "md54")
    db.insertFile("path/a/d", 100, "md52")
    db.insertFile("path/a/d", 200, "md52")
    db.insertFile("path/a/d", 200, "md55")
    db.insertFile("path/a/e", 100, "md52")
    db.insertFile("path/a/e", 100, "md54")

    db.commit()
    db.dumpTable("files")
    db.dumpTable("duplicates")

if __name__ == "__main__":
    main()
