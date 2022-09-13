import sqlite3
import pandas as pd

class Database():

    def __init__(self, db_path):
        self.conn = sqlite3.connect(db_path)
        self.curs = self.conn.cursor()

    def _execute(self, *args):
        self.curs.execute(*args)
        res = self.curs.fetchall()
        return res if res else None

    def _executeMany(self, *args):
        self.curs.executemany(*args)
        res = self.curs.fetchall()
        return res if res else None

    def _executeScript(self, script):
        self.curs.executescript(script)

    def _lastRowID(self):
        return self.curs.lastrowid

    def _dropAll(self):
        self._executeScript("""--sql
            DROP TABLE IF EXISTS files;
            DROP TABLE IF EXISTS dirs;
            DROP TABLE IF EXISTS duplicates;
        """)

    def initialize(self):
        self._dropAll()
        self._executeScript("""--sql
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
                md5_complete TEXT UNIQUE,
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

    def insertFile(self, path, size, md5):
        res = self._execute("""--sql
                SELECT id, duplicate_id
                FROM files AS f
                WHERE f.md5 = ? AND f.size = ?
                LIMIT 1
            """, (md5, size))

        (file_id, dup_id), = res if res else ((None, None), )

        # Freshly detected duplicate, insert new row in "duplicates"
        if file_id and (not dup_id) :
            self._execute("""--sql
                    INSERT INTO duplicates (type) VALUES ("file")
                """)

            dup_id = self._lastRowID()

            self._execute("""--sql
                UPDATE files
                SET duplicate_id = ?
                WHERE id = ?
            """, (dup_id, file_id))

        self._execute("""--sql
                INSERT INTO files (path, size , md5, duplicate_id) VALUES (?, ?, ?, ?)
            """, (path, size, md5, dup_id))


def main():
    db = Database(':memory:')

    db.initialize()

    db._executeMany("""--sql
        INSERT INTO files (path, size , md5) VALUES (?, ?, ?)
    """, (
        ("path/a/b", 100, "md51"),
        ("path/a/b", 100, "md52"),
        ("path/a/c", 200, "md53")
    ))

    db._executeMany("""--sql
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
