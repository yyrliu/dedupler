from dataclasses import asdict, dataclass, fields, field
from typing import Self
from collections.abc import Iterable, Generator, Callable
from contextlib import contextmanager
from sqlite3 import Cursor
import logging
import json


from db import Database

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

class SingleSet(set):
    def add(self, x):
        if x in self:
            raise KeyError(f'Value already exists')
        super().add(x)

    def update(self, x):
        return self.__ior__(x)

    def __ior__(self, x):
        if any(xx in self for xx in x):
            raise KeyError(f'Value already exists')
        return super().__ior__(x)

@dataclass(kw_only=True)
class Base:
    id: int = None

    def __post_init__(self):
        self._updated = SingleSet()
        self._deleted = False

    def __getattribute__(self, attr):
        if not attr.startswith("__") and object.__getattribute__(self, "_deleted"):
            raise AttributeError(f"Cannot access deleted object")
        return object.__getattribute__(self, attr)

    @classmethod
    def fieldNames(cls):
        return tuple(field.name for field in fields(cls))
    
    @classmethod
    def insert(cls, payload: dict, db: Database, returnAsInstance: bool = True):
        """Inserts a new row into the database and returns the new instance"""
        if not isinstance(db, Database):
            raise ValueError("Database connection must be provided to insert new row")
        # TODO: Make instance creation more elegant without creating new instance twice
        instance = cls(**payload)

        with db.query() as curs:
            curs.execute(*instance._sqlInsertQuery())
            if returnAsInstance:
                dbInstance = cls(**curs.fetchone())
                logger.debug(f"Inserted new row into {cls.__name__.lower()}s: {dbInstance.__repr__()}")
                return dbInstance
            else:
                return curs.fetchone()
    
    @classmethod
    def fromId(cls, id: int, db: Database) -> Self:
        """Returns an instance of the class with the given id"""
        if not isinstance(db, Database):
            raise ValueError("Database connection must be provided to retrieve from id")
        query = f"""--sql
            SELECT * FROM {cls.__name__.lower()}s WHERE id = ?
        """
        with db.query() as curs:
            curs.execute(query, (id, ))
            return cls(**curs.fetchone())
    
    @classmethod
    def getByParentDir(cls, parentDir: int, db: Database) -> tuple[Self]:
        """Returns all instances of the class with the given parentDir"""
        if not isinstance(db, Database):
            raise ValueError("Database connection must be provided to retrieve from parent_dir")
        
        query = cls._sqlGetByForeignKeyQuery(parent_dir=parentDir)
        logging.info(f"query={query}")
        
        with db.query() as curs:
            curs.execute(*query)
            # curs.execute(*cls._sqlGetByForeignKeyQuery(parent_dir=parentDir))
            return tuple(cls(**row) for row in curs.fetchall())
    
    @classmethod
    def sqlDumpTableQuery(cls) -> str:
        return f"""--sql
            SELECT * FROM {cls.__name__.lower()}s
        """
    def __update(self, **kwargs):
        for key, value in kwargs.items():
            if key in self.fieldNames():
                # keep track of which attribute has been modified for sqlUpdateQuery()
                # need to run before setattr to make sure won't be modified before syncing with db
                self._updated.add(key)
                setattr(self, key, value)
            else:
                raise ValueError(f"Invalid field name {key}, attribute not found in {self.__class__.__name__}")

    @classmethod
    def getLen(cls, cursor: Cursor, *query) -> int:
        """Returns the length of the query"""
        cursor.execute(*cls._sqlGetLen(*query))
        return cursor.fetchone()['length']

    @classmethod
    def _sqlGetLen(cls, query: str, *args) -> tuple[str, tuple]:
        query = query.rstrip().rstrip(';')
        getLenQuery = f"""--sql
            SELECT COUNT(*) AS length FROM ({query})
        """
        return getLenQuery, *args
    
    @classmethod
    def queryFuncSelector(cls, db: Database, as_iter: bool = True):

        # iterQuery should always be used with contextlib.close() to prevent unclosed transaction
        def iterQuery(query: tuple, as_instance_of: object | None = None, count: bool = True):
            with db.query() as curs:
                # if count is True, the first yield will be the length of the query
                if count:
                    length = cls.getLen(curs, *query)
                    yield length
                curs.execute(*query)
                if as_instance_of is not None:
                    yield from (as_instance_of(**row) for row in curs)
                else:
                    yield from (row for row in curs)
        
        def tupleQuery(query: tuple, as_instance_of: object | None = None, **kwargs):
            with db.query() as curs:
                curs.execute(*query)
                if as_instance_of is not None:
                    return tuple(as_instance_of(**row) for row in curs.fetchall())
                else:
                    return tuple(row for row in curs.fetchall())

        if as_iter:
            return iterQuery
        else:
            return tupleQuery

    def updateValue(self, **kwargs):
        """Updates the value of the given attributes"""
        self.__update(**kwargs)

    def update(self, db: Database, **kwargs):
        """Updates the value of the given attributes and syncs with database"""
        if not isinstance(db, Database):
            raise ValueError("Database connection must be provided to update")
        self.__update(**kwargs)

        with db.query() as curs:
            curs.execute(*self._sqlUpdateQuery())

        self._clearUpdatedSet()

    def delete(self, db: Database):
        """Deletes the row from the database"""
        if not isinstance(db, Database):
            raise ValueError("Database connection must be provided to delete")
        
        with db.query() as curs:
            curs.execute(*self._sqlDeleteQuery())

        self._deleted = True
            
    def _sqlInsertQuery(self) -> tuple[str, tuple]:
        dictToInsert = dict()
        for (k, v) in asdict(self).items():
            if v is not None:
                if k.endswith('_json'):
                    dictToInsert[k] = json.dumps(v)
                else:
                    dictToInsert[k] = v

        query = f"""--sql
            INSERT INTO {self.__class__.__name__.lower()}s
            ({', '.join(dictToInsert.keys())}) VALUES ({', '.join('?' * len(dictToInsert))})
            RETURNING *
        """
        values = tuple(dictToInsert.values())
        return query, values
    
    def _sqlUpdateQuery(self) -> tuple[str, tuple]:
        toBeUpdated = { k: getattr(self, k) for k in self._updated }
        if len(toBeUpdated) <= 0:
            raise IndexError("No attributes to be updated")
        query = f"""--sql
            UPDATE {self.__class__.__name__.lower()}s
            SET ({', '.join(toBeUpdated.keys())}) = ({', '.join('?' * len(toBeUpdated))})
            WHERE id = ?
        """
        values = tuple(toBeUpdated.values()) + (self.id, )
        return query, values
    
    def _sqlDeleteQuery(self) -> tuple[str, tuple]:
        query = f"""--sql
            DELETE FROM {self.__class__.__name__.lower()}s WHERE id = ?
        """
        values = (self.id, )
        return query, values
    
    @classmethod
    def _sqlGetByColumnQuery(cls, **kargs):
        keys = tuple(kargs.keys())
        for key in keys:
            if key not in cls.fieldNames():
                raise ValueError(f"Invalid field name {key}, attribute not found in {cls.__class__.__name__}")
        query = f"""--sql
            SELECT * FROM {cls.__name__.lower()}s WHERE {', '.join(f'{key} = ?' for key in keys)}
        """
        values = tuple(kargs.values())
        return query, values
    
    @classmethod
    def _sqlGetByForeignKeyQuery(cls, **kargs):
        if len(kargs) != 1:
            raise ValueError("Only one foreign key can be provided")
        return cls._sqlGetByColumnQuery(**kargs)

    def _clearUpdatedSet(self):
        self._updated.clear()
        
@dataclass(kw_only=True)
class File(Base):
    path: str
    size: int
    parent_dir: int
    hash: str = None
    complete_hash: str = None
    duplicate_id: int = None

    @classmethod
    def insert(cls, payload: dict, db: Database):
        if "parent_dir" not in payload or payload ["parent_dir"] is None:
            raise ValueError("parent_dir is required to insert a new row")
        
        return super().insert(payload, db)
    
@dataclass(kw_only=True)
class Photo(Base):
    file: int
    image_hash: str = None
    data_json: dict = field(default_factory=dict)

@dataclass(kw_only=True)
class Dir(Base):
    path: str
    parent_dir: int
    depth: int = None
    hash: str = None
    duplicate_id: int = None

    def _sqlGetChildenByDFSQuery(self) -> tuple[str, tuple]:
        query = """--sql
            WITH RECURSIVE cte (id, parent_dir, depth, path) AS (
                SELECT id, parent_dir, 1, path
                FROM dirs WHERE id = ?
                UNION ALL
                SELECT dirs.id, dirs.parent_dir, cte.depth + 1 AS depth, dirs.path 
                FROM dirs JOIN cte ON dirs.parent_dir = cte.id
                ORDER BY depth DESC
            )
            SELECT * FROM cte ORDER BY depth DESC;
        """
        values = (self.id, )
        return query, values
    
    @classmethod
    def _sqlGetAllRootDirsQuery(cls) -> str:
        return ("""--sql
                SELECT * FROM dirs WHERE parent_dir IS NULL
            """, )
    
    @classmethod
    def getAllRootDirs(cls, db: Database, as_iter: bool = False, count: bool = True, as_instance: bool = True) -> tuple[Self]:
        """Returns all root dirs in the database"""
        if not isinstance(db, Database):
            raise ValueError("Database connection must be provided to retrieve all root dirs")
        
        query = cls._sqlGetAllRootDirsQuery()
        query_func = cls.queryFuncSelector(db, as_iter=as_iter)
        return query_func(query, cls if as_instance else None, count=count)
    
    def getChildenByDFS(self, db: Database, as_iter: bool = False, count: bool = True, as_instance: bool = True) -> tuple[Self]:
        """Returns all dirs in the directory in DFS order sorted by depth (deepest first)"""
        if not isinstance(db, Database):
            raise ValueError("Database connection must be provided to retrieve dirs by DFS")
        
        query = self._sqlGetChildenByDFSQuery()
        query_func = self.queryFuncSelector(db, as_iter=as_iter)
        return query_func(query, self.__class__ if as_instance else None, count=count)
    
    def getFiles(self, db: Database, as_iter: bool = False, count: bool = True, as_instance: bool = True):
        """Returns all files in the directory"""
        if not isinstance(db, Database):
            raise ValueError("Database connection must be provided to retrieve files")
        
        query = self._sqlGetFilesQuery()
        query_func = self.queryFuncSelector(db, as_iter=as_iter)
        return query_func(query, File if as_instance else None, count=count)

    def _sqlGetFilesQuery(self) -> tuple[str, tuple]:
        query = f"""--sql
            SELECT * FROM files WHERE parent_dir = ?
        """
        return query, (self.id, )
    
    def _sqlInsertQuery(self) -> tuple[str, tuple]:
        hasValue = { k: v for (k, v) in asdict(self).items() if v is not None }
        query = f"""--sql
            INSERT INTO {self.__class__.__name__.lower()}s
            ({', '.join(hasValue.keys())}, depth)
            VALUES
            ({', '.join('?' * len(hasValue))} , COALESCE((SELECT depth FROM dirs WHERE id = ?), -1) + 1)
            RETURNING *
        """

        values = tuple(hasValue.values()) + (self.parent_dir, )
        return query, values

class CursorIterator():
    def __init__(self, curs: Cursor, length: int, yield_type: object | None = None):
        self.curs = curs
        self.yield_type = yield_type
        self.length = length

    def __iter__(self):
        return self
    
    def __next__(self):
        if self.yield_type is None:
            return next(self.curs)
        return self.yield_type(**next(self.curs))
        
    def __len__(self):
        return self.length
    
@dataclass(kw_only=True)
class Duplicate(Base):
    type: str
    hash: str = None
