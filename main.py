import argparse
from sqlite3 import Connection

import core
from scanner import Scanner
import db as DB



def scan(args):
    print(f"Scanning: {args.path}")
    scanner = Scanner(db_path=args.db)
    scanner.scan(args.path)
    if args.tables:
        dump_db(args, scanner.db)

def hash(args):
    print(f"Hashing: {args.db}")
    print(args)

def dump_db(args, db=None):
    print(f"Printing: {args.db}")

    if (db is None) or (not isinstance(db._conn, Connection)):
        db = DB.Database('./test/test.db')

    db.dumpTables(args.tables)
    

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--db', type=str, help='path to database')
    parser.add_argument('-p', '--print', type=str, action='append', dest='tables', help='print results')
    subparsers = parser.add_subparsers(help='Functions')

    parser_scan = subparsers.add_parser('scan', help='scan the given path')
    parser_scan.add_argument('path', type=str, help='path to scan')
    parser_scan.set_defaults(func=scan)

    parser_hash = subparsers.add_parser('hash', help='hash the database')
    parser_hash.set_defaults(func=hash)

    parser_print = subparsers.add_parser('print', help='print the database')
    parser_print.add_argument('tables', type=str, nargs='*', default='all', help='table to print')
    parser_print.set_defaults(func=dump_db)

    args = parser.parse_args()
    args.func(args)
    
if __name__ == "__main__":
    main()
