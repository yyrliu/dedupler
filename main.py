import argparse

import core
from scanner import Scanner
import db as DB



def scan(args):
    scanner = Scanner(db_path='./test/test.db')
    scanner.scan(args.path)

def hash(args):
    print(args)

def print(args):
    db = DB.Database('./test/test.db')
    db.dumpTable("dirs")
    db.dumpTable("files")
    db.dumpTable("duplicates")
    # rootDirs = core.Dir.getAllRootDirs(db._conn)
    # for i in rootDirs:
    #     print(core.Dir.getByParentDir(i.id, db._conn))

def main():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(help='Functions')

    parser_scan = subparsers.add_parser('scan', help='scan the given path')
    parser_scan.add_argument('path', type=str, help='path to scan')
    parser_scan.set_defaults(func=scan)

    parser_hash = subparsers.add_parser('hash', help='hash the database')
    parser_hash.set_defaults(func=hash)

    parser_print = subparsers.add_parser('print', help='print the database')
    parser_print.set_defaults(func=print)

    args = parser.parse_args()
    args.func(args)
    
if __name__ == "__main__":
    main()
