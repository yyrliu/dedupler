import argparse
import subprocess
import logging

from scanner import Scanner
import db as DB

logging_level = {
    0: logging.WARNING,
    1: logging.INFO,
    2: logging.DEBUG
}

def scan(args):
    print(f"Scanning: {args.path}")
    scanner = Scanner(db_path=args.db, overwrite_db=args.force)
    scanner.scan(args.path)
    if args.tables:
        dump_db(args, scanner.db)
    if args.browse:
        subprocess.run(['sqlite_web', '--no-browser', args.db])

def hash(args):
    print(f"Hashing: {args.db}")
    print(args)

def dump_db(args, db=None):
    print(f"Printing: {args.db}")

    if db is None:
        db = DB.Database(args.db)

    db.dumpTables(args.tables)
    
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--db', type=str, help='path to database')
    parser.add_argument('-p', '--print', type=str, action='append', dest='tables', help='print results')
    parser.add_argument('-b', '--browse', action='store_true', help='browse results in sqlite-web')
    parser.add_argument('-f', '--force', action='store_true', help='force overwrite of existing database')
    parser.add_argument('-v', '--verbose', action='count', default=0, help='verbose output')
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
    logging.basicConfig(level=logging_level[args.verbose], format='%(asctime)s: %(name)s [%(levelname)s] %(message)s')
    args.func(args)
    
if __name__ == "__main__":
    main()
