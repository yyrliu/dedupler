import db as DB
import scanner
import test.prep_mock as prep_mock


def main():
    global db
    db = DB.Database(':memory:')
    db.initialize()

    scanner.scan("./test/mock_data", db)

    db.dumpTable("files")
    db.dumpTable("duplicates")

if __name__ == "__main__":
    prep_mock.create_mock_data("./test/mock_data_file_tree.json", "./test/mock_data")
    # prep_mock.remove_mock_data("./test/mock_data")
    main()
