import scanner as sc
import test.prep_mock as prep_mock


def main():
    scanner = sc.Scanner(':memory:')
    scanner.scan("./test/mock_data")
    scanner.hash()
    scanner.dumpResults()
    
if __name__ == "__main__":
    prep_mock.create_mock_data("./test/mock_data_file_tree.json", "./test/mock_data")
    # prep_mock.remove_mock_data("./test/mock_data")
    main()
