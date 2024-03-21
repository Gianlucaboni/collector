import argparse
import glob
from modules.MySQLLoader import DataProcessor

def main(country_name):
    processor = DataProcessor(country_name)
    processor.process_data()
    processor.push_to_table()
    print(f"{country_name} DONE")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process data for a specific country")
    parser.add_argument("country_name", type=str, help="Name of the country to process")
    args = parser.parse_args()
    
    main(args.country_name)
