# -*- coding: utf-8 -*-
import csv
import os

import pandas as pd


def check_if_already_parsed(filename):
    with open('ListOfParsedCSV.CSV') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            if filename in row:
                return True
    return False


def ask_to_continue():
    user_input = input("The file has already been parsed. Do you wish to continue and rewrite the data? [y/n] ")
    while user_input not in ["y", "n"]:
        print("Invalid input. Please enter 'y' or 'n' ")
        user_input = input("The file has already been parsed. Do you wish to continue and rewrite the data? [y/n] ")
    if user_input == "y":
        return True
    else:
        return False


def add_to_list_of_parsed_csv(filename):
    with open('ListOfParsedCSV.CSV', 'a', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([filename])


def delete_file(filename):
    path = r"C:\Users\pablo\Documents\ProgrammingProjects\PycharmProjects\DataPVpanels"
    file_path = os.path.join(path, filename)
    if os.path.exists(file_path):
        os.remove(file_path)
    else:
        print("File doesn't exist on the given path")


def go_through_all_files(path):
    for file in os.listdir(path):
        if os.path.isfile(os.path.join(path, file)):
            name = file
            add_to_list_of_parsed_csv(name)


# Main
default_path = r"C:\Users\pablo\Documents\ProgrammingProjects\PycharmProjects\DataPVpanels"
filetolookfor = 'KLOG0540.CSV'
result = check_if_already_parsed(filetolookfor)
if not result:  # It is not on the log file, therefore it is a new log
    add_to_list_of_parsed_csv(filetolookfor)
    print("File not in log, continuing program")
    delete_file(filetolookfor)
else:
    continuing = ask_to_continue()
    if continuing:
        print("File is in log but we wish to re-process it")
        delete_file(filetolookfor)
    else:
        delete_file(filetolookfor)

# pd.options.display.max_rows = 40
# pd.set_option('display.max_columns', None)
# df = pd.read_csv('KLOG0572.CSV')
#  print(df)
"""
# Define the path and filename of the input CSV file
input_file = '/path/to/input_file.csv'

# Define the path and filename of the output CSV file
output_file = '/path/to/output_file.csv'

# Read the input CSV file into a Pandas DataFrame
df = pd.read_csv(input_file)

# Drop the first empty rows from the DataFrame
df = df.dropna(subset=[df.columns[0]])

# Save the cleaned DataFrame to a new CSV file
df.to_csv(output_file, index=False)
"""
