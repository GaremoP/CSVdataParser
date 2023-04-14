# -*- coding: utf-8 -*-
import csv
import os
import shutil
import sys

import numpy as np
import pandas as pd


def check_if_already_parsed(filename):
    with open('ListOfParsedCSV.CSV') as csvfile:  # Could be modified so it opens something different
        reader = csv.reader(csvfile)
        for row in reader:
            if filename in row:
                return True
    return False


def ask_to_continue():
    user_input = input("The file has already been parsed. Do you wish to continue and rewrite the data? [y/n] ")
    while user_input not in ["y", "n"]:
        print("Invalid input. Please enter 'y' or 'n' ")
        user_input = input(f"The file {filename_original} has already been parsed. Do you wish to continue and rewrite "
                           f"the data? [y/n] ")
    if user_input == "y":
        return True
    else:
        return False


def add_to_list_of_parsed_csv(filename):
    with open('ListOfParsedCSV.CSV', 'a', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([filename])


def delete_file(file_path):
    if os.path.exists(file_path):
        os.remove(file_path)
    else:
        print("File doesn't exist on the given path")


def format_first_lines(input_file_path, output_directory):
    filename = os.path.basename(input_file_path)
    output_path = os.path.join(output_directory, filename)  # output_directory + "\"  + filename
    with open(input_file_path, "r") as input_file, open(output_path, "w") as output_file:  # We read the original file
        for i in range(2):  # Skip first two lines
            next(input_file)

        for line in input_file:
            output_file.write(line)
        print(f"New file created at {output_path}")


def check_1440_records(file_path, output_ok_directory, output_error_directory):
    df = pd.read_csv(file_path)
    if len(df) == 1440:
        file_name = os.path.basename(file_path)
        output_path = os.path.join(output_ok_directory, file_name)
        shutil.copyfile(file_path, output_path)
        print(f"The file {file_name} has 1440 records.")
        return True
    else:
        file_name = os.path.basename(file_path)
        output_path = os.path.join(output_error_directory, file_name)
        shutil.copyfile(file_path, output_path)
        print(f"The file {file_name} has some missing records")
        return False


def revision(filename):
    df = pd.read_csv(filename)
    df['Datetime'] = pd.to_datetime(df['Date'] + ' ' + df['Time'])
    df.set_index('Datetime', inplace=True)
    new_df = df.resample('1T').asfreq()
    # Drop 'Date' and 'Time' columns
    new_df = new_df.drop(['Date', 'Time'], axis=1)
    # Interpolate missing values
    new_df = new_df.interpolate(method='linear')
    new_df.fillna('---', inplace=True)  # Cleaning empty cells
    new_df = new_df.round(decimals=3)  # Round to 3 decimal places as is the same precision the logger provides

    # Reset index to Datetime
    new_df.reset_index(inplace=True)
    file_name = os.path.basename(filename)
    path_treat_data = os.path.join(PATH_TREAT_DATA, file_name)
    new_df.to_csv(path_treat_data, index=False)


def datetime_together(filename):
    df = pd.read_csv(filename)
    df['Datetime'] = pd.to_datetime(df['Date'] + ' ' + df['Time'])
    df.set_index('Datetime', inplace=True)
    # Drop 'Date' and 'Time' columns
    df = df.drop(['Date', 'Time'], axis=1)
    # Reset index to Datetime
    df.reset_index(inplace=True)
    file_name = os.path.basename(filename)
    path_treat_data = os.path.join(PATH_TREAT_DATA, file_name)
    df.to_csv(path_treat_data, index=False)


def empty_csv_file(filename):  # DELETE LIST OF FILES (ONLY IN DEV MODE)
    # open the file in write mode to truncate it and delete all contents
    with open(filename, 'w', newline=''):
        pass


def treating_data(filename):
    file_name = os.path.basename(filename)
    df = pd.read_csv(filename)
    irradiance_cols = [col for col in df.columns if 'Irrad' in col]  # Irradiance measurements columns
    df.replace('---', 0.0, inplace=True)  # Replace non taken measures to 0.
    df[irradiance_cols] = df[irradiance_cols].apply(pd.to_numeric)
    for col in df.columns:
        if col in irradiance_cols:
            for x in df.index:
                if df.loc[x, col] < 0:
                    df.loc[x, col] = 0
    """""                    
    mask = (df[irradiance_cols] <= 0).any(axis=1)
    subset_df = df.loc[mask, irradiance_cols]
    df.loc[mask, irradiance_cols] = 0
    """""
    file_path = os.path.join(PATH_DATA_GIJON, file_name)
    print(df.head())
    df.to_csv(file_path, index=False)


# Main
# Constants for file management
PATH_RAW_FILES = r"C:\Users\pablo\Documents\ProgrammingProjects\PycharmProjects\InputData"
PATH_RAW_DATA_PARSED = r"C:\Users\pablo\Documents\Teleco2018-\5CURSO\TFG\RawDataParsed"
PATH_TREAT_DATA = r"C:\Users\pablo\Documents\Teleco2018-\5CURSO\TFG\TratarDatos"
PATH_REVISE_DATA = r"C:\Users\pablo\Documents\Teleco2018-\5CURSO\TFG\RevisarDatos"
PATH_TREATED_DATA = r"C:\Users\pablo\Documents\Teleco2018-\5CURSO\TFG\TratarDatos\DatosTratados"
PATH_DATA_GIJON = r"C:\Users\pablo\Documents\Teleco2018-\5CURSO\TFG\TratarDatos\DatosTratados\DatosTratados_1440_Gijon"
# This should be on a loop through all the files on a directory
file_full_path = 'KLOG0540.CSV'
filename_original = os.path.basename(file_full_path)

isParsed = check_if_already_parsed(filename_original)
if isParsed:
    continuing = ask_to_continue()
    if continuing:
        print("File is in log but we wish to re-process it")
    else:
        print("File is in log, error was made. Deleting file...")
        delete_file(file_full_path)
        sys.exit()  # Return or break to iterate to next file on InputData
else:  # It is not on the log file, therefore it is a new log
    add_to_list_of_parsed_csv(filename_original)
    print("File not in log, continuing program")

format_first_lines(file_full_path, PATH_RAW_DATA_PARSED)
#delete_file(file_full_path) We delete the original file on InputData

FILE_TO_CHECK_PATH = os.path.join(PATH_RAW_DATA_PARSED, filename_original)
has1440 = check_1440_records(FILE_TO_CHECK_PATH, PATH_TREAT_DATA, PATH_REVISE_DATA)
# Both functions write a file on TratarDatos with the Date and Time columns fused into 1
if not has1440:
    FILE_PATH_REVISE = os.path.join(PATH_REVISE_DATA, filename_original)
    revision(FILE_PATH_REVISE)
else:
    FILE_PATH_PARSE = os.path.join(PATH_RAW_DATA_PARSED, filename_original)
    datetime_together(FILE_PATH_PARSE)

FILE_PATH_TREAT = os.path.join(PATH_TREAT_DATA, filename_original)
treating_data(FILE_PATH_TREAT)

empty_csv_file('ListOfParsedCSV.CSV')  # To not have to clean it myself


"""
def go_through_all_files(path):
    for file in os.listdir(path):
        if os.path.isfile(os.path.join(path, file)):
"""