# -*- coding: utf-8 -*-
import csv
import os
import shutil
import sys
import time

import numpy as np
import pandas as pd


def check_if_already_parsed(filename, header):
    if 'Estaci' in ','.join(header):
        with open('ListOfParsedCSV_dat1.CSV') as csvfile:  # Could be modified so it opens something different
            reader = csv.reader(csvfile)
            for row in reader:
                if filename in row:
                    return True
        return False
    else:
        with open('ListOfParsedCSV_dat2.CSV') as csvfile:  # Could be modified so it opens something different
            reader = csv.reader(csvfile)
            for row in reader:
                if filename in row:
                    return True
        return False


def ask_to_continue(filename_original):
    user_input = input("The file has already been parsed. Do you wish to continue and rewrite the data? [y/n] ")
    while user_input not in ["y", "n"]:
        print("Invalid input. Please enter 'y' or 'n' ")
        user_input = input(f"The file {filename_original} has already been parsed. Do you wish to continue and rewrite "
                           f"the data? [y/n] ")
    if user_input == "y":
        return True
    else:
        return False


def add_to_list_of_parsed_csv(filename, header):
    if 'Estaci' in ','.join(header):
        with open('ListOfParsedCSV_dat1.CSV', 'a', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow([filename])
    else:
        with open('ListOfParsedCSV_dat2.CSV', 'a', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow([filename])


def delete_file(file_path):
    if os.path.exists(file_path):
        os.remove(file_path)
    else:
        print("File doesn't exist on the given path")


def format_first_lines(input_file_path, output_directory, header):
    original_name = os.path.basename(input_file_path)
    if 'Estaci' in ','.join(header):
        filename = 'Data' + original_name
    else:
        filename = 'DataEstaci' + original_name
    output_path = os.path.join(output_directory, filename)  # output_directory + "\"  + filename
    with open(input_file_path, "r") as input_file, open(output_path, "w") as output_file:  # We read the original file
        for i in range(2):  # Skip first two lines
            next(input_file)

        for line in input_file:
            output_file.write(line)
        print(f"New file added to {output_path}")
    return output_path


def change_namefile_datetime(rawdata_path):
    df = pd.read_csv(rawdata_path, on_bad_lines='skip')
    if df.empty:
        return None
    first_date = df['Date'].iloc[0]   # From column called Date, its first row
    if df.shape[1] > 14:  # Check which logbox it is
        first_date += 'Data.CSV'
    else:
        first_date += 'DataEstaci.CSV'
    return first_date


def check_1440_records(file_path, output_ok_directory, output_error_directory):
    df = pd.read_csv(file_path, on_bad_lines='skip')
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


def revision(path_raw_data, new_file_name):
    df = pd.read_csv(path_raw_data, on_bad_lines='skip').dropna()

    df['Datetime'] = pd.to_datetime(df['Date'] + ' ' + df['Time'])
    df.set_index('Datetime', inplace=True)

    first_row = df.head(1)  # First row does not follow the pattern
    remaining_rows = df.iloc[1:]
    # resample the remaining rows using a time frequency of 1 minute
    resampled_rows = remaining_rows.resample('1T').asfreq()

    # concatenate the first row with the resampled rows
    new_df = pd.concat([first_row, resampled_rows])
    # Drop 'Date' and 'Time' columns
    new_df = new_df.drop(['Date', 'Time'], axis=1)
    new_df.replace('---', 0.000, inplace=True)  # Replace non taken measures to 0.

    cols_to_cast = new_df.select_dtypes(exclude=['datetime64']).columns
    new_df[cols_to_cast] = new_df[cols_to_cast].astype(float)
    # Interpolate missing values
    new_df = new_df.interpolate(method='linear')
    new_df.fillna(0.0, inplace=True)  # Cleaning empty cells
    new_df = new_df.round(decimals=3)  # Round to 3 decimal places as is the same precision the logger provides
    # reset index to Datetime
    new_df.reset_index(inplace=True)
    if len(new_df) != 1440:
        # Get the start and end time of the day
        start_time = new_df['Datetime'].min().replace(hour=0, minute=0, second=0, microsecond=0)
        end_time = new_df['Datetime'].max().replace(hour=23, minute=59, second=0, microsecond=0)

        # Create a new DataFrame with one row per minute of the day
        index = pd.date_range(start=start_time, end=end_time, freq='T')
        empty_df = pd.DataFrame(index=index)

        # Merge the new DataFrame with the original DataFrame
        new_df = pd.merge(empty_df, new_df, left_index=True, right_on='Datetime', how='outer')
        new_df.fillna(0.0, inplace=True)
        # Round to 3 decimal places
        new_df = new_df.round(decimals=3)
        # reset index to Datetime
        new_df.set_index('Datetime', inplace=True)
    # save the output DataFrame to a CSV file
    path_treat_data = os.path.join(PATH_TREAT_DATA, new_file_name)
    new_df.to_csv(path_treat_data, index=True)


def datetime_together(path_raw_data, new_file_name):
    df = pd.read_csv(path_raw_data)
    df['Datetime'] = pd.to_datetime(df['Date'] + ' ' + df['Time'])
    df.set_index('Datetime', inplace=True)
    # Drop 'Date' and 'Time' columns
    df = df.drop(['Date', 'Time'], axis=1)
    # Reset index to Datetime
    df.reset_index(inplace=True)
    df.replace('---', 0.000, inplace=True)  # Replace non taken measures to 0.
    path_treat_data = os.path.join(PATH_TREAT_DATA, new_file_name)
    df.to_csv(path_treat_data, index=False)


def empty_csv_file(filename):  # DELETE LIST OF FILES (ONLY IN DEV MODE)
    # open the file in write mode to truncate it and delete all contents
    with open(filename, 'w', newline=''):
        pass


def treating_data(filename):
    file_name = os.path.basename(filename)
    df = pd.read_csv(filename)
    print(df.dtypes)
    irradiance_cols = [col for col in df.columns if 'Irrad' in col]  # Irradiance measurements columns
    df.replace('---', 0.0, inplace=True)  # Replace non taken measures to 0.
    df[irradiance_cols] = df[irradiance_cols].apply(pd.to_numeric)
    for col in df.columns:
        if col in irradiance_cols:
            for x in df.index:
                if df.loc[x, col] < 0:
                    df.loc[x, col] = 0
    file_path = os.path.join(PATH_DATA_GIJON, file_name)

    df.to_csv(file_path, index=False)


def main(file_full_path):
    # Main
    filename_original = os.path.basename(file_full_path)
    with open(file_full_path, 'r') as file_main:
        reader = csv.reader(file_main)
        header = next(reader)
        isparsed = check_if_already_parsed(filename_original, header)
    if isparsed:
        continuing = ask_to_continue(filename_original)
        if continuing:
            print("File is in log but we wish to re-process it")
        else:
            print("File is in log, error was made. Deleting file...")
            delete_file(file_full_path)
            return  # Return or break to iterate to next file on InputData
    else:  # It is not on the log file, therefore it is a new log
        add_to_list_of_parsed_csv(filename_original, header)
        print("File not in log, continuing program")

    rawdata_file_path = format_first_lines(file_full_path, PATH_RAW_DATA_PARSED, header)
    filename_new = change_namefile_datetime(rawdata_file_path)
    if filename_new is None:
        Error_message = f"The file {filename_original} has no records"
        print(Error_message)
        delete_file(file_full_path)
        return Error_message
    has1440 = check_1440_records(rawdata_file_path, PATH_TREAT_DATA, PATH_REVISE_DATA)
    # Both functions write a file on TratarDatos with the Date and Time columns fused into 1
    if not has1440:
        filename_raw_data = os.path.basename(rawdata_file_path)
        file_path_revise = os.path.join(PATH_REVISE_DATA, filename_raw_data)
        revision(file_path_revise, filename_new)
    else:
        datetime_together(rawdata_file_path, filename_new)

    file_path_treat = os.path.join(PATH_TREAT_DATA, filename_new)
    treating_data(file_path_treat)
    delete_file(file_full_path)


if __name__ == '__main__':
    # Constants for file management
    PATH_INPUT_FILES = r"C:\Users\pablo\Documents\ProgrammingProjects\PycharmProjects\InputData"
    PATH_RAW_DATA_PARSED = r"C:\Users\pablo\Documents\Teleco2018-\5CURSO\TFG\RawDataParsed"
    PATH_TREAT_DATA = r"C:\Users\pablo\Documents\Teleco2018-\5CURSO\TFG\TratarDatos"
    PATH_REVISE_DATA = r"C:\Users\pablo\Documents\Teleco2018-\5CURSO\TFG\RevisarDatos"
    PATH_TREATED_DATA = r"C:\Users\pablo\Documents\Teleco2018-\5CURSO\TFG\TratarDatos\DatosTratados"
    PATH_DATA_GIJON = r"C:\Users\pablo\Documents\Teleco2018-\5CURSO\TFG\TratarDatos\DatosTratados" \
                      r"\DatosTratados_1440_Gijon"
    while True:
        for file in os.listdir(PATH_INPUT_FILES):
            if file.lower().endswith('.csv'):
                file_to_process = os.path.join(PATH_INPUT_FILES, file)
                main(file_to_process)
        empty_csv_file('ListOfParsedCSV_dat2.CSV')  # To not have to clean it myself. DEV
        empty_csv_file('ListOfParsedCSV_dat1.CSV')
        time.sleep(10)
