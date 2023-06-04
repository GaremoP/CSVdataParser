# -*- coding: utf-8 -*-
import csv
import os
import shutil
import time
import numpy as np
import pandas as pd
from suntime import Sun, SunTimeException


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
    user_input = input(f"The file {filename_original} has already been parsed. "
                       f"Do you wish to continue and rewrite the data? [y/n] ")
    while user_input not in ["y", "n"]:
        print("Invalid input. Please enter 'y' or 'n' ")
        user_input = input(f"The file {filename_original} has already been parsed. "
                           f"Do you wish to continue and rewrite the data? [y/n] ")
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
    return output_path


def change_namefile_datetime(rawdata_path):
    df = pd.read_csv(rawdata_path, on_bad_lines='skip')
    if df.empty:
        return None
    first_date = df['Date'].iloc[0]  # From column called Date, its first row
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
        print(f"[OK] The file {file_name} has 1440 records.")
        return True
    else:
        file_name = os.path.basename(file_path)
        output_path = os.path.join(output_error_directory, file_name)
        shutil.copyfile(file_path, output_path)
        print(f"[ERROR] The file {file_name} has some missing records")
        return False


def get_sunrise_sunset(date):
    # Coordinates of Gijón
    latitude = 43.5359
    longitude = -5.6619
    sun = Sun(latitude, longitude)
    try:
        # Get the sunrise and sunset times for the given date
        sunrise = sun.get_sunrise_time(date)
        sunset = sun.get_sunset_time(date)

        # Format the sunrise and sunset times
        sunrise_time = sunrise.strftime("%Y-%m-%d %H:%M:%S")
        sunset_time = sunset.strftime("%Y-%m-%d %H:%M:%S")
        return sunrise_time, sunset_time

    except SunTimeException:
        return None, None


def revision(path_raw_data, new_file_name):
    df = pd.read_csv(path_raw_data, on_bad_lines='skip').dropna()

    # Format the Datetime column
    df['Datetime'] = pd.to_datetime(df['Date'] + ' ' + df['Time'], dayfirst=True)
    df.set_index('Datetime', inplace=True)

    first_row = df.head(1)  # First row does not follow the pattern
    remaining_rows = df.iloc[1:]
    # Resample the remaining rows using a time frequency of 1 minute
    resampled_rows = remaining_rows.resample('1T').asfreq()

    # Concatenate the first row with the resampled rows
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
    # Reset index to Datetime
    new_df.reset_index(inplace=True)

    # Missing the first or last few measures
    if len(new_df) != 1440:
        # Get the start and end time of the day
        start_time = new_df['Datetime'].min().replace(hour=0, minute=0, second=0, microsecond=0)
        end_time = new_df['Datetime'].max().replace(hour=23, minute=59, second=0, microsecond=0)

        # Create a new DataFrame with one row per minute of the day
        index = pd.date_range(start=start_time, end=end_time, freq='T')
        empty_df = pd.DataFrame(index=index)

        # Merge the new DataFrame with the original DataFrame
        new_df = pd.merge(empty_df, new_df, left_index=True, right_on='Datetime', how='outer')
        # Reset index to Datetime
        new_df.set_index('Datetime', inplace=True)
        new_df.reset_index(inplace=True)

    # Save the output DataFrame to a CSV file
    path_treat_data = os.path.join(PATH_TREAT_DATA, new_file_name)
    new_df.to_csv(path_treat_data, index=False)


def datetime_together(path_input_data, new_file_name):
    df = pd.read_csv(path_input_data)
    df['Datetime'] = pd.to_datetime(df['Date'] + ' ' + df['Time'], dayfirst=True)

    # Format the Datetime column
    df['Datetime'] = df['Datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')

    df.set_index('Datetime', inplace=True)
    # Drop 'Date' and 'Time' columns
    df = df.drop(['Date', 'Time'], axis=1)

    df.reset_index(inplace=True)
    df.replace('---', 0.000, inplace=True)  # Replace non taken measures to 0.
    path_treat_data = os.path.join(PATH_TREAT_DATA, new_file_name)
    df.to_csv(path_treat_data, index=False)


def treating_data(filename):
    basefilename = os.path.basename(filename)
    df = pd.read_csv(filename)
    # print(df.dtypes)
    # Irradiance measurements columns
    irradiance_cols = [col for col in df.columns if 'Irrad' in col]
    df.replace('---', 0.0, inplace=True)  # Replace non taken measures to 0.
    df[irradiance_cols] = df[irradiance_cols].apply(pd.to_numeric)

    # Set negative values in irradiance_cols to 0
    for col in df.columns:
        if col in irradiance_cols:
            df[col] = df[col].apply(lambda x: 0 if x < 0 else x)

    # Convert 'DateTime' column to datetime data type
    df['Datetime'] = pd.to_datetime(df['Datetime'])
    # Get the date (day) of the first value
    date = df['Datetime'].dt.date.iloc[0]
    sunrise_t, sunset_t = get_sunrise_sunset(date)

    # Set irradiance_cols values to 0 before sunrise_t and after sunset_t
    for col in irradiance_cols:
        df.loc[df['Datetime'] < sunrise_t, col] = 0
        df.loc[df['Datetime'] > sunset_t, col] = 0

    filename1440 = basefilename.split('Data')[0] + 'Data1440' + basefilename.split('Data')[1]
    output = os.path.join(PATH_DATA_GIJON_1440, filename1440)
    df.to_csv(output, index=False)
    return output


def resample_24UTC(filename):
    df = pd.read_csv(filename)
    # Convert 'Datetime' column to datetime type
    df['Datetime'] = pd.to_datetime(df['Datetime'])

    # Set 'Datetime' column as the index
    df.set_index('Datetime', inplace=True)

    # Resample to 1 record per hour and calculate the mean of each hour
    df_resampled = df.resample('H').mean()
    df_resampled = df_resampled.round(decimals=3)

    # Reset the index to have 'Datetime' as a column again
    df_resampled.reset_index(inplace=True)
    basefilename = os.path.basename(filename)
    filename24 = basefilename.split('Data')[0] + 'Data24UTC' + basefilename.split('Data')[1]
    output = os.path.join(PATH_DATA_GIJON_24UTC, filename24)
    # Save the resampled data to a new CSV file
    df_resampled.to_csv(output, index=False)


def resample_24HLocal(path_file_1440):
    df = pd.read_csv(path_file_1440)
    # Convert 'Datetime' column to datetime type
    df['Datetime'] = pd.to_datetime(df['Datetime'])
    # Set 'Datetime' column as the index
    df.set_index('Datetime', inplace=True)

    df = df.tz_localize('utc')
    df = df.tz_convert('Europe/Madrid')
    # Resample to 1 record per hour and calculate the mean of each hour
    df_resampled = df.resample('H').mean()
    df_resampled = df_resampled.round(decimals=3)

    # Reset the index to have 'Datetime' as a column again
    df_resampled.reset_index(inplace=True)
    # Remove the timezone from the column
    df_resampled['Datetime'] = df_resampled['Datetime'].dt.tz_localize(None)

    basefilename = os.path.basename(path_file_1440)
    filename24 = basefilename.split('Data')[0] + 'Data24Local' + basefilename.split('Data')[1]
    output = os.path.join(PATH_DATA_GIJON_24HORALOCAL, filename24)
    # Save the resampled data to a new CSV file
    df_resampled.to_csv(output, index=False)
    return output


def push_into_db1440(file1440utc, datalogger_db1440):
    new_data = pd.read_csv(file1440utc)

    database = pd.read_csv(datalogger_db1440)
    # Check if the new file has more columns than the database
    new_columns = set(new_data.columns) - set(database.columns)
    if new_columns:
        # Add the new columns to the database and set values to 0 for previous rows
        for column in new_columns:
            database[column] = np.nan

    # Append the new data to the database, overwriting rows with the same 'Datetime'
    database = pd.concat([database, new_data]).drop_duplicates(subset='Datetime', keep='last')

    # Order the database by 'Datetime' in ascending order
    database.sort_values(by='Datetime', inplace=True)

    # Delete columns with no values
    database.dropna(axis=1, how='all', inplace=True)
    print(f"File: {os.path.basename(file1440utc)}  has been added to database\n")
    # Save the updated database back to the CSV file
    database.to_csv(datalogger_db1440, index=False)


def push_into_db24(file24local, datalogger_db24):
    new_data = pd.read_csv(file24local)

    database = pd.read_csv(datalogger_db24)
    # Check if the new file has more columns than the database
    new_columns = set(new_data.columns) - set(database.columns)
    if new_columns:
        # Add the new columns to the database and set values to 0 for previous rows
        for column in new_columns:
            database[column] = np.nan

    # Append the new data to the database, overwriting rows with the same 'Datetime'
    database = pd.concat([database, new_data]).drop_duplicates(subset='Datetime', keep='last')

    # Order the database by 'Datetime' in ascending order
    database.sort_values(by='Datetime', inplace=True)

    # Delete columns with no values
    database.dropna(axis=1, how='all', inplace=True)
    print(f"File: {os.path.basename(file24local)}  has been added to database\n")
    # Save the updated database back to the CSV file
    database.to_csv(datalogger_db24, index=False)


def empty_csv_file(filename):  # DELETE LIST OF FILES (ONLY IN DEV MODE)
    # open the file in write mode to truncate it and delete all contents
    with open(filename, 'w', newline=''):
        pass


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
            return  # Return to iterate to next file on InputData
    else:  # It is not on the log file, therefore it is a new log
        add_to_list_of_parsed_csv(filename_original, header)
        print("File not in log, continuing program")

    rawdata_file_path = format_first_lines(file_full_path, PATH_RAW_DATA_PARSED, header)
    filename_new = change_namefile_datetime(rawdata_file_path)

    # Check if the file has any records
    if filename_new is None:
        error_message = f"The file {filename_original} has no records"
        print(error_message)
        delete_file(file_full_path)
        return error_message

    # Decide whether the file is complete or not. And we copy it to a directory
    has1440 = check_1440_records(rawdata_file_path, PATH_TREAT_DATA, PATH_REVISE_DATA)
    filename_raw_data = os.path.basename(rawdata_file_path)
    # In both cases we write a file on TratarDatos with the Date and Time columns fused into 1
    if not has1440:
        file_path_revise = os.path.join(PATH_REVISE_DATA, filename_raw_data)
        revision(file_path_revise, filename_new)
    else:
        datetime_together(rawdata_file_path, filename_new)
        # Delete unnecesary intermediary file
        delete_file(os.path.join(PATH_TREAT_DATA, filename_raw_data))

    file_path_treat = os.path.join(PATH_TREAT_DATA, filename_new)
    filename1440 = treating_data(file_path_treat)
    #file_path_treated = os.path.join(PATH_DATA_GIJON_1440, filename_new)
    # Convert into 24 rows per day. UTC time and Localtime
    resample_24UTC(filename1440)
    localtime_filename = resample_24HLocal(filename1440)
    # Check which database the file is going to be popped into
    if "Estaci" in filename_new:
        push_into_db1440(filename1440, DATALOGGER1_DB)
        push_into_db24(localtime_filename, DATALOGGER1_DB24)
    else:
        push_into_db1440(filename1440, DATALOGGER2_DB)
        push_into_db24(localtime_filename, DATALOGGER2_DB24)
    delete_file(file_full_path)
    print()


if __name__ == '__main__':
    # Constants for file management
    PATH_INPUT_FILES = r"C:\Users\pablo\Documents\ProgrammingProjects\PycharmProjects\InputData"
    PATH_RAW_DATA_PARSED = r"C:\Users\pablo\Documents\Teleco2018-\5CURSO\TFG\RawDataParsed"
    PATH_TREAT_DATA = r"C:\Users\pablo\Documents\Teleco2018-\5CURSO\TFG\TratarDatos"
    PATH_REVISE_DATA = r"C:\Users\pablo\Documents\Teleco2018-\5CURSO\TFG\RevisarDatos"
    PATH_TREATED_DATA = r"C:\Users\pablo\Documents\Teleco2018-\5CURSO\TFG\TratarDatos\DatosTratados"
    PATH_DATA_GIJON_1440 = r"C:\Users\pablo\Documents\Teleco2018-\5CURSO\TFG\TratarDatos\DatosTratados\DatosTratados_1440_Gijon"
    PATH_DATA_GIJON_24UTC = r"C:\Users\pablo\Documents\Teleco2018-\5CURSO\TFG\TratarDatos\DatosTratados\DatosTratadosUTC_24_Gijon"
    PATH_DATA_GIJON_24HORALOCAL = r"C:\Users\pablo\Documents\Teleco2018-\5CURSO\TFG\TratarDatos\DatosTratados\DatosTratadosHoraLocal_24_Gijon"
    DATALOGGER1_DB = r"C:\Users\pablo\Documents\Teleco2018-\5CURSO\TFG\TratarDatos\DatosTratados\Datalogger1.csv"
    DATALOGGER2_DB = r"C:\Users\pablo\Documents\Teleco2018-\5CURSO\TFG\TratarDatos\DatosTratados\Datalogger2.csv"
    DATALOGGER1_DB24 = r"C:\Users\pablo\Documents\Teleco2018-\5CURSO\TFG\TratarDatos\DatosTratados\Datalogger24hLocalEstaci.csv"
    DATALOGGER2_DB24 = r"C:\Users\pablo\Documents\Teleco2018-\5CURSO\TFG\TratarDatos\DatosTratados\Datalogger24hLocal.csv"

    while True:
        for file in os.listdir(PATH_INPUT_FILES):
            if file.lower().endswith('.csv'):
                file_to_process = os.path.join(PATH_INPUT_FILES, file)
                main(file_to_process)
        time.sleep(10)
