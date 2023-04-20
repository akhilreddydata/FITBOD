# Readme file

## Introduction

This code reads input files, processes the data, and writes the output to a summary csv file. The input files are in the CSV format, and the output file will also be in CSV format.

## Libraries used
* pandas

## Installation

Installation:

To run the script, you can simply extract the "requirements.txt" into the same pwd and install the required packages using the following command:
```
pip install -r requirements.txt
```

## Functions:

   This code has the following functions:
* `read_files(file1, file2, file3)`: This function loads the input files into pandas dataframes.
* `merge_user_alias_dataframe(users_df, alias_df)`: This function merges the users dataframe with the aliases dataframe using the user_id as a common parameter.
* `merge_final_dataframe(merged_df, events_df)`: This function merges the result with the events dataframes to get the feature assignments for each user.
* `Aggregate_by_feature(final_df)`: This function aggregates the data by feature_key and feature_value to get the final output.
* `write_to_csv(summary_df)`: This function writes the output to a summary file.

## Usage
   
* To use this code, simply ensure that the input files are in the CSV format and that the names are set correctly.
* Run the code, and the output will be written to the file summary.csv.
* The code can also be imported as a module, and the functions can be used separately.

## How to run
* To run the code, simply run the file in your Python environment.
* The code is written in Python 3.x and requires pandas library to be installed.
