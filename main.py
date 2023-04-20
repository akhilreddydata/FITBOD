import pandas as pd


def read_files(file1, file2, file3):
    """ Load the input files into pandas data frames
    """
    users_df = pd.read_csv(file1)
    alias_df = pd.read_csv(file2)
    events_df = pd.read_csv(file3)
    return users_df, alias_df, events_df


def merge_user_alias_dataframe(users_df, alias_df):
    """ Merge the users data frame with the aliases data frame using
        user_id as a common parameter
    """
    merged_df = pd.merge(users_df, alias_df, left_on='user_id',
                         right_on='user_id', how='left')
    # Drop the user_id column
    merged_df = merged_df.drop('user_id', axis=1)
    # Rename alias_user_id by user_id
    merged_df = merged_df.rename(columns={'alias_user_id': 'user_id'})
    return merged_df


def merge_final_dataframe(merged_df, events_df):
    """ Merge the result with the events data frames
        to get the feature assignments for each user
    """
    final_df = pd.merge(merged_df, events_df, on='user_id', how='left')
    return final_df


def Aggregate_by_feature(final_df):
    """ Aggregate the data by feature_key and feature_value to get the final output
    """
    summary_df = final_df.groupby(['feature_key', 'feature_value']).agg(
        {'user_id': 'nunique'}).reset_index()
    summary_df = summary_df.rename(columns={'user_id': 'user_count'})
    return summary_df


def write_to_csv(summary_df):
    """ Write the output to a summary file"""
    summary_df.to_csv('summary.csv', index=False)
    return "Data sucessfully loaded to a summary csv file"


if __name__ == "__main__":
    file1 = 'users.csv'
    file2 = 'alias.csv'
    file3 = 'events.csv'
    users_df, alias_df, events_df = read_files(file1, file2, file3)
    merged_df = merge_user_alias_dataframe(users_df, alias_df)
    final_df = merge_final_dataframe(merged_df, events_df)
    summary_df = Aggregate_by_feature(final_df)
    message = write_to_csv(summary_df)
    print(message)
