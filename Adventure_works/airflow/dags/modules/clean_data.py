import pandas as pd

# Define a reusable function to handle missing values
def handle_missing_values(df, method='mean', fill_value=None):
    if method == 'drop':
        return df.dropna()
    elif method == 'fill':
        return df.fillna(fill_value)
    elif method == 'mean':
        # Only calculate the mean for numeric columns
        numeric_cols = df.select_dtypes(include='number').columns
        df[numeric_cols] = df[numeric_cols].fillna(df[numeric_cols].mean())
        return df
    else:
        raise ValueError("Invalid method provided")
