# chunk_dataframe.py
import pandas as pd

def chunk_dataframe(df, chunksize):
    column = df.columns[0]
    df[column] = df[column].astype('category')

    last_value = None
    chunk_start_idx = 0
    chunk_size = 0

    column_values = df[column].values

    for i, current_value in enumerate(column_values):
        if chunk_size >= chunksize and current_value != last_value:
            yield df.iloc[chunk_start_idx:i]
            chunk_start_idx = i
            chunk_size = 0

        chunk_size += 1
        last_value = current_value

    if chunk_size > 0:
        yield df.iloc[chunk_start_idx:]
