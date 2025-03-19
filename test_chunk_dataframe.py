# tests/test_chunk_dataframe.py
import pandas as pd
from chunk_dataframe import chunk_dataframe

def create_test_dataframe():
    dfs = pd.to_datetime([
        "2023-01-01 00:00:01", "2023-01-01 00:00:01",
        "2023-01-01 00:00:02", "2023-01-01 00:00:02", "2023-01-01 00:00:02",
        "2023-01-01 00:00:03"
    ])
    df = pd.DataFrame({"dt": dfs})
    return df

def test_empty_dataframe():
    df = pd.DataFrame({"dt": []})
    chunks = list(chunk_dataframe(df, 2))
    assert len(chunks) == 0

def test_single_row():
    df = pd.DataFrame({"dt": [pd.Timestamp("2023-01-01 00:00:01")]})
    chunks = list(chunk_dataframe(df, 2))
    assert len(chunks) == 1
    assert len(chunks[0]) == 1

def test_chunksize_larger_than_dataframe():
    df = create_test_dataframe()
    chunks = list(chunk_dataframe(df, 10))
    assert len(chunks) == 1
    assert len(chunks[0]) == len(df)

def test_chunking_typical():
    df = create_test_dataframe()
    chunks = list(chunk_dataframe(df, 2))
    assert len(chunks) == 3
    assert len(chunks[0]) == 2
    assert len(chunks[1]) == 3
    assert len(chunks[2]) == 1

    chunks = list(chunk_dataframe(df, 5))
    assert len(chunks) == 2
    assert len(chunks[0]) == 5
    assert len(chunks[1]) == 1

    chunks = list(chunk_dataframe(df, 6))
    assert len(chunks) == 1
    assert len(chunks[0]) == len(df)

def test_large_dataframe():
    large_df = pd.DataFrame({"dt": pd.date_range("2023-01-01", periods=1_000_000, freq="s")})
    chunks = list(chunk_dataframe(large_df, 10_000))
    assert len(chunks) == 100
    assert len(chunks[0]) == 10_000
