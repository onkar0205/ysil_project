def fetch_data_from_table(connector, table_name, query):
    try:
        df = connector.read_table(query)
        print("Count:", df.count())
        df.show()
        return df
    except Exception as e:
        print(f"Error reading from {table_name}: {e}")
        return None