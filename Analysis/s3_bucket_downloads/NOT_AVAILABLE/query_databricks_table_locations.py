import general_functions.databricks_client as db_client
import pandas as pd
import delta_sharing


def main():
    profile_path = db_client.return_databricks_client()
    client = delta_sharing.SharingClient(profile_path)
    tables = client.list_all_tables()
    print(tables)


if __name__ == "__main__":
    main()
