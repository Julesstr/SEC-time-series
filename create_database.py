import pandas as pd
import sqlite3
import requests
import zipfile
import os
import datetime


def download_quarter(year, quarter):
    print(f"Downloading data for {year} Q{quarter}...")
    filename = f"https://www.sec.gov/files/dera/data/financial-statement-data-sets/{year}q{quarter}.zip"
    headers = {
        "User-Agent": "Jules Stremersch j.stremersch@gmail.com",
        "Accept-Encoding": "gzip"
    }
    response = requests.get(filename, headers=headers)

    if response.status_code != 200:
        raise Exception(f"Invalid request. Status code: {response.status_code}. Quarter does not exist.")

    
    with open(f"Raw Aggregate Data/{year}q{quarter}.zip", "wb") as file:
        file.write(response.content)
    
    with zipfile.ZipFile(f"Raw Aggregate Data/{year}q{quarter}.zip", "r") as zip_ref:
        zip_ref.extractall(f"Raw Aggregate Data/{year}q{quarter}")
    
    os.remove(f"Raw Aggregate Data/{year}q{quarter}.zip")


def load_quarter(year, quarter):
    print(f"Loading data for {year} Q{quarter}...")
    # SEC data appears to be lagged by a quarter. Q4 folder actually contains data EO Q3

    if quarter == 4:
        sec_year = year + 1
        sec_quarter = 1
    else:
        sec_year = year
        sec_quarter = quarter + 1

    if os.path.exists(f"Raw Aggregate Data/{sec_year}q{sec_quarter}"):
        pass
    else:
        download_quarter(sec_year, sec_quarter) 


    num_df = pd.read_csv(f"Raw Aggregate Data/{sec_year}q{sec_quarter}/num.txt", delimiter="\t", encoding="utf-8",
                         usecols=["adsh", "tag", "ddate", "qtrs", "value"])
    sub_df = pd.read_csv(f"Raw Aggregate Data/{sec_year}q{sec_quarter}/sub.txt", delimiter="\t", encoding="utf-8",
                         usecols=["adsh", "cik", "name", "sic", "fye", "form", "period", "fy", "fp"],
                         dtype={"cik": "int64"})
    tag_df = pd.read_csv(f"Raw Aggregate Data/{sec_year}q{sec_quarter}/tag.txt", delimiter="\t", encoding="utf-8",
                         usecols=["tag", "version", "custom"])
    

    return num_df, sub_df, tag_df


def create_ddate(year, quarter):
    quarter_map = {1: "0331", 2: "0630", 3: "0930", 4: "1231"}
    ddate = f"{year}{quarter_map[quarter]}"

    return int(ddate)


def include_only_quarter(num_df, year, quarter):
    ddate = create_ddate(year, quarter)
    num_df = num_df[num_df["ddate"] == ddate].reset_index(drop=True)
    num_df = num_df[num_df["qtrs"].isin([0,1])]

    return num_df


def merge_two_dfs(df1, df2, on_var):
    merged_df = df1.merge(df2, on=on_var, how="left")

    return merged_df


def create_database_table(year, quarter, database_name):
    con = sqlite3.connect(f"{database_name}.db")
    cursor = con.cursor()

    num_df, sub_df, tag_df = load_quarter(year, quarter)

    num_df = include_only_quarter(num_df, year, quarter)
    merged_df = merge_two_dfs(num_df, sub_df, on_var="adsh")
    merged_df = merge_two_dfs(merged_df, tag_df, on_var="tag")
    merged_df = merged_df[merged_df["custom"] == 0]

    # merged_df.info(memory_usage="deep")
    
    merged_df.to_csv("test.csv", index=False)
    merged_df.to_sql(f"{create_ddate(year, quarter)}", con, if_exists="replace", index=False)


def create_database(database_name):
    current_year = datetime.date.today().year
    for year in range(2009, current_year+1):
        for quarter in range(1,5):
            create_database_table(year, quarter, database_name)


def query_database(database, parameters, all_ciks, all_tags):
    con = sqlite3.connect(f"{database}.db")
    cursor = con.cursor()

    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [row[0] for row in cursor.fetchall()]

    relevant_tables = [
        table for table in tables
        if parameters["start_date"] <= int(table[:8]) <= parameters["end_date"]
    ]

    full_df = pd.DataFrame()

    for table in relevant_tables:
        print(f"Querying table {table} of database {database}")
        query = f"SELECT * FROM '{table}'"
        conditions = []
        values = []

        if not all_ciks:
            conditions.append(f"cik IN ({', '.join('?' for _ in parameters['ciks'])})")
            values.extend(parameters["ciks"])

        if not all_tags:
            conditions.append(f"tag IN ({', '.join('?' for _ in parameters['tags'])})")
            values.extend(parameters["tags"])

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        cursor.execute(query, values)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(rows, columns=columns)

        pivot_df = df.pivot_table(
        index=["cik", "ddate"],
        columns="tag",
        values="value",
        aggfunc="first"  # In case there are multiple values for same adsh/ddate/tag combination
        ).reset_index()

        full_df = pd.concat([full_df, pivot_df], ignore_index=True)


    con.close()
    return full_df


def find_all_items(database, start_date, end_date, variable):
    con = sqlite3.connect(f"{database}.db")
    cursor = con.cursor()

    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [row[0] for row in cursor.fetchall()]

    relevant_tables = [
        table for table in tables
        if start_date <= int(table[:8]) <= end_date
    ]

    items = set()

    for table in relevant_tables:
        cursor.execute(f"SELECT DISTINCT {variable} FROM '{table}'")
        items.update(row[0] for row in cursor.fetchall())

    con.close()
    with open(f"all_{variable}.txt", "w") as f:
        for item in list(items):
            if item is not None and str(item) != 'nan':
                f.write(f"{item}\n")


    return items


def main():

    database_name = "financials"
    # create_database(database_name)

    # with open("all_tag.txt", "r") as f:
    #     all_tag = [line.strip() for line in f if line.strip()]

    # with open("all_cik.txt", "r") as f:
    #     all_cik = [int(float(line.strip())) for line in f if line.strip()]

    parameters = {"start_date": 20090630, "end_date": 20240930, "ciks": [320193], "tags": ["Assets"]}
    df = query_database(database_name, parameters, all_ciks = False, all_tags = False)
    df.to_csv(f"results.csv", index=False)














    

 





if __name__ == "__main__":
    main()
