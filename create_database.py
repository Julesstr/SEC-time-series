import json
import pandas as pd
import sqlite3
import requests
import zipfile
import os
import sys
import time

# https://www.sec.gov/files/financial-statement-data-sets.pdf
# https://xbrlview.fasb.org/yeti/resources/yeti-gwt/Yeti.jsp#tax~(id~174*v~10231)!net~(a~3474*l~832)!lang~(code~en-us)!rg~(rg~32*p~12)


def download_quarter(year, quarter):
    print(f"Downloading data for {year} Q{quarter}...")
    filename = f"https://www.sec.gov/files/dera/data/financial-statement-data-sets/{year}q{quarter}.zip"
    headers = {
        "User-Agent": "Jules Stremersch j.stremersch@gmail.com",
        "Accept-Encoding": "gzip"
    }
    response = requests.get(filename, headers=headers)

    if response.status_code != 200:
        raise Exception(f"Invalid request. Status code: {response.status_code}")

    
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


    num_df = pd.read_csv(f"Raw Aggregate Data/{sec_year}q{sec_quarter}/num.txt", delimiter="\t", encoding="utf-8")
    sub_df = pd.read_csv(f"Raw Aggregate Data/{sec_year}q{sec_quarter}/sub.txt", delimiter="\t", encoding="utf-8")
    tag_df = pd.read_csv(f"Raw Aggregate Data/{sec_year}q{sec_quarter}/tag.txt", delimiter="\t", encoding="utf-8")
    

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

def drop_nonstandard_tags(df):
    standard_df = df[df["version"].str.startswith(("us-gaap", "ifrs"), na=False)]

    return standard_df

def create_database_table(year, quarter):
    con = sqlite3.connect("financials.db")
    cursor = con.cursor()

    num_df, sub_df, tag_df = load_quarter(year, quarter)  

    num_df = include_only_quarter(num_df, year, quarter)
    num_df = drop_nonstandard_tags(num_df)

    merged_df = merge_two_dfs(num_df, sub_df, on_var="adsh")

    merged_df.to_sql(f"{create_ddate(year, quarter)}_stacked", con, if_exists="replace", index=False)

def create_database():
    for year in range(2009, 2026):
        for quarter in range(1,5):
            create_database_table(year, quarter)


def query_database(database, parameters, all_ciks, all_tags):
    con = sqlite3.connect(f"{database}.db")
    cursor = con.cursor()

    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [row[0] for row in cursor.fetchall()]

    relevant_tables = [
        table for table in tables
        if table.endswith("_stacked") and parameters["start_date"] <= int(table[:8]) <= parameters["end_date"]
    ]

    full_df = pd.DataFrame()

    for table in relevant_tables:
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

        full_df = pd.concat([full_df, df], ignore_index=True)

    con.close()
    return full_df

def find_all_tags(database, parameters):
    con = sqlite3.connect(f"{database}.db")
    cursor = con.cursor()

    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [row[0] for row in cursor.fetchall()]

    relevant_tables = [
        table for table in tables
        if table.endswith("_stacked") and parameters["start_date"] <= int(table[:8]) <= parameters["end_date"]
    ]

    tags = set()


    for table in relevant_tables:
        cursor.execute(f"SELECT DISTINCT tag FROM '{table}'")
        tags.update(row[0] for row in cursor.fetchall())

    con.close()
    with open("all_tags.txt", "w") as f:
        for item in list(tags):
            f.write(f"{item}\n")


    return tags

def main():
    year = 2024
    quarter = 3
    # # TODO Request parameters: startdate, enddate, companies (by CIK), forms, desired data tags, 

    parameters = {"start_date": 20090630, "end_date": 20240930, "ciks": ["0000320193"], "tags": ["Assets", "LongTermDebt"]}
    df = query_database("financials", parameters, all_ciks = False, all_tags = True)
    df.to_csv("result.csv", index=False)

 
    pivoted_df = df.pivot_table(
    index=["cik", "ddate"],
    columns="tag",
    values="value",
    aggfunc="first"  # In case there are multiple values for same adsh/ddate/tag combination
    ).reset_index()
    print(pivoted_df)
    pivoted_df.to_csv("pivoted_df.csv", index=False)











    

 





if __name__ == "__main__":
    main()
