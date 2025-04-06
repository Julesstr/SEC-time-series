import json
import pandas as pd
import sqlite3
import requests
import zipfile
import os
import sys
import time

# https://www.sec.gov/files/financial-statement-data-sets.pdf
# 13,236,308


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


def unstack_df(df):
    df_pivot = df.groupby(["adsh", "tag"])[["value", "name", "ddate"]].first().unstack().reset_index()

    return df_pivot

def create_unstacked_data(year, quarter, drop_nonstandard):
    print("Unstacking data...")
    ddate = create_ddate(year, quarter)

    try:
        merged_df = pd.read_csv(f"Unstacked Quarterly Financials/{ddate}.csv")
        return merged_df

    except FileNotFoundError:
        pass
        
    num_df, sub_df, tag_df = load_quarter(year, quarter)  

    num_df = include_only_quarter(num_df, year, quarter)

    if drop_nonstandard:
        num_df = drop_nonstandard_tags(num_df)

    num_df = unstack_df(num_df)
    merged_df = merge_two_dfs(num_df, sub_df, "adsh")

    merged_df.drop_duplicates(inplace=True)
    


    merged_df["date"] = ddate
    
    merged_df.to_csv(f"Unstacked Quarterly Financials/{ddate}.csv", index=False)

    return merged_df

def create_database_table(year, quarter):
    con = sqlite3.connect("financials.db")
    cursor = con.cursor()

    num_df, sub_df, tag_df = load_quarter(year, quarter)  

    num_df = include_only_quarter(num_df, year, quarter)
    num_df = drop_nonstandard_tags(num_df)

    merged_df = merge_two_dfs(num_df, sub_df, on_var="adsh")

    merged_df.to_sql(f"{create_ddate(year, quarter)}_stacked", con, if_exists="replace", index=False)

def handle_query(database, query, ddate):
    print("Querying...")
    con = sqlite3.connect(f"{database}.db")
    cursor = con.cursor()
    s = time.time()
    cursor.execute(query, ("OperatingIncomeLoss",))
    rows = cursor.fetchall()
    print(time.time()-s)
    columns = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(rows, columns=columns)
    df = unstack_df(df)
    df["ddate"] = ddate
    df.to_csv("test2.csv", index=False)
    df["ddate"] = pd.to_datetime(df["ddate"], format="%Y%m%d")
    print(df)

    con.close()


def query_database(database, parameters):
    con = sqlite3.connect(f"{database}.db")
    cursor = con.cursor()

    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [row[0] for row in cursor.fetchall()]

    relevant_tables = [
    table for table in tables
    if table.endswith("_stacked") and parameters["start_date"] <= int(table[:8]) <= parameters["end_date"]
    ]
    print(relevant_tables)

    cik_placeholders = ", ".join("?" for _ in parameters["ciks"])
    tag_placeholders = ", ".join("?" for _ in parameters["tags"])
    print(cik_placeholders)
    print(tag_placeholders)

    full_df = pd.DataFrame()

    for table in relevant_tables:
        query = f"SELECT * FROM '{table}' WHERE cik in ({cik_placeholders}) AND tag in ({tag_placeholders})"
        cursor.execute(query, parameters["ciks"] + parameters["tags"])
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(rows, columns=columns)

        full_df = pd.concat([full_df, df], ignore_index=True)
        

    con.close()

    return full_df

# hello


def main():
    year = 2024
    quarter = 3
    # df = create_unstacked_data(year, quarter, True)
    # ddate = create_ddate(year, quarter)
    # print(ddate)
    # query = f'SELECT * FROM "{ddate}_stacked" WHERE tag = ?'
    # query_database("financials", query, ddate)
    # TODO Request parameters: startdate, enddate, companies (by CIK), forms, desired data tags, 
    parameters = {"start_date": 20090630, "end_date": 20240930, "ciks": ["0000320193"], "tags": ["Assets", "LongTermDebt"]}
    df = query_database("financials", parameters)
    df.to_csv("result.csv", index=False)








    

 





if __name__ == "__main__":
    main()
