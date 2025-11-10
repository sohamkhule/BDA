from multiprocessing import Pool
import pandas as pd
import sqlite3


# Mapper function: emits (Month, Burned_Area)
def mapper(row):
    return (row["Month"], row["Burned_Area_hectares"])


# Reducer function: calculates average per month
def reducer(mapped_data):
    result = {}
    for month, area in mapped_data:
        result.setdefault(month, []).append(area)
    return {m: sum(v) / len(v) for m, v in result.items()}


def run_mapreduce(df):
    with Pool() as p:
        mapped = p.map(mapper, [row for _, row in df.iterrows()])
    reduced = reducer(mapped)

    print("\n🔥 Average Fire Incidents (Burned Area Equivalent) per Month:")
    for m, t in reduced.items():
        print(f"{m}: {t:.2f}")
    return reduced


def top_fire_months(df, top_n=5):
    top = df.groupby("Month")["Burned_Area_hectares"].mean().sort_values(ascending=False).head(top_n)
    print(f"\n🌋 Top {top_n} Months with Highest Fire Incidents:\n")
    print(top)
    return top


def query_avg_area_by_month(conn):
    query = """
        SELECT Month, AVG(Burned_Area_hectares) AS avg_area
        FROM forestfires
        GROUP BY Month
        ORDER BY avg_area DESC;
    """
    result = pd.read_sql_query(query, conn)
    print("\n🧾 Average Burned Area by Month (SQL Query Result):")
    print(result)
    return result


def run_pipeline():
    print("=== 🌲 Forest Fire Analysis Pipeline Started ===\n")

    # Load dataset (amazon.csv)
    df = pd.read_csv(r"C:\Users\Soham Khule\Downloads\amazon.csv", encoding='latin1')
    print(f"✅ Loaded dataset with {len(df)} rows and {len(df.columns)} columns.\n")

    # Rename columns to match analysis logic
    df = df.rename(columns={
        "month": "Month",
        "number": "Burned_Area_hectares"
    })

    # Save dataset to SQLite DB
    conn = sqlite3.connect("forestfires.db")
    df.to_sql("forestfires", conn, if_exists="replace", index=False)
    print("✅ Data saved to SQLite database.\n")

    # Run Analytics
    run_mapreduce(df)
    top_fire_months(df)
    query_avg_area_by_month(conn)

    print("\n=== ✅ Pipeline Completed Successfully ===")


if __name__ == "__main__":
    run_pipeline()






# Original 


from multiprocessing import Pool
import pandas as pd
import sqlite3


def mapper(row):
    return (row["Month"], row["Temperature_Celsius"])


def reducer(mapped_data):
    result = {}
    for month, temp in mapped_data:
        result.setdefault(month, []).append(temp)
    return {m: sum(v) / len(v) for m, v in result.items()}


def run_mapreduce(df):
    with Pool() as p:
        mapped = p.map(mapper, [row for _, row in df.iterrows()])
    reduced = reducer(mapped)

    print("\n🌡️ Average Temperature per Month:")
    for m, t in reduced.items():
        print(f"{m}: {t:.2f}")
    return reduced


def top_fire_months(df, top_n=5):
    top = df.groupby("Month")["Burned_Area_hectares"].mean().sort_values(ascending=False).head(top_n)
    print(f"\n🔥 Top {top_n} Months with Largest Fire Area:\n{top}\n")
    return top


def temperature_area_correlation(df):
    corr = df["Temperature_Celsius"].corr(df["Burned_Area_hectares"])
    print(f"📊 Correlation between Temperature and Fire Area: {corr:.2f}")
    return corr


def query_avg_area_by_month(conn):
    query = """
        SELECT Month, AVG(Burned_Area_hectares) AS avg_area
        FROM forestfires
        GROUP BY Month
        ORDER BY avg_area DESC;
    """
    result = pd.read_sql_query(query, conn)
    print("\n🧾 Average Burned Area by Month (from SQL):")
    print(result)
    return result


def run_pipeline():
    print("=== 🌲 Forest Fire Analysis Pipeline Started ===\n")

    df = pd.read_csv("forestfires.csv")
    print(f"✅ Loaded dataset with {len(df)} rows and {len(df.columns)} columns.")

    conn = sqlite3.connect("forestfires.db")
    df.to_sql("forestfires", conn, if_exists="replace", index=False)
    print("✅ Data saved to SQLite database.\n")

    run_mapreduce(df)
    top_fire_months(df)
    temperature_area_correlation(df)
    query_avg_area_by_month(conn)

    print("\n=== ✅ Pipeline Completed Successfully ===")


if __name__ == "__main__":
    run_pipeline()

