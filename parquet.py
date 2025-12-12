import pandas as pd

pd.set_option('display.max_columns', None)

path = "Company_Census_File.csv"
parquet_output = "Company_Census_File.parquet"

print("Loading file...")
df = pd.read_csv(path, dtype=str)

keep = [
    "DOT_NUMBER", "LEGAL_NAME", "DBA_NAME",
    "COMPANY_OFFICER_1", "COMPANY_OFFICER_2",
    "TRUCK_UNITS", "POWER_UNITS",
    "TOTAL_CDL", "TOTAL_DRIVERS",
    "PHY_STREET", "PHY_CITY", "PHY_STATE", "PHY_ZIP", "PHY_CNTY",
    "EMAIL_ADDRESS",
    "CRGO_DRIVETOW"
]

df = df[keep].copy()

# Clean + coerce
df["TOTAL_DRIVERS"] = pd.to_numeric(df["TOTAL_DRIVERS"], errors="coerce")

new_england_plus_ny = {"ME", "NH", "VT", "MA", "RI", "CT", "NY"}

df = df[
    (df["TOTAL_DRIVERS"] >= 5) &
    (df["CRGO_DRIVETOW"] == "X") &
    (df["PHY_STATE"].isin(new_england_plus_ny))
    ]

print("Rows after filtering:", len(df))
print("Saving Parquet file:", parquet_output)

df.to_parquet(parquet_output)

print("Done. Parquet created successfully.")
print(df.head())
