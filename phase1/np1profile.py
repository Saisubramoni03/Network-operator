import pandas as pd

file_path = "../data/raw/sms-call-internet-mi-2013-11-01.csv"

df = pd.read_csv(file_path)

print(df.shape)
print(df.columns)
print(df.dtypes)

print(df.head())
print(df.columns.tolist())

#Rename to canonical names
column_mapping = {
    "datetime": "timestamp",
    "CellID": "grid_id",
    "countrycode": "country_code",
    "smsin": "sms_in",
    "smsout": "sms_out",
    "callin": "call_in",
    "callout": "call_out",
    "internet": "internet_activity"
}

df = df.rename(columns=column_mapping)

print(df.columns.tolist())
#Convert the timestamp
df["timestamp"] = pd.to_datetime(df["timestamp"])

print(df["timestamp"].min())
print(df["timestamp"].max())
print(df["timestamp"].nunique())

timestamps = sorted(df["timestamp"].dropna().unique())

print("Number of timestamps:", len(timestamps))

for i in range(1, len(timestamps)):
    difference = timestamps[i] - timestamps[i - 1]
    print(difference)

time_diff = pd.Series(timestamps).diff().dropna()

print(time_diff.value_counts())

assert len(timestamps) == 24
assert all(time_diff == pd.Timedelta(hours=1))


df["date"] = df["timestamp"].dt.date
df["hour"] = df["timestamp"].dt.hour
df["day_of_week"] = df["timestamp"].dt.dayofweek

print(df[["timestamp", "date", "hour", "day_of_week"]].head())


print(df.isnull().sum())


duplicate_count = df.duplicated().sum()

print("Exact duplicate rows:", duplicate_count)


#Check negative activity
activity_columns = [
    "sms_in",
    "sms_out",
    "call_in",
    "call_out",
    "internet_activity"
]

for column in activity_columns:
    negative_count = (df[column] < 0).sum()
    print(column, negative_count)


example = df[
    (df["grid_id"] == 4821) &
    (df["timestamp"] == df["timestamp"].iloc[0])
]
print(example)


print(df["grid_id"].min())
print(df["grid_id"].max())
print(df["grid_id"].nunique())


invalid_grids = df[
    (df["grid_id"] < 1) |
    (df["grid_id"] > 10000)
]

print("Invalid grids:", len(invalid_grids))

df["total_sms"] = df["sms_in"] + df["sms_out"]
df["total_calls"] = df["call_in"] + df["call_out"]

df["total_activity"] = (
    df["total_sms"]
    + df["total_calls"]
    + df["internet_activity"]
)

profiling_facts = {
    "unique_grids": df["grid_id"].nunique(),
    "time_range": (df["timestamp"].min(), df["timestamp"].max()),
    "cadence_hours": 24,
    "countrycode_categories": df["country_code"].nunique(),
    "busiest_hour": df.groupby("hour")["total_activity"].sum().idxmax(),
    "busiest_grid": df.groupby("grid_id")["total_activity"].sum().idxmax(),
    "null_counts": df.isnull().sum().to_dict(),
}

for k, v in profiling_facts.items():
    print(f"{k}: {v}")


    



