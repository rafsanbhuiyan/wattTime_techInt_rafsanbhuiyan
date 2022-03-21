import pandas as pd

#DATA INGESTION

#Reading input data files from local machine
gppd_df = pd.read_csv("/Users/rafsanbhuiyan/Documents/GitHub/wattTime_techInt_rafsanbhuiyan/data/gppd.csv")
platts_df = pd.read_csv("/Users/rafsanbhuiyan/Documents/GitHub/wattTime_techInt_rafsanbhuiyan/data/platts.csv")
entso_df = pd.read_csv("/Users/rafsanbhuiyan/Documents/GitHub/wattTime_techInt_rafsanbhuiyan/data/entso.csv")

#DATA TRANSFORMATION

#Create a function to transform string values of column in a dataframe into upper case
#First paramenter is the Dataframe name and the second parameter is the column name
def df_col_toupper(df, col):

    #Using apply function with paramenter str.upper to transform string values into upper case
    df[col] = df[col].apply(str.upper)

    return df[col]

#Transform string values from columns plant_name, country_long of gppd_df dataframe into upper case
df_col_toupper(gppd_df,"plant_name")
df_col_toupper(gppd_df,"country_long")
df_col_toupper(entso_df,"country")

#Create dictionary for the column names  and the new names
col_dict = {'unit_fuel' : 'plant_primary_fuel', 'country' : 'country_long'}

#Rename column names using the rename function
platts_df.rename(columns=col_dict, inplace=True)

#Creating funciton to add plant_first_name column to dataframe mentioned in the parameter
def add_first_name_col(df):
    st = df["plant_name"].str.split(" ", n=2, expand=True)
    df["plant_first_name"] = st[0]

    return  df["plant_first_name"]

#Adding firstname column to dataframes: gppd_df, platts_df, entso_df
add_first_name_col(gppd_df)
add_first_name_col(platts_df)
add_first_name_col(entso_df)

#JOINING gppd_df and platts_df together on name, country and fuel type
join_one_df = gppd_df.merge(platts_df, left_on = ["plant_first_name", "country_long", "plant_primary_fuel"], right_on =["plant_first_name", "country_long", "plant_primary_fuel"], how = "inner")

#TRANSFORMING entso_df

#Using String Manipulation to eliminate country abbreviateions in perenthesis in country column
#split function implementation, n defines the numbers of splits
#expand = True allows the split string to separate columns

#Split into two strings
s1 = entso_df["country"].str.split(" ", n=2, expand=True)

#Extract the first string and assign to country
entso_df["country"] = s1[0]

#renaming columns in entso_df dataframe
col_dict2 = {"unit_fuel" : "plant_primary_fuel", "country" : "country_long"}
entso_df.rename(columns=col_dict2, inplace=True)

#LEFT JOINING entso_df with join_one dataframe
mapping = entso_df.merge(join_one_df, left_on=["unit_capacity","plant_primary_fuel", "country_long"], right_on=["unit_capacity","plant_primary_fuel","country_long"], how="inner")

print(mapping.info())

#Output mapping.csv file
mapping.to_csv("/Users/rafsanbhuiyan/Documents/GitHub/wattTime_techInt_rafsanbhuiyan/mapping.csv", index=False)

