import mysql.connector
import pandas as pd

# Step 1: MySQL connection establish karo
connection = mysql.connector.connect(
    host="localhost",      # same as in SQLTools
    user="root",           # your MySQL username
    password="PMpranav@1228",  # your MySQL password
    database="coffee_india"  # your database name
)

# Step 2: Check connection
if connection.is_connected():
    print("✅ Connected to MySQL Database successfully!")

# Step 3: Run SQL Query and load data in pandas DataFrame
query = "SELECT * FROM sales;"   # Example: sales table se data fetch karna
df = pd.read_sql(query, connection)

# Step 4: Show data
print(df.head())

# Step 5: Close connection
connection.close()

from sqlalchemy import create_engine
import pandas as pd

# Corrected password encoding
engine = create_engine("mysql+pymysql://root:PMpranav%401228@localhost/coffee_india")

# Load table from MySQL
df = pd.read_sql("SELECT * FROM sales", engine)

# Display first few rows
print(df.head())

# Load summary or full sales table (careful with memory)
sales = pd.read_sql("SELECT * FROM sales", engine, parse_dates=['sale_date'])
stores = pd.read_sql("SELECT * FROM stores", engine)
products = pd.read_sql("SELECT * FROM products", engine)
cities = pd.read_sql("SELECT * FROM cities", engine)

df = sales.merge(stores[['store_id','city_id']], on='store_id', how='left') \
          .merge(cities[['city_id','city_name']], on='city_id', how='left') \
          .merge(products[['product_id','product_name','category']], on='product_id', how='left')

df['month'] = df['sale_date'].dt.to_period('M')

# Step 1: Define city_rev
city_rev = df.groupby('city_name')['total_amount'].sum().sort_values(ascending=False).reset_index()

# Step 2: Get top 5 cities
top5 = city_rev.head(5)['city_name'].tolist()
print("Top 5 Cities by Revenue:", top5)

# Step 3: Monthly trend for top 5 cities
monthly = df[df['city_name'].isin(top5)].groupby(['month','city_name'])['total_amount'].sum().unstack()
print("\nMonthly Sales Data (Top 5 Cities):")
print(monthly)

# Step 4: Plot
monthly.plot(figsize=(12,6), title='Monthly Sales - Top 5 Cities')

# Step 5: Close DB connection
print("✅ MySQL connection closed.")

prod_cat = df.groupby('category').agg({'qty':'sum','total_amount':'sum'}).sort_values('total_amount', ascending=False)
prod_cat
print("\nProduct Category Summary:")
print(prod_cat)

# Create price bands
df['price_band'] = pd.cut(df['unit_price'], bins=[0,50,100,150,300,1000], labels=['<50','50-100','100-150','150-300','300+'])
price_band = df.groupby('price_band')['total_amount'].sum().sort_values(ascending=False)
price_band
print("\nPrice Band Summary:")
print(price_band)

import datetime as dt
snapshot_date = df['sale_date'].max() + dt.timedelta(days=1)
rfm = df.groupby('customer_id').agg({
    'sale_date': lambda x: (snapshot_date - x.max()).days,
    'sale_id': 'nunique',
    'total_amount': 'sum'
}).rename(columns={'sale_date':'recency', 'sale_id':'frequency','total_amount':'monetary'})

rfm.head()
print("\nRFM Analysis:")
print(rfm.head())

#Test if average ticket differs between City A and City B (t-test):
from scipy import stats
a = df[df['city_name']=='Mumbai']['total_amount']
b = df[df['city_name']=='Bengaluru']['total_amount']
tstat, pval = stats.ttest_ind(a, b, equal_var=False)
tstat, pval
print(f"\nT-test between Mumbai and Bengaluru: t-statistic={tstat}, p-value={pval}")

#Save cleaned CSV for Excel / Power BI
df.to_csv('cleaned_sales_flat.csv', index=False)
# Step 5: Close DB connection
engine.dispose()