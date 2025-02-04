import pandas as pd
import requests
from io import StringIO
def fetch_csv(url):
    """Fetch CSV file from Dropbox and return as Pandas DataFrame."""
    response = requests.get(url)
    if response.status_code == 200:
        return pd.read_csv(StringIO(response.text), encoding='utf-8')
    else:
        raise Exception(f"Failed to download file from {url}, Status Code: {response.status_code}")

link_1 = 'https://www.dropbox.com/scl/fi/k4u1sazt93kz8ki49lk3j/transaction_data.csv?rlkey=a3yg9kd5uroevh86v6qprsn2r&raw=1'
link_2 = 'https://www.dropbox.com/scl/fi/u4dh2bkcr92nwf0ki4wih/purchase_behaviour.csv?rlkey=i6ofyygk8txiwuptaq32v8ap6&raw=1'
try:

    transaction_data = fetch_csv(link_1)
    customer_data = fetch_csv(link_2)

    print("Columns in transaction_data.csv:", list(transaction_data.columns))
    print("Columns in purchase_behaviour.csv:", list(customer_data.columns))

    if 'LYLTY_CARD_NBR' not in transaction_data.columns or 'LYLTY_CARD_NBR' not in customer_data.columns:
        raise Exception("Error: Column 'LYLTY_CARD_NBR' not found in one of the files.")

    merged_data = pd.merge(transaction_data, customer_data, on='LYLTY_CARD_NBR', how='outer')


    print("Merged Data Sample:\n", merged_data.head())
    merged_data.fillna({'PROD_NAME': 'Unknown', 'TOT_SALES': 0, 'PREMIUM_CUSTOMER': 'Unknown'}, inplace=True)
    if 'TOT_SALES' in merged_data.columns:
        top_products = merged_data.groupby('PROD_NAME')['TOT_SALES'].sum().reset_index()
        top_products = top_products.sort_values(by='TOT_SALES', ascending=False).head(3)
        print("\nTop 3 Most Profitable Products:\n", top_products)
    else:
        print("Error: Column 'TOT_SALES' not found in the dataset.")
    if {'LYLTY_CARD_NBR', 'LIFESTAGE', 'PREMIUM_CUSTOMER'}.issubset(merged_data.columns):
        loyal_customers = merged_data.groupby(['LIFESTAGE', 'PREMIUM_CUSTOMER'])['LYLTY_CARD_NBR'].nunique().reset_index()
        loyal_customers = loyal_customers.sort_values(by='LYLTY_CARD_NBR', ascending=False).head(3)
        print("\nTop 3 Most Loyal Customer Segments:\n", loyal_customers)
    else:
        print("Error: Required customer segment columns not found.")


    print("\nHypothesis on Customer Loyalty:")
    print("1. Customers from certain life stages (e.g., families) may have repeat purchases due to household needs.")
    print("2. Premium customers likely purchase high-end products, contributing more to revenue.")
    print("3. Budget buyers may purchase frequently in smaller amounts, making them key repeat customers.")

except pd.errors.ParserError:
    print("Error parsing CSV files. Please check the file format and try again.")
except Exception as e:
    print(f"Error: {e}")
