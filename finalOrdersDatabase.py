# Import required libraries
import requests
import pyodbc
import datetime

# Amazon SP-API credentials
import os

# Get the secret from environment variables
amazon_oauth_client_id = os.getenv("AMAZON_OAUTH_CLIENT_ID")
TOKEN_ENDPOINT = 'https://api.amazon.com/auth/o2/token'
ORDERS_ENDPOINT = "https://sellingpartnerapi-na.amazon.com/orders/v0/orders"
MARKETPLACE_ID = 'ATVPDKIKX0DER'

# Database connection details
server = '.'
database = 'ApiDatabase'  # Your database name
DB_CONNECTION_STRING = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;'

# Function to get a new access token using the refresh token
def get_access_token():
    data = {
        'grant_type': 'refresh_token',
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET,
        'refresh_token': REFRESH_TOKEN
    }
    response = requests.post(TOKEN_ENDPOINT, data=data)
    if response.status_code == 200:
        token_data = response.json()
        print(f"\n--- Amazon SP-API Tokens ---")
        print(f"Refresh Token: {REFRESH_TOKEN}")
        print(f"Bearer Token (Access Token): {token_data['access_token']}\n")
        return token_data['access_token']
    else:
        raise Exception(f"Failed to obtain access token: {response.text}")

# Function to create the Orders table if it doesn't exist
def create_orders_table_if_not_exists():
    create_table_query = """
    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'Orders' AND type = 'U')
    BEGIN
        CREATE TABLE Orders (
            AmazonOrderId NVARCHAR(50) PRIMARY KEY,
            PurchaseDate DATETIME,
            OrderStatus NVARCHAR(50),
            OrderTotalAmount DECIMAL(18, 2),
            CurrencyCode NVARCHAR(10),
            ShipServiceLevel NVARCHAR(100),
            BuyerEmail NVARCHAR(255),
            ShippingAddress1 NVARCHAR(255),
            City NVARCHAR(100),
            StateOrRegion NVARCHAR(100),
            PostalCode NVARCHAR(20),
            CountryCode NVARCHAR(10),
            ChangeDate DATETIME
        )
    END
    """

    # Database connection
    with pyodbc.connect(DB_CONNECTION_STRING) as conn:
        cursor = conn.cursor()
        cursor.execute(create_table_query)
        cursor.commit()
        print("Orders table created or already exists.")
        
# Function to fetch and display orders and insert into database (with pagination)
def fetch_and_store_orders(access_token):
    headers = {
        'x-amz-access-token': access_token,
        'Content-Type': 'application/json'
    }

    params = {
        "MarketplaceIds": MARKETPLACE_ID,
        "CreatedAfter": "2023-12-31",  # Example date; adjust as necessary
        "MaxResultsPerPage": 100
    }

    next_token = None
    has_next = True  # To control the loop for pagination

    while has_next:
        if next_token:
            # If we have a NextToken, update the params to use it
            params['NextToken'] = next_token

        print("Fetching orders from Amazon SP-API...\n")
        
        response = requests.get(ORDERS_ENDPOINT, headers=headers, params=params)
        
        if response.status_code == 200:
            order_data = response.json()
            print("Orders fetched successfully.\n")

            # Print out the order details as provided in the JSON response
            print("--- Amazon Order Details ---\n")
            orders = order_data.get('payload', {}).get('Orders', [])

            if not orders:
                print("No more orders found.")
                has_next = False
                break  # Exit loop if no orders are returned

            # Database connection
            with pyodbc.connect(DB_CONNECTION_STRING) as conn:
                cursor = conn.cursor()

                for order in orders:
                    print(f"Amazon Order ID: {order.get('AmazonOrderId')}")
                    print(f"Purchase Date: {order.get('PurchaseDate')}")
                    print(f"Order Status: {order.get('OrderStatus')}")
                    print(f"Order Total: {order.get('OrderTotal', {}).get('Amount')} {order.get('OrderTotal', {}).get('CurrencyCode')}")
                    print(f"Buyer Email: {order.get('BuyerInfo', {}).get('BuyerEmail')}")
                    print(f"Shipping Address: {order.get('ShippingAddress', {}).get('City')}, {order.get('ShippingAddress', {}).get('StateOrRegion')}, {order.get('ShippingAddress', {}).get('CountryCode')}")
                    print(f"Ship Service Level: {order.get('ShipServiceLevel')}")
                    print(f"Last Update Date: {order.get('LastUpdateDate')}")
                    print("-" * 60)

                    # Inserting the order into the database
                    insert_query = """
                        INSERT INTO Orders (
                            AmazonOrderId, PurchaseDate, OrderStatus, OrderTotalAmount, CurrencyCode, ShipServiceLevel,
                            BuyerEmail, ShippingAddress1, City, StateOrRegion, PostalCode, CountryCode, ChangeDate
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """
                    values = (
                        order.get('AmazonOrderId'),
                        order.get('PurchaseDate'),
                        order.get('OrderStatus'),
                        order.get('OrderTotal', {}).get('Amount'),
                        order.get('OrderTotal', {}).get('CurrencyCode'),
                        order.get('ShipServiceLevel'),
                        order.get('BuyerInfo', {}).get('BuyerEmail'),
                        order.get('ShippingAddress', {}).get('AddressLine1'),
                        order.get('ShippingAddress', {}).get('City'),
                        order.get('ShippingAddress', {}).get('StateOrRegion'),
                        order.get('ShippingAddress', {}).get('PostalCode'),
                        order.get('ShippingAddress', {}).get('CountryCode'),
                        datetime.datetime.now()
                    )

                    cursor.execute(insert_query, values)
                    cursor.commit()
                    print(f"Order {order.get('AmazonOrderId')} inserted successfully.\n")

            # Check for the next token to continue pagination
            next_token = order_data.get('payload', {}).get('NextToken', None)
            if not next_token:
                has_next = False  # Stop loop if no next token is available
        else:
            print(f"Failed to fetch orders: {response.text}")
            break  # Exit the loop if there is an error fetching orders

if __name__ == '__main__':
    try:
        # Step 1: Get access token
        print("Fetching access token...")
        access_token = get_access_token()
        
        # Step 2: Create Orders table if it doesn't exist
        create_orders_table_if_not_exists()
        
        # Step 3: Fetch and store orders using the access token
        fetch_and_store_orders(access_token)
        
    except Exception as e:
        print(f"An error occurred: {str(e)}")
