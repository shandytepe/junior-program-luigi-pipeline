import luigi
from helper.db_connection import mysql_connection
import json
import pandas as pd
from dotenv import load_dotenv
import os
import http.client

load_dotenv()

TOKEN_NOCODB = os.getenv("TOKEN_NOCODB")
CUSTOMERS_TABLE_ID = os.getenv("CUSTOMERS_TABLE_ID")
PRODUCTS_TABLE_ID = os.getenv("PRODUCTS_TABLE_ID")
TRANSACTIONS_TABLE_ID = os.getenv("TRANSACTIONS_TABLE_ID")
TRANSACTION_DETAILS_TABLE_ID = os.getenv("TRANSACTION_DETAILS_TABLE_ID")

class ExtractCustomersNocoDB(luigi.Task):

    table_name = "Customers"

    # jika membutuhkan task dari sebelumnya
    # kita perlu mengisi dengan nama task
    def requires(self):
        pass

    def run(self):

        # sesuaikan dengan token headers masing - masing
        headers = { 'xc-auth': TOKEN_NOCODB }

        # set connection
        conn = http.client.HTTPSConnection("app.nocodb.com")

        url = f"/api/v1/db/data/noco/pm8tgw3cbiw85ob/{self.table_name}/views/{CUSTOMERS_TABLE_ID}?offset=0&limit=10000&"

        # Kirim permintaan GET ke API NoCodeDB
        conn.request("GET", url, headers=headers)

        res = conn.getresponse()
        data = res.read()
        dataframes = pd.DataFrame(json.loads(data.decode("utf-8"))['list'])

        print(f"Data dari table {self.table_name}")
        print(dataframes)

        print(f"Simpan output table {self.table_name} dalam bentuk .csv")

        # simpan output ke dalam bentuk csv
        dataframes.to_csv(self.output().path, index = False)

    def output(self):
        return luigi.LocalTarget(f"data/raw/extract_{self.table_name}_data.csv")

class ExtractProductsNocoDB(luigi.Task):

    table_name = "Products"

    # jika membutuhkan task dari sebelumnya
    # kita perlu mengisi dengan nama task
    def requires(self):
        pass

    def run(self):

        # sesuaikan dengan token headers masing - masing
        headers = { 'xc-auth': TOKEN_NOCODB }

        # set connection
        conn = http.client.HTTPSConnection("app.nocodb.com")

        url = f"/api/v1/db/data/noco/pm8tgw3cbiw85ob/{self.table_name}/views/{PRODUCTS_TABLE_ID}?offset=0&limit=10000&"

        # Kirim permintaan GET ke API NoCodeDB
        conn.request("GET", url, headers=headers)

        res = conn.getresponse()
        data = res.read()
        dataframes = pd.DataFrame(json.loads(data.decode("utf-8"))['list'])

        print(f"Data dari table {self.table_name}")
        print(dataframes)

        print(f"Simpan output table {self.table_name} dalam bentuk .csv")

        # simpan output ke dalam bentuk csv
        dataframes.to_csv(self.output().path, index = False)

    def output(self):
        return luigi.LocalTarget(f"data/raw/extract_{self.table_name}_data.csv")

class ExtractTransactionsNocoDB(luigi.Task):

    table_name = "Transactions"

    # jika membutuhkan task dari sebelumnya
    # kita perlu mengisi dengan nama task
    def requires(self):
        pass

    def run(self):

        # sesuaikan dengan token headers masing - masing
        headers = { 'xc-auth': TOKEN_NOCODB }

        # set connection
        conn = http.client.HTTPSConnection("app.nocodb.com")

        url = f"/api/v1/db/data/noco/pm8tgw3cbiw85ob/{self.table_name}/views/{TRANSACTIONS_TABLE_ID}?offset=0&limit=10000&"

        # Kirim permintaan GET ke API NoCodeDB
        conn.request("GET", url, headers=headers)

        res = conn.getresponse()
        data = res.read()
        dataframes = pd.DataFrame(json.loads(data.decode("utf-8"))['list'])

        print(f"Data dari table {self.table_name}")
        print(dataframes)

        print(f"Simpan output table {self.table_name} dalam bentuk .csv")

        # simpan output ke dalam bentuk csv
        dataframes.to_csv(self.output().path, index = False)

    def output(self):
        return luigi.LocalTarget(f"data/raw/extract_{self.table_name}_data.csv")

class ExtractTransactionDetailsNocoDB(luigi.Task):

    table_name = "Transaction_Details"

    # jika membutuhkan task dari sebelumnya
    # kita perlu mengisi dengan nama task
    def requires(self):
        pass

    def run(self):

        # sesuaikan dengan token headers masing - masing
        headers = { 'xc-auth': TOKEN_NOCODB }

        # set connection
        conn = http.client.HTTPSConnection("app.nocodb.com")

        url = f"/api/v1/db/data/noco/pm8tgw3cbiw85ob/{self.table_name}/views/{TRANSACTION_DETAILS_TABLE_ID}?offset=0&limit=10000&"

        # Kirim permintaan GET ke API NoCodeDB
        conn.request("GET", url, headers=headers)

        res = conn.getresponse()
        data = res.read()
        dataframes = pd.DataFrame(json.loads(data.decode("utf-8"))['list'])

        print(f"Data dari table {self.table_name}")
        print(dataframes)

        print(f"Simpan output table {self.table_name} dalam bentuk .csv")

        # simpan output ke dalam bentuk csv
        dataframes.to_csv(self.output().path, index = False)

    def output(self):
        return luigi.LocalTarget(f"data/raw/extract_{self.table_name}_data.csv")

class ValidateCustomersData(luigi.Task):
    
    table_name = "Customers"
    
    def requires(self):
        return ExtractCustomersNocoDB()

    def run(self):
        data = pd.read_csv(self.input().path)

        print(f"Check Columns name in table {self.table_name}")
        print(data.columns)

        print("\n")
        print(f"Check data types in table {self.table_name}")
        print(data.dtypes)

        print("\n")
        print(f"Check missing values in table {self.table_name}")
        print(data.isnull().sum())

        print("\n")
        print(f"Check duplicated values in table {self.table_name}")
        is_duplicated = data.duplicated().any()

        if is_duplicated == False:
            print(f"Data di table {self.table_name} tidak ada yang duplikat")

        else:
            print("Terdapat data yang duplikat")

        data.to_csv(self.output().path, index = False)

    def output(self):
        return luigi.LocalTarget(f"data/validation/validation_{self.table_name}_data.csv")
    
class ValidateProductsData(luigi.Task):
    
    table_name = "Products"
    
    def requires(self):
        return ExtractProductsNocoDB()

    def run(self):
        data = pd.read_csv(self.input().path)

        print(f"Check Columns name in table {self.table_name}")
        print(data.columns)

        print("\n")
        print(f"Check data types in table {self.table_name}")
        print(data.dtypes)

        print("\n")
        print(f"Check missing values in table {self.table_name}")
        print(data.isnull().sum())

        print("\n")
        print(f"Check duplicated values in table {self.table_name}")
        is_duplicated = data.duplicated().any()

        if is_duplicated == False:
            print(f"Data di table {self.table_name} tidak ada yang duplikat")

        else:
            print("Terdapat data yang duplikat")

        data.to_csv(self.output().path, index = False)

    def output(self):
        return luigi.LocalTarget(f"data/validation/validation_{self.table_name}_data.csv")
    
class ValidateTransactionsData(luigi.Task):
    
    table_name = "Transactions"
    
    def requires(self):
        return ExtractTransactionsNocoDB()

    def run(self):
        data = pd.read_csv(self.input().path)

        print(f"Check Columns name in table {self.table_name}")
        print(data.columns)

        print("\n")
        print(f"Check data types in table {self.table_name}")
        print(data.dtypes)

        print("\n")
        print(f"Check missing values in table {self.table_name}")
        print(data.isnull().sum())

        print("\n")
        print(f"Check duplicated values in table {self.table_name}")
        is_duplicated = data.duplicated().any()

        if is_duplicated == False:
            print(f"Data di table {self.table_name} tidak ada yang duplikat")

        else:
            print("Terdapat data yang duplikat")

        data.to_csv(self.output().path, index = False)

    def output(self):
        return luigi.LocalTarget(f"data/validation/validation_{self.table_name}_data.csv")
    
class ValidateTransactionDetailsData(luigi.Task):
    
    table_name = "Transaction_Details"
    
    def requires(self):
        return ExtractTransactionDetailsNocoDB()

    def run(self):
        data = pd.read_csv(self.input().path)

        print(f"Check Columns name in table {self.table_name}")
        print(data.columns)

        print("\n")
        print(f"Check data types in table {self.table_name}")
        print(data.dtypes)

        print("\n")
        print(f"Check missing values in table {self.table_name}")
        print(data.isnull().sum())

        print("\n")
        print(f"Check duplicated values in table {self.table_name}")
        is_duplicated = data.duplicated().any()

        if is_duplicated == False:
            print(f"Data di table {self.table_name} tidak ada yang duplikat")

        else:
            print("Terdapat data yang duplikat")

        data.to_csv(self.output().path, index = False)

    def output(self):
        return luigi.LocalTarget(f"data/validation/validation_{self.table_name}_data.csv")
    
class TransformCustomersData(luigi.Task):
    
    table_name = "Customers"
    
    def requires(self):
        return ValidateCustomersData()

    def run(self):
        # read data from previous task
        data = pd.read_csv(self.input().path)

        # convert all columns into lower case
        data.columns = data.columns.str.lower()

        # reorder and select columns
        SELECTED_COLS = ["id", "first_name", "last_name",
                         "phone", "address", "city",
                         "postalcode", "country"]

        data = data[SELECTED_COLS]

        # save the output
        data.to_csv(self.output().path, index = False)

    def output(self):
        return luigi.LocalTarget(f"data/transform/transform_{self.table_name}_data.csv")
    
class TransformProductsData(luigi.Task):

    # initiate two table
    table_name_1 = "Products"
    table_name_2 = "product_category"

    def requires(self):
        return ValidateProductsData()

    def run(self):
        # read data from previous task
        data = pd.read_csv(self.input().path)

        # convert all columns into lower case
        data.columns = data.columns.str.lower()

        # rename `category` to `product_category_id`
        RENAME_COLS = {
            "category": "product_category_id"
        }

        data = data.rename(columns = RENAME_COLS)

        # create new table based on `product_category_id`
        get_product_category = data["product_category_id"].drop_duplicates().reset_index(drop=True)

        product_category_data = pd.DataFrame({
            "id": get_product_category.index + 1,
            "name": get_product_category
        })

        # mapping the value in `product_category_id` using 
        # product_category_data
        data["product_category_id"] = data["product_category_id"].map(product_category_data.set_index("name")["id"])

        # select and reorder columns
        SELECTED_COLS = ["id", "name", "product_category_id",
                         "scale", "vendor_name", "description",
                         "quantity_stock", "price"]

        data = data[SELECTED_COLS]

        # save the output
        data.to_csv(self.output()[0].path, index = False)
        product_category_data.to_csv(self.output()[1].path, index = False)


    def output(self):
        return [luigi.LocalTarget(f"data/transform/transform_{self.table_name_1}_data.csv"), # for Products table
                luigi.LocalTarget(f"data/transform/transform_{self.table_name_2}_data.csv")] # for product_category table
    
class TransformTransactionsData(luigi.Task):
    
    table_name = "Transactions"

    def requires(self):
        return ValidateTransactionsData()

    def run(self):
        # read data
        data = pd.read_csv(self.input().path)

        # convert all columns into lower case
        data.columns = data.columns.str.lower()

        # rename columns name
        RENAME_COLS = {
            "customers_id": "customer_id"
        }

        data = data.rename(columns = RENAME_COLS)

        # select and reorder columns
        SELECTED_COLS = ["id", "transaction_date",
                         "status", "customer_id"]

        data = data[SELECTED_COLS]

        # save the output
        data.to_csv(self.output().path, index = False)

    def output(self):
        return luigi.LocalTarget(f"data/transform/transform_{self.table_name}_data.csv")
    
class TransformTransactionDetailsData(luigi.Task):
    
    table_name = "Transaction_Details"

    def requires(self):
        return ValidateTransactionDetailsData()

    def run(self):
        # read data from previous task
        data = pd.read_csv(self.input().path)

        # convert all columns name into lowercase
        data.columns = data.columns.str.lower()

        # rename columns name
        RENAME_COLS = {
            "transactions_id": "transaction_id",
            "products_id": "product_id"
        }

        data = data.rename(columns = RENAME_COLS)

        # select and reorder columns
        SELECTED_COLS = ["id", "quantity",
                         "transaction_id", "product_id"]

        data = data[SELECTED_COLS]

        # save the output
        data.to_csv(self.output().path, index = False)

    def output(self):
        return luigi.LocalTarget(f"data/transform/transform_{self.table_name}_data.csv")
    
class LoadData(luigi.Task):
    def requires(self):
        # we will use all previous task
        return [TransformCustomersData(),
                TransformProductsData(),
                TransformTransactionsData(),
                TransformTransactionDetailsData()]

    def run(self):
        # create engine
        engine = mysql_connection()
        
        # order to read the data:
        # 1. customer
        # 2. product_category
        # 3. product
        # 4. transaction
        # 5. transaction_detail

        customer_data = pd.read_csv(self.input()[0].path)
        product_category_data = pd.read_csv(self.input()[1][1].path)
        product_data = pd.read_csv(self.input()[1][0].path)
        transaction_data = pd.read_csv(self.input()[2].path)
        transaction_detail_data = pd.read_csv(self.input()[3].path)

        # load to database
        print("Start Load data to Database...")

        customer_data.to_sql(name = "customers",
                             con = engine,
                             if_exists = "append",
                             index = False)
        
        product_category_data.to_sql(name = "product_category",
                                     con = engine,
                                     if_exists = "append",
                                     index = False)
        
        product_data.to_sql(name = "products",
                            con = engine,
                            if_exists = "append",
                            index = False)
        
        transaction_data.to_sql(name = "transactions",
                                con = engine,
                                if_exists = "append",
                                index = False)        
        transaction_detail_data.to_sql(name = "transaction_details",
                                       con = engine,
                                       if_exists = "append",
                                       index = False)

        print("Finish Load data to Database...")
    
    def output(self):
        pass

