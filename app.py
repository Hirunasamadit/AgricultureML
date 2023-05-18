import os
from datetime import datetime, timedelta
from typing import List

import motor.motor_asyncio
import pandas as pd
from bson import ObjectId
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from recommand import get_rec 

# show all columns
pd.set_option('display.max_columns', None)

app = FastAPI()

origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# load environment variables
from dotenv import load_dotenv

load_dotenv()

client = motor.motor_asyncio.AsyncIOMotorClient(os.environ["MONGODB_URL"])
db = client.agriculture_test

data_length = 100000


class PyObjectId(ObjectId):
    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, v):
        if not ObjectId.is_valid(v):
            raise ValueError("Invalid objectid")
        return ObjectId(v)

    @classmethod
    def __modify_schema__(cls, field_schema):
        field_schema.update(type="string")


# interaction
#     _id: { type: Schema.Types.ObjectId },
#     customerId: { type: Schema.Types.ObjectId, ref: "Customer" },
#     productId: { type: Schema.Types.ObjectId, ref: "Product" },
#     interactionType: { type: Number, enum: [1, 2, 3] }, // 1: click, 2: save, 3: unsave

class InteractionsModel(BaseModel):
    id: PyObjectId = Field(default_factory=PyObjectId, alias="_id")
    customerId: PyObjectId = Field(default_factory=PyObjectId)
    productId: PyObjectId = Field(default_factory=PyObjectId)
    interactionType: int = Field(..., gt=0, lt=4)

    class Config:
        allow_population_by_field_name = True
        arbitrary_types_allowed = True
        json_encoders = {ObjectId: str}
        schema_extra = {
            "example": {
                "_id": "5f9f1b9b0b9b9b9b9b9b9b9b",
                "customerId": "5f9f1b9b0b9b9b9b9b9b9b9b",
                "productId": "5f9f1b9b0b9b9b9b9b9b9b9b",
                "interactionType": 1
            }
        }


# List all foods
@app.get("/interactions", response_description="List all interactions", response_model=List[InteractionsModel])
async def list_interactions():
    interactions = await db["interactions"].find().to_list(data_length)
    return interactions


# customer
#     _id: { type: Schema.Types.ObjectId },
#     customerFirstName: { type: String },
#     customerLastName: { type: String },
#     email: { type: String },
#     customerPhoneNumber: { type: String },

class CustomerModel(BaseModel):
    id: PyObjectId = Field(default_factory=PyObjectId, alias="_id")
    customerFirstName: str = Field(...)
    customerLastName: str = Field(...)
    email: str = Field(...)
    customerPhoneNumber: str = Field(...)

    class Config:
        allow_population_by_field_name = True
        arbitrary_types_allowed = True
        json_encoders = {ObjectId: str}
        schema_extra = {
            "example": {
                "_id": "5f9f1b9b0b9b9b9b9b9b9b9b",
                "customerFirstName": "John",
                "customerLastName": "Doe",
                "email": "johndoe@gmail.com",
                "customerPhoneNumber": "+9423456789"
            }
        }


# List all customers
@app.get("/customers", response_description="List all customers", response_model=List[CustomerModel])
async def list_customers():
    customers = await db["customers"].find().to_list(data_length)
    return customers


# prodoutCatergories
#     _id: { type: Schema.Types.ObjectId },
#     productCatrgoryName: { type: String },
#     productCategoryId: { type: Number },

class ProductCategoriesModel(BaseModel):
    id: PyObjectId = Field(default_factory=PyObjectId, alias="_id")
    productCatrgoryName: str = Field(...)
    productCategoryId: int = Field(...)

    class Config:
        allow_population_by_field_name = True
        arbitrary_types_allowed = True
        json_encoders = {ObjectId: str}
        schema_extra = {
            "example": {
                "_id": "5f9f1b9b0b9b9b9b9b9b9b9b",
                "productCatrgoryName": "Fruits",
                "productCategoryId": 1
            }
        }


# List all product categories
@app.get("/product-categories", response_description="List all product categories",
         response_model=List[ProductCategoriesModel])
async def list_product_categories():
    product_categories = await db["prodoutCategory"].find().to_list(data_length)
    return product_categories


# products
#     _id: { type: Schema.Types.ObjectId },
#     productName: { type: String },
#     price: { type: Number },
#     productImage: { type: String },
#     description: { type: String },
#     productCatogoryId: { type: Schema.Types.ObjectId, ref: "FoodCatergory" }
#     AvailableQuantity: { type: Number },

class ProductsModel(BaseModel):
    id: PyObjectId = Field(default_factory=PyObjectId, alias="_id")
    productName: str = Field(...)
    price: float = Field(...)
    productImage: str = Field(...)
    description: str = Field(...)
    productCatogoryId: PyObjectId = Field(default_factory=PyObjectId)
    AvailableQuantity: int = Field(...)

    class Config:
        allow_population_by_field_name = True
        arbitrary_types_allowed = True
        json_encoders = {ObjectId: str}
        schema_extra = {
            "example": {
                "_id": "5f9f1b9b0b9b9b9b9b9b9b9b",
                "productName": "Apple",
                "price": 1.99,
                "productImage": "https://www.google.com",
                "description": "A red apple",
                "productCatogoryId": "5f9f1b9b0b9b9b9b9b9b9b9b",
                "AvailableQuantity": 100
            }
        }


# List all products
@app.get("/products", response_description="List all products", response_model=List[ProductsModel])
async def list_products():
    products = await db["products"].find().to_list(data_length)
    return products


base_url = "http://localhost:8000/"
save_path = "data/"


def analyze_data():
    # load csv
    df = pd.read_csv(save_path + "processed.csv")

    # data types
    print(df.dtypes)

    # null values
    print(df.isnull().sum())

    # duplicate values
    print(df.duplicated().sum())

    # unique values
    print(df.nunique())

    # describe
    print(df.describe())


def pre_process():
    # load csv
    df = pd.read_csv(save_path + "processed.csv")

    # remove column if 75% of the values are null
    df.dropna(thresh=len(df) * 0.25, axis=1, inplace=True)

    # remove null values
    df.dropna(inplace=True)

    # remove null values
    df.dropna(inplace=True)

    # remove duplicate values
    df.drop_duplicates(inplace=True)

    # save to csv
    df.to_csv(save_path + "pre_processed.csv", index=False)


def update_log():
    # create txt if not exists and write to it f.write("Last updated: " + datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
    if not os.path.exists(save_path + "log.txt"):
        f = open(save_path + "log.txt", "w")
        f.close()

    # open txt and clean write to it
    f = open(save_path + "log.txt", "w")
    f.write("Last updated: " + datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
    f.close()


def load_log():
    # load txt
    f = open(save_path + "log.txt", "r")
    read_data = f.read()
    print(read_data)
    f.close()

    # extract datetime
    new_datetime = read_data.split("Last updated: ")[1]

    # convert to datetime
    new_datetime = datetime.strptime(new_datetime, "%d/%m/%Y %H:%M:%S")

    # if last update was more than 1 hour ago
    if datetime.now() - new_datetime > timedelta(hours=1):
        return True
    else:
        return False


def aggregate_data():
    # load csv
    interactions = pd.read_csv(save_path + "interactions.csv")
    customers = pd.read_csv(save_path + "customers.csv")
    products = pd.read_csv(save_path + "products.csv")
    prodoutCategory = pd.read_csv(save_path + "prodoutCategory.csv")

    # merge dataframes
    df = pd.merge(interactions, customers, left_on="customerId", right_on="_id")
    # remove _id_y
    df.drop(columns=["_id_y"], inplace=True)
    # rename _id_x to _id
    df.rename(columns={"_id_x": "_id"}, inplace=True)

    df = pd.merge(df, products, left_on="productId", right_on="_id")
    df = pd.merge(df, prodoutCategory, left_on="productCatogoryId", right_on="_id")

    print(df.head())

    # save to csv
    df.to_csv(save_path + "aggregated.csv", index=False)


def process_data():
    # load csv
    df = pd.read_csv(save_path + "aggregated.csv")

    # remove columns
    df.drop(columns=["productCatogoryId", "_id", "AvailableQuantity", "productImage", "price", "_id_y",
                     "customerPhoneNumber", "email", "customerFirstName", "customerLastName"], inplace=True)

    # save to csv
    df.to_csv(save_path + "processed.csv", index=False)


# Recommendation
@app.get("/recommendation/{user_id}")
async def get_recommendation(user_id: str, num_of_rec: int = 5):
    if num_of_rec:
        recommendation, user_stat = get_rec(user_id, num_of_rec=num_of_rec)
    else:
        recommendation, user_stat = get_rec(user_id, num_of_rec=5)

    update_log()
    return {"recommendations": recommendation}


# Load data and Recommendation
@app.get("/recommendation-load/{user_id}")
async def get_recommendation_load(user_id: str, num_of_rec: int = 5):
    interactions = await db["interactions"].find().to_list(data_length)
    customers = await db["customers"].find().to_list(data_length)
    products = await db["products"].find().to_list(data_length)
    prodoutCategory = await db["prodoutCategory"].find().to_list(data_length)

    interactions = pd.DataFrame(interactions)
    customers = pd.DataFrame(customers)
    products = pd.DataFrame(products)
    prodoutCategory = pd.DataFrame(prodoutCategory)

    interactions.to_csv(save_path + "interactions.csv", index=False)
    customers.to_csv(save_path + "customers.csv", index=False)
    products.to_csv(save_path + "products.csv", index=False)
    prodoutCategory.to_csv(save_path + "prodoutCategory.csv", index=False)

    aggregate_data()
    process_data()
    pre_process()
    update_log()

    if num_of_rec:
        recommendation, user_stat = get_rec(user_id, num_of_rec=num_of_rec)
    else:
        recommendation, user_stat = get_rec(user_id, num_of_rec=5)
    return {"recommendations": recommendation}


# Load data
@app.get("/load-data")
async def load_data(request: Request):
    interactions = await db["interactions"].find().to_list(data_length)
    customers = await db["customers"].find().to_list(data_length)
    products = await db["products"].find().to_list(data_length)
    prodoutCategory = await db["prodoutCategory"].find().to_list(data_length)

    interactions = pd.DataFrame(interactions)
    customers = pd.DataFrame(customers)
    products = pd.DataFrame(products)
    prodoutCategory = pd.DataFrame(prodoutCategory)

    interactions.to_csv(save_path + "interactions.csv", index=False)
    customers.to_csv(save_path + "customers.csv", index=False)
    products.to_csv(save_path + "products.csv", index=False)
    prodoutCategory.to_csv(save_path + "prodoutCategory.csv", index=False)

    aggregate_data()
    process_data()
    pre_process()
    update_log()

    return {"status": "success"}


# Load data if last update is more than 1 hour and Recommendation
@app.get("/recommendation-load-update/{user_id}")
async def get_recommendation_load_update(user_id: str, num_of_rec: int = 5):
    if load_log():
        interactions = await db["interactions"].find().to_list(data_length)
        customers = await db["customers"].find().to_list(data_length)
        products = await db["products"].find().to_list(data_length)
        prodoutCategory = await db["prodoutCategory"].find().to_list(data_length)

        interactions = pd.DataFrame(interactions)
        customers = pd.DataFrame(customers)
        products = pd.DataFrame(products)
        prodoutCategory = pd.DataFrame(prodoutCategory)

        interactions.to_csv(save_path + "interactions.csv", index=False)
        customers.to_csv(save_path + "customers.csv", index=False)
        products.to_csv(save_path + "products.csv", index=False)
        prodoutCategory.to_csv(save_path + "prodoutCategory.csv", index=False)

        aggregate_data()
        process_data()
        pre_process()
        update_log()

    if num_of_rec:
        recommendation, user_stat = get_rec(user_id, num_of_rec=num_of_rec)
    else:
        recommendation, user_stat = get_rec(user_id, num_of_rec=5)
    return {"recommendations": recommendation}
