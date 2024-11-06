import os
import json
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from pymongo import MongoClient
import polars as pl
from pydantic import BaseModel, ValidationError, Field
from typing import Optional, List, Dict

def on_failure_callback(**context):
    print(f"Task {context['task_instance_key_str']} failed.")

# Pydantic model for validation
from pydantic import BaseModel
from typing import Optional, Dict, List, Union
from datetime import datetime

class ProductDataModel(BaseModel):
    name: str
    slug: str
    code: str
    type: str
    price: int
    sale_price: Dict[str, Union[bool, int, Dict[str, Union[bool, str]]]]
    cogs: Optional[int]
    variations: Dict[str, Union[str, List[str]]]
    wholesale: Optional[List]
    picture: str

class FormData(BaseModel):
    name: str

class CustomerDataWithMeta(BaseModel):
    name: str
    phone: str
    email: Optional[str]
    notes: Optional[str]
    address: Optional[str]
    state: Optional[str]
    city: Optional[str]
    district: Optional[str]
    province_id: Optional[int]
    postal_name: Optional[str]
    city_id: Optional[int]
    city_name: Optional[str]
    subdistrict_id: Optional[int]
    subdistrict_name: Optional[str]
    zip: Optional[int]
    variation: Optional[str]
    variations: Optional[Dict[str, str]]
    meta: Optional[Dict[str, Union[str, int, float]]]
    product_price: int
    product_weight: int

class Shipping(BaseModel):
    courier: Optional[str]
    service: Optional[str]
    cost: int
    cod: bool
    cod_cost: int
    cod_include_shipping: bool
    cod_fee_type: str
    cod_min: int
    cod_max: int
    cod_percentage: int
    markup: int
    original_cost: int
    origin_id: int
    weight: int
    mode: str

class Bump(BaseModel):
    checked: bool
    name: Optional[str]
    price: Optional[int]
    weight: Optional[int]

class Coupon(BaseModel):
    id: Optional[str]
    code: Optional[str]
    discount: int
    discount_for: Optional[str]

class UniqueCodePrice(BaseModel):
    type: str
    unique_code: int

class ProgressStatus(BaseModel):
    welcome: bool
    follow_up_1: bool
    follow_up_2: bool
    follow_up_3: bool
    follow_up_4: bool
    processing: bool
    closing: bool
    cancelled: bool
    up_selling: bool

class Payment(BaseModel):
    method: Optional[str]
    data: List
    channels: List
    status: str

class MetaData(BaseModel):
    url: str
    fbc: str
    fbp: str
    ip: str
    os: str
    browser: str
    location: Optional[str]
    network: Optional[str]
    device_type: str
    device_model: Optional[str]
    user_agent: str
    embed: bool
    referrer: str
    uuid: str

class MainDataModel(BaseModel):
    _id: Dict[str, str]
    user_id: Dict[str, str]
    product_id: Dict[str, str]
    form_id: Dict[str, str]
    is_deleted: bool
    order_id: int
    product_data: ProductDataModel
    form_data: FormData
    customer_data: CustomerDataWithMeta
    product_price: int
    product_weight: int
    shipping: Shipping
    bump: Bump
    coupon: Coupon
    total_product_price: int
    total_price: int
    unique_code_price: UniqueCodePrice
    cogs: Optional[int]
    progress: ProgressStatus
    progress_sms: ProgressStatus
    payment: Payment
    status: str
    is_printed: bool
    meta: MetaData
    belongs_to: List[Dict[str, str]]
    type: str
    hash: str
    other_cost: int
    dropship: Optional[Dict[str, Union[str, bool]]]
    is_stock_reduced: bool
    customer_id: Dict[str, str]
    updated_at: Dict[str, str]
    created_at: Dict[str, str]








# class ProductDataModel(BaseModel):
#     name: str
#     slug: str
#     code: str
#     type: str
#     price: int
#     sale_price: dict[str, int, object]
#     cogs: Optional[int]
#     variations: dict[str, str, str]
#     wholesale: Optional[List]
#     picture: str

# class CustomerDataModel1(BaseModel):
#     name: str
#     phone: str
#     email: int
#     notes: int
#     address: str
#     product_price: int
#     product_weight: int

# class CustomerDataModel2(BaseModel):
#     name: str
#     phone: str
#     email: int
#     notes: int
#     address: str
#     state: str
#     city: str
#     distirc: str
#     province_id: int
#     postal_name: str
#     city_id: int
#     city_name: str
#     subdistrict_id: int
#     subdistrict_name: str
#     zip: int
#     variation: str
#     quantity: int
#     product_price: int
#     product_weight: int

# class CustomerDataModel3(BaseModel):
#      name: str
#     phone: str
#     email: int
#     notes: int
#     address: str
#     state: str
#     city: str
#     distirc: str
#     province_id: int
#     postal_name: str
#     city_id: int
#     city_name: str
#     subdistrict_id: int
#     subdistrict_name: str
#     zip: int
#     variation: str
#     variations: dict[object,str]
#     meta: dict[object, object, object]
#     product_price: int

# class CustomerDataBase(BaseModel):
#     name: str
#     phone: str
#     email: int
#     notes: int
#     address: str
#     product_price: int
#     product_weight: int

# class CustomerDataWithLocation(CustomerDataBase):
#     state: str
#     city: str
#     district: str
#     province_id: int
#     postal_name: str
#     city_id: int
#     city_name: str
#     subdistrict_id: int
#     subdistrict_name: str
#     zip: int

# class CustomerDataWithMeta(CustomerDataWithLocation):
#     variation: str
#     variations: Dict[str, str]
#     meta: Dict[str, object]

# class Shipping(BaseModel):
#         courier: str
#         service: int
#         cost: int
#         cod: int
#         cod_cost: int
#         cod_include_shipping: bool
#         cod_fee_type: str
#         cod_min: int
#         cod_max: int
#         cod_percentage: int
#         markup: int
#         original_cost: int
#         sla_min: int
#         sla_max: int
#         origin_id: int
#         weight: int
#         mode: str   

# class MainDataModel(BaseModel):
#     _id: Dict[str, str]
#     user_id: Dict[str, str]
#     product_id: Dict[str, str]
#     form_id: Dict[str, str]
#     is_deleted: bool
#     order_id: int
#     product_data: ProductDataModel
#     form_data: dict
#     customer_data: dict
#     product_price: int
#     product_weight: int
#     shipping: dict
#     bump: dict
#     coupon: dict
#     total_product_price: int
#     total_price: int
#     unique_code_price: dict
#     cogs: Optional[int]
#     progress: dict
#     progress_sms: dict
#     payment: dict
#     status: str
#     is_printed: bool
#     meta: dict
#     belongs_to: List[Dict[str, str]]
#     type: str
#     hash: str
#     other_cost: int
#     dropship: dict
#     is_stock_reduced: bool
#     customer_id: Dict[str, str]
#     updated_at: Dict[str, str]
#     created_at: Dict[str, str]

# Extract function
def extract_data(**kwargs):
    # Simulating data retrieval (replace with actual retrieval)
    data = { 
        # JSON data you provided here for extraction example 
    }
    with open("/tmp/SampleData.json", "w") as f:
        json.dump(data, f)
    return "/tmp/data_extracted.json"

# Transform function using polars
def transform_data(**kwargs):
    ti = kwargs['ti']
    extracted_file_path = ti.xcom_pull(task_ids="extract_task")

    with open(extracted_file_path, "r") as f:
        data = json.load(f)

    # Flatten nested fields with Polars
    df = pl.DataFrame([data])
    df_flat = df.select([
        pl.col("product_data.*"),
        pl.col("form_data.*"),
        pl.col("customer_data.*"),
        pl.exclude(["product_data", "form_data", "customer_data"])
    ])
    
    transformed_data = df_flat.to_dicts()[0]
    transformed_file_path = "/tmp/data_transformed.json"
    with open(transformed_file_path, "w") as f:
        json.dump(transformed_data, f)

    return transformed_file_path

# Load function with validation using pydantic
def load_data(**kwargs):
    ti = kwargs['ti']
    transformed_file_path = ti.xcom_pull(task_ids="transform_task")

    with open(transformed_file_path, "r") as f:
        data = json.load(f)

    try:
        # Validate data using pydantic model
        validated_data = MainDataModel(**data)

        # MongoDB connection (replace with your MongoDB URI and database/collection names)
        client = MongoClient("mongodb://localhost:27017/")
        db = client["your_database"]
        collection = db["your_collection"]
        
        # Insert data into MongoDB
        collection.insert_one(validated_data.dict())
    except ValidationError as e:
        print("Validation Error:", e.json())

with DAG(
    dag_id="load_data_etl",
    schedule_interval=None,
    start_date=datetime(2024, 5, 11),
    catchup=False,
    tags=["Submission"],
    default_args={
        "owner": "Faisal Ashshidiq",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        'on_failure_callback': on_failure_callback
    }
) as dag:

    # Extract Task
    extract_task = PythonOperator(
        task_id="extract_task",
        python_callable=extract_data
    )

    # Transform Task
    transform_task = PythonOperator(
        task_id="transform_task",
        python_callable=transform_data,
        provide_context=True
    )

    # Load Task
    load_task = PythonOperator(
        task_id="load_task",
        python_callable=load_data,
        provide_context=True
    )

    # Task dependencies
    extract_task >> transform_task >> load_task
