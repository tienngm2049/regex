# regex

```
SELECT
    -- Tách Tỉnh/Thành phố
    REGEXP_EXTRACT(address, '(?:,|-|\s)(THANH PHO|TP\.|TINH)\s?([A-Za-z\s]+)(?:,|\s|$)', 2) AS province,
    
    -- Tách Quận/Huyện
    REGEXP_EXTRACT(address, '(?:,|\s|\/)(QUAN|HUYEN|Q\.)\s?([A-Za-z\s]+|\d+)(?:,|\s|$)', 2) AS district,

    -- Tách Xã/Phường
    REGEXP_EXTRACT(address, '(?:,|\s|\/)(PHUONG|P\.|XA|X\.)\s?([A-Za-z\s]+|\d+)(?:,|\s|$)', 2) AS ward
FROM
    your_table
WHERE
    address IS NOT NULL;
```


```
import re

# Hàm chuẩn hóa địa chỉ với các quy tắc chính xác hơn
def normalize_address(address):
    # Biến đổi viết tắt thành đầy đủ cho các tỉnh thành
    address = re.sub(r'\bTPHN\b|\bHN\b', 'HA NOI', address, flags=re.IGNORECASE)  # HN -> HA NOI
    address = re.sub(r'\bHCM\b|\bTPHCM\b|\bTP\.HCM\b', 'HO CHI MINH', address, flags=re.IGNORECASE)  # HCM -> HO CHI MINH
    
    # Biến đổi viết tắt thành đầy đủ cho các cấp hành chính
    address = re.sub(r'\bTP\.\b', 'THANH PHO', address, flags=re.IGNORECASE)  # TP. -> THANH PHO
    address = re.sub(r'\bT\.\b', 'TINH', address, flags=re.IGNORECASE)  # T. -> TINH
    
    # Biến đổi viết tắt cho quận, huyện, phường, xã, thị trấn
    address = re.sub(r'\bQ\.\s?(\d+)\b', r'QUAN \1', address, flags=re.IGNORECASE)  # Q.1 -> QUAN 1
    address = re.sub(r'\bQ\s?(\d+)\b', r'QUAN \1', address, flags=re.IGNORECASE)  # Q1 -> QUAN 1
    address = re.sub(r'\bH\.\s?(\d+)\b', r'HUYEN \1', address, flags=re.IGNORECASE)  # H.1 -> HUYEN 1
    
    # Biến đổi viết tắt cho phường và xã
    address = re.sub(r'\bP\.\s?(\d+)\b', r'PHUONG \1', address, flags=re.IGNORECASE)  # P.1 -> PHUONG 1
    address = re.sub(r'\bX\.\s?(\d+)\b', r'XA \1', address, flags=re.IGNORECASE)  # X.1 -> XA 1
    address = re.sub(r'\bT\.TRAN\b', 'THI TRAN', address, flags=re.IGNORECASE)  # T.TRAN -> THI TRAN
    
    # Biến đổi viết tắt "H." thành "HUYEN" nếu có và nối với tên huyện
    address = re.sub(r'\bH\.\s?([A-Za-z0-9\s]+)', r'HUYEN \1', address, flags=re.IGNORECASE)  # H. -> HUYEN
    
    # Loại bỏ các ký tự không cần thiết (ví dụ: dấu chấm, dấu chấm phẩy)
    address = re.sub(r'[^\w\s,]', '', address)
    
    # Chuyển tất cả về chữ hoa để đồng nhất
    address = address.upper()
    
    return address

# Ví dụ địa chỉ
addresses = [
    "SO 10 DUONG 1 PHUONG XUAN PHUONG, QUAN 1, HO CHI MINH",
    "TPHCM, Q.1, P. XUAN PHUONG",
    "QUAN 1, HO CHI MINH, PHUONG XUAN PHUONG",
    "TPHN, H. HOAI AN, X. SONG LAM",
    "Q 1, HO CHI MINH, P XUAN PHUONG",
    "Q.2, TP.HCM, P.14"
]

# Kiểm tra ví dụ địa chỉ sau khi chuẩn hóa
for address in addresses:
    print(f"Original: {address}")
    print(f"Normalized: {normalize_address(address)}")
    print("-" * 50)

```


```
# Import necessary libraries
import pandas as pd
import re

# Step 1: Run Spark SQL queries to get distinct values
province_df = spark.sql("SELECT DISTINCT province FROM dim_province_district").toPandas()
district_df = spark.sql("SELECT DISTINCT district FROM dim_province_district").toPandas()
ward_df = spark.sql("SELECT DISTINCT ward FROM dim_province_ward").toPandas()

# Step 2: Create regex patterns based on data fetched from SQL
# Convert provinces, districts, and wards into lists
provinces = province_df['province'].tolist()
districts = district_df['district'].tolist()
wards = ward_df['ward'].tolist()

# Create regex patterns using the fetched data
province_regex = r"(" + "|".join(provinces) + r")"
district_regex = r"(" + "|".join(districts) + r")"
ward_regex = r"(" + "|".join(wards) + r")"

# Example normalized address data (already normalized)
normalized_addresses = [
    "HO CHI MINH, QUAN 1, PHUONG XUAN PHUONG",
    "HO CHI MINH, QUAN 1, P XUAN PHUONG",
    "HA NOI, HUYEN HOAI AN, XA SONG LAM",
    "HO CHI MINH, QUAN 1, PHUONG 14"
]

# Step 3: Use regex to extract components from the address
for address in normalized_addresses:
    province_match = re.search(province_regex, address)
    district_match = re.search(district_regex, address)
    ward_match = re.search(ward_regex, address)

    province = province_match.group(0) if province_match else None
    district = district_match.group(0) if district_match else None
    ward = ward_match.group(0) if ward_match else None

    print(f"Address: {address}")
    print(f"Province: {province}")
    print(f"District: {district}")
    print(f"Ward: {ward}")
    print("-" * 50)

```


```
import re
import pandas as pd

# Sample normalized address data (you may replace this with actual normalized data)
normalized_addresses = [
    "HO CHI MINH, QUAN 1, PHUONG XUAN PHUONG",
    "HO CHI MINH, QUAN 1, P XUAN PHUONG",
    "HA NOI, HUYEN HOAI AN, XA SONG LAM",
    "HO CHI MINH, QUAN 1, PHUONG 14"
]

# Step 1: Fetch distinct values from Spark SQL
province_df = spark.sql("SELECT DISTINCT province FROM dim_province_district").toPandas()
district_df = spark.sql("SELECT DISTINCT district FROM dim_province_district").toPandas()
ward_df = spark.sql("SELECT DISTINCT ward FROM dim_province_ward").toPandas()

# Convert data to lists
provinces = province_df['province'].tolist()
districts = district_df['district'].tolist()
wards = ward_df['ward'].tolist()

# Create regex patterns based on data fetched from SQL
province_regex = r"(" + "|".join(provinces) + r")"
district_regex = r"(" + "|".join(districts) + r")"
ward_regex = r"(" + "|".join(wards) + r")"

# Step 2: Function to extract and validate province, district, and ward
def extract_and_validate(address):
    # Step 2.1: Check for predefined provinces directly
    province_match = re.search(province_regex, address)
    province = province_match.group(0) if province_match else None
    
    # If no predefined province, check for "TINH" or "THANH PHO"
    if not province:
        if re.search(r"\b(TINH|THANH PHO)\b", address, re.IGNORECASE):
            # Extract province from the database if it starts with TINH or THANH PHO
            province_search = re.search(r"\b(TINH|THANH PHO)\s+(\w+)", address, re.IGNORECASE)
            if province_search:
                province_candidate = province_search.group(2)
                # Query SQL to verify if this province exists
                verified_province_df = spark.sql(f"SELECT province FROM dim_province_district WHERE province = '{province_candidate}'").toPandas()
                if not verified_province_df.empty:
                    province = verified_province_df['province'][0]
    
    # Step 2.2: Match the district
    district_match = re.search(district_regex, address)
    district = district_match.group(0) if district_match else None
    
    # Step 2.3: Validate the combination of province and district
    if province and district:
        valid_district_df = spark.sql(f"SELECT district FROM dim_province_district WHERE province = '{province}' AND district = '{district}'").toPandas()
        if not valid_district_df.empty:
            # Step 2.4: Match the ward if the province and district are valid
            ward_match = re.search(ward_regex, address)
            ward = ward_match.group(0) if ward_match else None

            if ward:
                # Validate the combination of province, district, and ward
                valid_ward_df = spark.sql(f"SELECT ward FROM dim_province_ward WHERE province = '{province}' AND district = '{district}' AND ward = '{ward}'").toPandas()
                if not valid_ward_df.empty:
                    return {'Province': province, 'District': district, 'Ward': ward}
    
    return {'Province': None, 'District': None, 'Ward': None}

# Step 3: Apply the function to all addresses
for address in normalized_addresses:
    result = extract_and_validate(address)
    print(f"Address: {address}")
    print(f"Validation Result: {result}")
    print("-" * 50)

```
