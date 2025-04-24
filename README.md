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
