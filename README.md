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
