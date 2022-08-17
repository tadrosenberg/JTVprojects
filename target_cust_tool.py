#build a function that takes in requested show_brand, brand, department, host
#requires rerunning sql affinity tables - i could build a procedure for that and put it here probably?
#features to add: type in just a show brand or brand and have it figure which it is + which department to search
import cx_Oracle as cx
import pandas as pd
import sys
import string
from pathlib import Path

def load_data(connection, q):
    if connection == 0:
        print("Check Connection. Ending Script")
        sys.exit()
    else:
        cursor = connection.cursor()
        cursor.execute(q)
        raw = cursor.fetchall()
        cols = [i[0] for i in cursor.description]
    return raw, cols

def target_cust_list(num_total=10000, show_brand=None, brand=None, department=None, host=None, lifecycle_category=None):

    #format strings to match database
    if host is not None:
        host = string.capwords(host)
    if show_brand is not None:
        show_brand = string.capwords(show_brand)
    if brand is not None:
        brand = brand.upper()
    if department is not None:
        department = department.upper()

    # assign top attribute
    if show_brand is not None:
        top_attribute = {'type':'SHOW_BRAND', 'name':show_brand}
    elif brand is not None:
        top_attribute = {'type':'BRAND', 'name':brand}
    else:
        top_attribute = {'type':'DEPARTMENT', 'name':department}

    if top_attribute['type'] == 'DEPARTMENT':
        print("department is the top attribute")
        top_num = num_total*.8
    else:
        print("brand is the top attribute")
        top_num = num_total*.45
    level_num = num_total*.35
    clv_num = num_total*.4
    host_num = num_total*.3

    top_query = f"""
    SELECT A.CUST_ID,
           C.FULL_NAME,
           C.PRIMARY_ADDRESS1,
           C.PRIMARY_ADDRESS2,
           C.PRIMARY_CITY,
           C.PRIMARY_STATE,
           C.PRIMARY_ZIP5,
           C.PRIMARY_COUNTRY,
           C.PRIMARY_PHONE,
           C.PRIMARY_EMAIL
    FROM(
    SELECT 
           B.CUST_ID,
           B.{top_attribute['type']},
           B.{top_attribute['type']}_AFFINITY_SCORE,
           CLV.EXP_TRANS_5YR_LTV CLV
    FROM TADROS1.{top_attribute['type']}_AFFINITY_SCORE B
    JOIN BA_SCHEMA.CUSTOMER_LIFETIME_VALUE CLV
    ON B.CUST_ID = CLV.CUSTOMER_ID
    WHERE B.{top_attribute['type']} = '{top_attribute['name']}'
    AND CLV.WEEK_OF = TRUNC(SYSDATE, 'IW')
    ORDER BY B.{top_attribute['type']}_AFFINITY_SCORE DESC
    FETCH FIRST ({top_num}*2) ROWS ONLY
    ) A
    JOIN BA_SCHEMA.CUSTOMER C
    ON A.CUST_ID = C.CUSTOMER_ID
    ORDER BY A.CLV DESC
    FETCH FIRST {top_num} ROWS ONLY
    """

    if top_attribute['type'] == 'DEPARTMENT':
        level_query = None
    else:
        level_query = f"""
        SELECT A.CUST_ID,
           C.FULL_NAME,
           C.PRIMARY_ADDRESS1,
           C.PRIMARY_ADDRESS2,
           C.PRIMARY_CITY,
           C.PRIMARY_STATE,
           C.PRIMARY_ZIP5,
           C.PRIMARY_COUNTRY,
           C.PRIMARY_PHONE,
           C.PRIMARY_EMAIL
        FROM(
        SELECT 
               B.CUST_ID,
               B.DEPARTMENT,
               B.DEPARTMENT_AFFINITY_SCORE,
               CLV.EXP_TRANS_5YR_LTV CLV
        FROM TADROS1.DEPARTMENT_AFFINITY_SCORE B
        JOIN BA_SCHEMA.CUSTOMER_LIFETIME_VALUE CLV
        ON B.CUST_ID = CLV.CUSTOMER_ID
        WHERE B.DEPARTMENT = '{department}'
        AND CLV.WEEK_OF = TRUNC(SYSDATE, 'IW')
        ORDER BY B.DEPARTMENT_AFFINITY_SCORE DESC
        FETCH FIRST {level_num} ROWS ONLY
        ) A
        JOIN BA_SCHEMA.CUSTOMER C
        ON A.CUST_ID = C.CUSTOMER_ID
        """

    clv_query = f"""
    SELECT 
        CLV.CUSTOMER_ID CUST_ID,
        C.FULL_NAME,
        C.PRIMARY_ADDRESS1,
        C.PRIMARY_ADDRESS2,
        C.PRIMARY_CITY,
        C.PRIMARY_STATE,
        C.PRIMARY_ZIP5,
        C.PRIMARY_COUNTRY,
        C.PRIMARY_PHONE,
        C.PRIMARY_EMAIL
    FROM BA_SCHEMA.CUSTOMER_LIFETIME_VALUE CLV
    JOIN BA_SCHEMA.CUSTOMER C
    ON CLV.CUSTOMER_ID = C.CUSTOMER_ID
    WHERE CLV.WEEK_OF = TRUNC(SYSDATE, 'IW')
    AND CLV.EXP_TRANS_5YR_LTV IS NOT NULL
    ORDER BY CLV.EXP_TRANS_5YR_LTV DESC
    FETCH FIRST {clv_num} ROWS ONLY
    """

    host_query = f"""
    SELECT A.CUST_ID,
        C.FULL_NAME,
        C.PRIMARY_ADDRESS1,
        C.PRIMARY_ADDRESS2,
        C.PRIMARY_CITY,
        C.PRIMARY_STATE,
        C.PRIMARY_ZIP5,
        C.PRIMARY_COUNTRY,
        C.PRIMARY_PHONE,
        C.PRIMARY_EMAIL
    FROM TADROS1.HOST_AFFINITY_SCORE A
    JOIN BA_SCHEMA.CUSTOMER C
    ON A.CUST_ID = C.CUSTOMER_ID
    WHERE HOST = '{host}'
    AND HOST_AFFINITY_SCORE IS NOT NULL
    ORDER BY HOST_AFFINITY_SCORE DESC
    FETCH FIRST {host_num} ROWS ONLY
    """

    random_query = f"""
    SELECT 
        CLV.CUSTOMER_ID CUST_ID,
        C.FULL_NAME,
        C.PRIMARY_ADDRESS1,
        C.PRIMARY_ADDRESS2,
        C.PRIMARY_CITY,
        C.PRIMARY_STATE,
        C.PRIMARY_ZIP5,
        C.PRIMARY_COUNTRY,
        C.PRIMARY_PHONE,
        C.PRIMARY_EMAIL
    FROM BA_SCHEMA.CUSTOMER_LIFETIME_VALUE SAMPLE(1)  CLV
    JOIN BA_SCHEMA.CUSTOMER C
    ON CLV.CUSTOMER_ID = C.CUSTOMER_ID
    WHERE CLV.WEEK_OF = TRUNC(SYSDATE, 'IW')
    AND CLV.EXP_TRANS_5YR_LTV > 800
    AND CLV.EXP_TRANS_5YR_LTV < 1200
    """

    connection = cx.connect('tadros1', 'Sindelfingen08',
                                 'PROD02-SCAN.JEWELRY.ACN:1521/EDW.JEWELRY.ACN')

    raw_data, columns = load_data(connection, top_query)
    top_list = pd.DataFrame(raw_data, columns=columns)

    if top_attribute['type'] != 'DEPARTMENT':
        raw_data, columns = load_data(connection, level_query)
        level_list = pd.DataFrame(raw_data, columns=columns)

    raw_data, columns = load_data(connection, clv_query)
    clv_list = pd.DataFrame(raw_data, columns=columns)

    raw_data, columns = load_data(connection, host_query)
    host_list = pd.DataFrame(raw_data, columns=columns)

    raw_data, columns = load_data(connection, random_query)
    random_list = pd.DataFrame(raw_data, columns=columns)

    connection.close()

    if top_attribute['type'] != 'DEPARTMENT':
        cust_list = pd.concat([top_list, level_list, clv_list, host_list, random_list]).drop_duplicates().head(num_total)
    else:
        cust_list = pd.concat([top_list, clv_list, host_list, random_list]).drop_duplicates().head(num_total)
    print(cust_list)
    file_name = (top_attribute['name'].replace(" ", "_") + '_CUSTOMER_LIST_' + str(num_total)).upper()
    cust_list.to_excel(r'C:\Users\tadros1\Desktop\{file_name}.xlsx'.format(file_name=file_name), index = False, header=True)

    return cust_list

target_cust_list(show_brand='charles winston for bella luce',department='color silver', host='Nan Kelley')

