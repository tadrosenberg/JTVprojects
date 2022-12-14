DROP TABLE TADROS1.FOLDED_HOST_AFFINITY CASCADE CONSTRAINT PURGE;
CREATE TABLE TADROS1.FOLDED_HOST_AFFINITY AS
SELECT CUST_ID,
       -- Assign default affinity values if never purchased from host (values are the negative indexed order percentage company-wide)
       NVL(MISTY_MILLS, -13.95) AS MISTY_MILLS,
       NVL(MELISSA_WOLFE, -8.99) AS MELISSA_WOLFE,
       NVL(JENNIFER_MILLER, -8.58) AS JENNIFER_MILLER,
       NVL(ROBERT_THOMAS, -7) AS ROBERT_THOMAS,
       NVL(NIKKI_COGGINS, -6.57) AS NIKKI_COGGINS,
       NVL(SCOTT_HOOD, -6.19) AS SCOTT_HOOD,
       NVL(MANDY_STEINBACHER, -5.96) AS MANDY_STEINBACHER,
       NVL(LISSA_DAIGLE, -5.54) AS LISSA_DAIGLE,
       NVL(KRISTEN_KEECH, -5.12) AS KRISTEN_KEECH,
       NVL(SUSAN_THOMAS, -5.05) AS SUSAN_THOMAS,
       NVL(HEIDI_KOUNS, -4.05) AS HEIDI_KOUNS,
       NVL(LESLIE_DELERY, -3.73) AS LESLIE_DELERY,
       NVL(CRISTINA_YANEZ, -3.64) AS CRISTINA_YANEZ,
       NVL(DAWN_PAGE_TESH, -3.59) AS DAWN_PAGE_TESH,
       NVL(TOMMY_BROWN, -2.78) AS TOMMY_BROWN,
       NVL(SHARON_SCOTT, -2.71) AS SHARON_SCOTT,
       NVL(NAN_KELLEY, -2.23) AS NAN_KELLEY,
       NVL(KRISTIE_ROWAN, -1.48) AS KRISTIE_ROWAN,
       NVL(WENDY_CALDWELL, -1.39) AS WENDY_CALDWELL,
       NVL(CASEY_GAGLIARDI, -1.25) AS CASEY_GAGLIARDI,
       NVL(ALLYSON_SPELLMAN, -0.18) AS ALLYSON_SPELLMAN,
       NVL(ALICIA_HERZBRUN, -0.02) AS ALICIA_HERZBRUN,
       NVL(TIM_MATTHEWS, 0) AS TIM_MATTHEWS
FROM (
SELECT * FROM (
SELECT C.CUST_ID,
       C.HOST,
       -- Formula for affinity score (if <0, don't use multiplier)
       CASE WHEN (C.SPS_WEIGHTED_PERC_PLACED_ORDERS - H.PER_ORDERS) <= 0 THEN (C.SPS_WEIGHTED_PERC_PLACED_ORDERS - H.PER_ORDERS)
            ELSE ROUND((C.SPS_WEIGHTED_PERC_PLACED_ORDERS - H.PER_ORDERS) * ((LOG(2,C.PLACED_ORDERS_H)+1)+(LOG(2,C.CALCULATED_ORDER_TOTAL_H)/2)),2) END AS HOST_AFFINITY_SCORE
FROM (
-- Build customer purchase data
WITH HSQ AS (SELECT A.*,
                    -- SPS Weigting pt2
                    (1/SPS_INVERSE_WEIGHTED_ORDER) / (SUM(1 / A.SPS_INVERSE_WEIGHTED_ORDER) OVER (PARTITION BY A.ENTERPRISE_CUST_ID) / A.TOTAL_ORDERS) AS SPS_FINAL_WEIGHTED_ORDER
             FROM(
             SELECT
                    /*+ PARALLEL(32) */
                    S.CUSTOMER_ID AS ENTERPRISE_CUST_ID,
                    S.ORDER_NUMBER,
                    INITCAP(PS.HOST_FULL_NAME_1) HOST_FULL_NAME_1,
                    INITCAP(PS.HOST_FULL_NAME_2) HOST_FULL_NAME_2,
                    S.CALCULATED_ORDER_TOTAL AS CALCULATED_ORDER_TOTAL,
                    PS.SPS SPS_AT_PURCHASE,
                    COUNT(DISTINCT S.ORDER_NUMBER) OVER (PARTITION BY S.CUSTOMER_ID) AS TOTAL_ORDERS,
                    SUM(S.CALCULATED_ORDER_TOTAL) OVER (PARTITION BY S.CUSTOMER_ID) AS TOTAL_CALCULATED_ORDER_TOTAL,
                    SUM(PS.SPS) OVER (PARTITION BY S.CUSTOMER_ID) AS TOTAL_SPS,
                    -- SPS Weigting pt1
                    (COUNT(DISTINCT S.ORDER_NUMBER) OVER (PARTITION BY S.CUSTOMER_ID)/(SUM(PS.SPS) OVER (PARTITION BY S.CUSTOMER_ID))) * PS.SPS AS SPS_INVERSE_WEIGHTED_ORDER
             FROM BA_SCHEMA.SALES S
             INNER JOIN BA_SCHEMA.PRODUCT_SCORE PS
             ON S.SHOWING_ID = PS.SHOWING_ID
             WHERE S.BROADCAST_INFLUENCED = 'Influenced'
             AND S.ORDER_DATE_TIME >= TRUNC(SYSDATE,'IW')-(7*52)
             AND S.ORDER_DATE_TIME < TRUNC(SYSDATE,'IW')
             AND S.COMPANY = 'JTV'
             AND NVL(S.ORDER_CANCEL_REASON_GROUP_CODE,0) <> -100
             AND CALCULATED_ORDER_TOTAL <> 0)
              A)
             
SELECT /*+ PARALLEL(16) */
       A.ENTERPRISE_CUST_ID CUST_ID,
       A.HOST HOST,
       COUNT(DISTINCT A.ORDER_NUMBER) PLACED_ORDERS_H,
       ROUND(SUM(A.CALCULATED_ORDER_TOTAL),2) CALCULATED_ORDER_TOTAL_H,
       ROUND(SUM(A.SPS_FINAL_WEIGHTED_ORDER)/AVG(A.TOTAL_ORDERS),4)*100 SPS_WEIGHTED_PERC_PLACED_ORDERS,
       SUM(COUNT(DISTINCT A.ORDER_NUMBER)) OVER (PARTITION BY A.ENTERPRISE_CUST_ID) AGGREGATE_ORDERS
-- Assign the order to both hosts present
FROM (SELECT H1.ENTERPRISE_CUST_ID,
             H1.ORDER_NUMBER,
             H1.SPS_FINAL_WEIGHTED_ORDER,
             H1.TOTAL_ORDERS AS TOTAL_ORDERS,
             H1.CALCULATED_ORDER_TOTAL AS CALCULATED_ORDER_TOTAL,
             H1.HOST_FULL_NAME_1 HOST
             FROM HSQ H1
      UNION
      SELECT H2.ENTERPRISE_CUST_ID,
             H2.ORDER_NUMBER,
             H2.SPS_FINAL_WEIGHTED_ORDER,
             H2.TOTAL_ORDERS AS TOTAL_ORDERS,
             H2.CALCULATED_ORDER_TOTAL AS CALCULATED_ORDER_TOTAL,
             H2.HOST_FULL_NAME_2 HOST
      FROM HSQ H2 
)A
WHERE A.HOST IS NOT NULL
GROUP BY ENTERPRISE_CUST_ID, HOST
ORDER BY ENTERPRISE_CUST_ID, SPS_WEIGHTED_PERC_PLACED_ORDERS DESC
) C
INNER JOIN (
--Build index host percentages
WITH A1 AS (
SELECT /*+ PARALLEL(16) */
       S.ORDER_NUMBER,
       PS.HOST_FULL_NAME_1,
       PS.HOST_FULL_NAME_2
FROM BA_SCHEMA.SALES S
INNER JOIN BA_SCHEMA.PRODUCT_SCORE PS
ON S.SHOWING_ID = PS.SHOWING_ID
WHERE S.BROADCAST_INFLUENCED = 'Influenced'
AND S.ORDER_DATE_TIME >= TRUNC(SYSDATE,'IW')-(7*52)
AND S.ORDER_DATE_TIME < TRUNC(SYSDATE,'IW')
AND S.COMPANY = 'JTV'
AND NVL(S.ORDER_CANCEL_REASON_GROUP_CODE,0) <> -100
AND S.CALCULATED_ORDER_TOTAL <> 0),
T2 AS (
SELECT B1.HOST,
       SUM(B1.NUM_ORDERS) NUM_ORDERS
FROM(
SELECT HOST_FULL_NAME_1 HOST,
       COUNT(A1.ORDER_NUMBER) NUM_ORDERS
       FROM A1
       GROUP BY HOST_FULL_NAME_1
UNION 
SELECT HOST_FULL_NAME_2 HOST,
       COUNT(A1.ORDER_NUMBER) NUM_ORDERS
       FROM A1
       GROUP BY HOST_FULL_NAME_2
) B1
WHERE HOST IS NOT NULL
GROUP BY B1.HOST
)
SELECT /*+ PARALLEL(16) */
       HOST,
       ROUND(NUM_ORDERS *100 / (SELECT SUM(NUM_ORDERS) FROM T2),2) AS PER_ORDERS
FROM T2
) H
ON C.HOST = H.HOST
-- Currently we are omitting scores for newer customers until they build enough history to make a accurate assignment
WHERE C.AGGREGATE_ORDERS >= 10
) TEMP
-- Pivot table to display one row per customer with every host's score
PIVOT(
  MAX(HOST_AFFINITY_SCORE)
  FOR HOST IN (
    'Misty Mills' AS MISTY_MILLS,
    'Melissa Wolfe' AS MELISSA_WOLFE,
    'Jennifer Miller' AS JENNIFER_MILLER,
    'Robert Thomas' AS ROBERT_THOMAS,
    'Nikki Coggins' AS NIKKI_COGGINS,
    'Scott Hood' AS SCOTT_HOOD,
    'Mandy Steinbacher' AS MANDY_STEINBACHER,
    'Lissa Daigle' AS LISSA_DAIGLE,
    'Kristen Keech' AS KRISTEN_KEECH,
    'Susan Thomas' AS SUSAN_THOMAS,
    'Heidi Kouns' AS HEIDI_KOUNS,
    'Leslie Delery' AS LESLIE_DELERY,
    'Cristina Yanez' AS CRISTINA_YANEZ,
    'Dawn Page-Tesh' AS DAWN_PAGE_TESH,
    'Tommy Brown' AS TOMMY_BROWN,
    'Sharon Scott' AS SHARON_SCOTT,
    'Nan Kelley' AS NAN_KELLEY,
    'Kristie Rowan' AS KRISTIE_ROWAN,
    'Wendy Caldwell' AS WENDY_CALDWELL,
    'Casey Gagliardi' AS CASEY_GAGLIARDI,
    'Allyson Spellman' AS ALLYSON_SPELLMAN,
    'Alicia Herzbrun' AS ALICIA_HERZBRUN,
    'Tim Matthews' AS TIM_MATTHEWS)
)
) PT;

SELECT * FROM TADROS1.FOLDED_HOST_AFFINITY;
