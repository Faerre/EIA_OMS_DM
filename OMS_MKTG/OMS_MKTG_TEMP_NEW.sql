DECLARE
  YM_START NUMBER:=0;
  YM_END   NUMBER:=0;
BEGIN
  /*Loop counter in YEARMONTH_MONTHS*/
  YM_START:=DWSEAI01.YMM2YM({YMM}-2);
  /*Start of OMS Sales period (3 months including focus month, for R_IMC calculation*/
  YM_END:=DWSEAI01.YMM2YM({YMM});
  /*DWSEAI01.YMM2YM converts YEARMONTH_MONTHS to YEARMONTH*/
  INSERT
    /*+ append parallel(t,32)*/
  INTO DWSEAI01.OMS_MKTG_TEMP t
  SELECT
    /*+ parallel(s,32)*/
    *
  FROM
    (WITH ATM AS
    (SELECT CNTRY_ID,
      BASE_7_ITEM_NO,
      MAX(BUS_LN.GLBL_BUS_LN_DESC) AS GLBL_BUS_LN_DESC,
      MAX(CTGRY.GLBL_CTGRY_DESC)   AS GLBL_CTGRY_DESC,
      MAX(BRAND.GLBL_BRAND_DESC)   AS GLBL_BRAND_DESC
    FROM
      (SELECT TRIM(SUBSTR(
        CASE
          WHEN INSTR(REGEXP_REPLACE(SUBSTR(TRIM(ITEM), 5, LENGTH(TRIM(ITEM))                      -3), '[B-Z]', 'A'), 'A')>0
          THEN SUBSTR(TRIM(ITEM), 1, INSTR(REGEXP_REPLACE(SUBSTR(TRIM(ITEM), 4, LENGTH(TRIM(ITEM))-3), '[B-Z]', 'A'), 'A')+ 2)
          ELSE TRIM(ITEM)
        END, 1, 7))                                          AS BASE_7_ITEM_NO,
        CAST(NVL(ITEM_CNTRY_CD, INTGRT_CNTRY_CD) AS INTEGER) AS CNTRY_ID,
        ITEM_REV,
        MAX(ITEM_REV) OVER(PARTITION BY TRIM(SUBSTR(
        CASE
          WHEN INSTR(REGEXP_REPLACE(SUBSTR(TRIM(ITEM), 5, LENGTH(TRIM(ITEM))                      -3), '[B-Z]', 'A'), 'A')>0
          THEN SUBSTR(TRIM(ITEM), 1, INSTR(REGEXP_REPLACE(SUBSTR(TRIM(ITEM), 4, LENGTH(TRIM(ITEM))-3), '[B-Z]', 'A'), 'A')+ 2)
          ELSE TRIM(ITEM)
        END, 1, 7)), NVL(ITEM_CNTRY_CD, INTGRT_CNTRY_CD)) AS ITEM_REV_MAX,
        BUS_CATG_CD,
        BUS_LN_CD,
        BRAND_CD
      FROM DWSATM01.DWT42126_ITEM_EBS
      WHERE NVL(ITEM_CNTRY_CD, INTGRT_CNTRY_CD) IN ('030','240','210','620','650','390','800','340','820','810','150','590','490','140','450','660','080','060','480','160','120','090','470','460','280','370','750','740','570','270','250','110','300','040','430','830','420')
      ) DICT,
      DWSATM01.DWT43045_GLBL_BUS_LN BUS_LN,
      DWSATM01.DWT43046_GLBL_CTGRY CTGRY,
      DWSATM01.DWT43048_GLBL_BRAND BRAND
    WHERE DICT.BUS_CATG_CD = CTGRY.GLBL_CTGRY_CD(+)
    AND DICT.BUS_LN_CD     = BUS_LN.GLBL_BUS_LN_CD(+)
    AND DICT.BRAND_CD      = BRAND.GLBL_BRAND_CD(+)
      --  AND BUS_LN.DESCRIPTION<>'OTHER'
    AND DICT.ITEM_REV = DICT.ITEM_REV_MAX
    GROUP BY BASE_7_ITEM_NO,
      CNTRY_ID
    ),
    ITEM_DIM_0 AS
    (SELECT *
    FROM
      (SELECT CNTRY_KEY_NO,
        TRIM(SUBSTR(
        CASE
          WHEN instr(regexp_replace(SUBSTR(trim(ITEM_NO), 5, LENGTH(trim(ITEM_NO))                         -3), '[B-Z]', 'A'), 'A')>0
          THEN SUBSTR(trim(ITEM_NO), 1, instr(regexp_replace(SUBSTR(trim(ITEM_NO), 4, LENGTH(trim(ITEM_NO))-3), '[B-Z]', 'A'), 'A')+ 2)
          ELSE trim(ITEM_NO)
        END, 1, 7)) AS BASE_7_ITEM_NO,
        GLBL_BUS_LN_DESC,
        GLBL_BRAND_DESC,
        GLBL_CTGRY_DESC,
        GLBL_SUB_CTGRY_DESC,
        GLBL_PRDCT_GRP_DESC,
        GLBL_PRDCT_FMLY_DESC,
        GLBL_SUB_BRAND_DESC,
        GLBL_BASE_ITEM_DESC,
        DENSE_RANK() OVER(PARTITION BY CNTRY_KEY_NO, TRIM(SUBSTR(
        CASE
          WHEN instr(regexp_replace(SUBSTR(trim(ITEM_NO), 5, LENGTH(trim(ITEM_NO))                         -3), '[B-Z]', 'A'), 'A')>0
          THEN SUBSTR(trim(ITEM_NO), 1, instr(regexp_replace(SUBSTR(trim(ITEM_NO), 4, LENGTH(trim(ITEM_NO))-3), '[B-Z]', 'A'), 'A')+ 2)
          ELSE trim(ITEM_NO)
        END, 1, 7)) ORDER BY ITEM_KEY_NO DESC) AS ITEM_RNK
      FROM dwsavr02.dwv03000_item_dim
      WHERE cntry_key_no                                                                    IN (5, 26, 23, 134, 56, 39, 76, 34, 74, 75, 17, 54, 48, 16, 44, 57, 10, 8, 47, 18, 14, 11, 46, 45, 30, 37, 60, 59, 52, 29, 27, 13, 32, 6, 42, 114, 41)
      AND CAST(SUBSTR(mo_yr_curcy_comb_key_no, 1, 6) AS INTEGER) = EXTRACT(YEAR FROM SYSDATE)*100+EXTRACT(MONTH FROM SYSDATE)
      AND base_7_item_no                                        IS NOT NULL
      )
    WHERE ITEM_RNK=1
    ) ,
    ITEM_DIM AS
    (SELECT *
    FROM
      (SELECT CNTRY_KEY_NO,
        TRIM(SUBSTR(
        CASE
          WHEN instr(regexp_replace(SUBSTR(trim(ITEM_NO), 5, LENGTH(trim(ITEM_NO))                         -3), '[B-Z]', 'A'), 'A')>0
          THEN SUBSTR(trim(ITEM_NO), 1, instr(regexp_replace(SUBSTR(trim(ITEM_NO), 4, LENGTH(trim(ITEM_NO))-3), '[B-Z]', 'A'), 'A')+ 2)
          ELSE trim(ITEM_NO)
        END, 1, 7)) AS BASE_7_ITEM_NO,
        GLBL_BUS_LN_DESC,
        GLBL_BRAND_DESC,
        GLBL_CTGRY_DESC,
        GLBL_SUB_CTGRY_DESC,
        GLBL_PRDCT_GRP_DESC,
        GLBL_PRDCT_FMLY_DESC,
        GLBL_SUB_BRAND_DESC,
        GLBL_BASE_ITEM_DESC,
        DENSE_RANK() OVER(PARTITION BY CNTRY_KEY_NO, TRIM(SUBSTR(
        CASE
          WHEN instr(regexp_replace(SUBSTR(trim(ITEM_NO), 5, LENGTH(trim(ITEM_NO))                         -3), '[B-Z]', 'A'), 'A')>0
          THEN SUBSTR(trim(ITEM_NO), 1, instr(regexp_replace(SUBSTR(trim(ITEM_NO), 4, LENGTH(trim(ITEM_NO))-3), '[B-Z]', 'A'), 'A')+ 2)
          ELSE trim(ITEM_NO)
        END, 1, 7)) ORDER BY ITEM_KEY_NO DESC) AS ITEM_RNK
      FROM dwsavr02.dwv03000_item_dim
      WHERE cntry_key_no                                                                    IN (5, 26, 23, 134, 56, 39, 76, 34, 74, 75, 17, 54, 48, 16, 44, 57, 10, 8, 47, 18, 14, 11, 46, 45, 30, 37, 60, 59, 52, 29, 27, 13, 32, 6, 42, 114, 41)
      AND CAST(SUBSTR(mo_yr_curcy_comb_key_no, 1, 6) AS INTEGER) = EXTRACT(YEAR FROM SYSDATE)*100+EXTRACT(MONTH FROM SYSDATE)
      AND base_7_item_no                                        IS NOT NULL
      AND NVL(item_desc, 'N/A')                                 <> 'Inserted From OMS Orders'
      AND GLBL_BRAND_DESC                                       <>'UNDEFINED'
      AND TRIM(ITEM_DESC)                                       IS NOT NULL
      )
    WHERE ITEM_RNK=1
    ),
    ITEM_DIM_RAW1 AS
    (SELECT *
    FROM
      (SELECT CNTRY_KEY_NO,
        TRIM(SUBSTR(
        CASE
          WHEN instr(regexp_replace(SUBSTR(trim(ITEM_NO), 5, LENGTH(trim(ITEM_NO))                         -3), '[B-Z]', 'A'), 'A')>0
          THEN SUBSTR(trim(ITEM_NO), 1, instr(regexp_replace(SUBSTR(trim(ITEM_NO), 4, LENGTH(trim(ITEM_NO))-3), '[B-Z]', 'A'), 'A')+ 2)
          ELSE trim(ITEM_NO)
        END, 1, 7)) AS BASE_7_ITEM_NO,
        GLBL_BUS_LN_DESC,
        GLBL_BRAND_DESC,
        GLBL_CTGRY_DESC,
        GLBL_SUB_CTGRY_DESC,
        GLBL_PRDCT_GRP_DESC,
        GLBL_PRDCT_FMLY_DESC,
        GLBL_SUB_BRAND_DESC,
        GLBL_BASE_ITEM_DESC,
        DENSE_RANK() OVER(PARTITION BY CNTRY_KEY_NO, TRIM(SUBSTR(
        CASE
          WHEN instr(regexp_replace(SUBSTR(trim(ITEM_NO), 5, LENGTH(trim(ITEM_NO))                         -3), '[B-Z]', 'A'), 'A')>0
          THEN SUBSTR(trim(ITEM_NO), 1, instr(regexp_replace(SUBSTR(trim(ITEM_NO), 4, LENGTH(trim(ITEM_NO))-3), '[B-Z]', 'A'), 'A')+ 2)
          ELSE trim(ITEM_NO)
        END, 1, 7)) ORDER BY ITEM_KEY_NO DESC) AS ITEM_RNK
      FROM dwsavr02.dwv03000_item_dim
      WHERE cntry_key_no                                                                    IN (5, 26, 23, 134, 56, 39, 76, 34, 74, 75, 17, 54, 48, 16, 44, 57, 10, 8, 47, 18, 14, 11, 46, 45, 30, 37, 60, 59, 52, 29, 27, 13, 32, 6, 42, 114, 41)
      AND CAST(SUBSTR(mo_yr_curcy_comb_key_no, 1, 6) AS INTEGER) = EXTRACT(YEAR FROM SYSDATE)*100+EXTRACT(MONTH FROM SYSDATE)
      AND NVL(item_desc, 'N/A')                                 <> 'Inserted From OMS Orders'
      )
    WHERE ITEM_RNK=1
    ),
    ITM_ADM AS
    (SELECT ITEM_DIM_0.CNTRY_KEY_NO,
      T1.AFF_ID,
      T1.AFF_KEY_NO,
      T1.CNTRY_ID,
      ITEM_DIM_0.BASE_7_ITEM_NO,
      NVL(NVL(ITEM_DIM.GLBL_BUS_LN_DESC, ITEM_DIM_RAW1.GLBL_BUS_LN_DESC), ITEM_DIM_0.GLBL_BUS_LN_DESC)             AS GLBL_BUS_LN_DESC,
      NVL(NVL(ITEM_DIM.GLBL_BRAND_DESC, ITEM_DIM_RAW1.GLBL_BRAND_DESC), ITEM_DIM_0.GLBL_BRAND_DESC)                AS GLBL_BRAND_DESC,
      NVL(NVL(ITEM_DIM.GLBL_CTGRY_DESC, ITEM_DIM_RAW1.GLBL_CTGRY_DESC), ITEM_DIM_0.GLBL_CTGRY_DESC)                AS GLBL_CTGRY_DESC,
      NVL(NVL(ITEM_DIM.GLBL_SUB_CTGRY_DESC, ITEM_DIM_RAW1.GLBL_SUB_CTGRY_DESC), ITEM_DIM_0.GLBL_SUB_CTGRY_DESC)    AS GLBL_SUB_CTGRY_DESC,
      NVL(NVL(ITEM_DIM.GLBL_PRDCT_GRP_DESC, ITEM_DIM_RAW1.GLBL_PRDCT_GRP_DESC), ITEM_DIM_0.GLBL_PRDCT_GRP_DESC)    AS GLBL_PRDCT_GRP_DESC,
      NVL(NVL(ITEM_DIM.GLBL_PRDCT_FMLY_DESC, ITEM_DIM_RAW1.GLBL_PRDCT_FMLY_DESC), ITEM_DIM_0.GLBL_PRDCT_FMLY_DESC) AS GLBL_PRDCT_FMLY_DESC,
      NVL(NVL(ITEM_DIM.GLBL_SUB_BRAND_DESC, ITEM_DIM_RAW1.GLBL_SUB_BRAND_DESC), ITEM_DIM_0.GLBL_SUB_BRAND_DESC)    AS GLBL_SUB_BRAND_DESC,
      NVL(NVL(ITEM_DIM.GLBL_BASE_ITEM_DESC, ITEM_DIM_RAW1.GLBL_BASE_ITEM_DESC), ITEM_DIM_0.GLBL_BASE_ITEM_DESC)    AS GLBL_BASE_ITEM_DESC
    FROM ITEM_DIM_0 ITEM_DIM_0
    LEFT JOIN ITEM_DIM ITEM_DIM
    ON ITEM_DIM_0.BASE_7_ITEM_NO=ITEM_DIM.BASE_7_ITEM_NO
    AND ITEM_DIM_0.CNTRY_KEY_NO =ITEM_DIM.CNTRY_KEY_NO
    LEFT JOIN ITEM_DIM_RAW1 ITEM_DIM_RAW1
    ON ITEM_DIM_0.BASE_7_ITEM_NO =ITEM_DIM_RAW1.BASE_7_ITEM_NO
    AND ITEM_DIM_0.CNTRY_KEY_NO  =ITEM_DIM_RAW1.CNTRY_KEY_NO
    LEFT JOIN
      (SELECT CNTRY_KEY_NO,
        AFF_ID,
        AFF_KEY_NO,
        CAST(AMWAY_CNTRY_CD AS INTEGER) AS CNTRY_ID
      FROM DWSAVR02.AWV00004_CNTRY_AFF_DIM
      WHERE CNTRY_KEY_NO IN (6, 8, 10, 11, 13, 14, 16, 17, 18, 23, 27, 29, 30, 32, 34, 37, 39, 41, 42, 44, 45, 46, 47, 48, 52, 54, 56, 57, 59, 60, 74, 75, 76, 114, 134, 5, 26)
      ) T1
    ON ITEM_DIM_0.CNTRY_KEY_NO=T1.CNTRY_KEY_NO
    ),
    ITEM_DIM_CORR AS
    (SELECT DISTINCT ITM_ADM.CNTRY_KEY_NO,
      ITM_ADM.AFF_KEY_NO,
      ITM_ADM.CNTRY_ID,
      ITM_ADM.BASE_7_ITEM_NO                                                                               AS BASE_7_ITEM_NO,
      NVL(NVL(NULLIF(ITM_ADM.GLBL_BUS_LN_DESC, 'OTHER'), ATM.GLBL_BUS_LN_DESC), ITM_ADM.GLBL_BUS_LN_DESC)  AS GLBL_BUS_LN_DESC,
      NVL(NVL(NULLIF(ITM_ADM.GLBL_BRAND_DESC, 'UNDEFINED'), ATM.GLBL_BRAND_DESC), ITM_ADM.GLBL_BRAND_DESC) AS GLBL_BRAND_DESC,
      NVL(NVL(
      CASE
        WHEN ITM_ADM.GLBL_CTGRY_DESC IN ('OTHER', 'DEFAULT', 'NONE')
        THEN NULL
        ELSE ITM_ADM.GLBL_CTGRY_DESC
      END, ATM.GLBL_CTGRY_DESC), ITM_ADM.GLBL_CTGRY_DESC) AS GLBL_CTGRY_DESC,
      GLBL_SUB_CTGRY_DESC,
      GLBL_PRDCT_GRP_DESC,
      GLBL_PRDCT_FMLY_DESC,
      GLBL_SUB_BRAND_DESC,
      GLBL_BASE_ITEM_DESC
    FROM ITM_ADM ITM_ADM
    LEFT JOIN ATM ATM
    ON ITM_ADM.CNTRY_ID       =ATM.CNTRY_ID
    AND ITM_ADM.BASE_7_ITEM_NO=ATM.BASE_7_ITEM_NO
    ) ,
    EUR_MKTG_INPUT AS
    (SELECT SALES_T.SKU,
      MAX(NVL(UPPER(EUR_MAP.BUS_LINE), SALES_T.BUS_LINE)) AS BUSINESS_LINE,
      MAX(NVL(DECODE(SUBSTR(EUR_MAP.BRAND_GROUP, 1, 3),'N/A', UPPER(EUR_MAP.BRAND_GROUP), EUR_MAP.BRAND_GROUP), 'N/A - '
      ||SALES_T.BUS_LINE)) AS BRAND_GROUP,
      MAX(NVL(DECODE(SUBSTR(EUR_MAP."CATEGORY", 1, 3),'N/A', UPPER(EUR_MAP."CATEGORY"), EUR_MAP."CATEGORY"), 'N/A - '
      ||SALES_T.BUS_LINE)) AS "CATEGORY",
      MAX(NVL(DECODE(SUBSTR(EUR_MAP.SUB_CATEGORY, 1, 3),'N/A', UPPER(EUR_MAP.SUB_CATEGORY), EUR_MAP.SUB_CATEGORY), 'N/A - '
      ||SALES_T.BUS_LINE)) AS SUB_CATEGORY,
      MAX(NVL(DECODE(SUBSTR(EUR_MAP.PRODUCT_FAMILY, 1, 3),'N/A', UPPER(EUR_MAP.PRODUCT_FAMILY), EUR_MAP.PRODUCT_FAMILY), 'N/A - '
      ||SALES_T.BUS_LINE))                         AS PRODUCT_FAMILY,
      MAX(NVL(EUR_MAP.PRODUCT_NAME, 'N/A'))        AS PRODUCT_NAME,
      MAX(NVL(SALES_T.GLBL_BASE_ITEM_DESC, 'N/A')) AS PRODUCT_NAME_GLBL
    FROM
      (SELECT BASE_7_ITEM_NO     AS SKU,
        MAX(GLBL_BUS_LN_DESC)    AS BUS_LINE,
        MAX(GLBL_BASE_ITEM_DESC) AS GLBL_BASE_ITEM_DESC
      FROM ITEM_DIM_CORR
      GROUP BY BASE_7_ITEM_NO
      ) SALES_T,
      DWSEAI01.EUR_MKTG_INPUT_RAW EUR_MAP
    WHERE SALES_T.SKU =EUR_MAP.SKU(+)
    GROUP BY SALES_T.SKU
    ) ,
    OMS_MAIN AS
    (SELECT FACT_T.ORD_MO_YR_ID         AS YEARMONTH,
      ITM_T.GLBL_BUS_LN_DESC            AS BUS_LINE,
      TRIM(ITM_T.GLBL_CTGRY_DESC)       AS ITEM_CATEGORY,
      TRIM(ITM_T.GLBL_SUB_CTGRY_DESC)   AS ITEM_SUBCATEGORY,
      ITM_T.GLBL_PRDCT_GRP_DESC         AS P_GROUP,
      ITM_T.GLBL_PRDCT_FMLY_DESC        AS FAMILY,
      ITM_T.GLBL_BRAND_DESC             AS BRAND,
      ITM_T.GLBL_SUB_BRAND_DESC         AS SUB_BRAND,
      ITM_T.BASE_7_ITEM_NO              AS SKU,
      EUR_ITM_T.BUSINESS_LINE           AS BUS_LINE_EUR,
      EUR_ITM_T.BRAND_GROUP             AS BRAND_EUR,
      EUR_ITM_T."CATEGORY"              AS ITEM_CATEGORY_EUR,
      EUR_ITM_T.SUB_CATEGORY            AS ITEM_SUBCATEGORY_EUR,
      EUR_ITM_T.PRODUCT_FAMILY          AS FAMILY_EUR,
      EUR_ITM_T.PRODUCT_NAME            AS PRODUCT_NAME_EUR,
      TRIM(EUR_ITM_T.PRODUCT_NAME_GLBL) AS PRODUCT_NAME_GLBL,
      ACCOUNT_ID                        AS IMC_NO,
      OPER_AFF_ID,
      SUM(ADJ_LN_PV)      AS PV,
      SUM(ADJ_LN_BV_USD)  AS BV_USD,
      SUM(ADJ_LN_USD_NET) AS USD,
      SUM(
      CASE
        WHEN ADJ_LN_PV>0
        THEN ORD_QTY
        ELSE 0
      END) AS OMS_UNIT_CNT
    FROM
      (SELECT *
      FROM DWSEAI01.DS_DET_DAILY_V T0
      WHERE ORD_MO_YR_ID >= YM_START
      AND ORD_MO_YR_ID    < = YM_END
      AND ORD_LN_DISP_CD IN ('*', '1', 'E', 'Item Disposition Code:  S/S', 'Item Disposition Code:  S/T', 'S', 'Item Disposition Code:  S/B', 'Item Disposition Code:  S/N', '2')
      AND ORD_CANC_FLAG   ='false'
      AND PAY_REQ_FLAG   <>'N'
      AND OPER_CNTRY_ID  IN (30,240,210,620,650,390,800,340,820,810,150,590,490,140,450,660,80,60,480,160,120,90,470,460,280,370,750,740,570,270,250,110,300,40,430,830,420)
      ) FACT_T,
      ITEM_DIM_CORR ITM_T,
      EUR_MKTG_INPUT EUR_ITM_T
    WHERE ITM_T.CNTRY_KEY_NO IN (6,8,10,11,13,14,16,17,18,23,27 ,29,30,32,34,37,39,41,42,44,45,46,47,48,52,54,56,57,59,60,74, 75,76,114,134,5,26)
    AND ITM_T.BASE_7_ITEM_NO  =SUBSTR(FACT_T.ORD_ITEM_BASE_CD, 1, 7)
    AND ITM_T.CNTRY_ID        =FACT_T.OPER_CNTRY_ID
    AND ITM_T.BASE_7_ITEM_NO  =EUR_ITM_T.SKU(+)
    AND ADJ_LN_PV            <>0
    GROUP BY ORD_MO_YR_ID,
      ITM_T.GLBL_BUS_LN_DESC,
      TRIM(ITM_T.GLBL_CTGRY_DESC),
      TRIM(ITM_T.GLBL_SUB_CTGRY_DESC),
      ITM_T.GLBL_PRDCT_GRP_DESC,
      ITM_T.GLBL_PRDCT_FMLY_DESC,
      ITM_T.GLBL_BRAND_DESC,
      ITM_T.GLBL_SUB_BRAND_DESC,
      ITM_T.BASE_7_ITEM_NO,
      EUR_ITM_T.BUSINESS_LINE,
      EUR_ITM_T.BRAND_GROUP,
      EUR_ITM_T."CATEGORY",
      EUR_ITM_T.SUB_CATEGORY,
      EUR_ITM_T.PRODUCT_FAMILY,
      EUR_ITM_T.PRODUCT_NAME,
      TRIM(EUR_ITM_T.PRODUCT_NAME_GLBL),
      ACCOUNT_ID,
      OPER_AFF_ID
    ) ,
    OMS_OLD AS
    (SELECT T2.*,
      BL_BUYER_FLAG      /NULLIF(SUM(BL_BUYER_FLAG) OVER(PARTITION BY IMC_NO, OPER_AFF_ID, YEARMONTH, BUS_LINE), 0)                 AS BL_BUYER_FLAG_PART,
      CAT_BUYER_FLAG     /NULLIF(SUM(CAT_BUYER_FLAG) OVER(PARTITION BY IMC_NO, OPER_AFF_ID, YEARMONTH, ITEM_CATEGORY, BUS_LINE), 0) AS CAT_BUYER_FLAG_PART,
      SUBCAT_BUYER_FLAG  /NULLIF(SUM(SUBCAT_BUYER_FLAG) OVER(PARTITION BY IMC_NO, OPER_AFF_ID, YEARMONTH, ITEM_SUBCATEGORY), 0)     AS SUBCAT_BUYER_FLAG_PART,
      P_GROUP_BUYER_FLAG /NULLIF(SUM(P_GROUP_BUYER_FLAG) OVER(PARTITION BY IMC_NO, OPER_AFF_ID, YEARMONTH, P_GROUP), 0)             AS P_GROUP_BUYER_FLAG_PART,
      FAMILY_BUYER_FLAG  /NULLIF(SUM(FAMILY_BUYER_FLAG) OVER(PARTITION BY IMC_NO, OPER_AFF_ID, YEARMONTH, FAMILY), 0)               AS FAMILY_BUYER_FLAG_PART,
      BR_BUYER_FLAG      /NULLIF(SUM(BR_BUYER_FLAG) OVER(PARTITION BY IMC_NO, OPER_AFF_ID, YEARMONTH, BRAND), 0)                    AS BR_BUYER_FLAG_PART,
      S_BR_BUYER_FLAG    /NULLIF(SUM(S_BR_BUYER_FLAG) OVER(PARTITION BY IMC_NO, OPER_AFF_ID, YEARMONTH, SUB_BRAND, BRAND), 0)       AS S_BR_BUYER_FLAG_PART
    FROM
      (SELECT T1.*,
        CAST(CAST(YEARMONTH/100 AS INTEGER)*12+mod(YEARMONTH, 100) AS INTEGER) AS YEARMONTH_MONTHS,
        CASE
          WHEN SUM(PV) OVER(PARTITION BY IMC_NO, OPER_AFF_ID, YEARMONTH, BUS_LINE)>0
          THEN 1
          ELSE 0
        END BL_BUYER_FLAG,
        CASE
          WHEN SUM(PV) OVER(PARTITION BY IMC_NO, OPER_AFF_ID, YEARMONTH, ITEM_CATEGORY, BUS_LINE)>0
          THEN 1
          ELSE 0
        END CAT_BUYER_FLAG,
        CASE
          WHEN SUM(PV) OVER(PARTITION BY IMC_NO, OPER_AFF_ID, YEARMONTH, ITEM_SUBCATEGORY)>0
          THEN 1
          ELSE 0
        END SUBCAT_BUYER_FLAG,
        CASE
          WHEN SUM(PV) OVER(PARTITION BY IMC_NO, OPER_AFF_ID, YEARMONTH, P_GROUP)>0
          THEN 1
          ELSE 0
        END P_GROUP_BUYER_FLAG,
        CASE
          WHEN SUM(PV) OVER(PARTITION BY IMC_NO, OPER_AFF_ID, YEARMONTH, FAMILY)>0
          THEN 1
          ELSE 0
        END FAMILY_BUYER_FLAG,
        CASE
          WHEN SUM(PV) OVER(PARTITION BY IMC_NO, OPER_AFF_ID, YEARMONTH, BRAND)>0
          THEN 1
          ELSE 0
        END BR_BUYER_FLAG,
        CASE
          WHEN SUM(PV) OVER(PARTITION BY IMC_NO, OPER_AFF_ID, YEARMONTH, SUB_BRAND, BRAND)>0
          THEN 1
          ELSE 0
        END S_BR_BUYER_FLAG,
        CASE
          WHEN SUM(PV) OVER(PARTITION BY IMC_NO, OPER_AFF_ID, YEARMONTH, SKU)>0
          THEN 1
          ELSE 0
        END SKU_BUYER_FLAG,
        CASE
          WHEN SUM(PV) OVER(PARTITION BY IMC_NO, OPER_AFF_ID, YEARMONTH, BUS_LINE_EUR)>0
          THEN 1
          ELSE 0
        END BL_BUYER_FLAG_EUR,
        CASE
          WHEN SUM(PV) OVER(PARTITION BY IMC_NO, OPER_AFF_ID, YEARMONTH, BRAND_EUR)>0
          THEN 1
          ELSE 0
        END BR_BUYER_FLAG_EUR,
        CASE
          WHEN SUM(PV) OVER(PARTITION BY IMC_NO, OPER_AFF_ID, YEARMONTH, ITEM_CATEGORY_EUR)>0
          THEN 1
          ELSE 0
        END CAT_BUYER_FLAG_EUR,
        CASE
          WHEN SUM(PV) OVER(PARTITION BY IMC_NO, OPER_AFF_ID, YEARMONTH, ITEM_SUBCATEGORY_EUR)>0
          THEN 1
          ELSE 0
        END SUBCAT_BUYER_FLAG_EUR,
        CASE
          WHEN SUM(PV) OVER(PARTITION BY IMC_NO, OPER_AFF_ID, YEARMONTH, FAMILY_EUR)>0
          THEN 1
          ELSE 0
        END FAMILY_BUYER_FLAG_EUR
      FROM
        (SELECT YEARMONTH,
          BUS_LINE,
          ITEM_CATEGORY,
          ITEM_SUBCATEGORY,
          P_GROUP,
          FAMILY,
          BRAND,
          SUB_BRAND,
          SKU,
          BUS_LINE_EUR,
          BRAND_EUR,
          ITEM_CATEGORY_EUR,
          ITEM_SUBCATEGORY_EUR,
          FAMILY_EUR,
          PRODUCT_NAME_EUR,
          PRODUCT_NAME_GLBL,
          IMC_NO,
          OPER_AFF_ID,
          SUM(PV)           AS PV,
          SUM(BV_USD)       AS BV_USD,
          SUM(USD)          AS USD,
          SUM(OMS_UNIT_CNT) AS OMS_UNIT_CNT
        FROM OMS_MAIN
        GROUP BY YEARMONTH,
          BUS_LINE,
          ITEM_CATEGORY,
          ITEM_SUBCATEGORY,
          P_GROUP,
          FAMILY,
          BRAND,
          SUB_BRAND,
          SKU,
          BUS_LINE_EUR,
          BRAND_EUR,
          ITEM_CATEGORY_EUR,
          ITEM_SUBCATEGORY_EUR,
          FAMILY_EUR,
          PRODUCT_NAME_EUR,
          PRODUCT_NAME_GLBL,
          IMC_NO,
          OPER_AFF_ID
        ) T1
      ) T2
    )
  /*Main script*/
  SELECT *
  FROM
    (SELECT SALES_T.YEARMONTH,
      SALES_T.BUS_LINE,
      SALES_T.ITEM_CATEGORY,
      SALES_T.ITEM_SUBCATEGORY,
      SALES_T.P_GROUP,
      SALES_T.FAMILY,
      SALES_T.BRAND,
      SALES_T.SUB_BRAND,
      SALES_T.SKU,
      SALES_T.BUS_LINE_EUR,
      SALES_T.BRAND_EUR,
      SALES_T.ITEM_CATEGORY_EUR,
      SALES_T.ITEM_SUBCATEGORY_EUR,
      SALES_T.FAMILY_EUR,
      SALES_T.PRODUCT_NAME_EUR,
      SALES_T.PRODUCT_NAME_GLBL,
      SN_T.IMC_KEY_NO,
      SN_T.TENURE,
      SN_T.NEW_APP_FLAG,
      SN_T.U35_FLAG,
      SN_T.COUNTRY,
      SN_T.GL_SEGMENT,
      SN_T.SALES_REGION,
      SN_T.STATE,
      SN_T.IMC_TYPE,
      SN_T.DIST_TYPE,
      SN_T.BV_USD                                                                                                                                                                                                                                                                             AS BV_USD_TTL,
      SN_T.PV                                                                                                                                                                                                                                                                                 AS PV_TTL,
      SN_T.OMS_UNIT_CNT                                                                                                                                                                                                                                                                       AS OMS_UNIT_CNT_TTL,
      SN_T.BUYER_FLAG                                                                                                                                                                                                                                                                         AS BUYER_FLAG_TTL,
      COUNT(DISTINCT NULLIF(SN_T.BUYER_FLAG, 0)*SN_T.IMC_KEY_NO) OVER(PARTITION BY SN_T.COUNTRY, SN_T.GL_SEGMENT, SN_T.SALES_REGION, SN_T.STATE, SN_T.IMC_TYPE, SN_T.DIST_TYPE, SN_T.U35_FLAG, SN_T.NEW_APP_FLAG, SN_T.TENURE, SALES_T.YEARMONTH)                                             AS BUYERS_CNT_TTL,
      SN_T.R_IMC                                                                                                                                                                                                                                                                              AS R_IMC_TTL,
      NVL(SALES_T.PV, 0)                                                                                                                                                                                                                                                                      AS PV,
      NVL(SALES_T.BV_USD, 0)                                                                                                                                                                                                                                                                  AS BV_USD,
      NVL(SALES_T.OMS_UNIT_CNT, 0)                                                                                                                                                                                                                                                            AS OMS_UNIT_CNT,
      NVL(SALES_T.BL_BUYER_FLAG, 0)                                                                                                                                                                                                                                                           AS BL_BUYER_FLAG,
      NVL(SALES_T.CAT_BUYER_FLAG, 0)                                                                                                                                                                                                                                                          AS CAT_BUYER_FLAG,
      NVL(SALES_T.SUBCAT_BUYER_FLAG, 0)                                                                                                                                                                                                                                                       AS SUBCAT_BUYER_FLAG,
      NVL(SALES_T.BR_BUYER_FLAG, 0)                                                                                                                                                                                                                                                           AS BR_BUYER_FLAG,
      NVL(SALES_T.S_BR_BUYER_FLAG, 0)                                                                                                                                                                                                                                                         AS S_BR_BUYER_FLAG,
      NVL(SALES_T.P_GROUP_BUYER_FLAG, 0)                                                                                                                                                                                                                                                      AS P_GROUP_BUYER_FLAG,
      NVL(SALES_T.FAMILY_BUYER_FLAG, 0)                                                                                                                                                                                                                                                       AS FAMILY_BUYER_FLAG,
      NVL(SALES_T.SKU_BUYER_FLAG, 0)                                                                                                                                                                                                                                                          AS SKU_BUYER_FLAG,
      COUNT(DISTINCT NULLIF(SALES_T.SKU_BUYER_FLAG, 0)*SN_T.IMC_KEY_NO) OVER(PARTITION BY SN_T.COUNTRY, SN_T.GL_SEGMENT, SN_T.SALES_REGION, SN_T.STATE, SN_T.IMC_TYPE, SN_T.DIST_TYPE, SN_T.U35_FLAG, SN_T.NEW_APP_FLAG, SN_T.TENURE, SALES_T.YEARMONTH, SALES_T.SKU)                         AS SKU_BUYERS_CNT,
      SUM(NVL(SALES_T.PV, 0)) OVER(PARTITION BY SN_T.COUNTRY, SN_T.GL_SEGMENT, SN_T.SALES_REGION, SN_T.STATE, SN_T.IMC_TYPE, SN_T.DIST_TYPE, SN_T.U35_FLAG, SN_T.NEW_APP_FLAG, SN_T.TENURE, SALES_T.YEARMONTH, BUS_LINE_EUR)                                                                  AS BL_PV_EUR,
      SUM(NVL(SALES_T.PV, 0)) OVER(PARTITION BY SN_T.COUNTRY, SN_T.GL_SEGMENT, SN_T.SALES_REGION, SN_T.STATE, SN_T.IMC_TYPE, SN_T.DIST_TYPE, SN_T.U35_FLAG, SN_T.NEW_APP_FLAG, SN_T.TENURE, SALES_T.YEARMONTH, BRAND_EUR)                                                                     AS BR_PV_EUR,
      SUM(NVL(SALES_T.PV, 0)) OVER(PARTITION BY SN_T.COUNTRY, SN_T.GL_SEGMENT, SN_T.SALES_REGION, SN_T.STATE, SN_T.IMC_TYPE, SN_T.DIST_TYPE, SN_T.U35_FLAG, SN_T.NEW_APP_FLAG, SN_T.TENURE, SALES_T.YEARMONTH, ITEM_CATEGORY_EUR)                                                             AS CAT_PV_EUR,
      SUM(NVL(SALES_T.PV, 0)) OVER(PARTITION BY SN_T.COUNTRY, SN_T.GL_SEGMENT, SN_T.SALES_REGION, SN_T.STATE, SN_T.IMC_TYPE, SN_T.DIST_TYPE, SN_T.U35_FLAG, SN_T.NEW_APP_FLAG, SN_T.TENURE, SALES_T.YEARMONTH, ITEM_SUBCATEGORY_EUR)                                                          AS SUBCAT_PV_EUR,
      SUM(NVL(SALES_T.PV, 0)) OVER(PARTITION BY SN_T.COUNTRY, SN_T.GL_SEGMENT, SN_T.SALES_REGION, SN_T.STATE, SN_T.IMC_TYPE, SN_T.DIST_TYPE, SN_T.U35_FLAG, SN_T.NEW_APP_FLAG, SN_T.TENURE, SALES_T.YEARMONTH, FAMILY_EUR)                                                                    AS FAMILY_PV_EUR,
      SUM(NVL(SALES_T.PV, 0)) OVER(PARTITION BY SN_T.COUNTRY, SN_T.GL_SEGMENT, SN_T.SALES_REGION, SN_T.STATE, SN_T.IMC_TYPE, SN_T.DIST_TYPE, SN_T.U35_FLAG, SN_T.NEW_APP_FLAG, SN_T.TENURE, SALES_T.YEARMONTH)                                                                                AS TTL_PV_EUR,
      COUNT(DISTINCT NULLIF(SALES_T.BL_BUYER_FLAG_EUR, 0)    *SN_T.IMC_KEY_NO) OVER(PARTITION BY SN_T.COUNTRY, SN_T.GL_SEGMENT, SN_T.SALES_REGION, SN_T.STATE, SN_T.IMC_TYPE, SN_T.DIST_TYPE, SN_T.U35_FLAG, SN_T.NEW_APP_FLAG, SN_T.TENURE, SALES_T.YEARMONTH, SALES_T.BUS_LINE_EUR)         AS BL_BUYERS_CNT_EUR,
      COUNT(DISTINCT NULLIF(SALES_T.BR_BUYER_FLAG_EUR, 0)    *SN_T.IMC_KEY_NO) OVER(PARTITION BY SN_T.COUNTRY, SN_T.GL_SEGMENT, SN_T.SALES_REGION, SN_T.STATE, SN_T.IMC_TYPE, SN_T.DIST_TYPE, SN_T.U35_FLAG, SN_T.NEW_APP_FLAG, SN_T.TENURE, SALES_T.YEARMONTH, SALES_T.BRAND_EUR)            AS BR_BUYERS_CNT_EUR,
      COUNT(DISTINCT NULLIF(SALES_T.CAT_BUYER_FLAG_EUR, 0)   *SN_T.IMC_KEY_NO) OVER(PARTITION BY SN_T.COUNTRY, SN_T.GL_SEGMENT, SN_T.SALES_REGION, SN_T.STATE, SN_T.IMC_TYPE, SN_T.DIST_TYPE, SN_T.U35_FLAG, SN_T.NEW_APP_FLAG, SN_T.TENURE, SALES_T.YEARMONTH, SALES_T.ITEM_CATEGORY_EUR)    AS CAT_BUYERS_CNT_EUR,
      COUNT(DISTINCT NULLIF(SALES_T.SUBCAT_BUYER_FLAG_EUR, 0)*SN_T.IMC_KEY_NO) OVER(PARTITION BY SN_T.COUNTRY, SN_T.GL_SEGMENT, SN_T.SALES_REGION, SN_T.STATE, SN_T.IMC_TYPE, SN_T.DIST_TYPE, SN_T.U35_FLAG, SN_T.NEW_APP_FLAG, SN_T.TENURE, SALES_T.YEARMONTH, SALES_T.ITEM_SUBCATEGORY_EUR) AS SUBCAT_BUYERS_CNT_EUR,
      COUNT(DISTINCT NULLIF(SALES_T.FAMILY_BUYER_FLAG_EUR, 0)*SN_T.IMC_KEY_NO) OVER(PARTITION BY SN_T.COUNTRY, SN_T.GL_SEGMENT, SN_T.SALES_REGION, SN_T.STATE, SN_T.IMC_TYPE, SN_T.DIST_TYPE, SN_T.U35_FLAG, SN_T.NEW_APP_FLAG, SN_T.TENURE, SALES_T.YEARMONTH, SALES_T.FAMILY_EUR)           AS FAMILY_BUYERS_CNT_EUR,
      NVL(SALES_T.BL_BUYER_FLAG, 0)                          *
      CASE
        WHEN ROUND(SUM(SALES_T.BL_BUYER_FLAG_PART) OVER(PARTITION BY SN_T.IMC_KEY_NO, SALES_T.BUS_LINE ORDER BY CAST(SALES_T.YEARMONTH_MONTHS AS INTEGER) RANGE BETWEEN 2 PRECEDING AND CURRENT ROW))>1
        THEN 1
        ELSE 0
      END AS R_IMC_BL,
      NVL(SALES_T.CAT_BUYER_FLAG, 0) *
      CASE
        WHEN ROUND(SUM(SALES_T.CAT_BUYER_FLAG_PART) OVER(PARTITION BY SN_T.IMC_KEY_NO, SALES_T.ITEM_CATEGORY, SALES_T.BUS_LINE ORDER BY CAST(SALES_T.YEARMONTH_MONTHS AS INTEGER) RANGE BETWEEN 2 PRECEDING AND CURRENT ROW))>1
        THEN 1
        ELSE 0
      END AS R_IMC_CAT,
      NVL(SALES_T.SUBCAT_BUYER_FLAG, 0) *
      CASE
        WHEN ROUND(SUM(SALES_T.SUBCAT_BUYER_FLAG_PART) OVER(PARTITION BY SN_T.IMC_KEY_NO, SALES_T.ITEM_SUBCATEGORY ORDER BY CAST(SALES_T.YEARMONTH_MONTHS AS INTEGER) RANGE BETWEEN 2 PRECEDING AND CURRENT ROW))>1
        THEN 1
        ELSE 0
      END AS R_IMC_SUBCAT,
      NVL(SALES_T.P_GROUP_BUYER_FLAG, 0) *
      CASE
        WHEN ROUND(SUM(SALES_T.P_GROUP_BUYER_FLAG_PART) OVER(PARTITION BY SN_T.IMC_KEY_NO, SALES_T.P_GROUP ORDER BY CAST(SALES_T.YEARMONTH_MONTHS AS INTEGER) RANGE BETWEEN 2 PRECEDING AND CURRENT ROW))>1
        THEN 1
        ELSE 0
      END AS R_IMC_P_GROUP,
      NVL(SALES_T.FAMILY_BUYER_FLAG, 0) *
      CASE
        WHEN ROUND(SUM(SALES_T.FAMILY_BUYER_FLAG_PART) OVER(PARTITION BY SN_T.IMC_KEY_NO, SALES_T.FAMILY ORDER BY CAST(SALES_T.YEARMONTH_MONTHS AS INTEGER) RANGE BETWEEN 2 PRECEDING AND CURRENT ROW))>1
        THEN 1
        ELSE 0
      END AS R_IMC_FAMILY,
      NVL(SALES_T.BR_BUYER_FLAG, 0) *
      CASE
        WHEN ROUND(SUM(SALES_T.BR_BUYER_FLAG_PART) OVER(PARTITION BY SN_T.IMC_KEY_NO, SALES_T.BRAND ORDER BY CAST(SALES_T.YEARMONTH_MONTHS AS INTEGER) RANGE BETWEEN 2 PRECEDING AND CURRENT ROW))>1
        THEN 1
        ELSE 0
      END AS R_IMC_BR,
      NVL(SALES_T.S_BR_BUYER_FLAG, 0) *
      CASE
        WHEN ROUND(SUM(SALES_T.S_BR_BUYER_FLAG_PART) OVER(PARTITION BY SN_T.IMC_KEY_NO, SALES_T.SUB_BRAND, SALES_T.BRAND ORDER BY CAST(SALES_T.YEARMONTH_MONTHS AS INTEGER) RANGE BETWEEN 2 PRECEDING AND CURRENT ROW))>1
        THEN 1
        ELSE 0
      END                 AS R_IMC_S_BR,
      NVL(SALES_T.USD, 0) AS USD
    FROM OMS_OLD SALES_T
    LEFT JOIN
      (SELECT * FROM DWSEAI01.OMS_TEMP T0
      ) SN_T
    ON SALES_T.YEARMONTH   =SN_T.YEARMONTH
    AND SALES_T.IMC_NO     =SN_T.IMC_NO
    AND SALES_T.OPER_AFF_ID=SN_T.AFF_INT
    ) T00
  WHERE YEARMONTH = YM_END
  AND COUNTRY    IS NOT NULL
    ) s;
  COMMIT;
END;
