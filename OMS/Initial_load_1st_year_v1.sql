DECLARE
  YM_START NUMBER:=0;
  YM_END   NUMBER:=0;
BEGIN
  FOR i IN 24217..24258
  LOOP
    /*Loop counter in YEARMONTH_MONTHS*/
    YM_START:=DWSEAI01.YMM2YM(i-5);
    /*Start of OMS Sales period (6 months including focus month, for RF calculation*/
    YM_END:=DWSEAI01.YMM2YM(i);
    /*DWSEAI01.YMM2YM converts YEARMONTH_MONTHS to YEARMONTH*/
    INSERT
      /*+ append parallel(t,32)*/
    INTO DWSEAI01.OMS_TEMP_RMS_PART_TEST t
    SELECT
      /*+ parallel(s,32)*/
      *
    FROM
      (WITH COUNTRY_T AS
      (SELECT CNTRY_KEY_NO,
        AFF_ID,
        TRIM(CAST(CNTRY_SHORT_NM AS VARCHAR2(30 CHAR))) AS CNTRY_SHORT_NM,
        CAST(AMWAY_CNTRY_CD AS INTEGER)                 AS CNTRY_ID
      FROM DWSAVR02.AWV00004_CNTRY_AFF_DIM
      WHERE CNTRY_KEY_NO IN (5,26,6,8,10,11,13,14,16,17,18,23,27,29,30,32,34,37,39,41,42,44,45,46,47,48,52,54,56,57,59,60,74,75,76,114, 134)
      ),
      SNAPSHOT_TBL AS
      (SELECT SN_MAIN.*,
        CASE
          WHEN (GPK_TYPE     IN ('GPKh', 'Pending GPKh', 'Customer+')
          AND CNTRY_SHORT_NM IN ('RUSSIA', 'KAZAKHSTAN') )
          THEN GPK_TYPE
          WHEN (ACCOUNT_SUBTYPE ='Tax-Registered')
          THEN 'TAX REGISTERED'
          ELSE IMC_TYPE
        END AS DIST_TYPE,
        SEG_T.SEG_ALT_DESC
      FROM
        (SELECT T1.SNAP_MO_YR_KEY_NO,
          T1.SNAP_MO_YR_KEY_NO AS YEARMONTH,
          T1.SPON_IMC_KEY_NO   AS SPONSOR_IMC_KEY_NO,
          T1.IMC_KEY_NO        AS SPONSORED_ABO_KEY_NO,
          T1.GLOBL_IMC_TYPE_KEY_NO,
          T1.IMC_KEY_NO,
          T2.IMC_NO,
          T2.IMC_AFF_ID,
          T1.STATUS_KEY_NO,
          T1.POSTL_CD_KEY_NO,
          IMC_TYPE_MAPPING.ACCOUNT_SUBTYPE,
          T2.POSTL_CD_KEY_NO              AS POSTL_CD_KEY_NO_CURR,
          NVL(GPK_T.GPK_TYPE, 'NON-GPKh') AS GPK_TYPE,
          T1.IMC_CNTRY_KEY_NO,
          T2.CURR_APPL_DT_KEY_NO                      AS APPL_DT,
          CAST(T2.CURR_APPL_DT_KEY_NO/100 AS INTEGER) AS APPL_YM,
          T2.APP_1_BIRTH_DT_KEY_NO,
          COUNTRY_T.CNTRY_SHORT_NM,
          CASE
            WHEN IMC_TYPE_MAPPING.ACCOUNT_TYPE IN ('ABO', 'Amway')
            THEN 'ABO'
            WHEN IMC_TYPE_MAPPING.ACCOUNT_SUBTYPE IN ('Momspresso Customer', 'Customer')
            AND CONV_CUST.IBO_NO                  IS NULL
            THEN 'Customer'
            WHEN IMC_TYPE_MAPPING.ACCOUNT_SUBTYPE IN ('Customer')
            AND CONV_CUST.IBO_NO                  IS NOT NULL
            THEN 'Customer (converted from Members)'
            WHEN IMC_TYPE_MAPPING.ACCOUNT_SUBTYPE IN ('Member')
            THEN 'Member'
            ELSE 'ABO'
          END AS IMC_TYPE
        FROM (SELECT * FROM DWSAVR02.DWV00050_IMC_CCYYMM_FACT  WHERE NOT(IMC_CNTRY_KEY_NO IN (52,59,60)
          AND STATUS_KEY_NO NOT      IN (21,14))) T1
        LEFT JOIN
          (SELECT *
          FROM DWSAVR02.DWV01021_IMC_MASTER_DIM
          ) T2
        ON T1.IMC_KEY_NO =T2.IMC_KEY_NO
        LEFT JOIN DWSEAI01.GPK_HIST_GDW GPK_T
        ON T1.IMC_KEY_NO         = GPK_T.IMC_KEY_NO
        AND T1.SNAP_MO_YR_KEY_NO =GPK_T.YEARMONTH
        LEFT JOIN COUNTRY_T COUNTRY_T
        ON T1.IMC_CNTRY_KEY_NO =COUNTRY_T.CNTRY_KEY_NO
        LEFT JOIN DWSEAI01.IMC_TYPE_MAPPING IMC_TYPE_MAPPING
        ON NVL(NULLIF(TRIM(T1.bus_natr_cd), ''), 'N/A')=IMC_TYPE_MAPPING.BUS_NATR_CD
        AND NVL(NULLIF(trim(T1.imc_type_cd), ''), 'N/A') = IMC_TYPE_MAPPING.IMC_TYPE
        AND COUNTRY_T.AFF_ID=IMC_TYPE_MAPPING.AFF_ID
        LEFT JOIN
          (SELECT DISTINCT AFF_NO,
            IBO_NO
          FROM DWSEAI01.WWV01117_PRTY_LD_NM_TYP_INFMAT
          WHERE AFF_NO IN (210, 60, 160, 90, 470, 250, 110)
          ) CONV_CUST
        ON COUNTRY_T.AFF_ID        =CONV_CUST.AFF_NO
        AND T2.IMC_NO              =CONV_CUST.IBO_NO
        LEFT JOIN DWSEAI01.EXCL_CUST_EU EXCL_CUST_EU
        ON T1.IMC_KEY_NO=EXCL_CUST_EU.IMC_KEY_NO
        WHERE T1.IMC_CNTRY_KEY_NO IN (5,26,6,8,10,11,13,14,16,17,18,23,27,29,30,32,34,37,39,41,42,44,45,46,47,48,52,54,56,57,59,60,74,75,76,114, 134)
        AND T2.IMC_CNTRY_KEY_NO   IN (5,26,6,8,10,11,13,14,16,17,18,23,27,29,30,32,34,37,39,41,42,44,45,46,47,48,52,54,56,57,59,60,74,75,76,114, 134)
        AND EXCL_CUST_EU.IMC_KEY_NO IS NULL
       AND (T1.SNAP_MO_YR_KEY_NO  BETWEEN YM_START AND YM_END      
    OR T1.SNAP_MO_YR_KEY_NO+100 IN
      (YM_END))
        ) SN_MAIN
      LEFT JOIN
        (SELECT MO_YR_KEY_NO,
          IMC_KEY_NO,
          CNTRY_KEY_NO,
          SEGMENT_NO,
          CASE SEGMENT_NO
            WHEN 1
            THEN '1 - ABO Leader'
            WHEN 2
            THEN '2 - Building ABO'
            WHEN 3
            THEN '3 - Developing ABO'
            WHEN 4
            THEN '4 - Customer Equivalent'
            WHEN 5
            THEN '5 - Registered Customers'
            WHEN 6
            THEN '6 - Other'
            ELSE 'N/A'
          END AS SEG_ALT_DESC
        FROM DWSANL02.JC_SEG_WITH_FST
        ) SEG_T
      ON SN_MAIN.IMC_KEY_NO        =SEG_T.IMC_KEY_NO
      AND SN_MAIN.SNAP_MO_YR_KEY_NO=SEG_T.MO_YR_KEY_NO
      ),
      OMS_OLD AS
      (
      SELECT IMC_KEY_NO,
        IMC_NO,
        YEARMONTH,
        YEARMONTH_MONTHS,
        COUNTRY,
        NVL(SALES_REGION, 'Inactive') AS SALES_REGION,
        NVL(STATE, 'Inactive')        AS STATE,
        APPL_DT,
        GL_SEGMENT,
        IMC_TYPE,
        IMC_TYPE_PY,
        PV,
        BV_USD,
        USD,
        OMS_UNIT_CNT,
        PV_EOM,
        BV_USD_EOM,
        CASE
          WHEN ( AGE <35
          AND AGE   >=18 )
          THEN 1
          ELSE 0
        END AS U35_FLAG,
        DIST_TYPE,
        DIST_TYPE_PY
      FROM
        (SELECT T3.STATUS_KEY_NO,
          NVL(T3.YEARMONTH, SALES_T.YEARMONTH)                                                                                         AS YEARMONTH,
          CAST(CAST(NVL(T3.YEARMONTH, SALES_T.YEARMONTH)/100 AS INTEGER)*12+mod(NVL(T3.YEARMONTH, SALES_T.YEARMONTH), 100) AS INTEGER) AS YEARMONTH_MONTHS,
          CASE
            WHEN SALES_T.IMC_TYPE='FOA'
            THEN SALES_T.IMC_NO
            ELSE NVL(T3.IMC_KEY_NO, SALES_T.IMC_KEY_NO)
          END AS IMC_KEY_NO,
          CASE
            WHEN SALES_T.IMC_TYPE='FOA'
            THEN SALES_T.IMC_NO
            ELSE NVL(T3.IMC_NO, SALES_T.IMC_NO)
          END                                                             AS IMC_NO,
          NVL(NVL(T3.CNTRY_SHORT_NM, SALES_T.CNTRY_SHORT_NM), 'Inactive') AS COUNTRY,
          CASE
            WHEN T3.IMC_CNTRY_KEY_NO IN (114, 41, 42)
            THEN TRIM(CAST(POSTL_T.SALE_RGN_NM AS VARCHAR2(18 CHAR)))
            ELSE NVL(NVL(T3.CNTRY_SHORT_NM, SALES_T.CNTRY_SHORT_NM), 'Inactive')
          END AS SALES_REGION,
          CASE
            WHEN T3.IMC_CNTRY_KEY_NO IN (114, 41, 42)
            THEN TRIM(CAST(POSTL_T.STATE_NM AS VARCHAR2(30 CHAR)))
            ELSE NVL(NVL(T3.CNTRY_SHORT_NM, SALES_T.CNTRY_SHORT_NM), 'Inactive')
          END AS STATE,
          APPL_DT,
          NVL(T3.SEG_ALT_DESC, '6 - Other') AS GL_SEGMENT,
          CASE
            WHEN SALES_T.IMC_TYPE='FOA'
            THEN 'Guest Customer'
            WHEN SALES_T.IMC_TYPE='Employee'
            THEN 'Employee'
            ELSE NVL(T3.IMC_TYPE, SALES_T.IMC_TYPE)
          END AS IMC_TYPE,
          CASE
            WHEN SALES_T.IMC_TYPE='FOA'
            THEN 'Guest Customer'
            WHEN SALES_T.IMC_TYPE='Employee'
            THEN 'Employee'
            ELSE DIST_TYPE
          END AS DIST_TYPE,
          IMC_TYPE_PY,
          DIST_TYPE_PY,
          FLOOR(MONTHS_BETWEEN(TO_DATE(T3.YEARMONTH, 'YYYYMM'), TO_DATE( T3.APP_1_BIRTH_DT_KEY_NO, 'YYYYMMDD'))/12) AS AGE,
          SALES_T.PV,
          SALES_T.BV_USD,
          SALES_T.USD,
          SALES_T.OMS_UNIT_CNT,
          SALES_T.PV_EOM,
          SALES_T.BV_USD_EOM
        FROM
          (SELECT SNAPSHOT_T.*,
            SNAPSHOT_T.SNAP_MO_YR_KEY_NO AS YEARMONTH,
            CASE
              WHEN ( SNAPSHOT_T.IMC_CNTRY_KEY_NO IN (114, 41, 42)
              AND ( POSTL_T.SALE_RGN_NM           ='N/A'
              OR SNAPSHOT_T.POSTL_CD_KEY_NO       =0 ) )
              THEN SNAPSHOT_T.POSTL_CD_KEY_NO
              ELSE SNAPSHOT_T.POSTL_CD_KEY_NO
            END AS POSTL_CD_KEY_NO_CORR,
            POSTL_T.SALE_RGN_NM
          FROM
            (SELECT NVL(SN_CY.SNAP_MO_YR_KEY_NO, SN_PY.SNAP_MO_YR_KEY_NO+100) AS SNAP_MO_YR_KEY_NO,
              NVL(SN_CY.IMC_KEY_NO, SN_PY.IMC_KEY_NO)                         AS IMC_KEY_NO,
              NVL(SN_CY.IMC_NO, SN_PY.IMC_NO)                                 AS IMC_NO,
              SN_CY.IMC_CNTRY_KEY_NO,
              SN_CY.STATUS_KEY_NO,
              SN_CY.CNTRY_SHORT_NM,
              SN_CY.IMC_TYPE,
              SN_CY.DIST_TYPE,
              SN_CY.POSTL_CD_KEY_NO,
              SN_CY.APPL_DT,
              SN_CY.APP_1_BIRTH_DT_KEY_NO,
              SN_CY.SEG_ALT_DESC,
              SN_PY.IMC_TYPE  AS IMC_TYPE_PY,
              SN_PY.DIST_TYPE AS DIST_TYPE_PY
            FROM
              (SELECT SN_T.* FROM SNAPSHOT_TBL SN_T WHERE SNAP_MO_YR_KEY_NO BETWEEN YM_START AND YM_END
              ) SN_CY
            FULL OUTER JOIN
              (SELECT SN_T.*
          FROM SNAPSHOT_TBL SN_T
          WHERE STATUS_KEY_NO IN (14, 21)
            /*Business Status = Active*/
          AND SNAP_MO_YR_KEY_NO+100 IN
            (YM_END)
              ) SN_PY
            ON SN_CY.IMC_KEY_NO         =SN_PY.IMC_KEY_NO
            AND SN_CY.SNAP_MO_YR_KEY_NO =SN_PY.SNAP_MO_YR_KEY_NO+100
            ) SNAPSHOT_T
          LEFT JOIN DWSAVR02.DWV00540_POSTL_CD_DIM POSTL_T
          ON SNAPSHOT_T.IMC_CNTRY_KEY_NO =POSTL_T.CNTRY_KEY_NO
          AND SNAPSHOT_T.POSTL_CD_KEY_NO =POSTL_T.POSTL_CD_KEY_NO
          ) T3
        LEFT JOIN
          (SELECT CNTRY_KEY_NO,
            POSTL_CD_KEY_NO,
            SALE_RGN_NM,
            SALES_SUB_RGN_DESC,
            STATE_NM
          FROM DWSAVR02.DWV00540_POSTL_CD_DIM
          ) POSTL_T
        ON T3.IMC_CNTRY_KEY_NO      =POSTL_T.CNTRY_KEY_NO
        AND T3.POSTL_CD_KEY_NO_CORR =POSTL_T.POSTL_CD_KEY_NO
        FULL OUTER JOIN
          (SELECT FACT_T.ORD_MO_YR_ID AS YEARMONTH,
            SN_T.IMC_KEY_NO,
            FACT_T.ACCOUNT_ID             AS IMC_NO,
            MAX(FACT_T.ACCOUNT_TYPE_ORD)  AS IMC_TYPE,
            MAX(COUNTRY_T.CNTRY_SHORT_NM) AS CNTRY_SHORT_NM,
            /*For use only for identification of country if account is not in Snapshot - maily FOAs*/
            SUM(ADJ_LN_PV)      AS PV,
            SUM(ADJ_LN_BV)      AS BV,
            SUM(ADJ_LN_LC_NET)  AS IBO_PRICE_LC_AMT,
            SUM(ADJ_LN_USD_NET) AS USD,
            SUM(ADJ_LN_BV_USD)  AS BV_USD,
            SUM(
            CASE
              WHEN ADJ_LN_PV<=0
              THEN 0
              ELSE ORD_QTY
            END) AS OMS_UNIT_CNT,
            SUM(
            CASE
              WHEN FACT_T.EOM_SALES_FLAG='true'
              THEN ADJ_LN_PV
              ELSE 0
            END) AS PV_EOM,
            SUM(
            CASE
              WHEN FACT_T.EOM_SALES_FLAG='true'
              THEN ADJ_LN_BV_USD
              ELSE 0
            END) AS BV_USD_EOM
          FROM DWSEAI01.DS_DET_DAILY_V FACT_T
                 LEFT JOIN SNAPSHOT_TBL SN_T
        ON FACT_T.ACCOUNT_ID   =SN_T.IMC_NO
        AND FACT_T.OPER_AFF_ID =SN_T.IMC_AFF_ID
        AND FACT_T.ORD_MO_YR_ID=SN_T.SNAP_MO_YR_KEY_NO
        LEFT JOIN COUNTRY_T COUNTRY_T
          ON FACT_T.OPER_CNTRY_ID =CNTRY_ID
          WHERE ORD_MO_YR_ID      BETWEEN YM_START AND YM_END
          AND OPER_CNTRY_ID      IN (30,240,210,620,650,390,800,340,820,810,150,590,490,140,450,660,80,60,480,160,120,90,470,460,280,370,750,740,570,270,250,110,300,40,430,830,420)
          AND ORD_LN_DISP_CD     IN ('*', '1', 'E', 'Item Disposition Code:  S/S', 'Item Disposition Code:  S/T', 'S', 'Item Disposition Code:  S/B', 'Item Disposition Code:  S/N', '2')
          AND ORD_CANC_FLAG       ='false'
          AND PAY_REQ_FLAG       <>'N'
          GROUP BY ORD_MO_YR_ID,
            SN_T.IMC_KEY_NO,
            ACCOUNT_ID
          ) SALES_T ON T3.IMC_KEY_NO =SALES_T.IMC_KEY_NO
        AND T3.YEARMONTH             =SALES_T.YEARMONTH
        )
      WHERE STATUS_KEY_NO IN (14, 21)
      OR PV                >0
      ) ,
      RMS AS
      (SELECT *
      FROM
        (SELECT VOL_FCT.IMC_KEY_NO,
          VOL_FCT.MO_YR_KEY_NO,
          VOL_CD.VOL_TYPE_CD,
          SUM(
          CASE
            WHEN VOL_CD.VOL_TYPE_CD='002'
            THEN VOL_QTY*TO_USD_BUDGT_EXCHG_RT
            WHEN VOL_CD.VOL_TYPE_CD='006'
            THEN VOL_QTY*TO_USD_BUDGT_EXCHG_RT
            ELSE VOL_QTY
          END) AS RMS_VOL
        FROM DWSAVR02.DWV14041_VOL_FACT VOL_FCT
        LEFT JOIN DWSAVR02.AWV00140_VOL_TYPE_DIM VOL_CD
        ON VOL_FCT.VOL_KEY_NO =VOL_CD.VOL_KEY_NO
        LEFT JOIN DWSEAI01.EXCH_RATES CURR_T
        ON VOL_FCT.CNTRY_KEY_NO     =CURR_T.CNTRY_KEY_NO
        AND VOL_FCT.CURCY_KEY_NO    =CURR_T.CURCY_KEY_NO
        WHERE VOL_FCT.CNTRY_KEY_NO IN (5,26,6,8,10,11,13,14,16,17,18,23,27,29,30,32,34,37,39,41,42,44,45,46,47,48,52,54,56,57,59,60,74,75,76,114, 134)
        AND VOL_FCT.MO_YR_KEY_NO    =YM_END
        GROUP BY VOL_FCT.IMC_KEY_NO,
          VOL_FCT.MO_YR_KEY_NO,
          VOL_CD.VOL_TYPE_CD
        ) PIVOT(SUM(RMS_VOL) FOR VOL_TYPE_CD IN ('001' AS RMSVOL_001,'005' AS RMSVOL_005,'019' AS RMSVOL_019,'086' AS RMSVOL_086,'093' AS RMSVOL_093,'095' AS RMSVOL_095,'096' AS RMSVOL_096,'097' AS RMSVOL_097,'099' AS RMSVOL_099,'105' AS RMSVOL_105,'002' RMSVOL_002, '006' RMSVOL_006))
      ),
      QM AS
      (SELECT T0.*
      FROM
        (SELECT
          CASE
            WHEN CL_MO>=9
            THEN PERF_YR_KEY_NO-1
            ELSE PERF_YR_KEY_NO
          END*100+CAST(CL_MO AS INTEGER) AS YEARMONTH,
          IMC_KEY_NO,
          CASE
            WHEN CD IN (155, 213)
            THEN 'Q1'
            WHEN CD IN (156, 198)
            THEN 'Q2'
            WHEN CD IN (166, 181)
            THEN 'QV'
            WHEN CD IN (130, 158, 178)
            THEN 'A'
            ELSE 'N/A'
          END AS CODE
        FROM
          (SELECT PERF_YR_KEY_NO,
            IMC_KEY_NO,
            QUAL_CD_SEP_KEY_NO AS "9",
            QUAL_CD_OCT_KEY_NO AS "10",
            QUAL_CD_NOV_KEY_NO AS "11",
            QUAL_CD_DEC_KEY_NO AS "12",
            QUAL_CD_JAN_KEY_NO AS "1",
            QUAL_CD_FEB_KEY_NO AS "2",
            QUAL_CD_MAR_KEY_NO AS "3",
            QUAL_CD_APR_KEY_NO AS "4",
            QUAL_CD_MAY_KEY_NO AS "5",
            QUAL_CD_JUN_KEY_NO AS "6",
            QUAL_CD_JUL_KEY_NO AS "7",
            QUAL_CD_AUG_KEY_NO AS "8"
          FROM DWSAVR02.DWV14070_QUAL_TRACK_FACT T1
          LEFT JOIN DWSAVR02.DWV00006_AWD_DIM_VIEW T2
          ON T1.AWD_KEY_NO                       =T2.AWD_KEY_NO
          WHERE AWD_RNK_NO                       = '310'
          AND PERF_YR_KEY_NO                    >=2008
          ) UNPIVOT INCLUDE NULLS (CD FOR CL_MO IN ("1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12"))
        ) T0
      WHERE T0.YEARMONTH=YM_END
      ),
      AWD AS
      (SELECT IMC_KEY_NO,
        MAX(CAST(AWD_RNK_NO AS INTEGER)) AS MAX_AWD,
        MO_YR_KEY_NO                     AS YEARMONTH
      FROM DWSAVR02.DWV14050_AWD_FACT AWD_FCT
      WHERE AWD_FCT.MO_YR_KEY_NO=YM_END
      AND AWD_RNK_NO BETWEEN '205' AND '499'
      AND SUBSTR(AWD_RNK_NO, -1, 1) NOT IN ('3','8')
      GROUP BY IMC_KEY_NO,
        MO_YR_KEY_NO
      ),
      PIN AS
      (SELECT ABO_BEH.IMC_KEY_NO,
        ABO_BEH.MO_YR_KEY_NO AS YEARMONTH,
        CASE
          WHEN AWD_DIM.AWD_RNK_NO='*NA'
          THEN 0
          ELSE CAST(AWD_DIM.AWD_RNK_NO AS INTEGER)
        END AS PIN_AWD,
        CASE
          WHEN MAST_T.IMC_HIGH_AWD_MO_YR_KEY_NO>ABO_BEH.MO_YR_KEY_NO
            /*OR ABO_BEH.IMC_HIGH_AWD_KEY_NO<>MAST_T.IMC_HIGH_AWD_KEY_NO*/
          THEN 0
          ELSE MAST_T.IMC_HIGH_AWD_MO_YR_KEY_NO
        END AS PIN_YM
      FROM
        (SELECT *
        FROM DWSAVR02.DWV01034_ABO_BEHAVE_MAIN_DIM ABO_BEH
        WHERE ABO_BEH.MO_YR_KEY_NO=YM_END
        AND STATUS_KEY_NO        IN (14, 21)
        ) ABO_BEH
      LEFT JOIN DWSAVR02.DWV01021_IMC_MASTER_DIM MAST_T
      ON ABO_BEH.IMC_KEY_NO =MAST_T.IMC_KEY_NO
      LEFT JOIN DWSAVR02.DWV00006_AWD_DIM_VIEW AWD_DIM
      ON ABO_BEH.IMC_HIGH_AWD_KEY_NO=AWD_DIM.AWD_KEY_NO
      ),
      BNS_TTL AS
      (SELECT *
      FROM
        (SELECT YEARMONTH,
          IMC_KEY_NO,
          BNS_TYPE,
          BNS_EARNED,
          BNS_PAID
        FROM DWSEAI01.RMS_BONUS
        WHERE YEARMONTH                                                                =YM_END
        ) PIVOT(SUM(BNS_EARNED) AS BNS_EARNED, SUM(BNS_PAID) AS BNS_PAID FOR BNS_TYPE IN ('CORE' AS CORE,'CORE PLUS/GIP' AS CORE_PLUS_GIP,'FAA' AS FAA,'OTHER' AS OTHER))
      ),
      PERF_BNS AS
      (SELECT *
      FROM
        (SELECT IMC_KEY_NO,
          MO_YR_KEY_NO AS YEARMONTH,
          PERF_BNS_PCT,
          REWORK_NO,
          MAX(REWORK_NO) OVER(PARTITION BY IMC_KEY_NO, MO_YR_KEY_NO) AS MAX_RWK
        FROM DWSAVR02.DWV04010_IMC_BNS_DIM IMC_BNS
        WHERE IMC_BNS.MO_YR_KEY_NO=YM_END
        )
      WHERE MAX_RWK=REWORK_NO
      ),
      OMS AS
      (SELECT IMC_KEY_NO,
        IMC_NO,
        T0.YEARMONTH,
        APPL_DT,
        U35_FLAG,
        COUNTRY,
        GL_SEGMENT,
        SALES_REGION,
        STATE,
        IMC_TYPE,
        IMC_TYPE_PY,
        DIST_TYPE,
        DIST_TYPE_PY,
        BV_USD,
        USD,
        BV_USD_EOM,
        PV,
        PV_EOM,
        OMS_UNIT_CNT,
        BUYER_FLAG,
        NVL(SPONSORED_ABOS_CNT, 0) AS SPONSORED_ABOS_CNT,
        NVL(SPONSORED_RCS_CNT, 0)  AS SPONSORED_RCS_CNT,
        ABOS_SPONSORING_ABOS       AS ABOS_SPONSORING_ABOS_FLAG,
        ABOS_SPONSORING_RCS        AS ABOS_SPONSORING_RCS_FLAG,
        ABOS_SPONSORING_IMCS       AS ABOS_SPONSORING_IMCS_FLAG,
        HIGH_PPV_BUYER_FLAG        AS HIGH_IMC,
        HIGH_PPV_PV                AS HIGH_VOLUME,
        HIGH_PPV_BV_USD            AS HIGH_BV,
        R_BUYER_FLAG               AS R_IMC,
        R_BUYERS_PV                AS R_PV,
        RCNCY_CNT,
        FRQNCY_CNT,
        R_BUYERS_BV AS R_BV
      FROM
        (SELECT T11.*,
          CASE
            WHEN NVL(T21.SPONSORED_ABOS_CNT, 0)>0
            THEN 1
            ELSE 0
          END AS ABOS_SPONSORING_ABOS,
          T21.SPONSORED_ABOS_CNT,
          T31.SPONSORED_RCS_CNT,
          CASE
            WHEN NVL(T31.SPONSORED_RCS_CNT, 0)>0
            THEN 1
            ELSE 0
          END AS ABOS_SPONSORING_RCS,
          CASE
            WHEN (NVL(T21.SPONSORED_ABOS_CNT, 0)+NVL(T31.SPONSORED_RCS_CNT, 0))>0
            THEN 1
            ELSE 0
          END              AS ABOS_SPONSORING_IMCS,
          PV     *R_BUYER_FLAG AS R_BUYERS_PV,
          BV_USD *R_BUYER_FLAG AS R_BUYERS_BV
        FROM
          (SELECT T0.*,
            CASE
              WHEN SUM(BUYER_FLAG) OVER (PARTITION BY IMC_KEY_NO, IMC_NO, COUNTRY ORDER BY CAST( T0.YEARMONTH_MONTHS AS INTEGER) RANGE BETWEEN 2 PRECEDING AND CURRENT ROW)>=2
              AND BUYER_FLAG                                                                                                                                                =1
              THEN 1
              ELSE 0
            END                                                                                                                                                                                                                                    AS R_BUYER_FLAG,
            T0.YEARMONTH_MONTHS-NVL(NULLIF(MAX(T0.YEARMONTH_MONTHS*BUYER_FLAG) OVER (PARTITION BY IMC_KEY_NO, IMC_NO, COUNTRY ORDER BY CAST( T0.YEARMONTH_MONTHS AS INTEGER) RANGE BETWEEN 5 PRECEDING AND CURRENT ROW), 0), YEARMONTH_MONTHS+1)+1 AS RCNCY_CNT,
            SUM(BUYER_FLAG) OVER (PARTITION BY IMC_KEY_NO, IMC_NO, COUNTRY ORDER BY CAST( T0.YEARMONTH_MONTHS AS INTEGER) RANGE BETWEEN 5 PRECEDING AND CURRENT ROW)                                                                               AS FRQNCY_CNT
          FROM
            (SELECT T00.*,
              CASE
                WHEN NVL(T00.PV, 0)>0
                THEN 1
                ELSE 0
              END AS BUYER_FLAG,
              CASE
                WHEN NVL(T00.PV, 0)>=
                  CASE
                    WHEN T00.COUNTRY IN ('AUSTRALIA', 'NEW ZEALAND')
                    THEN 1125
                    ELSE 1500
                  END
                THEN 1
                ELSE 0
              END AS HIGH_PPV_BUYER_FLAG,
              CASE
                WHEN T00.PV>=
                  CASE
                    WHEN T00.COUNTRY IN ('AUSTRALIA', 'NEW ZEALAND')
                    THEN 1125
                    ELSE 1500
                  END
                THEN T00.PV
                ELSE 0
              END AS HIGH_PPV_PV,
              CASE
                WHEN T00.PV>=
                  CASE
                    WHEN T00.COUNTRY IN ('AUSTRALIA', 'NEW ZEALAND')
                    THEN 1125
                    ELSE 1500
                  END
                THEN T00.BV_USD
                ELSE 0
              END AS HIGH_PPV_BV_USD
            FROM OMS_OLD T00
            ) T0
          ) T11
        LEFT JOIN
          (SELECT T4.IMC_KEY_NO,
            COUNT(DISTINCT T3.SPONSORED_IMC_KEY_NO) AS SPONSORED_ABOS_CNT,
            T3.YEARMONTH
          FROM
            (SELECT SPONSOR_IMC_KEY_NO AS SPONSOR_IMC_KEY_NO,
              SPONSORED_ABO_KEY_NO     AS SPONSORED_IMC_KEY_NO,
              YEARMONTH
            FROM SNAPSHOT_TBL
            WHERE YEARMONTH                  =APPL_YM
            AND NOT ( GLOBL_IMC_TYPE_KEY_NO IN (2,3) )
            GROUP BY YEARMONTH,
              SPONSOR_IMC_KEY_NO,
              SPONSORED_ABO_KEY_NO
            ) T3
          LEFT JOIN
            ( SELECT IMC_NO, IMC_KEY_NO FROM DWSAVR02.DWV01021_IMC_MASTER_DIM
            ) T4
          ON T3.SPONSOR_IMC_KEY_NO=T4.IMC_KEY_NO
          GROUP BY T4.IMC_KEY_NO,
            T3.YEARMONTH
          ) T21 ON T11.IMC_KEY_NO=T21.IMC_KEY_NO
        AND T11.YEARMONTH        =T21.YEARMONTH
        LEFT JOIN
          (SELECT T4.IMC_KEY_NO,
            COUNT(DISTINCT T3.SPONSORED_IMC_KEY_NO) AS SPONSORED_RCS_CNT,
            T3.YEARMONTH
          FROM
            (SELECT SPONSOR_IMC_KEY_NO AS SPONSOR_IMC_KEY_NO,
              SPONSORED_ABO_KEY_NO     AS SPONSORED_IMC_KEY_NO,
              YEARMONTH
            FROM SNAPSHOT_TBL
            WHERE YEARMONTH            =APPL_YM
            AND GLOBL_IMC_TYPE_KEY_NO IN (2,3)
            GROUP BY YEARMONTH,
              SPONSOR_IMC_KEY_NO,
              SPONSORED_ABO_KEY_NO
            ) T3
          LEFT JOIN DWSAVR02.DWV01021_IMC_MASTER_DIM T4
          ON T3.SPONSOR_IMC_KEY_NO=T4.IMC_KEY_NO
          GROUP BY T4.IMC_KEY_NO,
            T3.YEARMONTH
          ) T31 ON T11.IMC_KEY_NO=T31.IMC_KEY_NO
        AND T11.YEARMONTH        =T31.YEARMONTH
        ) T0
      WHERE T0.YEARMONTH IN (YM_END)
      )
    /*Main script*/
    SELECT OMS.IMC_KEY_NO AS IMC_KEY_NO,
      OMS.IMC_NO,
      OMS.YEARMONTH,
      OMS.APPL_DT,
      OMS.U35_FLAG,
      OMS.COUNTRY,
      OMS.GL_SEGMENT,
      OMS.SALES_REGION,
      OMS.STATE,
      OMS.IMC_TYPE,
      OMS.IMC_TYPE_PY,
      OMS.DIST_TYPE,
      OMS.DIST_TYPE_PY,
      OMS.BV_USD,
      OMS.BV_USD_EOM,
      OMS.PV,
      OMS.PV_EOM,
      OMS.OMS_UNIT_CNT,
      OMS.BUYER_FLAG,
      OMS.SPONSORED_ABOS_CNT,
      OMS.SPONSORED_RCS_CNT,
      OMS.ABOS_SPONSORING_ABOS_FLAG,
      OMS.ABOS_SPONSORING_RCS_FLAG,
      OMS.ABOS_SPONSORING_IMCS_FLAG,
      OMS.HIGH_IMC,
      OMS.HIGH_VOLUME,
      OMS.HIGH_BV,
      OMS.R_IMC,
      OMS.RCNCY_CNT,
      OMS.FRQNCY_CNT,
      OMS.R_PV,
      OMS.R_BV,
      RMS.RMSVOL_001,
      RMS.RMSVOL_005,
      RMS.RMSVOL_019,
      RMS.RMSVOL_086,
      RMS.RMSVOL_093,
      RMS.RMSVOL_095,
      RMS.RMSVOL_096,
      RMS.RMSVOL_097,
      RMS.RMSVOL_099,
      RMS.RMSVOL_105,
      RMS.RMSVOL_002,
      RMS.RMSVOL_006,
      NVL(QM.CODE, 'N/A')           AS HOW_QUALIFIED,
      NVL(PERF_BNS.PERF_BNS_PCT, 0) AS PERF_BNS_PCT,
      NVL(AWD.MAX_AWD, 0)           AS MAX_AWD,
      CASE NVL(PIN.PIN_YM, 0)
        WHEN OMS.YEARMONTH
        THEN NVL(AWD.MAX_AWD, 0)
        ELSE NVL(PIN.PIN_AWD, 0)
      END                AS PIN_AWD,
      NVL(PIN.PIN_YM, 0) AS PIN_YM,
      BNS_TTL.CORE_BNS_EARNED,
      BNS_TTL.CORE_BNS_PAID,
      BNS_TTL.CORE_PLUS_GIP_BNS_EARNED,
      BNS_TTL.CORE_PLUS_GIP_BNS_PAID,
      BNS_TTL.FAA_BNS_EARNED,
      BNS_TTL.FAA_BNS_PAID,
      BNS_TTL.OTHER_BNS_EARNED,
      BNS_TTL.OTHER_BNS_PAID,
      OMS.USD
    FROM OMS OMS
    LEFT JOIN RMS
    ON OMS.YEARMONTH   =RMS.MO_YR_KEY_NO
    AND OMS.IMC_KEY_NO =RMS.IMC_KEY_NO
    LEFT JOIN QM
    ON OMS.YEARMONTH   =QM.YEARMONTH
    AND OMS.IMC_KEY_NO =QM.IMC_KEY_NO
    LEFT JOIN PERF_BNS
    ON OMS.YEARMONTH   =PERF_BNS.YEARMONTH
    AND OMS.IMC_KEY_NO =PERF_BNS.IMC_KEY_NO
    LEFT JOIN AWD
    ON OMS.YEARMONTH   =AWD.YEARMONTH
    AND OMS.IMC_KEY_NO =AWD.IMC_KEY_NO
    LEFT JOIN PIN
    ON OMS.YEARMONTH   =PIN.YEARMONTH
    AND OMS.IMC_KEY_NO =PIN.IMC_KEY_NO
    LEFT JOIN BNS_TTL
    ON OMS.YEARMONTH   =BNS_TTL.YEARMONTH
    AND OMS.IMC_KEY_NO =BNS_TTL.IMC_KEY_NO
      ) s;
    COMMIT;
  END LOOP;
END;