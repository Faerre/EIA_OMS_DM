DECLARE
  YM_FOCUS NUMBER:=0;
BEGIN
  YM_FOCUS:=DWSEAI01.YMM2YM({YMM});
  DELETE FROM DWSEAI01.GPK_HIST_GDW WHERE YEARMONTH=YM_FOCUS;
  INSERT /*+ append parallel(t,32)*/
  INTO DWSEAI01.GPK_HIST_GDW t
  SELECT DISTINCT YM_FOCUS AS YEARMONTH,
    T0.LAST_YM_RC_TYPE     AS GPK_TYPE,
    T0.IMC_NO,
    T1.IMC_KEY_NO
  FROM
    (SELECT 33.0 AS AFF_KEY_NO,
      BLCK_PRIV_TYPE_ID,
      DISTB_NBR AS IMC_NO,
      EFFECTIVE_DT,
      EXPRY_DT,
      CASE
        WHEN extract(YEAR FROM EFFECTIVE_DT)         * 100 + extract(MONTH FROM EFFECTIVE_DT)      <= YM_FOCUS
        AND NOT (COALESCE(extract(YEAR FROM EXPRY_DT)* 100 + extract(MONTH FROM EXPRY_DT), 240001) <= YM_FOCUS)
        THEN 'Customer+'
        ELSE 'N/A'
      END AS LAST_YM_RC_TYPE
    FROM DWSATM01.DWT41137_DISTB_BLCK_PRIV
    WHERE INTGRT_AFF_CD   ='420'
    AND BLCK_PRIV_TYPE_ID = 523
    AND
      CASE
        WHEN extract(YEAR FROM EFFECTIVE_DT)         * 100 + extract(MONTH FROM EFFECTIVE_DT)      <= YM_FOCUS
        AND NOT (COALESCE(extract(YEAR FROM EXPRY_DT)* 100 + extract(MONTH FROM EXPRY_DT), 240001) <= YM_FOCUS)
        THEN 'Customer+'
        ELSE 'N/A'
      END = 'Customer+'
    ) T0
  LEFT JOIN
    (SELECT IMC_NO,
      IMC_KEY_NO
    FROM DWSAVR02.DWV01021_IMC_MASTER_DIM
    WHERE IMC_CNTRY_KEY_NO IN (41, 114)
    ) T1
  ON T0.IMC_NO=T1.IMC_NO;
  COMMIT;
END;