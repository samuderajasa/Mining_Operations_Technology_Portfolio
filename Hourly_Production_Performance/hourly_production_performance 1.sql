CREATE VIEW [dbo].[hourly_production_performance]
AS


WITH CTE AS(
    SELECT DISTINCT
        a.haul_cycle_rec_ident,
        CAST(CONCAT(CAST(a.load_start_timestamp AS DATE), ' ', DATEPART(HOUR, a.load_start_timestamp),':00:00') AS DATETIME) datetime,
        CASE
            WHEN LAG(a.load_start_timestamp) OVER(PARTITION BY a.LOADING_UNIT_IDENT ORDER BY a.load_start_timestamp ASC) IS NULL THEN CAST(CONCAT(CAST(a.load_start_timestamp AS DATE), ' ', DATEPART(HOUR,a.load_start_timestamp),':00:00') AS DATETIME)
            ELSE LAG (a.load_start_timestamp) OVER (PARTITION BY a.LOADING_UNIT_IDENT ORDER BY a.load_start_timestamp ASC)
        END load_bef,
        start_timestamp,
        AVG(CAST(CASE
            WHEN truck.DESCRIP='CAT785C' THEN '61'
            WHEN truck.DESCRIP LIKE '%CAT777D%' THEN '43' 
            ELSE '28'
        END AS INT) * MAT_FILL_FACTOR)
        truck_factor,
        CASE
            WHEN LEFT(LOAD_LOCATION_SNAME,2)='BE' THEN 'BLOCK E' 
            WHEN LEFT(LOAD_LOCATION_SNAME,2)='BD' THEN 'BLOCK D'
        END Location,
        a.load_start_timestamp, 
        load_start_shift_date, 
        load_start_shift_ident,
        dump_end_timestamp,
        hauling_unit_ident,
        loading_unit_ident,
        load_location_sname,
        material_ident,
        dump_location_sname,
        empty_distance,
        haul_distance,
        truck.descrip Truck_type,
        digger.descrip Digger_Type,
        AVG(lu_loading.val) Loading_Time,
        AVG(ISNULL(spot_at_lu.val,0)) Spotting_time,
        AVG(ISNULL(Queue_at_LU.val,0)) Queue_At_Loading_Point,
        AVG(Traveling.val) Traveling,
        AVG(Hauling.val) Hauling,
        AVG(ISNULL(wait_at_dump.val,0)) Wait_At_Dump,
        AVG(Dumping.val) Dumping,
        AVG(CASE
            WHEN a.EMPTY_DISTANCE/NULLIF((Traveling.val/3600.00),0)>'50' THEN '50'
            ELSE a.EMPTY_DISTANCE/NULLIF((Traveling.val/3600.00),0)
        END) AS empty_speed,
        AVG(CASE
            WHEN a.HAUL_DISTANCE/NULLIF((Hauling.val/3600.00),0)> '35'THEN '35'
            ELSE a.HAUL_DISTANCE/NULLIF((Hauling.val/3600.00),0)
        END) AS hauling_speed
        ,MAT_MATERIAL_GROUP,
        CASE
            WHEN truck.descrip LIKE '%CAT777D%' AND ep.PROTOCOL_IDENT = 'VIMS' AND PAYLOAD_REPORTING<'79' THEN NULL
            WHEN truck.descrip LIKE '%CAT777D%' AND ep.PROTOCOL_IDENT = 'TPMS' AND LOAD_START_SHIFT_DATE < '2025-03-06' AND PAYLOAD_REPORTING <'79' THEN NULL
            WHEN truck.descrip LIKE '%CAT777D%' AND ep.PROTOCOL_IDENT = 'TPMS' AND LOAD_START_SHIFT_DATE >= '2025-03-06' AND PAYLOAD_REPORTING <'79' THEN NULL
            WHEN truck.descrip LIKE '%CAT777D%' AND ep.PROTOCOL_IDENT = 'TPMS' AND LOAD_START_SHIFT_DATE < '2025-03-06' AND PAYLOAD_REPORTING>'79' THEN PAYLOAD_REPORTING
            WHEN truck.descrip LIKE '%CAT777D%' AND ep.PROTOCOL_IDENT = 'TPMS' AND LOAD_START_SHIFT_DATE >= '2025-03-06' AND PAYLOAD_REPORTING>'79' THEN PAYLOAD_REPORTING
            WHEN truck.descrip LIKE '%CAT777D%' AND ep.PROTOCOL_IDENT = 'VIMS' AND PAYLOAD_REPORTING>'79' THEN PAYLOAD_REPORTING
            WHEN truck.DESCRIP='CAT785C' AND PAYLOAD_REPORTING<'113' THEN NULL  
            WHEN truck.DESCRIP='CAT785C' AND PAYLOAD_REPORTING>'113' THEN PAYLOAD_REPORTING
        END AS Prod_by_Payload

    FROM WencoReport.dbo.HAUL_CYCLE_TRANS a   WITH (nolock)
    LEFT JOIN WencoReport.dbo.EQUIP_HAULING_UNIT truck ON truck.EQUIP_IDENT = a.HAULING_UNIT_IDENT AND truck.EQUIP_IDENT LIKE 'RD%'
    LEFT JOIN WencoReport.dbo.EQUIP digger ON digger.EQUIP_IDENT = a.loading_unit_ident
    LEFT JOIN WencoReport.dbo.material  ON MAT_MATERIAL_IDENT = a.material_ident
    LEFT JOIN WencoReport.[dbo].[EQUIP_PROTOCOL] ep ON a.HAULING_UNIT_IDENT = ep.EQUIP_IDENT AND ep.PROTOCOL_IDENT IN ('VIMS', 'TPMS')

    OUTER APPLY
        (
            SELECT  SUM(DATEDIFF(SECOND, hcd.START_TIMESTAMP, hcd.END_TIMESTAMP))  val
            FROM WencoReport.dbo.LOAD_UNIT_STATUS_TRANS_COL hc
            JOIN WencoReport.dbo.EQUIPMENT_STATUS_TRANS hcd ON hcd.EQUIP_STATUS_REC_IDENT = hc.EQUIP_STATUS_REC_IDENT
            JOIN WencoReport.dbo.EQUIP_STATUS_CODE hsc ON hsc.STATUS_CODE = hcd.STATUS_CODE
            WHERE hc.HAUL_CYCLE_REC_IDENT = a.HAUL_CYCLE_REC_IDENT AND hsc.STATUS_ABBREV = 'LU Loading' GROUP BY hcd.END_TIMESTAMP
        ) LU_Loading

    OUTER APPLY
        (
            SELECT SUM(DATEDIFF(SECOND, hcd.START_TIMESTAMP, hcd.END_TIMESTAMP))  val
            FROM WencoReport.dbo.HAUL_UNIT_STATUS_TRANS_COL hc
            JOIN WencoReport.dbo.EQUIPMENT_STATUS_TRANS hcd ON hcd.EQUIP_STATUS_REC_IDENT = hc.EQUIP_STATUS_REC_IDENT
            JOIN WencoReport.dbo.EQUIP_STATUS_CODE hsc ON hsc.STATUS_CODE = hcd.STATUS_CODE
            WHERE hc.HAUL_CYCLE_REC_IDENT = a.HAUL_CYCLE_REC_IDENT AND hsc.STATUS_ABBREV IN ('Queue at LU', 'wait at LU')
        ) Queue_at_LU

    OUTER APPLY 
        (
            SELECT SUM(DATEDIFF(SECOND, hcd.START_TIMESTAMP, hcd.END_TIMESTAMP))  val
            FROM WencoReport.dbo.HAUL_UNIT_STATUS_TRANS_COL hc
            LEFT JOIN WencoReport.dbo.EQUIPMENT_STATUS_TRANS hcd ON hcd.EQUIP_STATUS_REC_IDENT = hc.EQUIP_STATUS_REC_IDENT
            JOIN WencoReport.dbo.EQUIP_STATUS_CODE hsc ON hsc.STATUS_CODE = hcd.STATUS_CODE
            WHERE hc.HAUL_CYCLE_REC_IDENT = a.HAUL_CYCLE_REC_IDENT AND hsc.STATUS_DESC = 'Spot At LU'
        ) Spot_At_LU

    OUTER APPLY 
        (
            SELECT SUM(DATEDIFF(SECOND, hcd.START_TIMESTAMP, hcd.END_TIMESTAMP))  val
            FROM WencoReport.dbo.HAUL_UNIT_STATUS_TRANS_COL hc
            JOIN WencoReport.dbo.EQUIPMENT_STATUS_TRANS hcd ON hcd.EQUIP_STATUS_REC_IDENT = hc.EQUIP_STATUS_REC_IDENT
            JOIN WencoReport.dbo.EQUIP_STATUS_CODE hsc ON hsc.STATUS_CODE = hcd.STATUS_CODE
            WHERE hc.HAUL_CYCLE_REC_IDENT = a.HAUL_CYCLE_REC_IDENT AND hsc.STATUS_ABBREV = 'Dumping'
        ) Dumping

    OUTER APPLY 
        (
            SELECT SUM(DATEDIFF(SECOND, hcd.START_TIMESTAMP, hcd.END_TIMESTAMP))  val
            FROM WencoReport.dbo.HAUL_UNIT_STATUS_TRANS_COL hc
            JOIN WencoReport.dbo.EQUIPMENT_STATUS_TRANS hcd ON hcd.EQUIP_STATUS_REC_IDENT = hc.EQUIP_STATUS_REC_IDENT
            JOIN WencoReport.dbo.EQUIP_STATUS_CODE hsc ON hsc.STATUS_CODE = hcd.STATUS_CODE
            WHERE hc.HAUL_CYCLE_REC_IDENT = a.HAUL_CYCLE_REC_IDENT AND hsc.STATUS_ABBREV = 'Wait At Dump'
        ) Wait_At_Dump

    OUTER APPLY
        (
            SELECT SUM(DATEDIFF(SECOND, hcd.START_TIMESTAMP, hcd.END_TIMESTAMP))  val
            FROM WencoReport.dbo.HAUL_UNIT_STATUS_TRANS_COL hc
            JOIN WencoReport.dbo.EQUIPMENT_STATUS_TRANS hcd ON hcd.EQUIP_STATUS_REC_IDENT = hc.EQUIP_STATUS_REC_IDENT
            JOIN WencoReport.dbo.EQUIP_STATUS_CODE hsc ON hsc.STATUS_CODE = hcd.STATUS_CODE
            WHERE hc.HAUL_CYCLE_REC_IDENT = a.HAUL_CYCLE_REC_IDENT AND hsc.STATUS_ABBREV = 'Empty'
        ) Traveling

    OUTER APPLY
        (
            SELECT SUM(DATEDIFF(SECOND, hcd.START_TIMESTAMP, hcd.END_TIMESTAMP))  val
            FROM WencoReport.dbo.HAUL_UNIT_STATUS_TRANS_COL hc
            JOIN WencoReport.dbo.EQUIPMENT_STATUS_TRANS hcd ON hcd.EQUIP_STATUS_REC_IDENT = hc.EQUIP_STATUS_REC_IDENT
            JOIN WencoReport.dbo.EQUIP_STATUS_CODE hsc ON hsc.STATUS_CODE = hcd.STATUS_CODE
            WHERE hc.HAUL_CYCLE_REC_IDENT = a.HAUL_CYCLE_REC_IDENT AND hsc.STATUS_ABBREV = 'Hauling' 
        ) Hauling

    WHERE a.load_start_shift_date = CAST(DATEADD(HOUR,-8, GETDATE()) AS DATE)
        AND a.load_start_timestamp IS NOT NULL
        AND a.LOAD_START_TIMESTAMP<CAST( CONCAT(CAST(GETDATE() AS DATE), ' ', DATEPART(HOUR,GETDATE()),':00:00') AS DATETIME)

    GROUP BY
        CAST( CONCAT(CAST(a.load_start_timestamp AS DATE), ' ', DATEPART(HOUR,a.load_start_timestamp),':00:00') AS DATETIME),
        LOADING_UNIT_IDENT,
        START_TIMESTAMP,
        CASE
            WHEN LEFT(LOAD_LOCATION_SNAME,2)='BE' THEN 'BLOCK E' 
            WHEN LEFT(LOAD_LOCATION_SNAME,2)='BD' THEN 'BLOCK D'
        END,
        LOAD_START_SHIFT_DATE,
        LOAD_START_SHIFT_IDENT,
        DUMP_END_TIMESTAMP,
        HAULING_UNIT_IDENT,
        LOAD_LOCATION_SNAME,
        MATERIAL_IDENT,
        DUMP_LOCATION_SNAME,
        truck.descrip,
        digger.descrip,
        load_start_timestamp,
        empty_distance,
        haul_distance,
        HAUL_CYCLE_REC_IDENT,
        MAT_MATERIAL_GROUP,
        CASE
            WHEN truck.descrip LIKE '%CAT777D%' AND ep.PROTOCOL_IDENT = 'VIMS' AND PAYLOAD_REPORTING<'79' THEN NULL
            WHEN truck.descrip LIKE '%CAT777D%' AND ep.PROTOCOL_IDENT = 'TPMS' AND LOAD_START_SHIFT_DATE < '2025-03-06' AND PAYLOAD_REPORTING <'79' THEN NULL
            WHEN truck.descrip LIKE '%CAT777D%' AND ep.PROTOCOL_IDENT = 'TPMS' AND LOAD_START_SHIFT_DATE >= '2025-03-06' AND PAYLOAD_REPORTING <'79' THEN NULL
            WHEN truck.descrip LIKE '%CAT777D%' AND ep.PROTOCOL_IDENT = 'TPMS' AND LOAD_START_SHIFT_DATE < '2025-03-06' AND PAYLOAD_REPORTING>'79' THEN PAYLOAD_REPORTING
            WHEN truck.descrip LIKE '%CAT777D%' AND ep.PROTOCOL_IDENT = 'TPMS' AND LOAD_START_SHIFT_DATE >= '2025-03-06' AND PAYLOAD_REPORTING>'79' THEN PAYLOAD_REPORTING
            WHEN truck.descrip LIKE '%CAT777D%' AND ep.PROTOCOL_IDENT = 'VIMS' AND PAYLOAD_REPORTING>'79' THEN PAYLOAD_REPORTING
            WHEN truck.DESCRIP='CAT785C' AND PAYLOAD_REPORTING<'113' THEN NULL  
            WHEN truck.DESCRIP='CAT785C' AND PAYLOAD_REPORTING>'113' THEN PAYLOAD_REPORTING
        END
),

CTZ AS (
    SELECT 
        date,
        shift,
        digger, 
        ISNULL(CAST(CAST(SUM(pwt) AS FLOAT)/3600 AS DECIMAL (10,2)),0) PWT,
        ISNULL(CAST(CAST(SUM(swt) AS FLOAT)/3600 AS DECIMAL (10,2)),0) SWT,
        ISNULL(CAST(CAST(SUM(iod) AS FLOAT)/3600 AS DECIMAL (10,2)),0) IOD,
        ISNULL(CAST(CAST(SUM(eod) AS FLOAT)/3600 AS DECIMAL (10,2)),0) EOD,
        ISNULL(CAST(CAST(SUM(down) AS FLOAT)/3600 AS DECIMAL (10,2)),0) DOWN,
        ISNULL(CAST(CAST(SUM(nrt) AS FLOAT)/3600 AS DECIMAL (10,2)),0) NRT
    FROM 
        (
            SELECT
                CAST( CONCAT(CAST(CASE WHEN jam>START_TIMESTAMP THEN jam ELSE START_TIMESTAMP END AS DATE), ' ', DATEPART(HOUR,CASE WHEN jam>START_TIMESTAMP THEN jam ELSE START_TIMESTAMP END),':00:00') AS DATETIME) datetime,
                shift_ident Shift,
                DATEPART(HOUR,CASE WHEN jam>START_TIMESTAMP THEN jam ELSE START_TIMESTAMP END) AS Jam,
                CAST(Date AS DATE) Date, 
                Digger, 
                STATUS_CODE, 
                STATUS_ABBREV,
                CASE
                    WHEN STATUS_CODE IN ('N11','N13','N14','135') THEN CAST(SUM(DATEDIFF(
                        SECOND,
                        CASE WHEN jam>START_TIMESTAMP THEN jam ELSE START_TIMESTAMP END,
                        CASE WHEN (CASE WHEN b.END_TIMESTAMP IS NULL THEN GETDATE() ELSE b.END_TIMESTAMP END) >DATEADD(HOUR,1,jam) THEN DATEADD(HOUR,1,jam)
                        ELSE END_TIMESTAMP END)) AS FLOAT)
                END AS 'PWT',
                CASE
                    WHEN (STATUS_ABBREV='SWT' or STATUS_ABBREV='Tramming' or STATUS_CODE='N19') THEN CAST(SUM(DATEDIFF(
                        SECOND,
                        CASE WHEN jam>START_TIMESTAMP THEN jam ELSE START_TIMESTAMP END,
                        CASE WHEN (CASE WHEN b.END_TIMESTAMP IS NULL THEN GETDATE() ELSE b.END_TIMESTAMP END) >DATEADD(HOUR,1,jam) THEN DATEADD(HOUR,1,jam)
                        ELSE END_TIMESTAMP END)) AS FLOAT)
                END AS 'SWT',
                CASE
                    WHEN STATUS_ABBREV='IOD' THEN CAST(SUM(DATEDIFF(
                        SECOND,
                        CASE WHEN jam>START_TIMESTAMP THEN jam ELSE START_TIMESTAMP END,
                        CASE WHEN (CASE WHEN b.END_TIMESTAMP IS NULL THEN GETDATE() ELSE b.END_TIMESTAMP END) >DATEADD(HOUR,1,jam) THEN DATEADD(HOUR,1,jam)
                        ELSE END_TIMESTAMP END)) AS FLOAT)
                END AS 'IOD',
                CASE
                    WHEN STATUS_ABBREV='EOD' THEN CAST(SUM(DATEDIFF(
                        SECOND,
                        CASE WHEN jam>START_TIMESTAMP THEN jam ELSE START_TIMESTAMP END,
                        CASE WHEN (CASE WHEN b.END_TIMESTAMP IS NULL THEN GETDATE() ELSE b.END_TIMESTAMP END) >DATEADD(HOUR,1,jam) THEN DATEADD(HOUR,1,jam)
                        ELSE END_TIMESTAMP END)) AS FLOAT)
                END AS'EOD',
                CASE
                    WHEN STATUS_ABBREV IN ('MEO','PMD','UMD') THEN CAST(SUM(DATEDIFF(
                        SECOND,
                        CASE WHEN jam>START_TIMESTAMP THEN jam ELSE START_TIMESTAMP END,
                        CASE WHEN (CASE WHEN b.END_TIMESTAMP IS NULL THEN GETDATE() ELSE b.END_TIMESTAMP END) >DATEADD(HOUR,1,jam) THEN DATEADD(HOUR,1,jam) 
                        ELSE END_TIMESTAMP END)) AS FLOAT)
                END AS 'DOWN',
                CASE
                    WHEN STATUS_ABBREV IN ('NRT') THEN CAST(SUM(DATEDIFF(
                        SECOND,
                        CASE WHEN jam>START_TIMESTAMP THEN jam ELSE START_TIMESTAMP END,
                        CASE WHEN (CASE WHEN b.END_TIMESTAMP IS NULL THEN GETDATE() ELSE b.END_TIMESTAMP END) >DATEADD(HOUR,1,jam) THEN DATEADD(HOUR,1,jam)
                        ELSE END_TIMESTAMP END)) AS FLOAT)
                END AS 'NRT'
        
            FROM WencoReport_mods.dbo.THIESS_JAM() a
            RIGHT JOIN
                (
                    SELECT 
                        SHIFT_DATE Date,
                        shift_ident,
                        hcd.EQUIP_IDENT Digger, 
                        hsc.STATUS_CODE,
                        STATUS_ABBREV,
                        CAST(START_TIMESTAMP AS DATETIME) START_TIMESTAMP, 
                        CAST(CASE WHEN END_TIMESTAMP IS NULL THEN GETDATE() ELSE end_timestamp END AS DATETIME ) AS END_TIMESTAMP
                    FROM WencoReport.dbo.EQUIPMENT_STATUS_TRANS hcd  WITH (nolock)
                    LEFT JOIN WencoReport.dbo.EQUIP_STATUS_CODE hsc ON hsc.STATUS_CODE = hcd.STATUS_CODE
                    LEFT JOIN WencoReport.dbo.EQUIP a ON hcd.EQUIP_IDENT = a.EQUIP_IDENT
                    WHERE 
                        [shift_date] = CAST(DATEADD(HOUR,-8, GETDATE()) AS DATE)
                        AND a.FLEET_IDENT=('ex')
                        AND a.DESCRIP IN ('9350','9250','9150','984')
                )b
            ON a.jam BETWEEN DATEADD(HOUR,-1,b.START_TIMESTAMP) AND (CASE WHEN b.END_TIMESTAMP IS NULL THEN GETDATE() ELSE b.END_TIMESTAMP END)
            GROUP BY
                CAST(CONCAT(CAST(start_timestamp AS DATE), ' ', DATEPART(HOUR,CASE WHEN jam>START_TIMESTAMP THEN jam ELSE START_TIMESTAMP END),':00:00') AS DATETIME),
                shift_ident,
                START_TIMESTAMP,
                JAM,
                END_TIMESTAMP,
                date,
                DIGGER,
                STATUS_CODE,
                STATUS_ABBREV
            HAVING
                CAST(CAST(DATEDIFF(
                    ss,
                    (CASE WHEN jam>START_TIMESTAMP THEN jam ELSE START_TIMESTAMP END),
                    (CASE WHEN (CASE WHEN b.END_TIMESTAMP IS NULL THEN GETDATE() ELSE b.END_TIMESTAMP END)>DATEADD(HOUR,1,jam) THEN DATEADD(HOUR,1,jam)
                    ELSE END_TIMESTAMP END)) AS FLOAT)/60 AS DECIMAL (10,2))>0
        ) b
    GROUP BY
        date,
        shift,
        digger
),

CTX AS(
    SELECT
        a.*, 
        CASE
            WHEN Queue_At_Loading_Point>0 AND hang.val >0 THEN NULL
            ELSE ISNULL(hang.val,0)
        END Hanging_Time
    FROM CTE a
    OUTER APPLY
        (
            SELECT SUM(DATEDIFF(SECOND, hcd.START_TIMESTAMP, hcd.END_TIMESTAMP))val
            FROM WencoReport.dbo.EQUIPMENT_STATUS_TRANS hcd 
            JOIN WencoReport.dbo.EQUIP_STATUS_CODE hsc ON hsc.STATUS_CODE = hcd.STATUS_CODE
            WHERE hsc.STATUS_ABBREV = 'Waiting' AND hcd.EQUIP_IDENT = a.LOADING_UNIT_IDENT
                AND hcd.start_timestamp BETWEEN a.load_bef AND a.load_start_timestamp
                AND CAST(hcd.START_TIMESTAMP AS DATE) = CAST(DATEADD(HOUR,-8, GETDATE()) AS DATE)
        ) hang
    WHERE MAT_MATERIAL_GROUP='w'
),

CTY AS(
    SELECT
		datetime,
        CAST(Load_start_shift_date AS DATE) Date,
		DATEPART(HOUR,datetime) Time,
        load_start_shift_ident Shift,
        Loading_unit_ident Digger,
        Digger_Type Type,
        COUNT(load_start_timestamp) Load_COUNT,
        CAST(SUM(truck_factor) AS INT) Production,
        CAST(SUM(Prod_by_Payload) AS INT) Prod_by_Payload,

        CAST(CAST(AVG(NULLIF(Loading_Time,0)) AS FLOAT)/60 AS DECIMAL (10,2)) Loading_Time,
        CAST(CAST(AVG(Hanging_Time) AS FLOAT)/60  AS DECIMAL (10,2))  Hanging_Time,
        CAST(CAST(AVG(Spotting_time) AS FLOAT)/60  AS DECIMAL (10,2))  Spotting_time,

        CAST(CAST(AVG(Queue_At_Loading_Point) AS FLOAT)/60  AS DECIMAL (10,2)) Queue_At_Loading_Point,
        CAST(CAST(AVG(Wait_at_Dump) AS FLOAT)/60  AS DECIMAL (10,2))  Wait_at_Dump,
        CAST(CAST(AVG(Dumping) AS FLOAT)/60  AS DECIMAL (10,2))  Dumping,


        CAST(CAST(AVG(Traveling) AS FLOAT)/60 AS DECIMAL (10,2)) Traveling,
        CAST(CAST(AVG(Hauling) AS FLOAT)/60  AS DECIMAL (10,2)) Hauling,
        CAST(AVG(Haul_distance) AS DECIMAL (10,3)) Haul_Distance,
        CAST(AVG(EMPTY_DISTANCE) AS DECIMAL (10,3)) Empty_Distance,
        CAST(AVG(NULLIF(empty_speed,0)) AS DECIMAL (10,2)) Empty_Speed,
        CAST(AVG(NULLIF(hauling_speed,0)) AS DECIMAL (10,2)) Hauling_Speed,
        Location
    FROM CTX a
    GROUP BY
		datetime,
        Loading_unit_ident,
        Load_start_shift_date,
        load_start_shift_ident,
        Digger_Type,
        location
) 

SELECT  
    a.datetime AS Datetime,
    b.Date,
    b.Shift,
    a.Time AS Time,
    a.Type,
    b.Digger,
    a.Production,
    PWT AS WT,
    CAST(a.production/NULLIF(pwt,0) AS INT) Prodty,
    [Load_COUNT],
    [Loading_Time],
    [Hanging_Time],
    [Spotting_time],
    [Queue_At_Loading_Point],
    [Wait_at_Dump],
    [Dumping],
    [Traveling],
    [Hauling],
    [Haul_Distance],
    [Empty_Distance],
    [Empty_Speed],
    [Hauling_Speed],
	CASE
        WHEN b.shift=1 THEN 'DS'
        WHEN b.shift=2 THEN 'NS'
    END AS Shift_Production,
    [Location],
    SWT,
    IOD,
    EOD,
    DOWN,
    CAST(([Empty_Speed]+[Hauling_Speed])/2 AS DECIMAL (10,2)) Speed,
    NRT,
    AVG(Target_Site) Target_Prodty,
    CAST(AVG(Target_WT) AS INT) Target_WT,
    a.Prod_by_Payload                            
FROM ctz b
LEFT JOIN cty a
    ON a.Digger=b.Digger AND a.date=b.date AND a.Shift=b.shift
LEFT JOIN WencoReport_mods.dbo.target c
    ON a.Type=c.digger_type
WHERE a.Datetime<CAST( CONCAT(CAST(GETDATE() AS DATE), ' ', DATEPART(HOUR,GETDATE()),':00:00') AS DATETIME)


GROUP BY
	a.datetime,
	a.Time,
    b.Digger,
    b.date,
    b.Shift, 
    a.Type,
    a.Production,
    [Load_COUNT],
    [Loading_Time],
    [Hanging_Time],
    [Spotting_time],
    [Queue_At_Loading_Point],
    [Wait_at_Dump],
    [Dumping],
    [Traveling],
    [Hauling],
    [Haul_Distance],
    [Empty_Distance],
    [Empty_Speed],
    [Hauling_Speed],
    CASE
        WHEN b.shift=1 THEN 'DS'
        WHEN b.shift=2 THEN 'NS'
    END,
    [Location],
    pwt,
    swt,
    iod,
    eod,
    down,
    nrt,
    a.Prod_by_Payload
GO


