CREATE View [dbo].[payload_truck_type] 
as


WITH CTE AS
    (
        SELECT DISTINCT
        a.haul_cycle_rec_ident,
        START_SHIFT_DATE,
        START_SHIFT_IDENT,
        DATEADD(HOUR, DATEDIFF(HOUR, 0, a.load_start_timestamp), 0) time,
        CASE
            WHEN LAG (a.load_start_timestamp) OVER (PARTITION BY a.LOADING_UNIT_IDENT ORDER BY a.load_start_timestamp ASC) IS NULL THEN DATEADD(HOUR, DATEDIFF(HOUR, 0, a.load_start_timestamp), 0)
            ELSE LAG (a.load_start_timestamp) OVER (PARTITION BY a.LOADING_UNIT_IDENT ORDER BY a.load_start_timestamp ASC)
        END AS load_bef,
        DATEDIFF(HOUR,wencoreport.dbo.[CLR_Shift_GetShiftStartSTTimestampByShiftDateAndIdent](a.START_SHIFT_DATE, a.START_SHIFT_IDENT),CONCAT(CAST(a.START_TIMESTAMP AS DATE), ' ', DATEPART(HOUR,a.START_TIMESTAMP),':00') ) hour_part,
        a.start_timestamp,
        PAYLOAD_REPORTING as PAYLOAD_REPORTING,
        a.load_start_timestamp, 
        load_start_shift_date, 
        load_start_shift_ident,
        dump_end_timestamp,
        Hauling_unit_payload,
        hauling_unit_ident,
        loading_unit_ident,
        material_ident,
        empty_distance,
        haul_distance,
        digger.descrip Digger_Type, 
        d.cst_Desc,
        AVG(CASE WHEN a.EMPTY_DISTANCE/nullif((Traveling.val/3600.00),0)>'50' THEN'50' ELSE a.EMPTY_DISTANCE/nullif((Traveling.val/3600.00),0) END) AS empty_speed,
        AVG(CASE WHEN a.HAUL_DISTANCE/nullif((Hauling.val/3600.00),0)> '35' THEN'35' ELSE a.HAUL_DISTANCE/nullif((Hauling.val/3600.00),0) END) AS hauling_speed,
        MAT_MATERIAL_GROUP,
        trucks.DESCRIP trucktype
        
        FROM  WencoReport.dbo.HAUL_CYCLE_TRANS a WITH (nolock)
        LEFT JOIN WencoReport.dbo.EQUIP_HAULING_UNIT truck ON truck.EQUIP_IDENT  = a.HAULING_UNIT_IDENT AND truck.EQUIP_IDENT LIKE 'RD%'
        LEFT JOIN WencoReport.dbo.EQUIP digger ON digger.EQUIP_IDENT  = a.loading_unit_ident
        LEFT JOIN WencoReport.dbo.material  ON MAT_MATERIAL_IDENT= a.material_ident
        LEFT JOIN WencoReport.dbo.EQUIP trucks ON a.HAULING_UNIT_IDENT=trucks.EQUIP_IDENT
        LEFT JOIN WencoReport.dbo.COST_CENTER_EQUIP_TRANS b
            ON a.LOAD_START_TIMESTAMP BETWEEN b.START_TIMESTAMP AND
            CASE
                WHEN b.END_TIMESTAMP IS NULL THEN getdate()
                ELSE b.END_TIMESTAMP
            END AND
            a.LOADING_UNIT_IDENT = b.EQUIP_IDENT
        LEFT JOIN WencoReport.dbo.COST_CENTER d ON b.COST_CODE=d.CST_COST_CODE

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
        
        WHERE start_shift_date BETWEEN CAST(DATEADD(day,-30, GETDATE()) AS DATE) AND CAST(DATEADD(day,0,GETDATE()) AS DATE)

        GROUP BY
            LOADING_UNIT_IDENT,
            a.START_TIMESTAMP,
            LOAD_START_SHIFT_DATE,
            LOAD_START_SHIFT_IDENT,
            DUMP_END_TIMESTAMP,
            Hauling_unit_payload,
            HAULING_UNIT_IDENT,
            MATERIAL_IDENT,
            digger.descrip,
            load_start_timestamp,
            empty_distance,
            haul_distance,
            HAUL_CYCLE_REC_IDENT,
            MAT_MATERIAL_GROUP,
            START_SHIFT_DATE,
            a.PAYLOAD_REPORTING,
            START_SHIFT_IDENT,
            trucks.DESCRIP,
            d.cst_Desc
)


SELECT
    START_SHIFT_DATE as START_SHIFT_DATE,
    load_start_shift_ident as START_SHIFT_IDENT,
    time,
    DATEPART(hh,time) jam,
    hour_part,
    LOADING_UNIT_IDENT,
    trucktype,
    CASE WHEN MATERIAL_IDENT IN ('TSH', 'TSS', 'TS') THEN 'Top Soil'
        WHEN MATERIAL_IDENT IN ('OBS', 'OBH', 'OBD', 'OBB', 'OB1') THEN 'OB Blasted'
        WHEN MATERIAL_IDENT IN ('FS', 'FH', 'OBF', 'OB2') THEN 'OB Freedig'
        WHEN MATERIAL_IDENT IN ('ORS', 'ORH', 'ORP') THEN 'Rehandle Production'
        WHEN MATERIAL_IDENT IN ('MUS', 'MUH', 'MUD') THEN 'Mud'
        WHEN MATERIAL_IDENT IN ('OMM') THEN 'OB Mix Mud'
    END AS Material, 
    COUNT(*) load_count,
    CASE
        WHEN  trucktype='CAT785C' THEN AVG(PAYLOAD_REPORTING)
        WHEN trucktype='CAT777D' THEN AVG(PAYLOAD_REPORTING)
    END AS Hauling_unit_payload,
    CASE
        WHEN DATEPART(hh,time)=7 THEN 1
        WHEN DATEPART(hh,time)=8 THEN 2
        WHEN DATEPART(hh,time)=9 THEN 3
        WHEN DATEPART(hh,time)=10 THEN 4
        WHEN DATEPART(hh,time)=11 THEN 5
        WHEN DATEPART(hh,time)=12 THEN 6
        WHEN DATEPART(hh,time)=13 THEN 7
        WHEN DATEPART(hh,time)=14 THEN 8
        WHEN DATEPART(hh,time)=15 THEN 9
        WHEN DATEPART(hh,time)=16 THEN 10
        WHEN DATEPART(hh,time)=17 THEN 11
        WHEN DATEPART(hh,time)=18 THEN 12
        WHEN DATEPART(hh,time)=19 THEN 13
        WHEN DATEPART(hh,time)=20 THEN 14
        WHEN DATEPART(hh,time)=21 THEN 15
        WHEN DATEPART(hh,time)=22 THEN 16
        WHEN DATEPART(hh,time)=23 THEN 17
        WHEN DATEPART(hh,time)=0 THEN 18
        WHEN DATEPART(hh,time)=1 THEN 19
        WHEN DATEPART(hh,time)=2 THEN 20
        WHEN DATEPART(hh,time)=3 THEN 21
        WHEN DATEPART(hh,time)=4 THEN 22
        WHEN DATEPART(hh,time)=5 THEN 23
        WHEN DATEPART(hh,time)=6 THEN 24
    END AS urutan,
    CASE WHEN  trucktype='CAT785C' THEN 148 ELSE 100 end as batas_underload,
    CASE WHEN load_start_shift_ident=1 THEN 'DS' ELSE 'NS' end as shift_prod,
    CASE
        WHEN DATEPART(HOUR,a.LOAD_START_TIMESTAMP) IN ('7') THEN '07:00-08:00'
        WHEN DATEPART(HOUR,a.LOAD_START_TIMESTAMP) IN ('8') THEN '08:00-09:00'
        WHEN DATEPART(HOUR,a.LOAD_START_TIMESTAMP) IN ('9') THEN '09:00-10:00'
        WHEN DATEPART(HOUR,a.LOAD_START_TIMESTAMP) IN ('10') THEN '10:00-11:00'
        WHEN DATEPART(HOUR,a.LOAD_START_TIMESTAMP) IN ('11') THEN '11:00-12:00'
        WHEN DATEPART(HOUR,a.LOAD_START_TIMESTAMP) IN ('12') THEN '12:00-13:00'
        WHEN DATEPART(HOUR,a.LOAD_START_TIMESTAMP) IN ('13') THEN '13:00-14:00'
        WHEN DATEPART(HOUR,a.LOAD_START_TIMESTAMP) IN ('14') THEN '14:00-15:00'
        WHEN DATEPART(HOUR,a.LOAD_START_TIMESTAMP) IN ('15') THEN '15:00-16:00'
        WHEN DATEPART(HOUR,a.LOAD_START_TIMESTAMP) IN ('16') THEN '16:00-17:00'
        WHEN DATEPART(HOUR,a.LOAD_START_TIMESTAMP) IN ('17') THEN '17:00-18:00'
        WHEN DATEPART(HOUR,a.LOAD_START_TIMESTAMP) IN ('18') THEN '18:00-19:00'
        WHEN DATEPART(HOUR,a.LOAD_START_TIMESTAMP) IN ('19') THEN '19:00-20:00'
        WHEN DATEPART(HOUR,a.LOAD_START_TIMESTAMP) IN ('20') THEN '20:00-21:00'
        WHEN DATEPART(HOUR,a.LOAD_START_TIMESTAMP) IN ('21') THEN '21:00-22:00'
        WHEN DATEPART(HOUR,a.LOAD_START_TIMESTAMP) IN ('22') THEN '22:00-23:00'
        WHEN DATEPART(HOUR,a.LOAD_START_TIMESTAMP) IN ('23') THEN '23:00-00:00'
        WHEN DATEPART(HOUR,a.LOAD_START_TIMESTAMP) IN ('0') THEN '00:00-01:00'
        WHEN DATEPART(HOUR,a.LOAD_START_TIMESTAMP) IN ('1') THEN '01:00-02:00'
        WHEN DATEPART(HOUR,a.LOAD_START_TIMESTAMP) IN ('2') THEN '02:00-03:00'
        WHEN DATEPART(HOUR,a.LOAD_START_TIMESTAMP) IN ('3') THEN '03:00-04:00'
        WHEN DATEPART(HOUR,a.LOAD_START_TIMESTAMP) IN ('4') THEN '04:00-05:00'
        WHEN DATEPART(HOUR,a.LOAD_START_TIMESTAMP) IN ('5') THEN '05:00-06:00'
        WHEN DATEPART(HOUR,a.LOAD_START_TIMESTAMP) IN ('6') THEN '06:00-07:00'
    END AS jam2,
    CASE
        WHEN trucktype = 'CAT785C' AND ISNULL(CAST(PAYLOAD_REPORTING AS DECIMAL (10,2)),null) < 140 THEN 'under load'
        WHEN trucktype = 'CAT785C' AND ISNULL(CAST(PAYLOAD_REPORTING  AS DECIMAL (10,2)),null) > 176.4 THEN 'over load'
        WHEN trucktype = 'CAT785C' AND ISNULL(CAST(PAYLOAD_REPORTING  AS DECIMAL (10,2)),null) BETWEEN 140 AND 147 THEN 'lower range'
        WHEN trucktype = 'CAT785C' AND ISNULL(CAST(PAYLOAD_REPORTING  AS DECIMAL (10,2)),null) BETWEEN 147 AND 161.7 THEN 'in range'
        WHEN trucktype = 'CAT785C' AND ISNULL(CAST(PAYLOAD_REPORTING  AS DECIMAL (10,2)),null) BETWEEN 161.7 AND 176.4 THEN 'upper range'

        WHEN trucktype = 'CAT777D' AND ISNULL(CAST(PAYLOAD_REPORTING   AS DECIMAL (10,2)),null) < 97.5 THEN 'under load'
        WHEN trucktype = 'CAT777D' AND ISNULL(CAST(PAYLOAD_REPORTING   AS DECIMAL (10,2)),null) > 126 THEN 'over load'
        WHEN trucktype = 'CAT777D' AND ISNULL(CAST(PAYLOAD_REPORTING   AS DECIMAL (10,2)),null) BETWEEN 97.5 AND 105 THEN 'lower range'
        WHEN trucktype = 'CAT777D' AND ISNULL(CAST(PAYLOAD_REPORTING   AS DECIMAL (10,2)),null) BETWEEN 105 AND 115.5 THEN 'in range'
        WHEN trucktype = 'CAT777D' AND ISNULL(CAST(PAYLOAD_REPORTING   AS DECIMAL (10,2)),null) BETWEEN 115.5 AND 126 THEN 'upper range'
        ELSE 'upper range'
    END AS payload_range,
    cst_Desc Interburden
FROM CTE a

GROUP BY
    cst_Desc,
    time,
    START_SHIFT_DATE,
    LOAD_START_SHIFT_IDENT,
    hour_part,
    trucktype,
    LOADING_UNIT_IDENT,
    a.LOAD_START_TIMESTAMP,
    MATERIAL_IDENT,
    PAYLOAD_REPORTING
