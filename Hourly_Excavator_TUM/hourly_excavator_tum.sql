CREATE VIEW [dbo].[hourly_excavator_tum]
AS

SELECT
    CASE WHEN jam='7' THEN '1'
        WHEN jam='8' THEN '2'
        WHEN jam='9' THEN '3'
        WHEN jam='10' THEN '4'
        WHEN jam='11' THEN '5'
        WHEN jam='12' THEN '6'
        WHEN jam='13' THEN '7'
        WHEN jam='14' THEN '8'
        WHEN jam='15' THEN '9'
        WHEN jam='16' THEN '10'
        WHEN jam='17' THEN '11'
        WHEN jam='18' THEN '12'
        WHEN jam='19' THEN '13'
        WHEN jam='20' THEN '14'
        WHEN jam='21' THEN '15'
        WHEN jam='22' THEN '16'
        WHEN jam='23' THEN '17'
        WHEN jam='0' THEN '18'
        WHEN jam='1' THEN '19'
        WHEN jam='2' THEN '20'
        WHEN jam='3' THEN '21'
        WHEN jam='4' THEN '22'
        WHEN jam='5' THEN '23'
        WHEN jam='6' THEN '24'
    END AS urutan,
    *,
    CASE
        WHEN category='IOD' THEN durasi ELSE '0'
    END AS 'IOD',
    CASE
        WHEN category='EOD' THEN durasi ELSE '0'
    END AS 'EOD',
    CASE
        WHEN category='DOWN' THEN durasi ELSE '0'
    END AS 'DOWN',
    CASE
        WHEN category='SWT' THEN durasi ELSE '0'
    END AS 'SWT',
    CASE
        WHEN category='PWT' THEN durasi ELSE '0'
    END AS 'PWT2'
FROM
    (
        SELECT
            CASE
                WHEN SHIFT=1 THEN 'DS'
                ELSE 'NS'
            END AS SHIFT,
            jam,
            digger,
            CASE
                WHEN category IN('PMD','UMD','MEO') THEN 'DOWN'
                WHEN category IN ('Waiting','LU Loading','Spotting') THEN 'PWT'
                WHEN status IN ('Standby','LU Tramming') THEN 'IOD'
                ELSE category
            END category,
            [status] AS status,
            CAST (SUM(durasi_menit)/3600 AS DECIMAL (10,8)) durasi,
            start,
            stop
        FROM
            (
                SELECT
                    SHIFT,
                    ROW_NUMBER () OVER (ORDER BY Digger,start_timestamp) Nomor, 
                    status_abbrev Category,
                    DATEPART(HOUR,CASE WHEN jam>START_TIMESTAMP THEN jam ELSE START_TIMESTAMP END) AS Jam,
                    Tanggal, 
                    Digger,
                    Status,
                    CASE
                        WHEN jam>START_TIMESTAMP THEN jam ELSE START_TIMESTAMP
                    END AS Start,
                    CASE
                        WHEN (CASE WHEN b.END_TIMESTAMP IS NULL THEN GETDATE() ELSE b.END_TIMESTAMP END) > DATEADD(HOUR,1,jam) THEN DATEADD(HOUR,1,jam)
                        ELSE END_TIMESTAMP
                    END AS Stop,
                    CAST(CAST(datediff
                        (
                            ss,
                            (CASE WHEN jam>START_TIMESTAMP THEN jam ELSE START_TIMESTAMP END),
                            (CASE
                                WHEN (CASE WHEN b.END_TIMESTAMP IS NULL THEN GETDATE() ELSE b.END_TIMESTAMP END) > DATEADD(HOUR,1,jam) THEN DATEADD(HOUR,1,jam)
                                ELSE END_TIMESTAMP
                            END)
                        )
                        AS FLOAT)
                    AS DECIMAL (10,2)) Durasi_Menit

                    FROM WencoReport_mods.dbo.THIESS_JAM () a
                    RIGHT JOIN
                        ( 
                            SELECT
                                shift_ident shift,
                                status_abbrev,
                                TRIM(CONVERT(VARCHAR(13),SHIFT_DATE,106)) Tanggal,
                                hcd.EQUIP_IDENT Digger, STATUS_DESC AS Status, 
                                CAST(START_TIMESTAMP AS DATETIME) START_TIMESTAMP ,
                                CAST(END_TIMESTAMP AS DATETIME ) AS END_TIMESTAMP
                        
                            FROM WencoReport.dbo.EQUIPMENT_STATUS_TRANS hcd
                            JOIN WencoReport.dbo.EQUIP_STATUS_CODE hsc ON hsc.STATUS_CODE = hcd.STATUS_CODE
                            INNER JOIN (SELECT * FROM WencoReport.dbo.EQUIP WHERE TEST = 'N') E ON hcd.EQUIP_IDENT = E.EQUIP_IDENT 
                            JOIN WencoReport.dbo.EQUIP a ON hcd.EQUIP_IDENT=a.EQUIP_IDENT

                            WHERE 
                                [shift_date] =CAST(DATEADD(HOUR,-7,GETDATE()) AS Date)
                                AND a.EQUIP_IDENT LIKE 'ex%'
                        )b
                        ON a.jam BETWEEN DATEADD(HOUR,-1,b.START_TIMESTAMP) AND (CASE WHEN b.END_TIMESTAMP IS NULL THEN GETDATE() ELSE b.END_TIMESTAMP END)
                    GROUP BY
                        shift,
                        START_TIMESTAMP,
                        JAM,
                        END_TIMESTAMP,
                        TANGGAL,
                        DIGGER,
                        STATUS,
                        status_abbrev
                    HAVING CAST(CAST(datediff
                        (
                            ss,
                            (CASE WHEN jam>START_TIMESTAMP THEN jam ELSE START_TIMESTAMP END),
                            (CASE
                                WHEN (CASE WHEN b.END_TIMESTAMP IS NULL THEN GETDATE() ELSE b.END_TIMESTAMP END) > DATEADD(HOUR,1,jam) THEN DATEADD(HOUR,1,jam)
                                ELSE END_TIMESTAMP
                            END)
                        ) AS FLOAT)/60 AS DECIMAL (10,2))>0
            )b
        GROUP BY 
            jam,
            digger, 
            CASE
                WHEN category IN('PMD','UMD','MEO') THEN 'DOWN'
                WHEN category IN ('Waiting','LU Loading','Spotting') THEN 'PWT'
                WHEN status IN ( 'Standby','LU Tramming') THEN 'IOD'
                ELSE category
            END,
            status,
            CASE
                WHEN SHIFT=1 THEN 'DS' ELSE 'NS'
            END ,
            Start,
            Stop)a
            WHERE Start <CAST( concat(CAST(GETDATE() AS date), ' ', datepart(HOUR,GETDATE()),':00:00') AS DATETIME)




