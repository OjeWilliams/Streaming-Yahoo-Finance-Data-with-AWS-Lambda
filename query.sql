WITH Group1 AS 
    (SELECT name, Round(high,2) As high, ts,SUBSTRING(ts,12,2) AS Hour, ts AS Datetime
    FROM "stocks-db"."01"),
    
    Group2 AS 
    (SELECT Group1.name AS Company,
         MAX(Group1.high) AS High,
         Group1.Hour AS Hour
    FROM Group1
    GROUP BY  Group1.name, Group1.Hour)
   
    
SELECT DISTINCT Group2.Company, Group2.High, Group2.Hour, Group1.DateTime

FROM Group1, Group2

WHERE Group1.name = Group2.Company
        AND Round(Group1.high,2) = Round(Group2.High,2)
        AND Group1.Hour = Group2.Hour

ORDER BY  Group2.Company, Group2.Hour