{{ config(materialized='table') }}
WITH sales_data AS (
  SELECT
    fact.SalesOrderID,
    territory.TerritoryID,
    territory.Name,
    territory.CountryRegionCode,
    status.OrderDate,
    detail.LineTotal,
    detail.OrderQty,
    currencyrate.EndOfDayRate
  FROM {{ source('warehouse_sales', 'SalesOrderFact') }} fact
  LEFT JOIN {{ source('warehouse_sales', 'SalesOrderStatus') }} status
    ON fact.SalesOrderID = status.SalesOrderID
  LEFT JOIN {{ source('warehouse_sales', 'SalesOrderDetail') }} detail
    ON fact.SalesOrderID = detail.SalesOrderID
  LEFT JOIN {{ source('warehouse_sales', 'CurrencyRate') }} currencyrate
    ON fact.CurrencyRateID = currencyrate.CurrencyRateID
  LEFT JOIN {{ source('warehouse_sales', 'SalesTerritory') }} territory
    ON fact.TerritoryID = territory.TerritoryID
  WHERE status.OrderDate BETWEEN '2013-01-01' AND '2014-12-31'
    AND currencyrate.ToCurrencyCode = 'EUR'
)

SELECT
  TerritoryID,
  Name,
  CountryRegionCode,
  COUNTIF(OrderDate < '2014-01-01') AS TotalOrder2013,
  COUNTIF(OrderDate >= '2014-01-01') AS TotalOrder2014,
  SUM(IF(OrderDate < '2014-01-01', LineTotal * EndOfDayRate, 0)) AS TotalSalesEUR2013,
  SUM(IF(OrderDate >= '2014-01-01', LineTotal * EndOfDayRate, 0)) AS TotalSalesEUR2014,
  AVG(IF(OrderDate < '2014-01-01', OrderQty, NULL)) AS AverageQty2013,
  AVG(IF(OrderDate >= '2014-01-01', OrderQty, NULL)) AS AverageQty2014
FROM sales_data
GROUP BY 1, 2, 3