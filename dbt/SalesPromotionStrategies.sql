{{ config(materialized='incremental',
        unique_key='OrderDate') }}


-- Macros
{% macro left_join(source_name, alias, join_condition) %}
  LEFT JOIN {{ source('warehouse_sales', source_name) }} AS {{ alias }} ON {{ join_condition }}
{% endmacro %}

{% macro countif(expression) %}
  COUNTIF({{ expression }})
{% endmacro %}

-- Query
WITH joined_data AS (
  SELECT
    CAST(status.OrderDate AS DATE) AS OrderDate,
    COUNT(fact.SalesOrderID) AS TotalOrder,
    AVG(OrderQty) AS AverageQty,
    AVG(LineTotal) AS AverageTotalPrice,
    SUM(CAST(status.OnlineOrderFlag AS INT)) AS TotalOnlineOrder,
    {{ countif("fact.CreditCardID IS NOT NULL") }} AS TotalUseCreditCard,
    {{ countif("contact.FirstName IN ('Mr.', 'Sr.')") }} AS TotalMenBuyer,
    {{ countif("contact.FirstName IN ('Ms.', 'Sra.')") }} AS TotalWomenBuyer,
    {{ countif("specialoffer.type = 'Excess Inventory'") }} AS DiscExcessInventoryCount,
    {{ countif("specialoffer.type = 'Seasonal Discount'") }} AS DiscSeasonalCount,
    {{ countif("specialoffer.type = 'Discontinued Product'") }} AS DiscDiscontinuedCount,
    {{ countif("specialoffer.type = 'New Product'") }} AS DiscNewProductCount,
    {{ countif("specialoffer.type = 'Volume Discount'") }} AS DiscVolumeCount
  FROM
    {{ source('warehouse_sales', 'SalesOrderFact') }} AS fact
  {{ left_join('SalesOrderDetail', 'detail', 'fact.SalesOrderID = detail.SalesOrderID') }}
  {{ left_join('SalesOrderStatus', 'status', 'fact.SalesOrderID = status.SalesOrderID') }}
  {{ left_join('CreditCard', 'creditcard', 'fact.CreditCardID = creditcard.CreditCardID') }}
  {{ left_join('ContactCreditCard', 'contactcreditcard', 'fact.CreditCardID = contactcreditcard.CreditCardID') }}
  {{ left_join('Contact', 'contact', 'contactcreditcard.ContactID = contact.ContactID') }}
  {{ left_join('SpecialOffer', 'specialoffer', 'detail.SpecialOfferID = specialoffer.SpecialOfferID') }}
  WHERE
    status.OrderDate = '2014-01-01'
  GROUP BY 1
)

SELECT * FROM joined_data