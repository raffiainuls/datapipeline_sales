def process_sum_transactions(spark):
    sum_transactions = spark.sql("""
    WITH 
    iofs AS (
    SELECT 
        'income' AS type,
        id AS sales_id,
        branch_id,
        0 AS employee_id,
        CONCAT('Sales ', product_name, ' totalling ', quantity) AS description,
        order_date AS date,
        amount
    FROM fact_sales
    WHERE is_online_transaction = false
    ), 
    ions AS (
    SELECT 
        'income' AS type,
        id AS sales_id,
        branch_id,
        0 AS employee_id,
        CONCAT('Sales ', product_name, ' totalling ', quantity) AS description,
        order_date AS date,
        price * quantity AS amount
    FROM fact_sales
    WHERE is_online_transaction = true
    ),
    oos AS (
    SELECT 
        'outcome' AS type,
        id AS sales_id,
        branch_id,
        0 AS employee_id,
        'expenses for shipping costs' AS description,
        order_date AS date,
        delivery_fee AS amount
    FROM fact_sales
    WHERE is_free_delivery_fee = 'true'
    ),
    ods AS (
    SELECT 
        'outcome' AS type,
        id AS sales_id,
        branch_id,
         0 AS employee_id,
        CONCAT('expenses for discon discount ', disc_name) AS description,
        order_date AS date,
        price * quantity * disc / 100 AS amount
    FROM fact_sales
    WHERE disc IS NOT NULL
    ),
    salary as (
        select
        'outcome' AS type,
        0 as sales_id,
        branch_id,
        e.id as employee_id,
        CONCAT('Monthly Salary ', e.name) as description,
        make_date(year(current_date()), m, 1) as date,
        e.salary as amount
    from tbl_employee e                             
    cross join (select explode(sequence(1, 12)) as m) months
    where e.active = 'true'
    ),
    operational_expenses as 
    (
        select 
        'outcome' as type,
        0 as sales_id,
        b.id as branch_id,
        0 as employee_id,
        concat('Expenses Operational Branch ', b.name) as description,
        d as date,
       -- CAST(200000 + (rand() * (400000 - 200000)) AS INT) AS amount
        400000 as amount
    from tbl_branch b 
    cross join (
    SELECT explode(sequence(to_date('2025-01-02'), to_date('2025-12-31'), interval 1 day)) AS d
    )
    ),
    electricity_water_expenses as
    (
        SELECT
        'outcome' AS type,
        0 AS sales_id,
        b.id AS branch_id,
        0 AS employee_id,
        CONCAT('Expenses Water and Electricity ', b.name) AS description,
        make_date(2025, m, 1) AS date,
      --  CAST(1000000 + (rand() * (2000000 - 1000000)) AS INT) AS amount
        2000000 as amount
    FROM tbl_branch b
    CROSS JOIN (SELECT explode(sequence(1, 12)) AS m)                           
    ),
    it_technology_it as 
    (
        SELECT
        'outcome' AS type,
        0 AS sales_id,
        b.id AS branch_id,
        0 AS employee_id,
        CONCAT('IT & Technology ', b.name) AS description,
        make_date(2025, m, 1) AS date,
    --    CAST(2000000 + (rand() * (6000000 - 3000000)) AS INT) AS amount
        6000000 as amount  
    FROM tbl_branch b
    CROSS JOIN (SELECT explode(sequence(1, 12)) AS m)                         
    ),
    maintenance_expenses as 
    (
        SELECT
        'outcome' AS type,
        0 AS sales_id,
        b.id AS branch_id,
        0 AS employee_id,
        CONCAT('Maintenance, Service, and Cleaning Branch ', b.name) AS description,
        date_add(to_date('2025-01-01'), CAST(rand() * 364 AS INT)) AS date,
      --  CAST(3000000 + (rand() * (7000000 - 3000000)) AS INT) AS amount
        7000000 as amount
    FROM tbl_branch b
    CROSS JOIN (SELECT explode(sequence(1, 3)) AS repeat_count)                        
    )              
    SELECT * FROM iofs
    UNION ALL
    SELECT * FROM ions
    UNION ALL
    SELECT * FROM oos
    UNION ALL
    SELECT * FROM ods
    UNION ALL
    select * from salary
    UNION ALL 
    select * from operational_expenses
    UNION ALL 
    select * from electricity_water_expenses
    UNION ALL 
    select * from it_technology_it
    UNION ALL 
    select * from maintenance_expenses
    """)
    return sum_transactions
