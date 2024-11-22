WITH __latest_actors_scd as (
    select
        max(current_year) as latest_date
    from actors_scd
),


actors_source AS (
    SELECT
        actorid,
        current_year,
        "quality_class",
        is_active,
        CASE
            WHEN is_active != LAG(is_active) OVER (PARTITION BY actorid ORDER BY current_year) THEN 1
            WHEN LAG(is_active) OVER (PARTITION BY actorid ORDER BY current_year) IS NULL THEN NULL
            WHEN "quality_class" != LAG("quality_class") OVER (PARTITION BY actorid ORDER BY current_year) THEN 1
            WHEN LAG("quality_class") OVER (PARTITION BY actorid ORDER BY current_year) IS NULL THEN NULL
            ELSE 0
        END as change_indicator
    FROM actors
    where current_year = (select (latest_date+1) from __latest_actors_scd)
    GROUP BY
        actorid,
        current_year,
        "quality_class",
        is_active
),

__changes AS (
    SELECT
        actorid,
        current_year,
        "quality_class",
        is_active,
        change_indicator
    FROM actors_source
    WHERE 
        change_indicator = 1
        OR change_indicator IS NULL
),

__final AS (
    SELECT
        actorid,
        current_year,
        quality_class,
        is_active,
        CASE
            WHEN 
                (LAG(change_indicator) OVER (PARTITION BY actorid) = 1) THEN current_year
            WHEN 
                (LAG(change_indicator) OVER (PARTITION BY actorid)) IS NULL THEN current_year
        END AS start_date,
        CASE
            WHEN 
                change_indicator = 1 AND current_year != MAX(current_year) OVER (PARTITION BY actorid) THEN current_year
            WHEN 
                (LAG(change_indicator) OVER (PARTITION BY actorid)) IS NULL THEN current_year
            WHEN current_year = MAX(current_year) OVER (PARTITION BY actorid) THEN 9999
        END AS end_date
    FROM __changes
    ORDER BY actorid, current_year DESC
)

INSERT INTO actors_scd (
    actorid,
    quality_class,
    is_active,
    start_date,
    end_date,
    current_year
)
SELECT
    actorid,
    quality_class,
    is_active,
    start_date,
    end_date,
    current_year
FROM __final;
