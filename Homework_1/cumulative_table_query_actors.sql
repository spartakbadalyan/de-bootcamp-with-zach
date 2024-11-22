WITH past AS (
    SELECT * FROM actors WHERE current_year = {prev_year}
),

present AS (
    SELECT
        *,
        CAST(
            CASE
                WHEN avg(rating) over (partition by actorid) > 8 THEN 'star'
                WHEN
                    avg(rating) over (partition by actorid) > 7
                    AND avg(rating) over (partition by actorid) <= 8
                    THEN 'good'
                WHEN
                    avg(rating) over (partition by actorid) > 6
                    AND avg(rating) over (partition by actorid) <= 7
                    THEN 'average'
                WHEN avg(rating) over (partition by actorid) <= 6 THEN 'bad'
            END AS quality_classes
        ),
        case
            when COUNT(filmid) over (partition by actorid) >= 1 then true
            else false
        end as is_active_this_year
    FROM actor_films
    WHERE year = {current_year}
),

__joined AS (
    SELECT
        COALESCE(pa.actorid, pr.actorid) AS actorid,
        COALESCE(pa.filmid, pr.filmid) AS filmid,
        COALESCE(pr.year, (pa.current_year + 1)) AS current_year,
        row_number() OVER (
            PARTITION BY coalesce(pr.actorid, pa.actorid)
            ORDER BY pr.filmid
        ) as last_yearly_perf_rn,
        CASE
            WHEN pr.year IS NOT NULL THEN pr.quality_classes
            ELSE LAST_VALUE(pa.quality_class) OVER (
                PARTITION BY pa.actorid, pa.current_year
                ORDER BY pa.current_year
            )
        END AS quality_class,
        CASE
            WHEN pa.films IS NULL
                THEN ARRAY[ROW(
                    pr.film,
                    pr.votes,
                    pr.rating,
                    pr.filmid
                )::films]
            WHEN pr.year IS NOT NULL
                THEN pa.films || ARRAY[ROW(
                    pr.film,
                    pr.votes,
                    pr.rating,
                    pr.filmid
                )::films]
            ELSE pa.films
        END AS films,
        CASE
            WHEN MAX(CASE
                WHEN pr.is_active_this_year IS NOT NULL
                    THEN
                        CASE WHEN pr.is_active_this_year THEN 1 ELSE 0 END
                ELSE 0
            END)
                OVER (PARTITION BY COALESCE(pa.actorid, pr.actorid))
            = 1 THEN true
            ELSE false
        END AS is_active_current_year
    FROM past pa
    FULL OUTER JOIN present pr
        ON
            pa.actorid = pr.actorid
            AND pa.filmid = pr.filmid
            and pa.current_year = pr.year
),

_yearly_perf as (
    select
        actorid,
        current_year,
        quality_class
    from __joined
    where last_yearly_perf_rn = 1
),

__final as (
    SELECT
        __joined.actorid,
        __joined.filmid,
        __joined.current_year,
        _yearly_perf.quality_class,
        __joined.films,
        __joined.is_active_current_year
    FROM __joined
    left join _yearly_perf
        on
            __joined.actorid = _yearly_perf.actorid
            and __joined.current_year = _yearly_perf.current_year
)

INSERT INTO actors (
    actorid,
    filmid,
    current_year,
    quality_class,
    films,
    is_active
)

SELECT
    actorid,
    filmid,
    current_year,
    quality_class,
    films,
    is_active_current_year
FROM __final;
