create table actors_scd (
    actorid text,
    quality_class quality_classes,
    is_active bool,
    start_date integer,
    end_date integer,
    current_year integer
    primary key (actorid, current_year)
);
