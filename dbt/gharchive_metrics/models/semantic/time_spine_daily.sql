select date_day
from unnest(generate_date_array(date '2026-01-01', date '2026-12-31', interval 1 day)) as date_day
