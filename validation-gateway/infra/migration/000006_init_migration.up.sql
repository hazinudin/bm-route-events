create index if not exists smd_job_route_idx on validation_jobs (
    data_type,
    (payload -> 'routes' ->> 0)
) where (payload -> 'routes') is not null;
