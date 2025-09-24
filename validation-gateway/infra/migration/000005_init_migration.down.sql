drop index if exists job_file_idx;

create index if not exists job_file_idx on validation_jobs (
    (payload ->> 'file_name')
) where (payload ->> 'file_name') is not null;

create index if not exists smd_job_route_idx on validation_jobs (
    data_type,
    (payload ->> 'route')
) where (payload ->> 'route') is not null;
