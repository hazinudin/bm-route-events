drop index if exists job_file_idx;
drop index if exists smd_job_route_idx;

create index if not exists job_file_idx on validation_jobs (
    (payload ->> 'file_name'),
    (payload -> 'routes' ->> 0)
) where (payload ->> 'file_name') is not null;