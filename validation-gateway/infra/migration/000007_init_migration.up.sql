create index if not exists invij_job_idx on validation_jobs (
    data_type,
    (payload ->> 'id_jbt')
) where (payload ->> 'id_jbt') is not null;