drop index if exists job_idx;
drop index if exists job_event_store_idx;
drop index if exists job_result_idx;

drop table if exists validation_job_error_trace;
drop table if exists validation_jobs_event_store;
drop table if exists validation_job_results;
drop table if exists validation_job_results_msg;

drop table if exists validation_jobs;

create table if not exists validation_jobs (
    job_id uuid primary key,
    data_type varchar(50),
    submitted_at bigint,
    payload jsonb
);

create index if not exists job_file_idx on validation_jobs (
    (payload ->> 'file_name')
) where (payload ->> 'file_name') is not null;

create index if not exists job_id_idx on validation_jobs (
    job_id
);

create index if not exists smd_job_route_idx on validation_jobs (
    data_type,
    (payload ->> 'route')
) where (payload ->> 'route') is not null;

create table if not exists validation_jobs_event_store (
    id int generated always as identity primary key,
    job_id uuid not null references validation_jobs(job_id) on delete cascade,
    event_name varchar(200) not null,
    occurred_at bigint,
    event jsonb not null
);

create index if not exists job_event_store_idx on validation_jobs_event_store (
    job_id,
    event_name,
    occurred_at desc
);

create table if not exists validation_job_results (
    job_id uuid not null references validation_jobs(job_id) on delete cascade,
    attempt_id smallint not null,
    status varchar(50) not null,
    message_count bigint,
    all_msg_status text[],
    ignorables text[],
    ignored_tags text[],
    primary key (job_id, attempt_id)
);

create index if not exists job_result_idx on validation_job_results (
    job_id,
    attempt_id
);

create table if not exists validation_job_results_msg (
    id int generated always as identity primary key,
    job_id uuid not null,
    attempt_id smallint not null,
    msg varchar(400),
    msg_status varchar(50),
    msg_status_idx smallint,
    ignore_in varchar(50),
    content_id varchar(50),
    foreign key (job_id, attempt_id) references validation_job_results(job_id, attempt_id) on delete cascade
);

create index if not exists job_result_msg_idx on validation_job_results_msg (
    job_id,
    attempt_id
);