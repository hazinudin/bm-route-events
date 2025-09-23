drop index if exists job_idx;
drop index if exists job_event_store_idx;
drop index if exists job_result_idx;

drop table if exists validation_job_error_trace;
drop table if exists validation_jobs_event_store;
drop table if exists validation_job_results_msg;
drop table if exists validation_job_results;

drop table if exists validation_jobs;

create table if not exists validation_jobs (
    job_id varchar(200) primary key,
    data_type varchar(50),
    submitted_at bigint,
    payload jsonb
);

create index if not exists job_idx on validation_jobs (
    data_type, 
    job_id
);

create table if not exists validation_jobs_event_store (
    id serial primary key,
    job_id varchar(200) not null references validation_jobs(job_id) on delete cascade,
    event_name varchar(200) not null,
    occurred_at bigint,
    event jsonb not null
);

create index if not exists job_event_store_idx on validation_jobs_event_store (
    job_id, 
    event_name, 
    occurred_at desc
);

create table if not exists validation_job_results_msg (
    id serial primary key,
    job_id varchar(200) not null references validation_jobs(job_id) on delete cascade,
    msg varchar(400),
    msg_status varchar(50),
    msg_status_idx smallint,
    ignore_in varchar(50),
    content_id varchar(50)
);

create index if not exists job_result_idx on validation_job_results_msg (
    job_id,
    msg_status_idx asc
);

create table if not exists validation_job_results (
    id serial primary key,
    job_id varchar(200) not null references validation_jobs(job_id) on delete cascade,
    status varchar(50) not null,
    message_count bigint,
    all_msg_status text[],
    ignorables text[]
);

create index if not exists validation_job_results_idx on validation_job_results (
    job_id
);