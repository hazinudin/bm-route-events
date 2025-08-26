create table if not exists validation_jobs (
    job_id varchar(200) primary key,
    data_type varchar(50),
    submitted_at bigint,
    payload jsonb
);

create table if not exists validation_jobs_event_store (
    id serial primary key,
    job_id varchar(200) not null references validation_jobs(job_id) on delete cascade,
    event_name varchar(200) not null,
    occurred_at bigint,
    event jsonb not null
);

create table if not exists validation_job_error_trace (
    id serial primary key,
    job_id varchar(200) not null references validation_jobs(job_id) on delete cascade,
    trace bytea
);

create table if not exists validation_job_results (
    id serial primary key,
    job_id varchar(200) not null references validation_jobs(job_id) on delete cascade,
    msg varchar(400),
    msg_status varchar(50),
    msg_status_idx smallint,
    ignore_in varchar(50),
    content_id varchar(50)
);

-- create table if not exists users as select * from temp_users;
-- drop table if exists temp_users;