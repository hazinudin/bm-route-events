create table if not exists validation_job_outbox (
    id serial primary key,
    job_id varchar(200) not null,
    event_name varchar(200) not null,
    payload jsonb
);

create publication outbox_publication FOR TABLE validation_job_outbox;