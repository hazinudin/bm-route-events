select pg_drop_replication_slot('outbox_slot');
drop publication if exists outbox_publication;
drop table if exists validation_job_outbox;