drop index if exists job_idx;
drop index if exists job_event_store_idx;
drop index if exists job_result_idx;

drop table if exists validation_job_error_trace;
drop table if exists validation_jobs_event_store;
drop table if exists validation_job_results;
drop table if exists validation_job_results_msg;

drop table if exists validation_jobs;