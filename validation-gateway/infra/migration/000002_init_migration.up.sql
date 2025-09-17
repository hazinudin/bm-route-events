alter table validation_job_results
add column if not exists ignored_tags text[],
add column if not exists attempt_id smallint;

alter table validation_job_results_msg
add column if not exists attempt_id smallint;
