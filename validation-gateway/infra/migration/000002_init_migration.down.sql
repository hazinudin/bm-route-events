alter table validation_job_results
drop column if exists ignored_tags,
drop column if exists attempt_id;

alter table validation_job_results_msg
drop column if exists attempt_id;