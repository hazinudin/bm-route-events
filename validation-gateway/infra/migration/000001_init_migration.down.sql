drop table if exists validation_job_error_trace;
drop table if exists validation_jobs_event_store;
drop table if exists validation_job_results;
drop table if exists validation_jobs;

drop index if exists job_idx;
drop index if exists job_event_store_idx;
drop index if exists job_result_idx;
-- For recovering the Users
-- drop table if exists temp_users;

-- DO $$
-- BEGIN
--     IF EXISTS (
--         SELECT 1
--         FROM information_schema.tables
--         WHERE table_schema = 'public'
--           AND table_name = 'users'
--     ) THEN
--         EXECUTE 'CREATE TABLE temp_users AS select * from users';
--     END IF;
-- END$$;

-- drop table if exists users;