CREATE OR REPLACE
	ALGORITHM=MERGE 
    DEFINER=`n_util_build` 
    SQL SECURITY DEFINER 
VIEW `n_util_i`.`crawler_job_monitor` AS 
SELECT *, 
	(
		(
			JSON_EXTRACT(chunk_last_tuple, '$[0]') -
			JSON_EXTRACT(boundary_start_tuple, '$[0]')
		) / (
			JSON_EXTRACT(boundary_end_tuple, '$[0]') - 
			JSON_EXTRACT(boundary_start_tuple, '$[0]')
		) * 100
	) AS approx_pct_progress,
	(
		(prefix_remaining_size / est_sample_size) #--How many chunks probably left?
        * (est_sample_secs) #--How long does a chunk take these days?
	) AS approx_remaining_secs
FROM (
	SELECT
		J.job_name,
		#--(JSON_EXTRACT(J.boundary_end_tuple, '$[0]') - JSON_EXTRACT(J.boundary_start_tuple, '$[0]')) AS approx_prefix_boundary_range_size,
        #--(JSON_EXTRACT(I.chunk_last_tuple, '$[0]') - (SELECT JSON_EXTRACT(P.chunk_last_tuple, '$[0]') FROM n_util.crawler_job_iteration AS P WHERE P.job_id = J.job_id AND P.iteration_num = I.resume_iteration_num)) AS approx_prefix_chunk_range_size,
		J.chunk_min AS chunk_size,
		#--J.throttle_secs,
		J.boundary_start_tuple,
		J.boundary_end_tuple,
		I.chunk_last_tuple,
        #--I.iteration_num,
		I.process_id,
		UNIX_TIMESTAMP(I.fetched_ts) - UNIX_TIMESTAMP(I.started_ts) AS fetch_chunk_secs,
		UNIX_TIMESTAMP(I.logged_ts) - UNIX_TIMESTAMP(I.fetched_ts) AS exec_chunk_secs,
		UNIX_TIMESTAMP(NOW(6)) - UNIX_TIMESTAMP(I.logged_ts) AS lastdone_chunk_secs,
	#--Use the following for remaining time estimation
        UNIX_TIMESTAMP(I.logged_ts) - UNIX_TIMESTAMP(E.logged_ts) AS est_sample_secs,
        JSON_EXTRACT(I.chunk_last_tuple, '$[0]') - JSON_EXTRACT(E.chunk_last_tuple, '$[0]') AS est_sample_size,
		(JSON_EXTRACT(J.boundary_end_tuple, '$[0]') - JSON_EXTRACT(I.chunk_last_tuple, '$[0]')) AS prefix_remaining_size
	FROM n_util_s.crawler_job AS J
	JOIN n_util.crawler_job_iteration AS I
		ON I.job_id = J.job_id
		AND I.iteration_num = (
			SELECT top.iteration_num
			FROM n_util.crawler_job_iteration AS top
			WHERE top.job_id = I.job_id
			ORDER BY top.job_id DESC, top.iteration_num DESC
			LIMIT 1
		)
	LEFT JOIN n_util.crawler_job_iteration AS E #--Estimator tuple
		ON E.job_id = J.job_id
        AND E.iteration_num = (
			SELECT P.iteration_num
            FROM n_util.crawler_job_iteration AS P
            WHERE TRUE
				AND P.job_id = I.job_id
                AND P.iteration_num < I.iteration_num
				AND P.process_id = I.process_id
			ORDER BY P.job_id ASC, P.iteration_num ASC
            LIMIT 1
		)
) AS stats
;

CREATE OR REPLACE
	ALGORITHM=MERGE 
    DEFINER=`n_util_build` 
    SQL SECURITY DEFINER 
VIEW `n_util`.`crawler_job_monitor` AS 
	SELECT * FROM `n_util_i`.`crawler_job_monitor`
;

#--Test:
#--SELECT * FROM `n_util`.`crawler_job_monitor` WHERE job_name LIKE 'multithreaded test%';