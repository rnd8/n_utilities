/*
    Copyright 2022 Christopher McGowan

    This file is part of N_utilities.

    N_utilities is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    N_utilities is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with N_utilities.  If not, see <https://www.gnu.org/licenses/>.
*/
use `n_util_i`;

DELIMITER $$
DROP FUNCTION IF EXISTS `n_util_i`.`crawler_job_latest_iteration`$$
CREATE DEFINER=`n_util_build`@`%` FUNCTION `n_util_i`.`crawler_job_latest_iteration`(
    `p_job_id` INT UNSIGNED
) RETURNS INT UNSIGNED
    READS SQL DATA
    DETERMINISTIC
    SQL SECURITY INVOKER
    COMMENT 'Errors early when a string would cause a later error or unexpected behavior when used in dynamic SQL as an alias'
BEGIN 
	RETURN (
		SELECT iteration_num
		FROM n_util.crawler_job_iteration FORCE INDEX (PRIMARY)
		WHERE job_id = p_job_id
		ORDER BY job_id DESC, iteration_num DESC
		LIMIT 1
    );
END$$
DELIMITER ;

DELIMITER $$
DROP FUNCTION IF EXISTS `n_util_i`.`crawler_job_estimation_iteration`$$
CREATE DEFINER=`n_util_build`@`%` FUNCTION `n_util_i`.`crawler_job_estimation_iteration`(
    `p_job_id` INT UNSIGNED,
    `p_process_id` BIGINT UNSIGNED
) RETURNS INT UNSIGNED
    READS SQL DATA
    DETERMINISTIC
    SQL SECURITY INVOKER
    COMMENT 'Errors early when a string would cause a later error or unexpected behavior when used in dynamic SQL as an alias'
BEGIN 
	RETURN (
		SELECT iteration_num
		FROM n_util.crawler_job_iteration
		WHERE TRUE
			AND process_id = p_process_id
            AND job_id = p_job_id
		ORDER BY process_id ASC, job_id ASC, iteration_num ASC
		LIMIT 1
    );
END$$
DELIMITER ;

CREATE OR REPLACE
	ALGORITHM=MERGE 
    DEFINER=`n_util_build` 
    SQL SECURITY DEFINER 
VIEW `n_util_i`.`crawler_job_and_resume` AS 
SELECT
	J.*,
    `n_util_i`.`crawler_job_latest_iteration`(J.job_id) AS iteration_num
FROM n_util_i.crawler_job AS J
;
#--TEST:
#--EXPLAIN SELECT * FROM `n_util_i`.`crawler_job_and_resume`;

    
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
	FROM `n_util_i`.`crawler_job_and_resume` AS J
	JOIN n_util.crawler_job_iteration AS I
		ON I.job_id = J.job_id
		AND I.iteration_num = J.iteration_num
	LEFT JOIN n_util.crawler_job_iteration AS E #--Estimator tuple
		ON E.job_id = I.job_id
        AND E.iteration_num = `n_util_i`.`crawler_job_estimation_iteration`(I.job_id, I.process_id)
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
#--EXPLAIN SELECT * FROM `n_util`.`crawler_job_monitor` WHERE job_name LIKE 'multithreaded test%';