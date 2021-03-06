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

DELIMITER $$
DROP PROCEDURE IF EXISTS `n_util`.`async_run_crawler_job`$$
CREATE DEFINER=`n_util_build` PROCEDURE `n_util`.`async_run_crawler_job`(
	IN p_job_name VARCHAR(64),
    IN p_deadline TIMESTAMP(6)
)
    MODIFIES SQL DATA
    SQL SECURITY DEFINER
    COMMENT 'Schedules the execution of a job with the MySQL event scheduler.'
BEGIN
	DECLARE v_job_id INT UNSIGNED;
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
        BEGIN
			#--TODO: Add a specific error message for duplicate entry (a task for this job is already running)
			ROLLBACK;
            RESIGNAL;
        END;
        
	SELECT `job_id`
    INTO v_job_id
    FROM `n_util_i`.`crawler_job`
    WHERE `job_name` = p_job_name
    ;
	
    #--Only clear (for replacement) prior tasks that haven't yet been started
	START TRANSACTION;
		DELETE FROM `n_util_i`.`async_task`
        WHERE `crawler_job_id` = v_job_id
			AND ( #--Not reportedly running now
				`started_ts` IS NULL OR 
                `ended_ts` IS NOT NULL
			)
		;
		INSERT INTO `n_util_i`.`async_task` SET
			`created_ts` = NOW(6),
			`wait_until_ts` = NOW(6),
			`deadline_ts` = p_deadline,
			`crawler_job_id` = v_job_id
		;
	COMMIT;
    
END$$
DELIMITER ;

DELIMITER $$
DROP PROCEDURE IF EXISTS `n_util`.`async_run_multithreaded_crawler_job`$$
CREATE DEFINER=`n_util_build` PROCEDURE `n_util`.`async_run_multithreaded_crawler_job`(
	IN p_job_name VARCHAR(48),
    IN p_deadline TIMESTAMP(6)
)
    MODIFIES SQL DATA
    SQL SECURITY DEFINER
    COMMENT 'Schedules the execution of a job with the MySQL event scheduler.'
BEGIN
	DECLARE v_job_id INT UNSIGNED;
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
        BEGIN
			#--TODO: Add a specific error message for duplicate entry (a task for this job is already running)
			ROLLBACK;
            RESIGNAL;
        END;
	
    DROP TEMPORARY TABLE IF EXISTS `n_util_i`.`MTCJ_threads`;
    CREATE TEMPORARY TABLE `n_util_i`.`MTCJ_threads`(
		`crawler_job_id` INT UNSIGNED NOT NULL,
        PRIMARY KEY (`crawler_job_id`)
	) ENGINE = MEMORY;
    
    INSERT INTO `n_util_i`.`MTCJ_threads`
	SELECT `job_id`
    FROM `n_util_i`.`crawler_job`
    WHERE `job_name` LIKE CONCAT(p_job_name, ' --- thread #%')
    ;
    	
    #--Only clear (for replacement) prior tasks that haven't yet been started
	START TRANSACTION;
		DELETE T 
        FROM `n_util_i`.`async_task` AS T
        JOIN `n_util_i`.`MTCJ_threads` AS J
			USING (`crawler_job_id`)
        WHERE TRUE
			AND ( #--Not reportedly running now
				T.`started_ts` IS NULL OR 
                T.`ended_ts` IS NOT NULL
			)
		;
		INSERT INTO `n_util_i`.`async_task` (`created_ts`, `wait_until_ts`, `deadline_ts`, `crawler_job_id`)
        SELECT 
			NOW(6) AS `created_ts`,
			NOW(6) AS `wait_until_ts`,
			p_deadline AS `deadline_ts`,
			J.crawler_job_id AS `crawler_job_id`
		FROM `n_util_i`.`MTCJ_threads` AS J
		;
	COMMIT;
    
    DROP TEMPORARY TABLE IF EXISTS `n_util_i`.`MTCJ_threads`;
END$$
DELIMITER ;

DELIMITER $$
DROP PROCEDURE IF EXISTS `n_util`.`reset_crawler_job`$$
CREATE DEFINER=`n_util_build` PROCEDURE `n_util`.`reset_crawler_job`(
	IN p_job_name VARCHAR(64)
)
    MODIFIES SQL DATA
    SQL SECURITY DEFINER
    COMMENT 'Clears out the iteration log so that the job will start back from the beginning when next executed.'
BEGIN
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
        BEGIN
            RESIGNAL;
        END;

	SET FOREIGN_KEY_CHECKS = 0; #--They reference each other including a self-reference, and the chain is too long by normal means
	DELETE I
	FROM `n_util`.`crawler_job_iteration` AS I
	JOIN `n_util_i`.`crawler_job` AS J
		USING (job_id)
	WHERE `job_name` = p_job_name;
	SET FOREIGN_KEY_CHECKS = 1;
END$$
DELIMITER ;

DELIMITER $$
DROP PROCEDURE IF EXISTS `n_util`.`reset_crawler_threads`$$
CREATE DEFINER=`n_util_build` PROCEDURE `n_util`.`reset_crawler_threads`(
	IN p_job_name VARCHAR(48)
)
    MODIFIES SQL DATA
    SQL SECURITY DEFINER
    COMMENT 'Clears out the iteration log so that the job threads will start back from the beginning when next executed.'
BEGIN
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
        BEGIN
            RESIGNAL;
        END;

	SET FOREIGN_KEY_CHECKS = 0; #--They reference each other including a self-reference, and the chain is too long by normal means
	DELETE I
	FROM `n_util`.`crawler_job_iteration` AS I
	JOIN `n_util_i`.`crawler_job` AS J
		USING (job_id)
	WHERE `job_name` LIKE CONCAT(p_job_name, ' --- thread #%');
	SET FOREIGN_KEY_CHECKS = 1;
END$$
DELIMITER ;

DELIMITER $$
DROP PROCEDURE IF EXISTS `n_util`.`delete_crawler_thread_jobs`$$
CREATE DEFINER=`n_util_build` PROCEDURE `n_util`.`delete_crawler_thread_jobs`(
	IN p_job_name VARCHAR(48)
)
    MODIFIES SQL DATA
    SQL SECURITY DEFINER
    COMMENT 'Completely removes job threads from registration and iteration log tables.'
BEGIN
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
        BEGIN
			ROLLBACK;
            RESIGNAL;
        END;
	START TRANSACTION;
		CALL `n_util`.`reset_crawler_threads`(p_job_name);
		DELETE J
		FROM `n_util_i`.`crawler_job` AS J
		WHERE `job_name` LIKE CONCAT(p_job_name, ' --- thread #%');
	COMMIT;
END$$
DELIMITER ;

DELIMITER $$
DROP PROCEDURE IF EXISTS `n_util`.`register_multithreaded_crawler`$$
CREATE DEFINER=`n_util_build` PROCEDURE `n_util`.`register_multithreaded_crawler`(
	IN p_title VARCHAR(48),
    IN p_chunk_min SMALLINT UNSIGNED,
    IN p_chunk_max SMALLINT UNSIGNED,
    IN p_thread_count TINYINT UNSIGNED,
	IN p_throttle_secs DOUBLE UNSIGNED,
	IN p_workset_schema VARCHAR(64),
	IN p_workset_table VARCHAR(64),
	IN p_sproc_schema VARCHAR(64),
	IN p_sproc_name VARCHAR(64),
	IN p_ordinal_columns JSON,
    IN p_min_col_vals JSON,
    IN p_first_col_boundary_low BIGINT,
    IN p_first_col_boundary_high BIGINT
)
    MODIFIES SQL DATA
    SQL SECURITY DEFINER
    COMMENT 'Registers N jobs, one for each thread desired and corresponding to an even portion of the range specified on first work tuple column (assumed even distribution) for crawler tasks with purely integer (bounded by the range of signed bigints) columns comprising the work tuples'
BEGIN
	DECLARE v_thread_num TINYINT UNSIGNED;
    DECLARE v_boundary_start_tuple, v_boundary_end_tuple, v_range_val_low, v_range_val_high, v_range_size BIGINT SIGNED;
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
        BEGIN
            RESIGNAL;
        END;
        
	SET v_range_size = CEIL((p_first_col_boundary_high - p_first_col_boundary_low) / p_thread_count);
    
    INSERT INTO `n_util_i`.`crawler_job` (
		`job_name`,
		`workset_schema`,
		`workset_table`,
		`sproc_schema`,
		`sproc_name`,
		`ordinal_columns`,
		`chunk_mode`,
		`chunk_min`,
		`chunk_max`,
		`throttle_secs`,
		`is_forward_only`,
		#--`skip_gaps`,
		#--`ordering`,
		`boundary_start_tuple`,
		`boundary_end_tuple`       
    )
    SELECT 
		CONCAT(p_title,' --- thread #',thread_num) AS job_name,
		p_workset_schema AS `workset_schema`,
		p_workset_table AS `workset_table`,
		p_sproc_schema AS `sproc_schema`,
		p_sproc_name AS `sproc_name`,
		p_ordinal_columns AS `ordinal_columns`,
		'limit' AS `chunk_mode`,
		p_chunk_min AS `chunk_min`,
		p_chunk_max AS `chunk_max`,
		p_throttle_secs AS `throttle_secs`,
		TRUE AS `is_forward_only`,
	  #--`skip_gaps` TINYINT UNSIGNED NOT NULL DEFAULT 1,
	  #--`ordering` ENUM('asc', 'desc') NULL COMMENT 'NULL is arbitrary (engine\'s choice)',
		JSON_REPLACE(p_min_col_vals, '$[0]', range_start) AS `boundary_start_tuple`,
		JSON_REPLACE(p_min_col_vals, '$[0]', (range_start + v_range_size)) AS `boundary_end_tuple`
	FROM (
		SELECT 
			T.v AS thread_num,
            (v_range_size * T.v + p_first_col_boundary_low) AS range_start
		FROM `n_util`.`tiny_sequence` AS T
		WHERE T.v < p_thread_count
	) AS R
	;
END$$
DELIMITER ;

#--Test (run 09_test_work.sql first)
/*
use `n_util_s`;
TRUNCATE TABLE `n_test_app`.`crawler_target`;
SET FOREIGN_KEY_CHECKS = 0;
DELETE I, J
FROM `n_util_s`.`crawler_job` AS J
LEFT JOIN `n_util`.`crawler_job_iteration` AS I
	USING (job_id)
WHERE `job_name` LIKE 'multithreaded test --- thread #%';
SET FOREIGN_KEY_CHECKS = 1;
CALL `n_util`.`register_multithreaded_crawler`(
	'multithreaded test',
    80,
    120,
    8,
	0.000001,
	'n_util_s',
	'n_test_app_crawler_work_item',
	'n_util_s',
	'chunk_handler',
	JSON_ARRAY('ordinal1', 'ordinal2'),
    JSON_ARRAY(0, 0),
    10000,
    16000
);
SELECT * FROM `n_util_s`.`crawler_job` WHERE job_name LIKE 'multithreaded test --- thread #%';
SELECT T.* FROM `n_util_i`.`async_task` AS T JOIN `n_util_s`.`crawler_job` AS J ON J.job_id = T.crawler_job_id WHERE job_name LIKE 'multithreaded test --- thread #%';
CALL `n_util`.`async_run_multithreaded_crawler_job`('multithreaded test',NOW(6) + INTERVAL 5 DAY);
SELECT T.* FROM `n_util_i`.`async_task` AS T JOIN `n_util_s`.`crawler_job` AS J ON J.job_id = T.crawler_job_id WHERE job_name LIKE 'multithreaded test --- thread #%';
SHOW EVENTS IN `n_util_i`;
SHOW PROCESSLIST;
DO SLEEP(6);
CALL `n_util`.`delete_crawler_thread_jobs`('multithreaded test');
SELECT * FROM `n_util_s`.`crawler_job` WHERE job_name LIKE 'multithreaded test --- thread #%';
SELECT T.* FROM `n_util_i`.`async_task` AS T JOIN `n_util_s`.`crawler_job` AS J ON J.job_id = T.crawler_job_id WHERE job_name LIKE 'multithreaded test --- thread #%';

		SELECT 'Check side-effect on target table' AS `significance`;
		SELECT * FROM `n_test_app`.`crawler_target`;
*/