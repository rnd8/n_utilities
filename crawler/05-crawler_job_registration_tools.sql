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
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
        BEGIN
            RESIGNAL;
        END;
	
    SET @n_util__crawler_DSQL = CONCAT("
		CREATE DEFINER = `n_util` EVENT `n_util`.`",`n_util_i`.`check_alias`(REPLACE(REPLACE(CONCAT('N_Util Crawler Job: ',p_job_name),' ','_'),'`','')),"`
			ON SCHEDULE AT CURRENT_TIMESTAMP
			ON COMPLETION NOT PRESERVE
			ENABLE
			COMMENT 'Created by N_Util to asynchronously execute the job in the name'
			DO
				CALL `n_util`.`run_crawler_job`(
					'",p_job_name,"', #--Job name
					NULL, #--Status Frequency
					'",p_deadline,"' #--Deadline
				)
			;
	");
	PREPARE n_util__create_async_crawler_event FROM @n_util__crawler_DSQL;
	EXECUTE n_util__create_async_crawler_event;
	DEALLOCATE PREPARE n_util__create_async_crawler_event;
    
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
	JOIN `n_util_s`.`crawler_job` AS J
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
	JOIN `n_util_s`.`crawler_job` AS J
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
            RESIGNAL;
        END;
	START TRANSACTION;
		CALL `n_util`.`reset_crawler_threads`(p_job_name);
		DELETE J
		FROM `n_util_s`.`crawler_job` AS J
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
    
    INSERT INTO `n_util_s`.`crawler_job` (
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
    11600
);
SELECT * FROM `n_util_s`.`crawler_job` WHERE job_name LIKE 'multithreaded test --- thread #%';
CALL `n_util`.`async_run_crawler_job`('multithreaded test --- thread #0',NOW(6) + INTERVAL 5 SECOND);
SELECT @n_util__crawler_DSQL;
SHOW EVENTS IN `n_util`;
SHOW PROCESSLIST;
DO SLEEP(6);
CALL `n_util`.`delete_crawler_thread_jobs`('multithreaded test');
SELECT * FROM `n_util_s`.`crawler_job` WHERE job_name LIKE 'multithreaded test --- thread #%';
*/