DROP TABLE IF EXISTS `n_util_i`.`async_task`;
CREATE TABLE `n_util_i`.`async_task` (
  `task_id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
  `process_id` BIGINT UNSIGNED NULL,
  `created_ts` TIMESTAMP(6) NOT NULL,
  `wait_until_ts` TIMESTAMP(6) NOT NULL,
  `deadline_ts` TIMESTAMP(6) NOT NULL,
  `started_ts` TIMESTAMP(6) NULL,
  `ended_ts` TIMESTAMP(6) NULL,
  `error_occurred` TINYINT UNSIGNED NULL,
  `crawler_job_id` INT UNSIGNED NULL,
  PRIMARY KEY (`task_id`),
  INDEX available_idx (`started_ts`, `wait_until_ts`, `deadline_ts`),
  INDEX still_running (`started_ts`, `ended_ts`),
  UNIQUE INDEX `crawler_job` (`crawler_job_id` ASC),
  CONSTRAINT `async_task2c_job`
    FOREIGN KEY (`crawler_job_id`)
    REFERENCES `n_util_i`.`crawler_job` (`job_id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE
)
COMMENT = 'A crawling job and the associated options'
;

CREATE OR REPLACE
	ALGORITHM=MERGE 
    DEFINER=`n_util_build` 
    SQL SECURITY INVOKER 
VIEW `n_util_i`.`async_task__AVAILABLE` AS 
	SELECT 
		`task_id`
	FROM `n_util_i`.`async_task`
	WHERE TRUE 
		AND `started_ts` IS NULL 
        AND `wait_until_ts` <= NOW(6)
        AND `deadline_ts` > NOW(6)
;
    
DELIMITER $$
DROP PROCEDURE IF EXISTS `n_util_i`.`async_task_enable_thread_launchers`$$
CREATE DEFINER=`n_util_build` PROCEDURE `n_util_i`.`async_task_enable_thread_launchers`()
    MODIFIES SQL DATA
    SQL SECURITY DEFINER
    COMMENT 'Enables all async thread launcher events'
BEGIN
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
        BEGIN
            RESIGNAL;
        END;
	
	ALTER DEFINER = `n_util_build` EVENT `n_util_i`.`async_task_thread_launcher0` ENABLE;
	ALTER DEFINER = `n_util_build` EVENT `n_util_i`.`async_task_thread_launcher1` ENABLE;
	ALTER DEFINER = `n_util_build` EVENT `n_util_i`.`async_task_thread_launcher2` ENABLE;
	ALTER DEFINER = `n_util_build` EVENT `n_util_i`.`async_task_thread_launcher3` ENABLE;
	ALTER DEFINER = `n_util_build` EVENT `n_util_i`.`async_task_thread_launcher4` ENABLE;
	ALTER DEFINER = `n_util_build` EVENT `n_util_i`.`async_task_thread_launcher5` ENABLE;
	ALTER DEFINER = `n_util_build` EVENT `n_util_i`.`async_task_thread_launcher6` ENABLE;
	ALTER DEFINER = `n_util_build` EVENT `n_util_i`.`async_task_thread_launcher7` ENABLE;
    
END$$
DELIMITER ;
#--TEST:
#--CALL `n_util_i`.`async_task_enable_thread_launchers`;

DELIMITER $$
DROP PROCEDURE IF EXISTS `n_util_i`.`async_task_disable_thread_launchers`$$
CREATE DEFINER=`n_util_build` PROCEDURE `n_util_i`.`async_task_disable_thread_launchers`()
    MODIFIES SQL DATA
    SQL SECURITY DEFINER
    COMMENT 'Disables all async thread launcher events'
BEGIN
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
        BEGIN
            RESIGNAL;
        END;
	
	ALTER DEFINER = `n_util_build` EVENT `n_util_i`.`async_task_thread_launcher0` DISABLE;
	ALTER DEFINER = `n_util_build` EVENT `n_util_i`.`async_task_thread_launcher1` DISABLE;
	ALTER DEFINER = `n_util_build` EVENT `n_util_i`.`async_task_thread_launcher2` DISABLE;
	ALTER DEFINER = `n_util_build` EVENT `n_util_i`.`async_task_thread_launcher3` DISABLE;
	ALTER DEFINER = `n_util_build` EVENT `n_util_i`.`async_task_thread_launcher4` DISABLE;
	ALTER DEFINER = `n_util_build` EVENT `n_util_i`.`async_task_thread_launcher5` DISABLE;
	ALTER DEFINER = `n_util_build` EVENT `n_util_i`.`async_task_thread_launcher6` DISABLE;
	ALTER DEFINER = `n_util_build` EVENT `n_util_i`.`async_task_thread_launcher7` DISABLE;
    
END$$
DELIMITER ;
#--TEST:
#--CALL `n_util_i`.`async_task_disable_thread_launchers`;


DELIMITER $$
DROP PROCEDURE IF EXISTS `n_util_i`.`async_task_watchdog`$$
CREATE DEFINER=`n_util_build` PROCEDURE `n_util_i`.`async_task_watchdog`()
    MODIFIES SQL DATA
    SQL SECURITY DEFINER
    COMMENT 'Responsible for noticing and re-enabling the async thread-mill event when unclaimed tasks exist'
BEGIN
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
        BEGIN
            RESIGNAL;
        END;
	
	IF(EXISTS (SELECT task_id FROM `n_util_i`.`async_task__AVAILABLE` LIMIT 1)) THEN
		CALL `n_util_i`.`async_task_enable_thread_launchers`;
	END IF;
    
END$$
DELIMITER ;
#--TEST:
#--CALL `n_util_i`.`async_task_watchdog`;

DELIMITER $$
DROP PROCEDURE IF EXISTS `n_util_i`.`async_task_thread_launcher`$$
CREATE DEFINER=`n_util_build` PROCEDURE `n_util_i`.`async_task_thread_launcher`()
    MODIFIES SQL DATA
    SQL SECURITY DEFINER
    COMMENT 'Responsible for noticing and re-enabling the async thread-mill event when unclaimed tasks exist'
BEGIN
	DECLARE v_task_id, v_crawler_job_id INT UNSIGNED DEFAULT NULL;
    DECLARE v_deadline_ts TIMESTAMP(6);
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
        BEGIN
			UPDATE `n_util_i`.`async_task`
			SET 
				`error_occurred` = TRUE,
                `ended_ts` = IFNULL(`ended_ts`, NOW(6))
            WHERE `task_id` = v_task_id;
           RESIGNAL;
        END;
	
    #--Reduce odds of engaging locks to prevent double-execution
    CALL `n_util_i`.`async_task_disable_thread_launchers`;
    
    #--Determine and lock our target
    START TRANSACTION;
    SELECT task_id INTO v_task_id FROM `n_util_i`.`async_task__AVAILABLE` LIMIT 1 FOR UPDATE;
    
	IF(v_task_id IS NOT NULL) THEN
		UPDATE `n_util_i`.`async_task` 
        SET
			`process_id` = CONNECTION_ID(),
            `started_ts` = NOW(6)
		WHERE `task_id` = v_task_id
        ;
        COMMIT; #--Release locks
        
        #--Allow another thread to find a different task (since OUR task is now marked as unavailable)
        CALL `n_util_i`.`async_task_enable_thread_launchers`;
        
		#--Fetch task details
        SELECT `crawler_job_id`, `deadline_ts` INTO v_crawler_job_id, v_deadline_ts FROM `n_util_i`.`async_task` WHERE task_id = v_task_id;
        
        #--Execute task
        IF (v_crawler_job_id IS NOT NULL) THEN
			CALL `n_util`.`run_crawler_job`(
				(SELECT job_name FROM `n_util_s`.`crawler_job` WHERE job_id = v_crawler_job_id), #--Job name
				NULL, #--Status Frequency
				v_deadline_ts #--Deadline
			);
		END IF;
        
        #--Mark task as complete
		UPDATE `n_util_i`.`async_task` SET `ended_ts` = NOW(6) WHERE `task_id` = v_task_id;
        
	ELSE
		COMMIT; #--Release locks
	END IF;
    
END$$
DELIMITER ;
#--TEST:
#--CALL `n_util_i`.`async_task_thread_launcher`;