/*
    Copyright 2021 Christopher McGowan

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


/*
Create a test case:

In file 09-test-work...
1. DROP/CREATE a test "app" schema
2. Creates source/work view (DONE)
3. Create a target dumping table
4. Create work SProc that copies the temp table into target table
5. Register the job with a small bounded range in the MIDDLE of the work table (1 minute deadline)
6. Call the job
7. Select from the chunk log
8. Update the job to have a small bounded range after a small gap after original range (1 minute deadline)
9. Call the job
10. Select from the chunk log
11. Update the job to have no boundaries (1 minute deadline)
12. Call the job
13. Select from the chunk log
14. SELECT the anti-join of target from the work (should be empty)
*/

#--Set reuseable values:
SET 
	@crawler_test_title = 'test_medium_sequence',
    @boundary_start_tuple = '[10000,200]',
    @boundary_end_tuple = '[10100,100]'
;

#--Reset for rebuild
use `n_util_s`;
SET FOREIGN_KEY_CHECKS = 0;
/*
SET FOREIGN_KEY_CHECKS = 0;
TRUNCATE TABLE `n_util`.`crawler_job_iteration`;
TRUNCATE TABLE `n_util_s`.`crawler_job`;
TRUNCATE TABLE `n_test_app`.`crawler_target`;
SET FOREIGN_KEY_CHECKS = 1;
*/
DELETE I, J
FROM `n_util_s`.`crawler_job` AS J
LEFT JOIN `n_util`.`crawler_job_iteration` AS I
	USING (job_id)
WHERE `job_name` = @crawler_test_title;
SET FOREIGN_KEY_CHECKS = 1;
DROP SCHEMA IF EXISTS `n_test_app`;
CREATE SCHEMA `n_test_app` DEFAULT CHARACTER SET UTF8;

#--Create the source view sources work items
CREATE OR REPLACE
	ALGORITHM=MERGE 
    #--DEFINER=`n_test_app`
    SQL SECURITY INVOKER 
VIEW `n_test_app`.`crawler_work_item__SRC` AS 
#--VIEW `n_test_app`.`crawler_work_item` AS 
	SELECT 
		S.v AS ordinal1,
        T.v AS ordinal2
    FROM `n_util`.`small_sequence` AS S
    STRAIGHT_JOIN `n_util`.`tiny_sequence` AS T
    WHERE S.v < 16384 AND T.v < 64 #--Faster build
;

CREATE TABLE `n_test_app`.`crawler_work_item` SELECT * FROM `n_test_app`.`crawler_work_item__SRC`;#-- WHERE ordinal1 BETWEEN 0 AND 20000;
ALTER TABLE `n_test_app`.`crawler_work_item` ADD PRIMARY KEY (ordinal1, ordinal2);

#--Confirm work table
SELECT 'Confirming work table' AS `significance`;
SELECT * FROM `n_test_app`.`crawler_work_item` LIMIT 1024;

#--Create a privilege conveying view, so that n_util can access the work table owned by the application
CREATE OR REPLACE
	ALGORITHM=MERGE 
    #--DEFINER=`n_test_app`
    SQL SECURITY DEFINER #--Important!
VIEW `n_util_s`.`n_test_app_crawler_work_item` AS 
	SELECT * FROM `n_test_app`.`crawler_work_item`
;

#--Create the target table
CREATE TABLE `n_test_app`.`crawler_target` (
	`ordinal1` SMALLINT UNSIGNED,
    `ordinal2` TINYINT UNSIGNED,
    `product` MEDIUMINT UNSIGNED,
	PRIMARY KEY (`ordinal1`, `ordinal2`)
) ENGINE = INNODB;

SELECT 'Confirming target table' AS `significance`;
SELECT * FROM `n_test_app`.`crawler_target` LIMIT 1024;

#--Create app's chunk handling SProc
DELIMITER $$
DROP PROCEDURE IF EXISTS `n_test_app`.`chunk_handler`$$
CREATE DEFINER=`n_util_build` PROCEDURE `n_test_app`.`chunk_handler`()
    MODIFIES SQL DATA
    SQL SECURITY INVOKER
BEGIN
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
        BEGIN
            RESIGNAL;
        END;

	INSERT INTO `n_test_app`.`crawler_target`
    SELECT
		`ordinal1`,
		`ordinal2`,
		(`ordinal1` * `ordinal2`) AS `product`
	FROM `n_util`.`crawler_work_tuple`
	;
END$$
DELIMITER ;

#--Create an accessible alias
DELIMITER $$
DROP PROCEDURE IF EXISTS `n_util_s`.`chunk_handler`$$
CREATE /*DEFINER=`n_util_build`*/ PROCEDURE `n_util_s`.`chunk_handler`()
    MODIFIES SQL DATA
    SQL SECURITY DEFINER
BEGIN
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
        BEGIN
            RESIGNAL;
        END;

	CALL `n_test_app`.`chunk_handler`;
END$$
DELIMITER ;

#--Register the job
INSERT INTO `n_util_s`.`crawler_job`
SET
  #--`job_id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
  `job_name` = @crawler_test_title,
  `workset_schema` = 'n_util_s',
  `workset_table` = 'n_test_app_crawler_work_item',
  `sproc_schema` = 'n_util_s',
  `sproc_name` = 'chunk_handler',
  `ordinal_columns` = JSON_ARRAY('ordinal1', 'ordinal2'),
  `chunk_mode` = 'limit',
  `chunk_min` = 300,
  #--`chunk_max` = 8192,
  `throttle_secs` = 0.000001, #--1 microsecond...just enough to say we have a throttle... we are about to process millions and don't want to be adding even a minute to the total time
  `is_forward_only` = TRUE,
  #--`skip_gaps` TINYINT UNSIGNED NOT NULL DEFAULT 1,
  #--`ordering` ENUM('asc', 'desc') NULL COMMENT 'NULL is arbitrary (engine\'s choice)',
  `boundary_start_tuple` = NULL,#--@boundary_start_tuple,
  `boundary_end_tuple` = NULL#@boundary_end_tuple
;
SET @test_job_id = last_insert_id();

SELECT 'Confirming job registered' AS `significance`;
SELECT * FROM `n_util_s`.`crawler_job` WHERE `job_id` = @test_job_id;

#--Run the job 
CALL `n_util`.`run_crawler_job`(
	@crawler_test_title, #--Job name
    NULL, #--Status Frequency
    NOW() + INTERVAL 1000 SECOND #--Deadline
);

#--Check side-effects
	#--Target:
		#--Should take less than 1 minute or at least exit within 1 minute
		#--Should have copied in values between the boundaries set, all of them if it finishes within 1 minute
		SELECT 'Check side-effect on target table' AS `significance`;
		SELECT * FROM `n_test_app`.`crawler_target`;
	#--Chunk Log:
		#--Chunk boundaries
		#--Should have copied in values between the boundaries set, all of them if it finishes within 1 minute
		SELECT 'Check side-effect on job chunk table' AS `significance`;
        SELECT * FROM `n_util`.`crawler_job_iteration` WHERE job_id = @test_job_id;
        SELECT 'Specific checks' AS `significance`;
		SELECT 
            (
				(
					I.`iteration_num` + 
					(I.`chunk_last_tuple` != pI.`chunk_last_tuple` OR pI.`chunk_last_tuple` IS NULL)  #--The starting point of a run references itself, and won't begin where a previous iteration began (probably no previous iteration)
                ) = I.`resume_iteration_num`
			) AS is_consecutively_enumerated,
            (NOT EXISTS (
				SELECT TRUE
                FROM `n_util`.`crawler_job_iteration` AS bad
				WHERE TRUE
					AND bad.`job_id` = pI.`job_id`
                    AND bad.`iteration_num` NOT IN (pI.iteration_num, I.iteration_num) 
                    AND bad.`chunk_last_tuple` BETWEEN pI.`chunk_last_tuple` AND I.`chunk_last_tuple`
            )) AS is_consecutively_executed,
			J.* 
        FROM `n_util_s`.`crawler_job` AS J
        JOIN `n_util`.`crawler_job_iteration` AS I
			ON I.`job_id` = J.`job_id`
        LEFT JOIN `n_util`.`crawler_job_iteration` AS pI #--The previous iteration... in terms of enumeration
			ON pI.`job_id` = I.`job_id` 
            AND pI.`iteration_num` = (I.`iteration_num` - 1)		
        WHERE J.`job_id` = @test_job_id
        ORDER BY I.`job_id`, I.`iteration_num`;
	#--Missed entries:
		#--Should be empty, or entries not yet gotten to when testing the deadline
			SELECT 'Check missed work entries' AS `significance`;
			SELECT * 
            FROM `n_util_s`.`n_test_app_crawler_work_item` AS W
			LEFT JOIN `n_test_app`.`crawler_target` AS T
				USING (ordinal1, ordinal2)
			WHERE T.ordinal1 IS NULL
            ;