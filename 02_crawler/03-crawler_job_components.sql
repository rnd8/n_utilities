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

DELIMITER $$
DROP PROCEDURE IF EXISTS `n_util_i`.`crawler_job__ADVANCE_RESUME_POINT`$$
CREATE DEFINER=`n_util_build` PROCEDURE `n_util_i`.`crawler_job__ADVANCE_RESUME_POINT`()
    MODIFIES SQL DATA
    SQL SECURITY DEFINER
BEGIN
	DELETE FROM `n_util`.`crawler_resume_point`;
	EXECUTE n_util__crawler_advance_resume_tuple;
END$$
DELIMITER ;


DELIMITER $$
DROP PROCEDURE IF EXISTS `n_util_i`.`crawler_job__RESUME_INITIALIZATION`$$
CREATE DEFINER=`n_util_build` PROCEDURE `n_util_i`.`crawler_job__RESUME_INITIALIZATION`(
	IN `p_job_id` INT UNSIGNED,
    OUT `p_resume_iteration_num` INT UNSIGNED,
    OUT `p_ordinal_columns` JSON
)
    MODIFIES SQL DATA
    SQL SECURITY DEFINER
BEGIN
	DECLARE v_resume_tuple, v_boundary_end_tuple JSON;
    DECLARE v_next_iteration_num INT UNSIGNED;
    DECLARE v_started_ts TIMESTAMP(6);
	
    #--Start timer
    SET v_started_ts = NOW(6);
    
	#--Fetch settings and determine where to resume from (last chunk resume point or start boundary...or neither if neither is given)
	SELECT 
		ordinal_columns,
        resume_tuple,
        `boundary_end_tuple`,
        `iteration_num` AS resume_iteration_num,
        (IFNULL(iteration_num,0) + 1) AS next_iteration_num
    INTO
		`p_ordinal_columns`,
        v_resume_tuple,
        v_boundary_end_tuple,
		`p_resume_iteration_num`,
        v_next_iteration_num
	FROM (
		SELECT 
			J.ordinal_columns,
            /* #--MySQL doesn't handle JSON arrays correctly with GREATEST, so we must use IF and the > comparison instead
			GREATEST( #--If one is null, substitute the other, if both are not null, use the furthest ahead
				COALESCE(J.`boundary_start_tuple`, I.`chunk_last_tuple`),
				COALESCE(I.`chunk_last_tuple`, J.`boundary_start_tuple`)
			) AS `resume_tuple`,
            */
            IF(
				COALESCE(J.`boundary_start_tuple`, I.`chunk_last_tuple`) > COALESCE(I.`chunk_last_tuple`, J.`boundary_start_tuple`),
                COALESCE(J.`boundary_start_tuple`, I.`chunk_last_tuple`),
                COALESCE(I.`chunk_last_tuple`, J.`boundary_start_tuple`)
            ) AS `resume_tuple`,
            J.`boundary_end_tuple`,
            #--I.`chunk_last_tuple`,
			I.`iteration_num`
		FROM `n_util_s`.`crawler_job` AS J
		LEFT JOIN `n_util`.`crawler_job_iteration` AS I
			ON I.job_id = J.job_id
		WHERE TRUE
			AND J.job_id = `p_job_id`
            #--KISS #--AND I_next.job_id IS NULL  #--Anti-join; we don't resume from iterations that were already resumed from...only the end of a chain
		ORDER BY I.job_id DESC, I.`iteration_num` DESC
		LIMIT 1
	) AS derv
    ;

	#--Update the boundary end tuple (in case it changed)
	DELETE FROM `n_util_i`.`crawler_boundary_end`;
    IF(v_boundary_end_tuple IS NOT NULL) THEN
		SET @n_util__crawler_DSQL = CONCAT('
			INSERT INTO `n_util_i`.`crawler_boundary_end`
			SELECT ',`n_util_i`.`DSQL_select`(v_boundary_end_tuple, `p_ordinal_columns`)
		);
		PREPARE n_util__crawler_set_boundary_end_tuple FROM @n_util__crawler_DSQL;
		EXECUTE n_util__crawler_set_boundary_end_tuple;
		DEALLOCATE PREPARE n_util__crawler_set_boundary_end_tuple;
	END IF;
    
	#--Set the resume tuple
	DELETE FROM `n_util`.`crawler_resume_point`;
    IF(v_resume_tuple IS NOT NULL) THEN
		SET @n_util__crawler_DSQL = CONCAT('
			INSERT INTO `n_util`.`crawler_resume_point`
			SELECT ',`n_util_i`.`DSQL_select`(v_resume_tuple, `p_ordinal_columns`)
		);
		PREPARE n_util__crawler_set_resume_tuple FROM @n_util__crawler_DSQL;
		EXECUTE n_util__crawler_set_resume_tuple;
		DEALLOCATE PREPARE n_util__crawler_set_resume_tuple;
    END IF;

    #--Handle the case where the job start boundary should serve as the resume point (initial run)
	IF(`p_resume_iteration_num` IS NULL AND v_resume_tuple IS NOT NULL) THEN
		CALL `n_util_i`.`crawler_job__LOG_CHUNK`(`p_job_id`, `p_resume_iteration_num`, v_started_ts, NOW(6));
	END IF;
END$$
DELIMITER ;

/*
#--TEST:
#--Run the test script first, to prepare a job
SET @resume_iteration_num = NULL, @ordinal_columns = NULL;
SET @ordinal_columns = JSON_ARRAY('ordinal1', 'ordinal2');
CALL `n_util_i`.`crawler_job__PREP_tmptbls`('n_util_s','n_test_app_crawler_work_item',@ordinal_columns);
SET @ordinal_columns = JSON_ARRAY('`ordinal1`', '`ordinal2`');
CALL `n_util_i`.`crawler_job__SET_RESUME_POINT`((SELECT job_id FROM `n_util_s`.`crawler_job` WHERE job_name = @crawler_test_title),@resume_iteration_num,@ordinal_columns);
SELECT @n_util__crawler_DSQL;
SELECT * FROM `n_util`.`crawler_resume_point`;
SELECT * FROM `n_util_i`.`crawler_boundary_end`;
*/

DELIMITER $$
DROP PROCEDURE IF EXISTS `n_util_i`.`crawler_job__LOAD_CHUNK_WORK`$$
CREATE DEFINER=`n_util_build` PROCEDURE `n_util_i`.`crawler_job__LOAD_CHUNK_WORK`(
	IN `p_chunk_size_limit` INT UNSIGNED,
    #--TODO: Make these unnecessary by making the DSQL up-front
	IN `p_workset_schema` VARCHAR(64), 
    IN `p_workset_table` VARCHAR(64),
    IN `p_ordinal_columns` JSON
)
    MODIFIES SQL DATA
    SQL SECURITY DEFINER
BEGIN
	SET @start_tuple_json = NULL, @end_tuple_json = NULL;
    
    IF(EXISTS (SELECT TRUE FROM `n_util`.`crawler_resume_point` LIMIT 1)) THEN
		EXECUTE n_util__crawler_fetch_start_Jtuple;
	END IF;
    
    IF(EXISTS (SELECT TRUE FROM `n_util_i`.`crawler_boundary_end` LIMIT 1)) THEN
		EXECUTE n_util__crawler_fetch_end_Jtuple;
	END IF;
    
	SET @n_util__crawler_DSQL = CONCAT('
		INSERT INTO `n_util`.`crawler_work_tuple`
		SELECT ',`n_util_i`.`DSQL_select`(`n_util_i`.`qualify_column_array`(`p_workset_schema`,`p_workset_table`,'W',`p_ordinal_columns`),NULL),'
		FROM  ',`n_util_i`.`qualify_table`(`p_workset_schema`,`p_workset_table`),' AS W
        WHERE TRUE',
			IF(@start_tuple_json IS NOT NULL, CONCAT('
				AND (',`n_util_i`.`DSQL_tuple_gt`(`n_util_i`.`qualify_column_array`(`p_workset_schema`,`p_workset_table`,'W',`p_ordinal_columns`),@start_tuple_json,FALSE),')'
                #--`n_util_i`.`DSQL_rowconstructor`(`n_util_i`.`qualify_column_array`(`p_workset_schema`,`p_workset_table`,'W',`p_ordinal_columns`)),') > (',`n_util_i`.`DSQL_rowconstructor`(@start_tuple_json),')'
            ),''),
            IF(@end_tuple_json IS NOT NULL, CONCAT('
				AND (',`n_util_i`.`DSQL_tuple_gt`(@end_tuple_json, `n_util_i`.`qualify_column_array`(`p_workset_schema`,`p_workset_table`,'W',`p_ordinal_columns`), TRUE),')'
                #--`n_util_i`.`DSQL_rowconstructor`(`n_util_i`.`qualify_column_array`(`p_workset_schema`,`p_workset_table`,'W',`p_ordinal_columns`)),') <= (',`n_util_i`.`DSQL_rowconstructor`(@end_tuple_json),')'
			),''), '
        GROUP BY ',`n_util_i`.`DSQL_rowconstructor`(`n_util_i`.`qualify_column_array`(`p_workset_schema`,`p_workset_table`,'W',`p_ordinal_columns`)),'
		ORDER BY ',`n_util_i`.`DSQL_orderby`(`n_util_i`.`qualify_column_array`(`p_workset_schema`,`p_workset_table`,'W',`p_ordinal_columns`),'ASC',NULL),'
		LIMIT ',p_chunk_size_limit
	);
    
	PREPARE n_util__crawler_fetch_work_tuples FROM @n_util__crawler_DSQL;
    TRUNCATE TABLE `n_util`.`crawler_work_tuple`;
	EXECUTE n_util__crawler_fetch_work_tuples;
	DEALLOCATE PREPARE n_util__crawler_fetch_work_tuples;     
    
	#--Debug:
		#--SELECT @n_util__crawler_DSQL, @start_tuple_json, @end_tuple_json;
		/*
			SET @n_util__crawler_DSQL = CONCAT('EXPLAIN ',@n_util__crawler_DSQL);
			PREPARE n_util__crawler_fetch_work_tuples FROM @n_util__crawler_DSQL;
			EXECUTE n_util__crawler_fetch_work_tuples;
			DEALLOCATE PREPARE n_util__crawler_fetch_work_tuples;
		*/
END$$
DELIMITER ;
/*
#--TEST:
#--Run the test script first, to prepare a job
SET @resume_iteration_num = NULL, @ordinal_columns = NULL;
SET @test_job_id = (SELECT job_id FROM `n_util_s`.`crawler_job` WHERE job_name = @crawler_test_title), @ordinal_columns = JSON_ARRAY('ordinal1', 'ordinal2');
CALL `n_util_i`.`crawler_job__PREP_tmptbls`('n_util_s','n_test_app_crawler_work_item',@ordinal_columns);
SET @ordinal_columns = JSON_ARRAY('`ordinal1`', '`ordinal2`');
CALL `n_util_i`.`crawler_job__SET_RESUME_POINT`((SELECT job_id FROM `n_util_s`.`crawler_job` WHERE job_name = @crawler_test_title),@resume_iteration_num,@ordinal_columns);
SET @test_started_ts = NOW(6), @resume_iteration_num = (SELECT MAX(iteration_num) FROM `n_util`.`crawler_job_iteration` WHERE job_id = @test_job_id);
CALL `n_util_i`.`crawler_job__LOAD_CHUNK_WORK`(1024,'n_util_s','n_test_app_crawler_work_item',@ordinal_columns);
SELECT @n_util__crawler_DSQL;
SELECT * FROM `n_util`.`crawler_resume_point`;
SELECT * FROM `n_util_i`.`crawler_boundary_end`;
SELECT * FROM `n_util`.`crawler_work_tuple`;
*/

#--Log iteration's chunk
DELIMITER $$
DROP PROCEDURE IF EXISTS `n_util_i`.`crawler_job__LOG_CHUNK`$$
CREATE DEFINER=`n_util_build` PROCEDURE `n_util_i`.`crawler_job__LOG_CHUNK`(
	IN `p_job_id` INT UNSIGNED,
    INOUT `p_resume_iteration_num` INT UNSIGNED,
    IN `p_started_ts` TIMESTAMP(6),
    IN `p_fetched_ts` TIMESTAMP(6)
)
    MODIFIES SQL DATA
    SQL SECURITY DEFINER
BEGIN
	DECLARE `v_next_iteration_num` INT UNSIGNED DEFAULT NULL;

	#--Fetch the next enumerating value
    SET `v_next_iteration_num` = IFNULL((
		SELECT (iteration_num + 1) FROM `n_util`.`crawler_job_iteration` WHERE `job_id` = `p_job_id` ORDER BY `job_id` DESC, `iteration_num` DESC LIMIT 1 FOR UPDATE
    ),1);
    
    #--Debug:
		#--SELECT * FROM `n_util`.`crawler_resume_point` AS C LIMIT 1;
                    
    SET #--DSQL parameters
		@n_util__crawler_next_iteration_num = `v_next_iteration_num`,
		@n_util__crawler_resume_iteration_num = `p_resume_iteration_num`,
        @n_util__crawler_started_ts = `p_started_ts`,
        @n_util__crawler_fetched_ts = `p_fetched_ts`
	;
        
	EXECUTE n_util__crawler_log_chunk USING @n_util__crawler_next_iteration_num, @n_util__crawler_resume_iteration_num, @n_util__crawler_started_ts, @n_util__crawler_fetched_ts;
    
    SET `p_resume_iteration_num` = `v_next_iteration_num`; #--Continue from this newly completed chunk

END$$
DELIMITER ;
/*
#--TEST:
#--Run the test script first, to prepare a job
SET @resume_iteration_num = NULL, @ordinal_columns = NULL;
SET @test_job_id = (SELECT job_id FROM `n_util_s`.`crawler_job` WHERE job_name = @crawler_test_title), @ordinal_columns = JSON_ARRAY('ordinal1', 'ordinal2');
CALL `n_util_i`.`crawler_job__PREP_tmptbls`('n_util_s','n_test_app_crawler_work_item',@ordinal_columns);
SET @ordinal_columns = JSON_ARRAY('`ordinal1`', '`ordinal2`');
CALL `n_util_i`.`crawler_job__SET_RESUME_POINT`(@test_job_id,@resume_iteration_num,@ordinal_columns);
SET @test_started_ts = NOW(6), @resume_iteration_num = (SELECT MAX(iteration_num) FROM `n_util`.`crawler_job_iteration` WHERE job_id = @test_job_id);
CALL `n_util_i`.`crawler_job__LOAD_CHUNK_WORK`(1024,'n_util_s','n_test_app_crawler_work_item',@ordinal_columns);
SET @test_fetched_ts = NOW(6);
CALL `n_util_i`.`crawler_job__LOG_CHUNK`(@test_job_id, @resume_iteration_num, @test_started_ts, @test_fetched_ts, @ordinal_columns);
SELECT @n_util__crawler_DSQL;
SELECT * FROM `n_util`.`crawler_resume_point`;
SELECT * FROM `n_util_i`.`crawler_boundary_end`;
SELECT * FROM `n_util`.`crawler_work_tuple`;
SELECT * FROM `n_util`.`crawler_job_iteration` WHERE job_id = @test_job_id;
*/

#--NOTE: The main loop should be a DO ... WHILE.... and while there exists a row in `n_util`.`crawler_work_tuple`













