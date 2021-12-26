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
DROP PROCEDURE IF EXISTS `n_util_i`.`crawler_job__PREP_tmptbls`$$
CREATE DEFINER=`n_util_build` PROCEDURE `n_util_i`.`crawler_job__PREP_tmptbls`(
    IN q_ordinal_table VARCHAR(133),
    IN q_ordinal_columns JSON
)
    MODIFIES SQL DATA
    SQL SECURITY DEFINER
BEGIN
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
        BEGIN
            RESIGNAL;
        END;
    
    #--Clear our reserved table names:
    DROP TEMPORARY TABLE IF EXISTS `n_util`.`crawler_work_tuple`; #--Work tuples taken from the source table
    DROP TEMPORARY TABLE IF EXISTS `n_util`.`crawler_resume_point`; #--The previous ending tuple
    #--DROP TEMPORARY TABLE IF EXISTS `n_util_i`.`crawler_chunk_start`;
    #--DROP TEMPORARY TABLE IF EXISTS `n_util_i`.`crawler_chunk_end`;
    DROP TEMPORARY TABLE IF EXISTS `n_util_i`.`crawler_boundary_end`;
    
    #--Work tuples DSQL
		SET @n_util__crawler_DSQL = CONCAT('
			CREATE TEMPORARY TABLE `n_util`.`crawler_work_tuple` AS 
			(SELECT ',`n_util_i`.`DSQL_select`(q_ordinal_columns,NULL),' FROM ',q_ordinal_table,' LIMIT 0)
			;
		');
		PREPARE create_work_tuples_tmp_tbl FROM @n_util__crawler_DSQL;
		SET @n_util__crawler_DSQL = CONCAT('
			ALTER TABLE `n_util`.`crawler_work_tuple`
				ADD PRIMARY KEY (',`n_util_i`.`DSQL_rowconstructor`(q_ordinal_columns),')'
		);
		PREPARE PK_work_tuples_tmp_tbl FROM @n_util__crawler_DSQL;
	#--Iterations DSQL
		SET @n_util__crawler_DSQL = CONCAT('
			ALTER TABLE `n_util`.`crawler_work_tuple`
				ADD COLUMN `n_util__iteration_num` INT UNSIGNED NOT NULL FIRST,
                ADD PRIMARY KEY (`n_util__iteration_num`),
				ADD KEY chunk_boundary (',`n_util_i`.`DSQL_rowconstructor`(q_ordinal_columns),')'
		);
		PREPARE rekey_iteration_tmp_tbl FROM @n_util__crawler_DSQL;
        
	#--Make tuple temp tables
		EXECUTE create_work_tuples_tmp_tbl;
        EXECUTE PK_work_tuples_tmp_tbl;
        ALTER TABLE `n_util`.`crawler_work_tuple` RENAME TO `n_util_i`.`crawler_boundary_end`;
		EXECUTE create_work_tuples_tmp_tbl;
        EXECUTE PK_work_tuples_tmp_tbl;
        ALTER TABLE `n_util`.`crawler_work_tuple` RENAME TO `n_util`.`crawler_resume_point`;
/* #--We don't need these in the current version
		EXECUTE create_work_tuples_tmp_tbl;
        EXECUTE PK_work_tuples_tmp_tbl;#rekey_iteration_tmp_tbl;
        ALTER TABLE `n_util`.`crawler_work_tuple` RENAME TO `n_util_i`.`crawler_chunk_start`;
		EXECUTE create_work_tuples_tmp_tbl;
        EXECUTE PK_work_tuples_tmp_tbl;#rekey_iteration_tmp_tbl;
        ALTER TABLE `n_util`.`crawler_work_tuple` RENAME TO `n_util_i`.`crawler_chunk_end`;
*/
		EXECUTE create_work_tuples_tmp_tbl;
        EXECUTE PK_work_tuples_tmp_tbl;
        DEALLOCATE PREPARE create_work_tuples_tmp_tbl;
		DEALLOCATE PREPARE PK_work_tuples_tmp_tbl;
        DEALLOCATE PREPARE rekey_iteration_tmp_tbl;

END$$
DELIMITER ;
/*
#--TEST:
CALL `n_util_i`.`crawler_job__PREP_tmptbls`('n_util','tiny_sequence',JSON_ARRAY('v'));
SELECT * FROM `n_util`.`crawler_work_tuple`;
SELECT * FROM `n_util`.`crawler_resume_point`;
SELECT * FROM `n_util_i`.`crawler_chunk_start`;
SELECT * FROM `n_util_i`.`crawler_chunk_end`;
SELECT * FROM `n_util_i`.`crawler_boundary_end`;
*/

#--TODO! Move all DSQL to here!
DELIMITER $$
DROP PROCEDURE IF EXISTS `n_util_i`.`crawler_job__PREP_DSQL`$$
CREATE DEFINER=`n_util_build` PROCEDURE `n_util_i`.`crawler_job__PREP_DSQL`(
	IN `p_job_id` INT UNSIGNED
)
    MODIFIES SQL DATA
    SQL SECURITY DEFINER
BEGIN
	DECLARE p_chunk_min, p_chunk_max INT UNSIGNED;
    DECLARE v_resume_iteration_num INT UNSIGNED;
    DECLARE v_test_started_ts, v_test_fetched_ts TIMESTAMP(6);
    DECLARE p_throttle_secs DOUBLE UNSIGNED;
    DECLARE p_workset_schema, p_workset_table, p_sproc_schema, p_sproc_name VARCHAR(64);
    DECLARE p_is_forward_only, p_skip_gaps TINYINT UNSIGNED;
    DECLARE p_chunk_mode ENUM('limit', 'range');
    DECLARE p_ordering ENUM('asc', 'desc');
    DECLARE p_ordinal_columns, problemspace_low_tuple, problemspace_high_tuple, chunk_first_tuple, chunk_last_tuple JSON DEFAULT NULL;
    DECLARE ordinal_qualified_table VARCHAR(133);
    DECLARE ordinal_qualified_rowconstructor, iteration_first_rowconstructor, iteration_last_rowconstructor TEXT DEFAULT NULL;
	DECLARE q_ordinal_columns JSON DEFAULT NULL;
    DECLARE q_ordinal_table VARCHAR(133);
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
        BEGIN
            RESIGNAL;
        END;
    
    #--Load job settings
	SELECT `workset_schema`, `workset_table`, `ordinal_columns`, `n_util_i`.`qualify_alias`(`sproc_schema`), `n_util_i`.`qualify_alias`(`sproc_name`), `chunk_mode`, `chunk_min`, `chunk_max`, `throttle_secs`, `is_forward_only`, `skip_gaps`, `ordering`
	INTO p_workset_schema, p_workset_table, p_ordinal_columns, 		p_sproc_schema, 							p_sproc_name, 								p_chunk_mode, p_chunk_min, p_chunk_max, p_throttle_secs, p_is_forward_only, p_skip_gaps, p_ordering
	FROM `n_util_s`.`crawler_job`
	WHERE `job_id` = p_job_id
	;
    
	#--Qualify DSQL components
    SET q_ordinal_columns = `n_util_i`.`qualify_column_array`(p_workset_schema, p_workset_table, NULL, p_ordinal_columns);
    SET q_ordinal_table = `n_util_i`.`qualify_table`(p_workset_schema, p_workset_table);
    
    #--Debug:
    #--SELECT q_ordinal_columns, q_ordinal_table;
       
    #--Prepare temp tables relevant to the DSQL
    CALL `n_util_i`.`crawler_job__PREP_tmptbls`(q_ordinal_table, q_ordinal_columns);
    
    #--Ensure we are cleaned up from the last execution:
    CALL `n_util_i`.`crawler_job__DEALLOC_DSQL`;    
    
    #--crawler_exec_user_def_sproc
	SET @n_util__crawler_DSQL = CONCAT('CALL ', p_sproc_schema, '.', p_sproc_name); #--TODO: Make a SProc qualifier for this for security
	PREPARE n_util__crawler_exec_user_def_sproc FROM @n_util__crawler_DSQL;
    
    #--crawler_advance_resume_tuple
	SET @n_util__crawler_DSQL = CONCAT('
		INSERT INTO `n_util`.`crawler_resume_point`
		SELECT * 
		FROM `n_util`.`crawler_work_tuple`
		ORDER BY ',`n_util_i`.`DSQL_orderby`(`p_ordinal_columns`,'DESC',NULL),'
		LIMIT 1'
	);
	PREPARE n_util__crawler_advance_resume_tuple FROM @n_util__crawler_DSQL;
    
    #--crawler_fetch_start_Jtuple
	SET @n_util__crawler_DSQL = CONCAT('
		SELECT JSON_ARRAY(',`n_util_i`.`DSQL_rowconstructor`(`n_util_i`.`qualify_column_array`(`p_workset_schema`,`p_workset_table`,'S',`p_ordinal_columns`)),')
		INTO @start_tuple_json
		FROM `n_util`.`crawler_resume_point` AS S
	');
	PREPARE n_util__crawler_fetch_start_Jtuple FROM @n_util__crawler_DSQL;
    
	#--crawler_fetch_start_Jtuple
	SET @n_util__crawler_DSQL = CONCAT('
		SELECT JSON_ARRAY(',`n_util_i`.`DSQL_rowconstructor`(`n_util_i`.`qualify_column_array`(`p_workset_schema`,`p_workset_table`,'E',`p_ordinal_columns`)),')
		INTO @end_tuple_json
		FROM `n_util_i`.`crawler_boundary_end` AS E
	');
	PREPARE n_util__crawler_fetch_end_Jtuple FROM @n_util__crawler_DSQL;
    
    #--crawler_log_chunk
	SET @n_util__crawler_DSQL = CONCAT('
	INSERT INTO `n_util`.`crawler_job_iteration`
	SET 
		`job_id` = ',`p_job_id`,',
		`iteration_num` = ?,
		`resume_iteration_num` = ?,
		`chunk_last_tuple` = (
			SELECT JSON_ARRAY(',`n_util_i`.`DSQL_rowconstructor`(`p_ordinal_columns`),')
			FROM `n_util`.`crawler_resume_point` AS C
			LIMIT 1
		),
		`process_id` = ',connection_id(),',
		`started_ts` = ?,
		`fetched_ts` = ?,
		`logged_ts` = NOW(6)'
	);
    PREPARE n_util__crawler_log_chunk FROM @n_util__crawler_DSQL;
END$$
DELIMITER ;


DELIMITER $$
DROP PROCEDURE IF EXISTS `n_util_i`.`crawler_job__DEALLOC_DSQL`$$
CREATE DEFINER=`n_util_build` PROCEDURE `n_util_i`.`crawler_job__DEALLOC_DSQL`(
)
    MODIFIES SQL DATA
    SQL SECURITY DEFINER
    COMMENT 'Deallocate the prepared statements used within crawler'
BEGIN
	DECLARE CONTINUE HANDLER FOR 1243
		BEGIN
		END;
	DEALLOCATE PREPARE n_util__crawler_exec_user_def_sproc;
    DEALLOCATE PREPARE n_util__crawler_advance_resume_tuple;
    DEALLOCATE PREPARE n_util__crawler_fetch_start_Jtuple;
    DEALLOCATE PREPARE n_util__crawler_fetch_end_Jtuple;
    DEALLOCATE PREPARE n_util__crawler_log_chunk;
END$$
DELIMITER ;

/*
#--Test
#--Run the test script first, to prepare a job
CALL `n_util_i`.`crawler_job__PREP_DSQL`((SELECT job_id FROM `n_util_s`.`crawler_job` WHERE job_name = @crawler_test_title));
CALL `n_util_i`.`crawler_job__DEALLOC_DSQL`;
SELECT @n_util__crawler_DSQL;
*/