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
DROP PROCEDURE IF EXISTS `n_util`.`run_crawler_job`$$
CREATE DEFINER=`n_util_build` PROCEDURE `n_util`.`run_crawler_job`(
	IN `p_job_name` VARCHAR(64), 
    IN `p_status_frequency` DOUBLE,
    IN `p_deadline_ts` TIMESTAMP(6)
/*    , 
    IN `p_boundary_low` JSON, #--Go no lower than this tuple (range is inclusive)
    IN `p_boundary_high` JSON  #--Go no higher than this tuple (range is inclusive)
*/
)
    MODIFIES SQL DATA
    SQL SECURITY DEFINER
BEGIN
	DECLARE p_job_id, p_chunk_min, p_chunk_max INT UNSIGNED;
    DECLARE v_resume_iteration_num INT UNSIGNED;
    DECLARE v_test_started_ts, v_test_fetched_ts TIMESTAMP(6);
    DECLARE p_throttle_secs DOUBLE UNSIGNED;
    DECLARE p_workset_schema, p_workset_table VARCHAR(64);
    DECLARE q_workset_table VARCHAR(133);
    DECLARE p_is_forward_only, p_skip_gaps TINYINT UNSIGNED;
    DECLARE p_chunk_mode ENUM('limit', 'range');
    DECLARE p_ordering ENUM('asc', 'desc');
    DECLARE p_ordinal_columns, problemspace_low_tuple, problemspace_high_tuple, chunk_first_tuple, chunk_last_tuple JSON DEFAULT NULL;
    DECLARE ordinal_qualified_table VARCHAR(133);
    DECLARE ordinal_qualified_rowconstructor, iteration_first_rowconstructor, iteration_last_rowconstructor TEXT DEFAULT NULL;
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
        BEGIN
			SELECT 'uhoh!'; #--Debug
			CALL `n_util_i`.`crawler_job__DEALLOC_DSQL`();
            RESIGNAL;
        END;
	
    #--Load job settings
	SELECT `job_id`, `workset_schema`, `workset_table`, `ordinal_columns`, `chunk_mode`, `chunk_min`, `chunk_max`, `throttle_secs`, `is_forward_only`, `skip_gaps`, `ordering`
	INTO p_job_id, p_workset_schema, p_workset_table, p_ordinal_columns, p_chunk_mode, p_chunk_min, p_chunk_max, p_throttle_secs, p_is_forward_only, p_skip_gaps, p_ordering
	FROM `n_util_s`.`crawler_job`
	WHERE `job_name` = p_job_name
	;
    
    SET q_workset_table = `n_util_i`.`qualify_table`(p_workset_schema, p_workset_table);
    
	#--Prepare DSQL queries
		CALL `n_util_i`.`crawler_job__PREP_tmptbls`(q_workset_table, p_ordinal_columns);
        CALL `n_util_i`.`crawler_job__DEALLOC_DSQL`();
        CALL `n_util_i`.`crawler_job__PREP_DSQL`(p_job_id);
 
	#--Initialize our state
		CALL `n_util_i`.`crawler_job__RESUME_INITIALIZATION`(p_job_id, v_resume_iteration_num, p_ordinal_columns);
       
	#--Begin iterating over the work table (a "chunk" per iteration)
    crawler_main_loop: LOOP
		SET v_test_started_ts = NOW(6);
        
        #--Exit if we are past the deadline
		IF (v_test_started_ts > p_deadline_ts) THEN
			LEAVE crawler_main_loop; 
        END IF;
		
		CALL `n_util_i`.`crawler_job__LOAD_CHUNK_WORK`(p_chunk_min, p_workset_schema, p_workset_table, p_ordinal_columns);
        CALL `n_util_i`.`crawler_job__ADVANCE_RESUME_POINT`;
        
        #--Exit if no work is found
        IF (NOT EXISTS (SELECT TRUE FROM `n_util`.`crawler_resume_point` LIMIT 1)) THEN
			LEAVE crawler_main_loop; 
        END IF;
        
        SET v_test_fetched_ts = NOW(6);
        
        #--Execute the user-defined procedure
		EXECUTE n_util__crawler_exec_user_def_sproc;
        
        #--Log the completed chunk, so that we can provide status and resume from it later
        CALL `n_util_i`.`crawler_job__LOG_CHUNK`(p_job_id, v_resume_iteration_num, v_test_started_ts, v_test_fetched_ts);
		
		DO SLEEP(p_throttle_secs);
    END LOOP crawler_main_loop;
    
    CALL `n_util_i`.`crawler_job__DEALLOC_DSQL`();

END$$
DELIMITER ;