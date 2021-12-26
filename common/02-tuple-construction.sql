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

#--Emit a rowconstructor from a JSON array
DELIMITER $$
DROP FUNCTION IF EXISTS `n_util_i`.`array_to_rowconstructor`$$
CREATE DEFINER=`n_util_build`@`%` FUNCTION `n_util_i`.`array_to_rowconstructor`(
    `p_array` JSON #--Array of up to 256 elements
) RETURNS TEXT#--VARCHAR(17153)
    READS SQL DATA
    DETERMINISTIC
    SQL SECURITY INVOKER
    COMMENT 'Returns the values (each as a string) within the given JSON array as row-constructor string'
BEGIN 
	DECLARE prior_max_GC INT;
    DECLARE row_constructor TEXT;
    SET prior_max_GC = @@group_concat_max_len;
	SET @@group_concat_max_len = 65535; #--Extend max length to accomodate output
    
    SELECT 
		GROUP_CONCAT(
			IF(JSON_TYPE(JSON_EXTRACT(`p_array`, CONCAT('$[', `s`.`v`, ']'))) = 'NULL', 'NULL', JSON_EXTRACT(`p_array`, CONCAT('$[', `s`.`v`, ']'))) #--When the value is NULL, convey NULL
		ORDER BY `s`.`v` ASC SEPARATOR ',')
    INTO row_constructor
	FROM `n_util`.`tiny_sequence` `s`
	WHERE `s`.`v` < JSON_LENGTH(`p_array`)
	;

    SET @@group_concat_max_len = prior_max_GC; #--Set it back to the original value 
	RETURN row_constructor;
END$$
DELIMITER ;
/*
#--Test:
SELECT 	`n_util_i`.`array_to_rowconstructor`(JSON_ARRAY(1,"hat","frank",NULL,2.0));
*/



#--Emit aliased values for a SELECT clause from K and V JSON arrays
DELIMITER $$
DROP FUNCTION IF EXISTS `n_util_i`.`array_to_columns`$$
CREATE DEFINER=`n_util_build`@`%` FUNCTION `n_util_i`.`array_to_columns`(
    `p_key_array` JSON, #--AS aliases for use in dynamic SQL
    `p_value_array` JSON #--Array of up to 256 elements
) RETURNS TEXT#--VARCHAR(17153)
    READS SQL DATA
    DETERMINISTIC
    SQL SECURITY INVOKER
    COMMENT 'Returns the values (each as a string) within the given JSON array as row-constructor string'
BEGIN 
	DECLARE prior_max_GC INT;
    DECLARE row_constructor TEXT;
    SET prior_max_GC = @@group_concat_max_len;
	SET @@group_concat_max_len = 65535; #--Extend max length to accomodate output
    
    SELECT 
		GROUP_CONCAT(
			CONCAT(
				IF(JSON_TYPE(JSON_EXTRACT(`p_value_array`, CONCAT('$[', `s`.`v`, ']'))) = 'NULL', 'NULL', JSON_EXTRACT(`p_value_array`, CONCAT('$[', `s`.`v`, ']'))), #--When the value is NULL, convey NULL
				IF(JSON_TYPE(JSON_EXTRACT(`p_key_array`, CONCAT('$[', `s`.`v`, ']'))) = 'NULL', '', CONCAT(' AS `', JSON_UNQUOTE(JSON_EXTRACT(`p_key_array`, CONCAT('$[', `s`.`v`, ']'))), '`')) #--When the alias is NULL, don't give that column an alias
			) ORDER BY `s`.`v` ASC SEPARATOR ', '
		)
    INTO row_constructor
	FROM `n_util`.`tiny_sequence` `s`
	WHERE `s`.`v` < JSON_LENGTH(`p_key_array`) #--Tuple length will be limited to the length of the shortest array provided (unless keys are optionally set to NULL so that they are not considered)
		AND `n_util_i`.`check_alias`(JSON_EXTRACT(`p_key_array`, CONCAT('$[', `s`.`v`, ']'))) #--Check the alias for issues with dynamic SQL
	;

    SET @@group_concat_max_len = prior_max_GC; #--Set it back to the original value 
	RETURN row_constructor;
END$$
DELIMITER ;
/*
#--Test:
SELECT 	`n_util_i`.`array_to_columns`(JSON_ARRAY('a','b','c','d','e'), JSON_ARRAY(1,"hat","frank",2.0,'7'));
SELECT 	`n_util_i`.`array_to_columns`(JSON_ARRAY('a','null',NULL,'d','e'), JSON_ARRAY(1,"hat","frank",2.0,'7')); #--With a NULL valued alias
SELECT 	`n_util_i`.`array_to_columns`(JSON_ARRAY('a','b','c','d','e'), JSON_ARRAY(1,"hat",NULL,'null','7')); #--With a NULL valued value
SELECT 	`n_util_i`.`array_to_columns`(JSON_ARRAY(NOW(),'1.0',2.0,1,'2'), JSON_ARRAY(1,"hat","frank",2.0,'7')); #--With strange aliases
SELECT 	`n_util_i`.`array_to_columns`(JSON_ARRAY('a','b','c','d'), JSON_ARRAY(1,"hat","frank",2.0,'7')); #--With less aliases than values
SELECT 	`n_util_i`.`array_to_columns`(JSON_ARRAY('a','b','c','d','e','f'), JSON_ARRAY(1,"hat","frank",2.0,'7')); #--With more aliases than values
SELECT 	`n_util_i`.`array_to_columns`(JSON_ARRAY('a','b','c`','d','e','f'), JSON_ARRAY(1,"hat","frank",2.0,'7')); #--Invalid alias
*/