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

#--Construct a SELECT clause
DELIMITER $$
DROP FUNCTION IF EXISTS `n_util_i`.`DSQL_select`$$
CREATE DEFINER=`n_util_build`@`%` FUNCTION `n_util_i`.`DSQL_select`( 
    `p_expressions` JSON, #--Array of up to 255 expressions
    `p_aliases` JSON #--Array of up to 255 corresponding aliases
) RETURNS TEXT#--VARCHAR(17153)
    READS SQL DATA
    DETERMINISTIC
    SQL SECURITY INVOKER
    COMMENT 'Given arrays of expressions and optional aliases, returns a column list for a clause'
BEGIN 
	DECLARE prior_max_GC INT;
    DECLARE dsql TEXT;
    SET prior_max_GC = @@group_concat_max_len;
	SET @@group_concat_max_len = 65535; #--Extend max length to accomodate output
        
    SELECT 
		CONCAT(/*'SELECT ', */GROUP_CONCAT(
			CONCAT(
				`expression`,
                IFNULL(CONCAT(' AS ',`n_util_i`.`qualify_alias`(`alias`)),'') #--Add the alias (don't add if NULL)
			) 
        ORDER BY C.ord ASC SEPARATOR ', '))
    INTO dsql
    FROM (
		SELECT 
			`s`.`v` AS ord, 
            JSON_UNQUOTE(JSON_EXTRACT(`p_expressions`, CONCAT('$[', `s`.`v`, ']'))) AS `expression`,
            JSON_UNQUOTE(JSON_EXTRACT(`p_aliases`, CONCAT('$[', `s`.`v`, ']'))) AS `alias`
		FROM `n_util`.`tiny_sequence` `s`
		WHERE `s`.`v` < JSON_LENGTH(`p_expressions`)
	) AS C
	;

    SET @@group_concat_max_len = prior_max_GC; #--Set it back to the original value 
	RETURN dsql;
END$$
DELIMITER ;

/*
#--Test:

	#--With alias
	SELECT 
		JSON_ARRAYAGG(COLUMN_NAME) AS column_names,
		`n_util_i`.`DSQL_select`(	
			`n_util_i`.`qualify_column_array`(TABLE_SCHEMA,TABLE_NAME,'T',JSON_ARRAYAGG(COLUMN_NAME)),
            JSON_ARRAYAGG(COLUMN_NAME)
		)
	FROM information_schema.columns
	GROUP BY TABLE_SCHEMA, TABLE_NAME
	LIMIT 10
	;
    
	#--Without alias
	SELECT 
		JSON_ARRAYAGG(COLUMN_NAME) AS column_names,
		`n_util_i`.`DSQL_select`(	
			`n_util_i`.`qualify_column_array`(TABLE_SCHEMA,TABLE_NAME,'T',JSON_ARRAYAGG(COLUMN_NAME)),
            NULL
		)
	FROM information_schema.columns
	GROUP BY TABLE_SCHEMA, TABLE_NAME
	LIMIT 10
	;
*/

#--Construct a row constructor
DELIMITER $$
DROP FUNCTION IF EXISTS `n_util_i`.`DSQL_rowconstructor`$$
CREATE DEFINER=`n_util_build`@`%` FUNCTION `n_util_i`.`DSQL_rowconstructor`( 
    `p_expressions` JSON #--Array of up to 255 expressions
) RETURNS TEXT#--VARCHAR(17153)
    READS SQL DATA
    DETERMINISTIC
    SQL SECURITY INVOKER
    COMMENT 'Given arrays of expressions and optional aliases, returns the expressions for a row constructor'
BEGIN 
	DECLARE prior_max_GC INT;
    DECLARE dsql TEXT;
    SET prior_max_GC = @@group_concat_max_len;
	SET @@group_concat_max_len = 65535; #--Extend max length to accomodate output
        
    SELECT 
		CONCAT(/*'(', */GROUP_CONCAT(
			`expression`
        ORDER BY C.ord ASC SEPARATOR ', ')/*,')'*/)
    INTO dsql
    FROM (
		SELECT 
			`s`.`v` AS ord, 
            JSON_UNQUOTE(JSON_EXTRACT(`p_expressions`, CONCAT('$[', `s`.`v`, ']'))) AS `expression`
		FROM `n_util`.`tiny_sequence` `s`
		WHERE `s`.`v` < JSON_LENGTH(`p_expressions`)
	) AS C
	;

    SET @@group_concat_max_len = prior_max_GC; #--Set it back to the original value 
	RETURN dsql;
END$$
DELIMITER ;

/*
#--Test:
	SELECT 
		JSON_ARRAYAGG(COLUMN_NAME) AS column_names,
		`n_util_i`.`DSQL_rowconstructor`(	
			`n_util_i`.`qualify_column_array`(TABLE_SCHEMA,TABLE_NAME,'T',JSON_ARRAYAGG(COLUMN_NAME))
		)
	FROM information_schema.columns
	GROUP BY TABLE_SCHEMA, TABLE_NAME
	LIMIT 10
	;
*/

#--Construct tuple > comparison
DELIMITER $$
DROP FUNCTION IF EXISTS `n_util_i`.`DSQL_tuple_gt`$$
CREATE DEFINER=`n_util_build`@`%` FUNCTION `n_util_i`.`DSQL_tuple_gt`( 
    `p_expressions1` JSON, #--Operand 1: Array of up to 255 expressions
	`p_expressions2` JSON, #--Operand 2: Array of up to 255 expressions
    `or_equal` TINYINT UNSIGNED #--If the comparison should also allow for the case that they are equal
) RETURNS TEXT#--VARCHAR(17153)
    READS SQL DATA
    DETERMINISTIC
    SQL SECURITY INVOKER
    COMMENT 'Given an array for each of two operand tuples (ordered list of expressions comprising the tuple), output the set of comparisons required to represent (tuple1) > (tuple2), or (tuple1) >= (tuple2) when or_equal is true'
BEGIN 
	DECLARE prior_max_GC INT UNSIGNED;
    DECLARE expression_cnt TINYINT UNSIGNED;
    DECLARE dsql TEXT;
    SET prior_max_GC = @@group_concat_max_len;
	SET @@group_concat_max_len = 16776960; #--Extend max length to accomodate output

	SET expression_cnt = LEAST(JSON_LENGTH(`p_expressions1`),JSON_LENGTH(`p_expressions2`));
    
    SELECT CONCAT('(', GROUP_CONCAT(`dsql_component` SEPARATOR ') OR \n('), ')')
    INTO dsql
	FROM (
		SELECT CONCAT(
			'(',
			GROUP_CONCAT(
				`expression1`,
                IF(C.ord = `o`.`v`, ' > ', ' = '),
                `expression2`
				ORDER BY C.ord ASC SEPARATOR ') AND ('
			),
			')'
		) AS dsql_component
		FROM `n_util`.`tiny_sequence` AS `o`
		JOIN (
			SELECT 
				`s`.`v` AS ord, 
				JSON_UNQUOTE(JSON_EXTRACT(`p_expressions1`, CONCAT('$[', `s`.`v`, ']'))) AS `expression1`,
				JSON_UNQUOTE(JSON_EXTRACT(`p_expressions2`, CONCAT('$[', `s`.`v`, ']'))) AS `expression2`
			FROM `n_util`.`tiny_sequence` AS `s`
			WHERE `s`.`v` < expression_cnt
		) AS C
			ON C.ord <= `o`.`v`
		WHERE `o`.`v` < (expression_cnt + `or_equal`) #--There will be an extra set for the OR EQUAL case if it's TRUE (1)
        GROUP BY `o`.`v`
	) AS derv
	;

    SET @@group_concat_max_len = prior_max_GC; #--Set it back to the original value 
	RETURN dsql;
END$$
DELIMITER ;

/*
#--Test:
	SELECT 
		JSON_ARRAYAGG(COLUMN_NAME) AS column_names,
		`n_util_i`.`DSQL_tuple_gt`(	
			`n_util_i`.`qualify_column_array`(TABLE_SCHEMA,TABLE_NAME,'A',JSON_ARRAYAGG(COLUMN_NAME)),
            `n_util_i`.`qualify_column_array`(TABLE_SCHEMA,TABLE_NAME,'B',JSON_ARRAYAGG(COLUMN_NAME)),
            TRUE
		)
	FROM information_schema.columns
    WHERE TABLE_SCHEMA = 'n_test_app'
	GROUP BY TABLE_SCHEMA, TABLE_NAME
	LIMIT 10
	;
*/

#--Construct an ORDER BY clause
DELIMITER $$
DROP FUNCTION IF EXISTS `n_util_i`.`DSQL_orderby`$$
CREATE DEFINER=`n_util_build`@`%` FUNCTION `n_util_i`.`DSQL_orderby`( 
    `p_expressions` JSON, #--Array of up to 255 expressions
    `p_default_order` ENUM('ASC','DESC') CHARACTER SET UTF8MB4,
    `p_column_is_desc_array` JSON #--Array of up to 255 booleans representing DESC with TRUE and ASC with FALSE
) RETURNS TEXT#--VARCHAR(17153)
    READS SQL DATA
    DETERMINISTIC
    SQL SECURITY INVOKER
    COMMENT 'Given arrays of expressions and whether asc or desc, returns the column list for an ORDER BY clause'
BEGIN 
	DECLARE prior_max_GC INT;
    DECLARE dsql TEXT;
    SET prior_max_GC = @@group_concat_max_len;
	SET @@group_concat_max_len = 65535; #--Extend max length to accomodate output
    
    SELECT
		CONCAT(/*'ORDER BY ', */GROUP_CONCAT(
			CONCAT(
				`expression`,
                ' ',
				CASE `is_desc` 
					WHEN TRUE THEN 'DESC'
                    WHEN FALSE THEN 'ASC'
                    ELSE IFNULL(`p_default_order`, 'ASC')
				END #--Add the direction (ASC is default)
			)
        ORDER BY C.ord ASC SEPARATOR ','))
    INTO dsql
    FROM (
		SELECT 
			`s`.`v` AS ord, 
            JSON_UNQUOTE(JSON_EXTRACT(`p_expressions`, CONCAT('$[', `s`.`v`, ']'))) AS `expression`,
            CAST(JSON_EXTRACT(`p_column_is_desc_array`, CONCAT('$[', `s`.`v`, ']')) AS UNSIGNED INTEGER) AS `is_desc`
		FROM `n_util`.`tiny_sequence` `s`
		WHERE `s`.`v` < JSON_LENGTH(`p_expressions`)
	) AS C
	;

    SET @@group_concat_max_len = prior_max_GC; #--Set it back to the original value 
	RETURN dsql;
END$$
DELIMITER ;

#--Test:
/*
	#--With undefined orders
	SELECT 
		JSON_ARRAYAGG(COLUMN_NAME) AS column_names,
		`n_util_i`.`DSQL_orderby`(	
			`n_util_i`.`qualify_column_array`(TABLE_SCHEMA,TABLE_NAME,'T',JSON_ARRAYAGG(COLUMN_NAME)),
            NULL,
            NULL
		)
	FROM information_schema.columns
	GROUP BY TABLE_SCHEMA, TABLE_NAME
	LIMIT 10
	;

	#--With default order
	SELECT 
		JSON_ARRAYAGG(COLUMN_NAME) AS column_names,
		`n_util_i`.`DSQL_orderby`(	
			`n_util_i`.`qualify_column_array`(TABLE_SCHEMA,TABLE_NAME,'T',JSON_ARRAYAGG(COLUMN_NAME)),
            'DESC',
            NULL
		)
	FROM information_schema.columns
	GROUP BY TABLE_SCHEMA, TABLE_NAME
	LIMIT 10
	;
    
	#--With mixed order
	SELECT 
		JSON_ARRAYAGG(COLUMN_NAME) AS column_names,
		`n_util_i`.`DSQL_orderby`(	
			`n_util_i`.`qualify_column_array`(TABLE_SCHEMA,TABLE_NAME,'T',JSON_ARRAYAGG(COLUMN_NAME)),
            NULL,
            '[1,0,null]'
		)
	FROM information_schema.columns
	GROUP BY TABLE_SCHEMA, TABLE_NAME
	LIMIT 10
	;

	#--With mixed order with default
	SELECT 
		JSON_ARRAYAGG(COLUMN_NAME) AS column_names,
		`n_util_i`.`DSQL_orderby`(	
			`n_util_i`.`qualify_column_array`(TABLE_SCHEMA,TABLE_NAME,'T',JSON_ARRAYAGG(COLUMN_NAME)),
            'DESC',
            '[1,0,null]'
		)
	FROM information_schema.columns
	GROUP BY TABLE_SCHEMA, TABLE_NAME
	LIMIT 10
	;

*/