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

#--Check an alias
DELIMITER $$
DROP FUNCTION IF EXISTS `n_util_i`.`check_alias`$$
CREATE DEFINER=`n_util_build`@`%` FUNCTION `n_util_i`.`check_alias`(
    `p_alias` VARCHAR(64) #--Alias for dynamic SQL
) RETURNS TINYINT
    READS SQL DATA
    DETERMINISTIC
    SQL SECURITY INVOKER
    COMMENT 'Errors early when a string would cause a later error or unexpected behavior when used in dynamic SQL as an alias'
BEGIN 
    #--Prevent dynamic SQL issues
    IF(`p_alias` LIKE '%`%') THEN
		SIGNAL SQLSTATE '45000'
			SET MESSAGE_TEXT = 'Invalid schema/table/column name or alias parameter value passed into an n_util function.'
		;
        RETURN FALSE; #--FAIL
	END IF;

	RETURN TRUE; #--PASS
END$$
DELIMITER ;
/*
#--Test:
SELECT `n_util_i`.`check_alias`('fun police'); #--Should pass
SELECT `n_util_i`.`check_alias`('fun`police'); #--Should error
*/

#--Qualify an alias
DELIMITER $$
DROP FUNCTION IF EXISTS `n_util_i`.`qualify_alias`$$
CREATE DEFINER=`n_util_build`@`%` FUNCTION `n_util_i`.`qualify_alias`(
    `p_alias` VARCHAR(64) #--Alias for dynamic SQL
) RETURNS VARCHAR(66)
    READS SQL DATA
    DETERMINISTIC
    SQL SECURITY INVOKER
    COMMENT 'Errors early when a string would cause a later error or unexpected behavior when used in dynamic SQL as an alias'
BEGIN 
    #--Prevent dynamic SQL issues
    IF(`n_util_i`.`check_alias`(`p_alias`)) THEN
        RETURN CONCAT('`',`p_alias`,'`');
	END IF;

	RETURN NULL; #--We shouldn't get here, since the check errors on our behalf
END$$
DELIMITER ;
/*
#--Test:
SELECT `n_util_i`.`qualify_alias`('fun police'); #--Should pass
SELECT `n_util_i`.`qualify_alias`('fun`police'); #--Should error
*/

#--Qualify a table
DELIMITER $$
DROP FUNCTION IF EXISTS `n_util_i`.`qualify_table`$$
CREATE DEFINER=`n_util_build`@`%` FUNCTION `n_util_i`.`qualify_table`(
	`p_schema_name` VARCHAR(64),
    `p_table_name` VARCHAR(64)
) RETURNS VARCHAR(133)
    READS SQL DATA
    DETERMINISTIC
    SQL SECURITY INVOKER
    COMMENT 'Verifies and returns a table name as fully qualified (with schema name), or NULL if it does not actually exist'
BEGIN 
	RETURN (
		SELECT CONCAT('`',`TABLE_SCHEMA`,'`.`',`TABLE_NAME`,'`')
        FROM `information_schema`.`TABLES`
        WHERE `TABLE_SCHEMA` = `p_schema_name` AND `TABLE_NAME` = `p_table_name`
        LIMIT 1
    );
END$$
DELIMITER ;

#--Qualify a column
DELIMITER $$
DROP FUNCTION IF EXISTS `n_util_i`.`qualify_column`$$
CREATE DEFINER=`n_util_build`@`%` FUNCTION `n_util_i`.`qualify_column`(
	`p_schema_name` VARCHAR(64),
    `p_table_name` VARCHAR(64), 
    `p_column_name` VARCHAR(64)
) RETURNS VARCHAR(64)
    READS SQL DATA
    DETERMINISTIC
    SQL SECURITY INVOKER
    COMMENT 'Verifies and returns a column name as fully qualified, or NULL if it does not actually exist'
BEGIN 
	RETURN (
		SELECT CONCAT('`',`COLUMN_NAME`,'`')
        FROM `information_schema`.`COLUMNS`
        WHERE `TABLE_SCHEMA` = `p_schema_name` AND `TABLE_NAME` = `p_table_name` AND `COLUMN_NAME` = `p_column_name`
        LIMIT 1
    );
END$$
DELIMITER ;
/*
#--Test:
SELECT 
	`n_util_i`.`qualify_table`(TABLE_SCHEMA,TABLE_NAME), 
	`n_util_i`.`qualify_column`(TABLE_SCHEMA,TABLE_NAME,COLUMN_NAME)
FROM information_schema.columns;
LIMIT 100
*/

#--Qualify a column array
DELIMITER $$
DROP FUNCTION IF EXISTS `n_util_i`.`qualify_column_array`$$
CREATE DEFINER=`n_util_build`@`%` FUNCTION `n_util_i`.`qualify_column_array`(
	`p_schema_name` VARCHAR(64),
    `p_table_name` VARCHAR(64), 
    `p_table_alias` VARCHAR(64),
    `p_column_names` JSON #--Array of up to 255 column names
) RETURNS JSON#--VARCHAR(17153)
    READS SQL DATA
    DETERMINISTIC
    SQL SECURITY INVOKER
    COMMENT 'Verifies and returns an array the subset of elements that match columns for the given table, omitting non-real columns'
BEGIN 
	DECLARE q_table_alias VARCHAR(66);
    SET q_table_alias = `n_util_i`.`qualify_alias`(`p_table_alias`);
    RETURN (
		SELECT JSON_ARRAYAGG(
			CONCAT(
				IFNULL(CONCAT(q_table_alias,'.'),''),
				'`',`COLUMN_NAME`,'`'
            )
		) AS return_value
        FROM (
			SELECT `COLUMN_NAME`
			FROM `information_schema`.`columns` AS I
			/*STRAIGHT_*/JOIN ( #--MySQL 5.7 compatibility
				SELECT 
					`s`.`v` AS ord, 
					JSON_UNQUOTE(JSON_EXTRACT(`p_column_names`, CONCAT('$[', `s`.`v`, ']'))) AS `COLUMN_NAME`
				FROM `n_util`.`tiny_sequence` `s`
				WHERE `s`.`v` < JSON_LENGTH(`p_column_names`)
			) AS C
				USING (`COLUMN_NAME`)
			WHERE `TABLE_SCHEMA` = `p_schema_name` AND `TABLE_NAME` = `p_table_name`
			ORDER BY C.ord ASC
		) AS ord_A
	);
END$$
DELIMITER ;

/*
#--Test:

	#--Without alias
	SELECT 
		JSON_ARRAYAGG(COLUMN_NAME) AS column_names,
		`n_util_i`.`qualify_column_array`(TABLE_SCHEMA,TABLE_NAME,NULL,JSON_ARRAYAGG(COLUMN_NAME))
	FROM information_schema.columns
	GROUP BY TABLE_SCHEMA, TABLE_NAME
	LIMIT 10
	;
    
	#--With alias
	SELECT 
		JSON_ARRAYAGG(COLUMN_NAME) AS column_names,
		`n_util_i`.`qualify_column_array`(TABLE_SCHEMA,TABLE_NAME,'alias',JSON_ARRAYAGG(COLUMN_NAME))
	FROM information_schema.columns
	GROUP BY TABLE_SCHEMA, TABLE_NAME
	LIMIT 10
	;
*/

#--Qualify a column list
DELIMITER $$
DROP FUNCTION IF EXISTS `n_util_i`.`qualify_column_list`$$
CREATE DEFINER=`n_util_build`@`%` FUNCTION `n_util_i`.`qualify_column_list`(
	`p_schema_name` VARCHAR(64),
    `p_table_name` VARCHAR(64), 
    `p_column_names` JSON, #--Array of up to 255 column names
    `p_prefix` VARCHAR(8), #--Optional alias to prefix with (for dynamic SQL)
    `p_postfix` VARCHAR(8) #--Optional alias to prefix with (for dynamic SQL)
) RETURNS TEXT#--VARCHAR(17153)
    READS SQL DATA
    DETERMINISTIC
    SQL SECURITY INVOKER
    COMMENT 'Verifies and returns a sub-tuble as a fully qualified row-constructor, omitting non-real columns, or NULL if none actually exist'
BEGIN 
	DECLARE prior_max_GC INT;
    DECLARE row_constructor TEXT;
    SET prior_max_GC = @@group_concat_max_len;
	SET @@group_concat_max_len = 65535; #--Extend max length to accomodate output
    
	DO `n_util_i`.`check_alias`(`p_alias`); #--Check that the alias will not break dynamic SQL
    
    SELECT 
		CONCAT(GROUP_CONCAT(
			CONCAT(
				IF(`p_prefix` IS NULL, '', p_prefix), #--Add the prefix, if given
				'`',`COLUMN_NAME`,'`',
                IF(`p_postfix` IS NULL, '', p_postfix) #--Add the postfix, if given
			) 
        ORDER BY C.ord ASC SEPARATOR ','))
    INTO row_constructor
    FROM `information_schema`.`columns` AS I
    /*STRAIGHT_*/JOIN ( #--Not using straight_join for MySQL 5.7 compatibility
		SELECT 
			`s`.`v` AS ord, 
            JSON_UNQUOTE(JSON_EXTRACT(`p_column_names`, CONCAT('$[', `s`.`v`, ']'))) AS `COLUMN_NAME`
		FROM `n_util`.`tiny_sequence` `s`
		WHERE `s`.`v` < JSON_LENGTH(`p_column_names`)
	) AS C
		USING (`COLUMN_NAME`)
	WHERE `TABLE_SCHEMA` = `p_schema_name` AND `TABLE_NAME` = `p_table_name`
	;

    SET @@group_concat_max_len = prior_max_GC; #--Set it back to the original value 
	RETURN row_constructor;
END$$
DELIMITER ;

/*
#--Test:

	#--Without alias
	SELECT 
		JSON_ARRAYAGG(COLUMN_NAME) AS column_names,
		`n_util_i`.`qualify_tuple`(TABLE_SCHEMA,TABLE_NAME,JSON_ARRAYAGG(COLUMN_NAME),NULL)
	FROM information_schema.columns
	GROUP BY TABLE_SCHEMA, TABLE_NAME
	LIMIT 10
	;
    
	#--With alias
	SELECT 
		JSON_ARRAYAGG(COLUMN_NAME) AS column_names,
		`n_util_i`.`qualify_tuple`(TABLE_SCHEMA,TABLE_NAME,JSON_ARRAYAGG(COLUMN_NAME),'alias')
	FROM information_schema.columns
	GROUP BY TABLE_SCHEMA, TABLE_NAME
	LIMIT 10
	;
*/