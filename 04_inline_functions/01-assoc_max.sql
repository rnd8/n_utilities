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
DROP FUNCTION IF EXISTS `n_util`.`amax_int_int`$$
CREATE DEFINER=`n_util_build`@`%` FUNCTION `n_util`.`amax_int_int`( 
    `p_compare_val` BIGINT,
    `p_associated_val` BIGINT
) RETURNS BIGINT
    NO SQL
    NOT DETERMINISTIC
    SQL SECURITY INVOKER
    COMMENT 'Given a comparison value, and an associated value, emits the highest associated value ever given since last NULL or session start.'
BEGIN 
	RETURN IF(
		`p_compare_val` <=> (@n_util_amax_int_int_compare_val := GREATEST(IFNULL(@n_util_amax_int_int_compare_val,`p_compare_val`), `p_compare_val`)), #--Use~set the new/old associated value
		@n_util_amax_int_int_associated_val := `p_associated_val`, #--Use+set the new associated value
        @n_util_amax_int_int_associated_val #--Use the old associated value
    );
END$$
DELIMITER ;
/*
#--TEST:
#--EXPLAIN
DO `n_util`.`amax_int_int`(NULL, NULL);
SELECT
	v,
    `n_util`.`amax_int_int`(IF(v mod 8 = 5, NULL, (RAND()*256)),v) AS high_assoc,
    (v mod 8 = 5) AS `reset`
FROM `n_util`.`tiny_sequence` AS S
;
*/

DELIMITER $$
DROP FUNCTION IF EXISTS `n_util`.`amax_int_json`$$
CREATE DEFINER=`n_util_build`@`%` FUNCTION `n_util`.`amax_int_json`( 
    `p_compare_val` BIGINT,
    `p_associated_val` JSON
) RETURNS JSON
    NO SQL
    NOT DETERMINISTIC
    SQL SECURITY INVOKER
    COMMENT 'Given a comparison value, and an associated value, emits the highest associated value ever given since last NULL or session start.'
BEGIN 
	RETURN IF(
		`p_compare_val` <=> (@n_util_amax_int_json_compare_val := GREATEST(IFNULL(@n_util_amax_int_json_compare_val,`p_compare_val`), `p_compare_val`)), #--Use~set the new/old associated value
		@n_util_amax_int_json_associated_val := `p_associated_val`, #--Use+set the new associated value
        @n_util_amax_int_json_associated_val #--Use the old associated value
    );
END$$
DELIMITER ;
/*
#--TEST:
#--EXPLAIN
DO `n_util`.`amax_int_json`(NULL, NULL);
SELECT
	v,
    `n_util`.`amax_int_json`(IF(v mod 8 = 5, NULL, (RAND()*256)),JSON_ARRAY(v,1,2,3)) AS high_assoc,
    (v mod 8 = 5) AS `reset`
FROM `n_util`.`tiny_sequence` AS S
;
*/

DELIMITER $$
DROP FUNCTION IF EXISTS `n_util`.`amax_dbl_int`$$
CREATE DEFINER=`n_util_build`@`%` FUNCTION `n_util`.`amax_dbl_int`( 
    `p_compare_val` DOUBLE,
    `p_associated_val` BIGINT
) RETURNS BIGINT
    NO SQL
    NOT DETERMINISTIC
    SQL SECURITY INVOKER
    COMMENT 'Given a comparison value, and an associated value, emits the highest associated value ever given since last NULL or session start.'
BEGIN 
	RETURN IF(
		`p_compare_val` <=> (@n_util_amax_dbl_int_compare_val := GREATEST(IFNULL(@n_util_amax_dbl_int_compare_val,`p_compare_val`), `p_compare_val`)), #--Use~set the new/old associated value
		@n_util_amax_dbl_int_associated_val := `p_associated_val`, #--Use+set the new associated value
        @n_util_amax_dbl_int_associated_val #--Use the old associated value
    );
END$$
DELIMITER ;
/*
#--TEST:
#--EXPLAIN
DO `n_util`.`amax_dbl_int`(NULL, NULL);
SELECT
	v,
    `n_util`.`amax_dbl_int`(IF(v mod 8 = 5, NULL, RAND()),v) AS high_assoc,
    (v mod 8 = 5) AS `reset`
FROM `n_util`.`tiny_sequence` AS S
;
*/

DELIMITER $$
DROP FUNCTION IF EXISTS `n_util`.`amax_dbl_json`$$
CREATE DEFINER=`n_util_build`@`%` FUNCTION `n_util`.`amax_dbl_json`( 
    `p_compare_val` BIGINT,
    `p_associated_val` JSON
) RETURNS JSON
    NO SQL
    NOT DETERMINISTIC
    SQL SECURITY INVOKER
    COMMENT 'Given a comparison value, and an associated value, emits the highest associated value ever given since last NULL or session start.'
BEGIN 
	RETURN IF(
		`p_compare_val` <=> (@n_util_amax_dbl_json_compare_val := GREATEST(IFNULL(@n_util_amax_dbl_json_compare_val,`p_compare_val`), `p_compare_val`)), #--Use~set the new/old associated value
		@n_util_amax_dbl_json_associated_val := `p_associated_val`, #--Use+set the new associated value
        @n_util_amax_dbl_json_associated_val #--Use the old associated value
    );
END$$
DELIMITER ;
/*
#--TEST:
#--EXPLAIN
DO `n_util`.`amax_dbl_json`(NULL, NULL);
SELECT
	v,
    `n_util`.`amax_dbl_json`(IF(v mod 8 = 5, NULL, RAND()),JSON_ARRAY(v,1,2,3)) AS high_assoc,
    (v mod 8 = 5) AS `reset`
FROM `n_util`.`tiny_sequence` AS S
;
*/


DELIMITER $$
DROP FUNCTION IF EXISTS `n_util`.`amax_ts_int`$$
CREATE DEFINER=`n_util_build`@`%` FUNCTION `n_util`.`amax_ts_int`( 
    `p_compare_val` TIMESTAMP(6),
    `p_associated_val` BIGINT
) RETURNS BIGINT
    NO SQL
    NOT DETERMINISTIC
    SQL SECURITY INVOKER
    COMMENT 'Given a comparison value, and an associated value, emits the highest associated value ever given since last NULL or session start.'
BEGIN 
	RETURN IF(
		`p_compare_val` <=> (@n_util_amax_ts_int_compare_val := GREATEST(IFNULL(@n_util_amax_ts_int_compare_val,`p_compare_val`), `p_compare_val`)), #--Use~set the new/old associated value
		@n_util_amax_ts_int_associated_val := `p_associated_val`, #--Use+set the new associated value
        @n_util_amax_ts_int_associated_val #--Use the old associated value
    );
END$$
DELIMITER ;
/*
#--TEST:
#--EXPLAIN
DO `n_util`.`amax_ts_int`(NULL, NULL);
SELECT
	v,
    `n_util`.`amax_ts_int`(IF(v mod 8 = 5, NULL, NOW(6) - INTERVAL RAND() SECOND),v) AS high_assoc,
    (v mod 8 = 5) AS `reset`
FROM `n_util`.`tiny_sequence` AS S
;
*/

DELIMITER $$
DROP FUNCTION IF EXISTS `n_util`.`amax_ts_json`$$
CREATE DEFINER=`n_util_build`@`%` FUNCTION `n_util`.`amax_ts_json`( 
    `p_compare_val` TIMESTAMP(6),
    `p_associated_val` JSON
) RETURNS JSON
    NO SQL
    NOT DETERMINISTIC
    SQL SECURITY INVOKER
    COMMENT 'Given a comparison value, and an associated value, emits the highest associated value ever given since last NULL or session start.'
BEGIN 
	RETURN IF(
		`p_compare_val` <=> (@n_util_amax_ts_json_compare_val := GREATEST(IFNULL(@n_util_amax_ts_json_compare_val,`p_compare_val`), `p_compare_val`)), #--Use~set the new/old associated value
		@n_util_amax_ts_json_associated_val := `p_associated_val`, #--Use+set the new associated value
        @n_util_amax_ts_json_associated_val #--Use the old associated value
    );
END$$
DELIMITER ;
/*
#--TEST:
#--EXPLAIN
DO `n_util`.`amax_ts_json`(NULL, NULL);
SELECT
	v,
    `n_util`.`amax_ts_json`(IF(v mod 8 = 5, NULL, NOW(6) - INTERVAL RAND() SECOND),JSON_ARRAY(v,1,2,3)) AS high_assoc,
    (v mod 8 = 5) AS `reset`
FROM `n_util`.`tiny_sequence` AS S
;
*/