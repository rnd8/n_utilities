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
    along with Foobar.  If not, see <https://www.gnu.org/licenses/>.
*/

DELIMITER $$
DROP FUNCTION IF EXISTS `n_util_i`.`array_to_normalize`$$
CREATE DEFINER=`n_util_build` FUNCTION `n_util_i`.`array_to_normalize`() 
    RETURNS JSON
    NO SQL
    DETERMINISTIC
    SQL SECURITY INVOKER
BEGIN 
    #--TODO: Add error handling for NULL
    RETURN @n_util__array_to_normalize; 
END$$
DELIMITER ;

DELIMITER $$
DROP PROCEDURE IF EXISTS `n_util_i`.`build_array_handling`$$
CREATE DEFINER=`n_util_build` PROCEDURE `n_util_i`.`build_array_handling`()
    MODIFIES SQL DATA
    SQL SECURITY DEFINER
BEGIN
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
        BEGIN
            RESIGNAL;
        END;

#--Create the array decoposition view
CREATE OR REPLACE
    ALGORITHM=MERGE 
    DEFINER=`n_util_build`
    SQL SECURITY INVOKER 
VIEW `n_util_i`.`array_to_normalize__JSON` AS 
    SELECT 
        S.v AS element_ord, 
        JSON_EXTRACT(J.the_array,CONCAT("$[",S.v,"]")) AS `v`
    FROM (SELECT `n_util_i`.`array_to_normalize`() AS the_array) AS J
    JOIN `n_util`.`small_sequence` AS S
        ON S.v < JSON_LENGTH(J.the_array)
;

#--Convey a view that materializes on-the-fly as raw JSON (generic)
CREATE OR REPLACE
    ALGORITHM=TEMPTABLE 
    DEFINER=`n_util_build`
    SQL SECURITY DEFINER 
VIEW `n_util`.`array_to_normalize__ANY` AS 
    SELECT * 
    FROM `n_util_i`.`array_to_normalize__JSON`
    LIMIT 65536 #--Inform the optimizer of the maximum size
;
/*
#--Try it: 
    SET @n_util__array_to_normalize = CONCAT('[10,-20,"cat",40000000,"',NOW(),'"]');
    SELECT * FROM `n_util`.`array_to_normalize__ANY`;
*/

#--Convey a view as BIGINT (generic)
CREATE OR REPLACE
    ALGORITHM=TEMPTABLE 
    DEFINER=`n_util_build`
    SQL SECURITY DEFINER 
VIEW `n_util`.`array_to_normalize__BIGINT` AS 
    SELECT 
        element_ord,
        CAST(`v` AS SIGNED INTEGER)
    FROM `n_util_i`.`array_to_normalize__JSON`
    LIMIT 65536 #--Inform the optimizer of the maximum size
;
/*
#--Try it: 
    SET @n_util__array_to_normalize = CONCAT('[10,-20,"cat",40000000,"',NOW(),'"]');
    SELECT * FROM `n_util`.`array_to_normalize__BIGINT`;
*/

#--Convey a view as UNSIGNED BIGINT (generic)
CREATE OR REPLACE
    ALGORITHM=TEMPTABLE 
    DEFINER=`n_util_build`
    SQL SECURITY DEFINER 
VIEW `n_util`.`array_to_normalize__UNS_BIGINT` AS 
    SELECT 
        element_ord,
        CAST(`v` AS UNSIGNED INTEGER)
    FROM `n_util_i`.`array_to_normalize__JSON`
    LIMIT 65536 #--Inform the optimizer of the maximum size
;
/*
#--Try it: 
    SET @n_util__array_to_normalize = CONCAT('[10,-20,"cat",40000000,"',NOW(),'"]');
    SELECT * FROM `n_util`.`array_to_normalize__UNS_BIGINT`;
*/

END$$
DELIMITER ;

CALL `n_util_i`.`build_array_handling`;
