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
DROP PROCEDURE IF EXISTS `n_util_i`.`build_sequences`$$
CREATE DEFINER=`n_util_build` PROCEDURE `n_util_i`.`build_sequences`()
    MODIFIES SQL DATA
    SQL SECURITY DEFINER
BEGIN
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
        BEGIN
            RESIGNAL;
        END;

#--Create the source view that generates bool
CREATE OR REPLACE
    ALGORITHM=TEMPTABLE 
    DEFINER=`n_util_build`
    SQL SECURITY INVOKER 
VIEW `n_util_i`.`bool_sequence__SRC` AS 
    SELECT FALSE AS `v`
    UNION ALL
    SELECT TRUE AS `v`
;

#--Create the alias view that conveys bool
CREATE OR REPLACE
    ALGORITHM=TEMPTABLE 
    DEFINER=`n_util_build`
    SQL SECURITY DEFINER 
VIEW `n_util`.`bool_sequence` AS 
    SELECT * FROM `n_util_i`.`bool_sequence__SRC`
;
#--Try it: SELECT * FROM `n_util`.`bool_sequence`;

#--Create the source view that generates micro_sequence
CREATE OR REPLACE 
    ALGORITHM=TEMPTABLE 
    DEFINER=`n_util_build`
    SQL SECURITY INVOKER 
VIEW `n_util_i`.`nibble_sequence__SRC` AS
    SELECT 0
        | b3.v << 3
        | b2.v << 2
        | b1.v << 1
        | b0.v << 0
        AS `v`
    FROM                `n_util`.`bool_sequence` AS b0
    STRAIGHT_JOIN 	`n_util`.`bool_sequence` AS b1
    STRAIGHT_JOIN 	`n_util`.`bool_sequence` AS b2
    STRAIGHT_JOIN 	`n_util`.`bool_sequence` AS b3
;

#--Materialize the physical micro_sequence table
DROP TABLE IF EXISTS `n_util`.`nibble_sequence`;
CREATE TABLE `n_util`.`nibble_sequence` (
  `v` TINYINT UNSIGNED NOT NULL COMMENT 'The value at and of the ordinal position of the sequence that is represented by each tuple.',
  PRIMARY KEY (`v`)
)
ENGINE = INNODB
COMMENT = 'The values in a nibble 0 - 15 inclusive.'
SELECT * FROM `n_util_i`.`nibble_sequence__SRC`
;
#--Try it: SELECT * FROM `n_util`.`nibble_sequence`;

#--Create the source view that generates tiny_sequence
CREATE OR REPLACE
    ALGORITHM=TEMPTABLE 
    DEFINER=`n_util_build`
    SQL SECURITY INVOKER 
VIEW `n_util_i`.`tiny_sequence__SRC` AS 
    SELECT nS1.v << 4 | nS0.v AS `v`
    FROM `n_util`.`nibble_sequence` AS nS0
    STRAIGHT_JOIN `n_util`.`nibble_sequence` AS nS1
;

#--Materialize the physical tiny_sequence table
DROP TABLE IF EXISTS `n_util`.`tiny_sequence`;
CREATE TABLE `n_util`.`tiny_sequence` (
  `v` TINYINT UNSIGNED NOT NULL COMMENT 'The value at and of the ordinal position of the sequence that is represented by each tuple.',
  PRIMARY KEY (`v`)
)
ENGINE = INNODB
COMMENT = 'The values in a byte 0 - 255 inclusive.'
SELECT * FROM `n_util_i`.`tiny_sequence__SRC`
;
#--Try it: SELECT * FROM `n_util`.`tiny_sequence`;

#--Create the source view that generates small_sequence
CREATE OR REPLACE
    ALGORITHM=TEMPTABLE 
    DEFINER=`n_util_build`
    SQL SECURITY INVOKER 
VIEW `n_util_i`.`small_sequence__SRC` AS 
    SELECT tS1.v << 8 | tS0.v AS `v`
    FROM `n_util`.`tiny_sequence` AS tS0
    STRAIGHT_JOIN `n_util`.`tiny_sequence` AS tS1
;

#--Materialize the physical small_sequence table
DROP TABLE IF EXISTS `n_util`.`small_sequence`;
CREATE TABLE `n_util`.`small_sequence` (
  `v` SMALLINT UNSIGNED NOT NULL COMMENT 'The value at and of the ordinal position of the sequence that is represented by each tuple.',
  PRIMARY KEY (`v`)
)
ENGINE = INNODB
COMMENT = 'The values in a byte 0 - 255 inclusive.'
SELECT * FROM `n_util_i`.`small_sequence__SRC`
;
#--Try it: SELECT * FROM `n_util`.`small_sequence`;

END$$
DELIMITER ;

CALL `n_util_i`.`build_sequences`;
