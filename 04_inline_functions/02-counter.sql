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
DROP FUNCTION IF EXISTS `n_util`.`counter`$$
CREATE DEFINER=`n_util_build`@`%` FUNCTION `n_util`.`counter`( 
    `p_increment` BIGINT
) RETURNS BIGINT
    NO SQL
    NOT DETERMINISTIC
    SQL SECURITY INVOKER
    COMMENT 'Given the amount to add to the current value, returns the new value. This implies that 0 returns the number unaffected. NULL resets it to 0.'
BEGIN 
	RETURN @n_util_counter_value := (IFNULL(@n_util_counter_value + `p_increment`,IFNULL(`p_increment`,0)));
END$$
DELIMITER ;
/*
#--TEST:
#--EXPLAIN
DO `n_util`.`counter`(NULL); #--Reset
SELECT
	v,
    FLOOR(v/16) AS increment,
    `n_util`.`counter`(FLOOR(v/16)) AS counter
FROM `n_util`.`tiny_sequence` AS S
;
*/