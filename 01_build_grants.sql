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
DROP PROCEDURE IF EXISTS `n_util_i`.`build_grants`$$
CREATE DEFINER=`n_util_build` PROCEDURE `n_util_i`.`build_grants`()
    MODIFIES SQL DATA
    SQL SECURITY DEFINER
BEGIN
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
        BEGIN
            RESIGNAL;
        END;
	
    #--Idempotently add privileges we need in the current version
	GRANT EXECUTE, SELECT, INSERT, SHOW VIEW 
		ON `n_util_i`.* TO 'n_util' WITH GRANT OPTION;
	GRANT SELECT, EXECUTE ON `n_util`.* TO 'n_util' WITH GRANT OPTION;
    
    #--Idempotently remove privileges we no longer need in the current version
    REVOKE UPDATE, DELETE, CREATE TEMPORARY TABLES
		ON `n_util_i`.* FROM 'n_util';
    
END$$
DELIMITER ;

CALL `n_util_i`.`build_grants`;
