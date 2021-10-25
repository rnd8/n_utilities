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
