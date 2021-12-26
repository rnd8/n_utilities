#--Initial setup for n_util (run as the migration user or an administrator sufficiently privileged to create/grant a build user)
CREATE USER IF NOT EXISTS 'n_util' ACCOUNT LOCK; #--This user will define public artifacts for external use (planned for later versions)
CREATE USER IF NOT EXISTS 'n_util_build' ACCOUNT LOCK; #--This user will define artifacts at build time; defining internal artifacts as itself to convey some powers at execution time where the security of the artifact is set to DEFINER
GRANT ALL ON `n_util_i`.* TO 'n_util_build' WITH GRANT OPTION;
GRANT SELECT, EXECUTE ON `n_util_s`.* TO 'n_util_build'; #--Limited access to user defined artifacts in the sourcing schema
GRANT ALL ON `n_util`.* TO 'n_util_build' WITH GRANT OPTION; 


#--Grant your application and other users read/execute access to the public n_util schema
	#--We recommend reviewing the available artifacts in the n_util schema in order to determine which users should have access
    #--Artifacts in this schema are intended to be safer to allow access to than artifacts in the _i schema
#--Example:
#--GRANT EXECUTE, SELECT ON `n_util`.* TO 'appuser'@'%';