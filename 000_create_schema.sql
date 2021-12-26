#--Create schemata for n_util (run as the migration user or an administrator sufficiently privileged to create new schemata)
CREATE SCHEMA IF NOT EXISTS `n_util_i` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin ; #--Private inner artifacts schema
CREATE SCHEMA IF NOT EXISTS `n_util_s` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin ; #--Sourcing artifact schema (user defined artifacts that access other schemata or are populated by the user with source data)
CREATE SCHEMA IF NOT EXISTS `n_util` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin ; #--Publicly available artifacts schema