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

DROP TABLE IF EXISTS `n_util`.`crawler_job_iteration`;
DROP TABLE IF EXISTS `n_util_s`.`crawler_job`;
CREATE TABLE `n_util_s`.`crawler_job` (
  `job_id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
  `job_name` VARCHAR(64) NOT NULL,
  `workset_schema` VARCHAR(64) NOT NULL,
  `workset_table` VARCHAR(64) NOT NULL,
  `sproc_schema` VARCHAR(64) NOT NULL,
  `sproc_name` VARCHAR(64) NOT NULL,
  `ordinal_columns` JSON NOT NULL,
  `chunk_mode` ENUM('limit', 'range') NOT NULL DEFAULT 'limit',
  `chunk_min` INT UNSIGNED NOT NULL DEFAULT 1,
  `chunk_max` INT UNSIGNED NOT NULL DEFAULT 1,
  `throttle_secs` DOUBLE UNSIGNED DEFAULT 0,
  `is_forward_only` TINYINT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'If new entries appear in the work source view/table behind the current position (with respect to ordering), they will not be processed if is_forward_only is set to TRUE.',
  `skip_gaps` TINYINT UNSIGNED NOT NULL DEFAULT 1,
  `ordering` ENUM('asc', 'desc') NULL COMMENT 'NULL is arbitrary (engine\'s choice)',
  `boundary_start_tuple` JSON NULL COMMENT 'If not null, only process tuples past (and not including) this value',
  `boundary_end_tuple` JSON NULL COMMENT 'If not null, only process tuples until (and including) this value',
  PRIMARY KEY (`job_id`),
  UNIQUE INDEX `job_name_UNIQUE` (`job_name` ASC) VISIBLE
)
COMMENT = 'A crawling job and the associated options'
;

/*
DROP TABLE IF EXISTS `n_util_s`.`crawler_job_range`;
CREATE TABLE `n_util_s`.`crawler_job_range` (
  `job_id` INT UNSIGNED NOT NULL,
  `range_num` SMALLINT UNSIGNED NOT NULL,
  `boundary_start_tuple` JSON NOT NULL COMMENT 'If null, only process tuples past (and not including) this value',
  `boundary_end_tuple` JSON NOT NULL COMMENT 'If not null, only process tuples until (and including) this value',
  PRIMARY KEY (`job_id`, `range_num`)
)
COMMENT = 'A crawling range for splitting up parallel processes, retracing, or resuming'
;
*/

CREATE TABLE `n_util`.`crawler_job_iteration` (
  `job_id` INT UNSIGNED NOT NULL,
  `iteration_num` INT UNSIGNED NOT NULL,
  `resume_iteration_num` INT UNSIGNED NULL COMMENT 'The iteration that we consider to be a checkpoint (skipping to first thing past its chunk_last_tuple)  (references iteration_num on another tuple in this same table)',
  `chunk_last_tuple` JSON NOT NULL COMMENT 'The last tuple sent for processing in this chunk',
  `process_id` BIGINT UNSIGNED NOT NULL,
  `started_ts` TIMESTAMP(6) NOT NULL DEFAULT NOW(6),
  `fetched_ts` TIMESTAMP(6) NOT NULL DEFAULT NOW(6),
  `logged_ts` TIMESTAMP(6) NOT NULL DEFAULT NOW(6),
  PRIMARY KEY (`job_id`, `iteration_num`),
  #--INDEX `work_first` (`job_id` ASC, `chunk_first_tuple` ASC) VISIBLE, #--TODO: Index JSON values
  #--INDEX `work_last` (`job_id` ASC, `chunk_last_tuple` ASC) VISIBLE, #--TODO: Index JSON values
  INDEX created_ts (logged_ts ASC),
  INDEX process_id (process_id ASC),
  INDEX resume_iteration_num (job_id ASC, resume_iteration_num ASC),
  CONSTRAINT `c_job_i2c_job`
    FOREIGN KEY (`job_id`)
    REFERENCES `n_util_s`.`crawler_job` (`job_id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE,
  CONSTRAINT `c_job_i__resume`
    FOREIGN KEY (`job_id`, `resume_iteration_num`)
    REFERENCES `n_util`.`crawler_job_iteration` (`job_id`, `iteration_num`)
    ON DELETE RESTRICT
    ON UPDATE RESTRICT
)
COMMENT = 'A write-ahead-log of job iteration parameters'
;
/*
CREATE TABLE `n_util`.`crawler_job_iteration` (
  `job_id` INT UNSIGNED NOT NULL,
  `iteration_num` INT UNSIGNED NOT NULL,
  `chunk_low_tuple` JSON NOT NULL,
  `chunk_high_tuple` JSON NOT NULL,
  `process_id` BIGINT UNSIGNED NOT NULL,
  `created_ts` TIMESTAMP(6) NOT NULL DEFAULT NOW(6),
  PRIMARY KEY (`job_id`, `iteration_num`),
  #--INDEX `work_first` (`job_id` ASC, `chunk_first_tuple` ASC) VISIBLE, #--TODO: Index JSON values
  #--INDEX `work_last` (`job_id` ASC, `chunk_last_tuple` ASC) VISIBLE, #--TODO: Index JSON values
  INDEX created_ts (created_ts ASC),
  INDEX process_id (process_id ASC),
  CONSTRAINT `c_job_i2c_job`
    FOREIGN KEY (`job_id`)
    REFERENCES `n_util_s`.`crawler_job` (`job_id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE
)
COMMENT = 'A write-ahead-log of job iteration parameters'
;
*/