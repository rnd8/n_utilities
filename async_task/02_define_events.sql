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

DROP EVENT IF EXISTS `n_util_i`.`async_task_watchdog`;
CREATE DEFINER = `n_util_build` EVENT `n_util_i`.`async_task_watchdog`
	ON SCHEDULE EVERY 1 SECOND
	ON COMPLETION PRESERVE
	ENABLE
	COMMENT 'Ensures that the thread launchers are enabled when there are any active tasks.'
	DO
		CALL `n_util_i`.`async_task_watchdog`
	;
    
DROP EVENT IF EXISTS `n_util_i`.`async_task_thread_launcher0`;
CREATE DEFINER = `n_util_build` EVENT `n_util_i`.`async_task_thread_launcher0`
	ON SCHEDULE EVERY 1 SECOND
	ON COMPLETION PRESERVE
	DISABLE
	COMMENT 'There are multiple launcher events because MySQL does not yet support fractional second scheduling.'
	DO
		CALL `n_util_i`.`async_task_thread_launcher`
	;
DROP EVENT IF EXISTS `n_util_i`.`async_task_thread_launcher1`;
CREATE DEFINER = `n_util_build` EVENT `n_util_i`.`async_task_thread_launcher1`
	ON SCHEDULE EVERY 1 SECOND
	ON COMPLETION PRESERVE
	DISABLE
	COMMENT 'There are multiple launcher events because MySQL does not yet support fractional second scheduling.'
	DO
		CALL `n_util_i`.`async_task_thread_launcher`
	;
DROP EVENT IF EXISTS `n_util_i`.`async_task_thread_launcher2`;
CREATE DEFINER = `n_util_build` EVENT `n_util_i`.`async_task_thread_launcher2`
	ON SCHEDULE EVERY 1 SECOND
	ON COMPLETION PRESERVE
	DISABLE
	COMMENT 'There are multiple launcher events because MySQL does not yet support fractional second scheduling.'
	DO
		CALL `n_util_i`.`async_task_thread_launcher`
	;
DROP EVENT IF EXISTS `n_util_i`.`async_task_thread_launcher3`;
CREATE DEFINER = `n_util_build` EVENT `n_util_i`.`async_task_thread_launcher3`
	ON SCHEDULE EVERY 1 SECOND
	ON COMPLETION PRESERVE
	DISABLE
	COMMENT 'There are multiple launcher events because MySQL does not yet support fractional second scheduling.'
	DO
		CALL `n_util_i`.`async_task_thread_launcher`
	;
DROP EVENT IF EXISTS `n_util_i`.`async_task_thread_launcher4`;
CREATE DEFINER = `n_util_build` EVENT `n_util_i`.`async_task_thread_launcher4`
	ON SCHEDULE EVERY 1 SECOND
	ON COMPLETION PRESERVE
	DISABLE
	COMMENT 'There are multiple launcher events because MySQL does not yet support fractional second scheduling.'
	DO
		CALL `n_util_i`.`async_task_thread_launcher`
	;
DROP EVENT IF EXISTS `n_util_i`.`async_task_thread_launcher5`;
CREATE DEFINER = `n_util_build` EVENT `n_util_i`.`async_task_thread_launcher5`
	ON SCHEDULE EVERY 1 SECOND
	ON COMPLETION PRESERVE
	DISABLE
	COMMENT 'There are multiple launcher events because MySQL does not yet support fractional second scheduling.'
	DO
		CALL `n_util_i`.`async_task_thread_launcher`
	;
DROP EVENT IF EXISTS `n_util_i`.`async_task_thread_launcher6`;
CREATE DEFINER = `n_util_build` EVENT `n_util_i`.`async_task_thread_launcher6`
	ON SCHEDULE EVERY 1 SECOND
	ON COMPLETION PRESERVE
	DISABLE
	COMMENT 'There are multiple launcher events because MySQL does not yet support fractional second scheduling.'
	DO
		CALL `n_util_i`.`async_task_thread_launcher`
	;
DROP EVENT IF EXISTS `n_util_i`.`async_task_thread_launcher7`;
CREATE DEFINER = `n_util_build` EVENT `n_util_i`.`async_task_thread_launcher7`
	ON SCHEDULE EVERY 1 SECOND
	ON COMPLETION PRESERVE
	DISABLE
	COMMENT 'There are multiple launcher events because MySQL does not yet support fractional second scheduling.'
	DO
		CALL `n_util_i`.`async_task_thread_launcher`
	;