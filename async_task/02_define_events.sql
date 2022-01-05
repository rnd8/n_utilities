DROP EVENT IF EXISTS `n_util_i`.`async_task_watchdog`;
CREATE DEFINER = `n_util` EVENT `n_util_i`.`async_task_watchdog`
	ON SCHEDULE EVERY 1 SECOND
	ON COMPLETION PRESERVE
	ENABLE
	COMMENT 'Ensures that the thread launchers are enabled when there are any active tasks.'
	DO
		CALL `n_util_i`.`async_task_watchdog`
	;
    
DROP EVENT IF EXISTS `n_util_i`.`async_task_thread_launcher0`;
CREATE DEFINER = `n_util` EVENT `n_util_i`.`async_task_thread_launcher0`
	ON SCHEDULE EVERY 1 SECOND
	ON COMPLETION PRESERVE
	DISABLE
	COMMENT 'There are multiple launcher events because MySQL does not yet support fractional second scheduling.'
	DO
		CALL `n_util_i`.`async_task_thread_launcher`
	;
DROP EVENT IF EXISTS `n_util_i`.`async_task_thread_launcher1`;
CREATE DEFINER = `n_util` EVENT `n_util_i`.`async_task_thread_launcher1`
	ON SCHEDULE EVERY 1 SECOND
	ON COMPLETION PRESERVE
	DISABLE
	COMMENT 'There are multiple launcher events because MySQL does not yet support fractional second scheduling.'
	DO
		CALL `n_util_i`.`async_task_thread_launcher`
	;
DROP EVENT IF EXISTS `n_util_i`.`async_task_thread_launcher2`;
CREATE DEFINER = `n_util` EVENT `async_task_thread_launcher2`
	ON SCHEDULE EVERY 1 SECOND
	ON COMPLETION PRESERVE
	DISABLE
	COMMENT 'There are multiple launcher events because MySQL does not yet support fractional second scheduling.'
	DO
		CALL `n_util_i`.`async_task_thread_launcher`
	;
DROP EVENT IF EXISTS `n_util_i`.`async_task_thread_launcher3`;
CREATE DEFINER = `n_util` EVENT `n_util_i`.`async_task_thread_launcher3`
	ON SCHEDULE EVERY 1 SECOND
	ON COMPLETION PRESERVE
	DISABLE
	COMMENT 'There are multiple launcher events because MySQL does not yet support fractional second scheduling.'
	DO
		CALL `n_util_i`.`async_task_thread_launcher`
	;
DROP EVENT IF EXISTS `n_util_i`.`async_task_thread_launcher4`;
CREATE DEFINER = `n_util` EVENT `n_util_i`.`async_task_thread_launcher4`
	ON SCHEDULE EVERY 1 SECOND
	ON COMPLETION PRESERVE
	DISABLE
	COMMENT 'There are multiple launcher events because MySQL does not yet support fractional second scheduling.'
	DO
		CALL `n_util_i`.`async_task_thread_launcher`
	;
DROP EVENT IF EXISTS `n_util_i`.`async_task_thread_launcher5`;
CREATE DEFINER = `n_util` EVENT `n_util_i`.`async_task_thread_launcher5`
	ON SCHEDULE EVERY 1 SECOND
	ON COMPLETION PRESERVE
	DISABLE
	COMMENT 'There are multiple launcher events because MySQL does not yet support fractional second scheduling.'
	DO
		CALL `n_util_i`.`async_task_thread_launcher`
	;
DROP EVENT IF EXISTS `n_util_i`.`async_task_thread_launcher6`;
CREATE DEFINER = `n_util` EVENT `n_util_i`.`async_task_thread_launcher6`
	ON SCHEDULE EVERY 1 SECOND
	ON COMPLETION PRESERVE
	DISABLE
	COMMENT 'There are multiple launcher events because MySQL does not yet support fractional second scheduling.'
	DO
		CALL `n_util_i`.`async_task_thread_launcher`
	;
DROP EVENT IF EXISTS `n_util_i`.`async_task_thread_launcher7`;
CREATE DEFINER = `n_util` EVENT `n_util_i`.`async_task_thread_launcher7`
	ON SCHEDULE EVERY 1 SECOND
	ON COMPLETION PRESERVE
	DISABLE
	COMMENT 'There are multiple launcher events because MySQL does not yet support fractional second scheduling.'
	DO
		CALL `n_util_i`.`async_task_thread_launcher`
	;