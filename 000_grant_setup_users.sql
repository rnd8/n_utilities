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

#--Initial setup for n_util (run as the migration user or an administrator sufficiently privileged to create/grant a build user)
CREATE USER IF NOT EXISTS 'n_util' ACCOUNT LOCK;
CREATE USER IF NOT EXISTS 'n_util_build' ACCOUNT LOCK;
GRANT ALL ON `n_util_i`.* TO 'n_util_build' WITH GRANT OPTION;
GRANT ALL ON `n_util`.* TO 'n_util_build' WITH GRANT OPTION;

#--Grant your application and other users read/execute access to the public n_util schema
	#--We recommend reviewing the available artifacts in the n_util schema in order to determine which users should have access
    #--Artifacts in this schema are intended to be safer to allow access to than artifacts in the _i schema
#--GRANT SELECT, EXECUTE ON `n_util`.* TO 'app_user';
