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

#--Create schemata for n_util (run as the migration user or an administrator sufficiently privileged to create new schemata)
CREATE SCHEMA IF NOT EXISTS `n_util_i` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin ;
CREATE SCHEMA IF NOT EXISTS `n_util` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin ;
