# @file GetIgnoredReleases.R
#
#
# Copyright 2021 Observational Health Data Sciences and Informatics
#
# This file is part of AresIndexer
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#' Get Ignored Releases
#'
#' @details Generates a list of data releases that will be ignored when building for a given source
#'
#' @param sourceFolder The location of the source folder.
#'
#' @return A list object of ignored data releases.
#'
#'
#' @export
getIgnoredReleases <- function(sourceFolder) {
    releaseFolders <- list.dirs(sourceFolder, recursive = F)
    releaseFolders <- sort(releaseFolders, decreasing = T)

    ignoredReleases<- list()

    for (releaseFolder in releaseFolders) {
      if (file.exists(file.path(releaseFolder, ".aresIndexIgnore")) |
          file.exists(file.path(sourceFolder, ".aresIndexIgnore"))) {
        ignoredReleases <- append(ignoredReleases, releaseFolder)
      }
    }

  return(ignoredReleases)
}
