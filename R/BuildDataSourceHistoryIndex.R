# @file BuildDataSourceHistoryIndex.R
#
# Copyright 2020 Observational Health Data Sciences and Informatics
#
# This file is part of DataQualityDashboard
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

#' Data Quality Historical Indexing
#'
#' @param sourceFolder       Location of a data source folder
#'
#' @import lubridate
#' @import jsonlite
#' @import dplyr
#' @importFrom data.table data.table
#'
#' @export
buildDataSourceHistoryIndex <- function(sourceFolder) {
  domain_index <- data.table::data.table()

  releaseDirectories <-
    list.dirs(sourceFolder, full.names = T, recursive = F)

  substrRight <- function(x, n) {
    substr(x, nchar(x) - n + 1, nchar(x))
  }

  for (releaseDirectory in releaseDirectories) {
    releaseKey <- substrRight(releaseDirectory, 8)
    releaseDate <- format(lubridate::ymd(releaseKey), "%Y-%m-%d")
    recordsByDomainFile <- file.path(releaseDirectory, "records-by-domain.csv")
    if (file.exists(recordsByDomainFile)) {
      recordsByDomainData <- data.table::fread(recordsByDomainFile)
      recordsByDomainData$release_date <- releaseDate
      domain_index <- rbind(domain_index, recordsByDomainData)
    }
  }

  index <- {
  }
  index$domainRecords <- domain_index
  return(index)
}
