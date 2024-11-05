# @file BuildDataQualityHistoryIndex.R
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
#' @param sourceFolder       Location of a data source source folder
#'
#' @import lubridate
#' @import jsonlite
#' @import dplyr
#' @importFrom data.table data.table
#' @importFrom rlang .data
#'
#' @export
buildDataQualityHistoryIndex <-
  function(sourceFolder) {
    total_index <- data.table::data.table()
    stratified_index <- data.table::data.table()

    addResultsToIndex <- function(json) {
      cdm_source_name <- json$Metadata[1,"cdmSourceName"]
      cdm_source_abbreviation <- json$Metadata[1,"cdmSourceAbbreviation"]
      vocabulary_version <- json$Metadata[1,"vocabularyVersion"]
      cdm_release_date <- format(lubridate::ymd(json$Metadata[1,"cdmReleaseDate"]),"%Y-%m-%d")
      count_passed <- as.numeric(json$Overview$countPassed)
      count_failed <- as.numeric(json$Overview$countOverallFailed)
      count_total <- count_passed + count_failed
      dqd_execution_date <- format(lubridate::ymd_hms(json$endTimestamp),"%Y-%m-%d")

      stratifiedAggregates <- json$CheckResults %>%
        filter(.data$failed==1) %>%
        group_by(.data$category, toupper(.data$cdmTableName)) %>%
        summarise(count_value=n())
      names(stratifiedAggregates) <- c("category", "cdm_table_name", "count_value")
      stratifiedAggregates$dqd_execution_date <- dqd_execution_date
      stratifiedAggregates$cdm_release_date <- cdm_release_date

      stratified_index <<- dplyr::bind_rows(stratified_index, stratifiedAggregates)

      total_index <<- dplyr::bind_rows(total_index,
          list(
            cdm_source_name = cdm_source_name,
            cdm_source_abbreviation = cdm_source_abbreviation,
            count_passed = count_passed,
            count_failed =count_failed,
            count_total = count_total,
            cdm_release_date = cdm_release_date,
            dqd_execution_date = dqd_execution_date,
            vocabulary_version = vocabulary_version
          )
        )
    }

    directories <- list.dirs(sourceFolder, full.names = T, recursive = F)

    for (directory in directories) {
      resultFile <- file.path(directory, "dq-result.json")
      if (file.exists(resultFile) && directory %in% AresIndexer::getIgnoredReleases(sourceFolder)) {
          writeLines(paste("AresIndexIgnore file present, skipping release dq file: ", resultFile))
        }
        else if (file.exists(resultFile)) {

          writeLines(paste("processing", resultFile))
          fileContents <- readLines(resultFile, warn = FALSE)
          resultJson <- jsonlite::fromJSON(fileContents)
          addResultsToIndex(resultJson)

        }
        else {
        writeLines(paste("missing", resultFile))
      }
    }

    colnames(total_index) <-
      c(
        "cdm_source_name",
        "cdm_source_abbreviation",
        "count_passed",
        "count_failed",
        "count_total",
        "cdm_release_date",
        "dqd_execution_date",
        "vocabulary_version"
      )

    index <- {
    }
    index$dataQualityRecords <- total_index
    index$dataQualityRecordsStratified <- stratified_index

    return(index)
  }
