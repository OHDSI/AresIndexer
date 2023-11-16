# @file AugmentConceptFiles.R
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

#' @name Augment Concept Files
#'
#' @details Adds Data Quality and Temporal details to concept data files
#'
#' @param releaseFolder Folder containing a specific release of a data source
#'
#' @import jsonlite
#' @import dplyr
#' @importFrom data.table fwrite
#'

#' @export
augmentConceptFiles <- function(releaseFolder, format) {
  library("DBI")
  dataQualityResultsFile <- file.path(releaseFolder, "dq-result.json")
  duckdbCon <- NULL
  if (format == "duckdb") {
    duckdbCon <- DBI::dbConnect(duckdb::duckdb(), dbdir = file.path(releaseFolder, "concepts", 'data.duckdb'), read_only = FALSE)
    table <- tbl(duckdbCon, DBI::Id(schema = "concepts", table = "concept_metadata"))
    table <- as.data.frame(table)
    table <- table %>% mutate(COUNT_FAILED = NA, IS_STATIONARY = NA, SEASONALITY_SCORE = NA)
  }

  if (file.exists(dataQualityResultsFile)) {
    writeLines("updating concept files with data quality results")
    dataQualityResults <- jsonlite::fromJSON(dataQualityResultsFile)
    results <- dataQualityResults$CheckResults

    # augment achilles concept files with data quality failure count for relevant concept checks
    conceptAggregates <- results %>%
      filter(!is.na(results$conceptId) & results$failed == 1) %>%
      count(conceptId, tolower(cdmTableName))
    names(conceptAggregates) <- c("concept_id", "cdm_table_name", "count_failed")
    writeLines(paste0(nrow(conceptAggregates), " concept level data quality issues found."))
    if (nrow(conceptAggregates) > 0) {
      for (row in 1:nrow(conceptAggregates)) {
        print(trimws(conceptAggregates[row, "concept_id"]))
        concept_id <- trimws(conceptAggregates[row, "concept_id"])
        count_failed <- conceptAggregates[row, "count_failed"]

        writeLines(paste0(row, "/", nrow(conceptAggregates), " - inserting data quality results"))

        if (format == "duckdb") {
          table <- table %>% mutate(COUNT_FAILED = ifelse(CONCEPT_ID == concept_id, count_failed, COUNT_FAILED))
          DBI::dbWriteTable(duckdbCon, DBI::Id(schema = "concepts", table = "concept_metadata"), table, overwrite = TRUE)
        }
        if (format == "json") {
          conceptFileName <- paste0("concept_", trimws(conceptAggregates[row, "concept_id"]), ".json")
          conceptFile <- file.path(releaseFolder, "concepts", trimws(conceptAggregates[row, "cdm_table_name"]), conceptFileName)

          if (file.exists(conceptFile)) {
            conceptContent <- readLines(conceptFile)
            conceptData <- jsonlite::fromJSON(conceptContent)
            conceptData$COUNT_FAILED <- conceptAggregates[row, "count_failed"]
            conceptJson <- jsonlite::toJSON(conceptData)
            write(conceptJson, conceptFile)
          }
        }
      }
    }
  } else {
    writeLines(paste("missing data quality result file ", dataQualityResultsFile))
  }

  temporalCharacterizationFile <- file.path(releaseFolder, "temporal-characterization.csv")
  if (file.exists(temporalCharacterizationFile)) {
    temporalCharacterization <- read.csv(temporalCharacterizationFile, header = T)
    writeLines(paste0(nrow(temporalCharacterization), " temporal characterization insights found."))
    # augment achilles concept files with temporal characterization check results
    if (nrow(temporalCharacterization) > 0) {
      for (row in 1:nrow(temporalCharacterization)) {
        writeLines(paste0(row, "/", nrow(temporalCharacterization), " - inserting temporal characterization details"))
        concept_id <- temporalCharacterization[row, "CONCEPT_ID"]
        is_stationary <- temporalCharacterization[row, "IS_STATIONARY"]
        seasonality_score <- temporalCharacterization[row, "SEASONALITY_SCORE"]
        if (format == "duckdb") {
          table <- table %>% mutate(IS_STATIONARY = ifelse(CONCEPT_ID == concept_id, is_stationary, IS_STATIONARY))
          table <- table %>% mutate(SEASONALITY_SCORE = ifelse(CONCEPT_ID == concept_id, seasonality_score, SEASONALITY_SCORE))
          DBI::dbWriteTable(duckdbCon, DBI::Id(schema = "concepts", table = "concept_metadata"), table, overwrite = TRUE)
        }
        if (format == "json") {
          conceptFileName <- paste0("concept_", trimws(concept_id), ".json")
          conceptFile <- file.path(releaseFolder, "concepts", trimws(tolower(temporalCharacterization[row, "CDM_TABLE_NAME"])), conceptFileName)
          if (file.exists(conceptFile)) {
            conceptContent <- readLines(conceptFile)
            conceptData <- jsonlite::fromJSON(conceptContent)
            conceptData$IS_STATIONARY <- is_stationary
            conceptData$SEASONALITY_SCORE <- seasonality_score
            conceptJson <- jsonlite::toJSON(conceptData)
            write(conceptJson, conceptFile)
          }
        }
      }
    }
  } else {
    writeLines(paste("missing temporal characterization data ", temporalCharacterizationFile))
  }
}
