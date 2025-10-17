# @file augmentDataQualityFiles.R
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

#' Augment Data Quality Files

#' @name augmentDataQualityFiles
#' @details Updates dq-result.json files with information on changes from the previous release
#'
#' @param sourceFolders A vector of folder locations that contain the files
#' exported from Achilles in the ARES Option format (Achilles::exportAO)
#' to be included in the network data quality index.
#'
#' @import jsonlite
#' @import dplyr
#' @importFrom rlang .data
#'
#' @export
augmentDataQualityFiles <- function(sourceFolders) {
  loadDataQualityFile <- function(dir) {
    filePath <- file.path(dir, "dq-result.json")
    fromJSON(filePath, simplifyVector = TRUE)
  }

  isFailure <- function(failed, isError = NA, passed = NA, notApplicable = NA) {
    dplyr::case_when(
      !is.na(isError) & isError == 1 ~ TRUE,
      !is.na(passed) & passed == 1 ~ FALSE,
      !is.na(notApplicable) & notApplicable == 1 ~ FALSE,
      failed == 1 ~ TRUE,
      TRUE ~ FALSE
    )
  }

  for (sourceFolder in sourceFolders) {
    writeLines(paste0('Augmenting data quality files for: ', basename(sourceFolder)))
    releases <- list.dirs(sourceFolder, recursive = FALSE)
    loadedData <- list()

    for (i in seq_along(releases)) {
      currentReleaseName <- basename(releases[[i]])
      writeLines(paste0('Processing issues delta for release: ', currentReleaseName))
      currentQualityFile <- loadDataQualityFile(releases[i])
      currentChecks <- currentQualityFile$CheckResults
      currentChecks$checkId <- as.character(currentChecks$checkId)

      if ("delta" %in% names(currentChecks)) {
        currentChecks$delta <- NULL
      }

      originalColumns <- names(currentChecks)

      loadedData[[length(loadedData) + 1]] <- currentChecks

      if (length(loadedData) > 1) {
        previousData <- loadedData[[length(loadedData) - 1]]
        previousData$checkId <- as.character(previousData$checkId)

        if ("delta" %in% names(previousData)) {
          previousData$delta <- NULL
        }

        missingColsCurr <- setdiff(names(previousData), names(currentChecks))
        missingColsPrev <- setdiff(names(currentChecks), names(previousData))

        for (col in missingColsCurr) {
          currentChecks[[col]] <- NA
        }
        for (col in missingColsPrev) {
          previousData[[col]] <- NA
        }

        currentChecks <- currentChecks[, sort(names(currentChecks))]
        previousData <- previousData[, sort(names(previousData))]

        mergedData <- currentChecks %>%
          dplyr::left_join(previousData, by = "checkId", suffix = c("", "_previous")) %>%
          dplyr::mutate(
            failed_now = isFailure(
              .data$failed,
              if("isError" %in% names(.)) .data$isError else NA,
              if("passed" %in% names(.)) .data$passed else NA,
              if("notApplicable" %in% names(.)) .data$notApplicable else NA
            ),

            failed_prev = isFailure(
              .data$failed_previous,
              if("isError_previous" %in% names(.)) .data$isError_previous else NA,
              if("passed_previous" %in% names(.)) .data$passed_previous else NA,
              if("notApplicable_previous" %in% names(.)) .data$notApplicable_previous else NA
            ),

            delta = dplyr::case_when(
              is.na(.data$failed_previous) & failed_now ~ "NEW",
              failed_now  & !failed_prev ~ "NEW",
              failed_now  &  failed_prev ~ "EXISTING",
              !failed_now &  failed_prev ~ "RESOLVED",
              !failed_now & !failed_prev ~ "STABLE",
              TRUE ~ "STABLE"
            )
          )

        columnsToKeep <- c(originalColumns, "delta")
        mergedData <- mergedData[, columnsToKeep[columnsToKeep %in% names(mergedData)]]

        currentQualityFile$CheckResults <- mergedData

      } else {
        currentChecks <- currentChecks %>%
          dplyr::mutate(
            is_failed = isFailure(
              .data$failed,
              if("isError" %in% names(.)) .data$isError else NA,
              if("passed" %in% names(.)) .data$passed else NA,
              if("notApplicable" %in% names(.)) .data$notApplicable else NA
            ),
            delta = ifelse(is_failed, "NEW", "STABLE")
          ) %>%
          dplyr::select(-is_failed)

        currentQualityFile$CheckResults <- currentChecks
      }

      json <- jsonlite::toJSON(currentQualityFile, pretty = TRUE, auto_unbox = TRUE, null = "null")
      write(json, file.path(releases[i], "dq-result.json"))

      if (length(loadedData) > 2) {
        loadedData <- loadedData[-1]
      }
    }
  }
}