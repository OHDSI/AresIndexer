# @file BuildNetworkUnmappedSourceCodeIndex
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

#' Build Unmapped Source Code Network Index
#'
#' @details Builds an aggregate index of the unmapped source codes across all source folders.
#'
#' @param sourceFolders A vector of data source folders
#'
#' @param outputFolder Location of the Ares data folder
#' @return
#' A data frame with a the network unmapped source code index
#'
#' @import jsonlite
#' @import dplyr
#' @import stringr
#' @importFrom data.table fwrite
#' @importFrom utils read.csv
#' @importFrom rlang .data
#'
#' @export
buildNetworkUnmappedSourceCodeIndex <-
  function(sourceFolders, outputFolder) {

    options(dplyr.summarise.inform = FALSE)

    # this should contain a line per source code, count of the number of data sources that list it as unmapped, and total
    # of unmapped records across all databases.  It should have a link back to the data source release key where it came from.

    # iterate on sources
    networkIndex <- data.frame()
    for (sourceFolder in sourceFolders) {

      # skip index for source if ignore file present

      if (file.exists(file.path(sourceFolder,".aresIndexIgnore"))){

        writeLines(paste("AresIndexIngore file present, skipping source folder: ", sourceFolder))
      } else {
        # find the latest release in the source folder
        releaseFolders <- list.dirs(sourceFolder, recursive = F)
        releaseFolders <- sort(releaseFolders, decreasing = T)

        # skip for releases where ignore file present
        releaseFolders <- releaseFolders[!releaseFolders %in% AresIndexer::getIgnoredReleases(sourceFolder)]

        if (length(releaseFolders) > 0) {
          latestReleaseFolder <- releaseFolders[1]
          completenessFile <-
            file.path(latestReleaseFolder, "quality-completeness.csv")
          cdmSourceFile <-
            file.path(latestReleaseFolder, "cdmsource.csv")
          if (file.exists(cdmSourceFile)) {
            if (file.exists(completenessFile)) {
              cdmSourceData <- read.csv(cdmSourceFile)
              completenessData <- read.csv(completenessFile)
              withSourceValue <- dplyr::filter(completenessData, nchar(stringr::str_trim(completenessData$SOURCE_VALUE))>0)
              if (nrow(withSourceValue >0)) {
                withSourceValue$DATA_SOURCE <- cdmSourceData$CDM_SOURCE_ABBREVIATION
                networkIndex <- dplyr::bind_rows(networkIndex, withSourceValue)
              }
            }
          }
        }
      }
    }

    networkResults <-
      networkIndex %>%
      group_by(.data$CDM_TABLE_NAME, .data$CDM_FIELD_NAME, .data$SOURCE_VALUE) %>%
      summarise(
        RECORD_COUNT = sum(.data$RECORD_COUNT),
        DATA_SOURCE_COUNT = n_distinct(.data$DATA_SOURCE),
        DATA_SOURCES = paste(.data$DATA_SOURCE, collapse=",")
      ) %>%
      as.data.frame()

    topResults <- networkResults %>%
      filter(.data$DATA_SOURCE_COUNT > 1) %>%
      slice_max(order_by=.data$RECORD_COUNT,n=100000, with_ties=F)

    data.table::fwrite(topResults,
                       file.path(outputFolder, "network-unmapped-source-codes.csv"))
    invisible(topResults)
  }
