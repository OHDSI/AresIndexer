# @file BuildNetworkPerformanceIndex
#
#
# Copyright 2022 Observational Health Data Sciences and Informatics
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

#' Build Network Performance Index
#'
#' @name buildNetworkPerformanceIndex
#'
#' @details Builds an index with network performance results for Achilles
#' and DQD execution across all source folders.
#' @param sourceFolder Path to source folder
#'
#' @return Network performance results object.
#'
#' @import jsonlite
#' @import dplyr
#' @import stringr
#' @importFrom rlang .data
#'
#' @export
buildNetworkPerformanceIndex <-
  function(sourceFolder) {

    options(dplyr.summarise.inform = FALSE)
    networkIndex <- data.frame()
    analysisDetails <-
      Achilles::getAnalysisDetails() %>%
      ## different versions of Achilles may use upper or lower case column names
      dplyr::rename_with(toupper) %>%
      dplyr::select(c("ANALYSIS_ID", "CATEGORY")) %>%
      dplyr::rename(dplyr::all_of(c(TASK = "ANALYSIS_ID")))
      releaseFolders <- list.dirs(sourceFolder, recursive = F)
      latestRelease <- max(releaseFolders)

            dataQualityResultsFile <- file.path(latestRelease, "dq-result.json")
            dataQualityResultsFileExists <- file.exists(dataQualityResultsFile)
            if (FALSE == dataQualityResultsFileExists) {
              writeLines(paste("missing data quality result file: ",dataQualityResultsFile))
            }

            achillesPerformanceFile <- file.path(latestRelease, "achilles-performance.csv")
            achillesPerformanceFileExists <- file.exists(achillesPerformanceFile)
            if (FALSE == achillesPerformanceFileExists) {
              writeLines(paste("missing achilles performance file: ",achillesPerformanceFile))
            }

            if (dataQualityResultsFileExists & achillesPerformanceFileExists) {
              dqdData <- jsonlite::fromJSON(dataQualityResultsFile)
              dqdData <- as.data.frame(dqdData)

              performanceData <- read.csv(achillesPerformanceFile)

              performanceTable <- dplyr::select(performanceData, c("analysis_id", "elapsed_seconds")) %>%
                rename(dplyr::all_of(c(TASK = "analysis_id", TIMING = "elapsed_seconds"))) %>% mutate(PACKAGE = "ACHILLES")

              performanceTable <- merge(x=performanceTable,y=analysisDetails,by="TASK",all.x=TRUE)

              dqdTable <- dplyr::select(dqdData, c("CheckResults.checkId", "CheckResults.executionTime", "CheckResults.category")) %>%
                rename(dplyr::all_of(c(TASK = "CheckResults.checkId", TIMING = "CheckResults.executionTime", CATEGORY = "CheckResults.category"))) %>% mutate(PACKAGE = "DQD") %>%
                mutate_at("TIMING", str_replace, " secs", "")

              names(performanceTable) <- toupper(names(performanceTable))
              names(dqdTable) <- toupper(names(dqdTable))

              mergedTable <- rbind(performanceTable, dqdTable)

              mergedTable <- mergedTable  %>%
                mutate(SOURCE = basename(sourceFolder))

              networkIndex <- rbind(networkIndex, mergedTable)
            }


    return(networkIndex)
  }
