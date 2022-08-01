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

#' @name buildNetworkPerformanceIndex
#'
#' @details Builds an index with network performance results across all source folders.
#' @param sourceFolder Path to source folder
#'
#' @return Network performance results object.
#'
#' @import jsonlite
#' @import dplyr
#' @import stringr
#'
#' @export
library(data.table)
buildNetworkPerformanceIndex <-
  function(sourceFolder) {

    options(dplyr.summarise.inform = FALSE)
    networkIndex <- data.frame()
    analysisDetails <- dplyr::select(Achilles::getAnalysisDetails(), c("ANALYSIS_ID", "CATEGORY")) %>%
      rename(TASK = ANALYSIS_ID)
      releaseFolders <- list.dirs(sourceFolder, recursive = F)
      if (length(releaseFolders) > 0) {
        # iterate through release folders
        for(releaseFolder in releaseFolders) {

            dataQualityResultsFile <- file.path(releaseFolder, "dq-result.json")
            dataQualityResultsFileExists <- file.exists(dataQualityResultsFile)
            if (FALSE == dataQualityResultsFileExists) {
              writeLines(paste("missing data quality result file: ",dataQualityResultsFile))
            }

            achillesPerformanceFile <- file.path(releaseFolder, "achilles-performance.csv")
            achillesPerformanceFileExists <- file.exists(achillesPerformanceFile)
            if (FALSE == achillesPerformanceFileExists) {
              writeLines(paste("missing achilles performance file: ",achillesPerformanceFile))
            }

            if (dataQualityResultsFileExists & achillesPerformanceFileExists) {
              dqdData <- jsonlite::fromJSON(dataQualityResultsFile)
              dqdData <- as.data.frame(dqdData)

              performanceData <- read.csv(achillesPerformanceFile)

              performanceTable <- dplyr::select(performanceData, c("analysis_id", "elapsed_seconds")) %>%
                rename(TASK = analysis_id, TIMING = elapsed_seconds) %>% mutate(PACKAGE = "achilles")

              performanceTable <- merge(x=performanceTable,y=analysisDetails,by="TASK",all.x=TRUE)

              dqdTable <- dplyr::select(dqdData, c("CheckResults.checkId", "CheckResults.EXECUTION_TIME", "CheckResults.CATEGORY")) %>%
                rename(TASK = CheckResults.checkId, TIMING = CheckResults.EXECUTION_TIME, CATEGORY = CheckResults.CATEGORY) %>% mutate(PACKAGE = "dqd") %>%
                mutate_at("TIMING", str_replace, " secs", "")

              mergedTable <- rbind(performanceTable, dqdTable)

              mergedTable <- mergedTable  %>%
                mutate(SOURCE = basename(sourceFolder), RELEASE = basename(releaseFolder))

              networkIndex <- rbind(networkIndex, mergedTable)
            }
        }
      }

    return(networkIndex)
  }
