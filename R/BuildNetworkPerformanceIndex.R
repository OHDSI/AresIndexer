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

#' Network Performance Index
#'
#' @details
#' @name buildNetworkPerformanceIndex
#' @param sourceFolder Path to source folder
#' @param sourceFolder
#'
#' @return
#'
#' @import jsonlite
#' @import dplyr
#' @import stringr
#'
#' @export
library(data.table)
buildNetworkPerformanceIndex <-
  function(sourceFolder, outputFolder) {
    dir.create(file.path(outputFolder, "logs"))
    dir.create(file.path(paste(outputFolder, "logs", sep="/"), basename(sourceFolder)))
    options(dplyr.summarise.inform = FALSE)
    networkIndex <- data.frame()
    analysisDetails <- dplyr::select(Achilles::getAnalysisDetails(), c("ANALYSIS_ID", "CATEGORY")) %>%
      rename(TASK = ANALYSIS_ID)
      releaseFolders <- list.dirs(sourceFolder, recursive = F)
      if (length(releaseFolders) > 0) {
        # iterate through release folders
        for(releaseFolder in releaseFolders) {
          dir.create(file.path(paste(outputFolder, "logs", basename(sourceFolder), sep = "/"), basename(releaseFolder)))
          logsDirectory <- paste(outputFolder, "logs", basename(sourceFolder), basename(releaseFolder), sep="/")

            dqdData <- jsonlite::fromJSON(file.path(releaseFolder, "dq-result.json"))
            dqdData <- as.data.frame(dqdData)
            performanceData <-
              read.csv(file.path(releaseFolder, "achilles-performance.csv"))

            write.table(performanceData, file = file.path(logsDirectory, paste("read", "performance.csv", sep="_")), sep = ",",
                        quote = FALSE,
                        col.names = TRUE, row.names = FALSE)
            dqdData <- as.data.frame(dqdData)

            write.table(dqdData, file = file.path(logsDirectory, paste("parsed", "dqd.csv", sep="_")), sep = ",",
                        quote = FALSE,
                        col.names = TRUE, row.names = FALSE)

            performanceTable <- dplyr::select(performanceData, c("analysis_id", "elapsed_seconds")) %>%
              rename(TASK = analysis_id, TIMING = elapsed_seconds) %>% mutate(PACKAGE = "achilles")

            write.table(performanceTable, file = file.path(logsDirectory, paste("mutated", "performance.csv", sep="_")), sep = ",",
                        quote = FALSE,
                        col.names = TRUE, row.names = FALSE)

            performanceTable <- merge(x=performanceTable,y=analysisDetails,by="TASK",all.x=TRUE)
            write.table(performanceTable, file = file.path(logsDirectory, paste("merged", "performance.csv", sep="_")), sep = ",",
                        quote = FALSE,
                        col.names = TRUE, row.names = FALSE)

            dqdTable <- dplyr::select(dqdData, c("CheckResults.checkId", "CheckResults.EXECUTION_TIME", "CheckResults.CATEGORY")) %>%
              rename(TASK = CheckResults.checkId, TIMING = CheckResults.EXECUTION_TIME, CATEGORY = CheckResults.CATEGORY) %>% mutate(PACKAGE = "dqd") %>%
              mutate_at("TIMING", str_replace, " secs", "")

            write.table(dqdTable, file = file.path(logsDirectory, paste("mutated", "dqd.csv", sep="_")), sep = ",",
                        quote = FALSE,
                        col.names = TRUE, row.names = FALSE)

            mergedTable <- rbind(performanceTable, dqdTable)
            write.table(mergedTable, file = file.path(logsDirectory, paste("merged", "table.csv", sep="_")), sep = ",",
                        quote = FALSE,
                        col.names = TRUE, row.names = FALSE)

            mergedTable <- mergedTable  %>%
              mutate(SOURCE = basename(sourceFolder), RELEASE = basename(releaseFolder))

            write.table(mergedTable, file = file.path(logsDirectory, paste("mutated", "table.csv", sep="_")), sep = ",",
                        quote = FALSE,
                        col.names = TRUE, row.names = FALSE)

            networkIndex <- rbind(networkIndex, mergedTable)

        }
      }

    return(networkIndex)
  }
