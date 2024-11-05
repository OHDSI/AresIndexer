# @file BuildDataQualityIndex.R
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

#' Build the Data Quality Index
#'
#' @details Creates a data quality stratified network index
#'
#' @param sourceFolders A vector of folder locations that contain the dq-result.json files
#' to be included in the network data quality index.
#'
#' @param outputFolder Location of the Ares data folder
#' @return
#' A data frame with a the necessary network data quality data
#'
#' @import jsonlite
#' @import dplyr
#' @importFrom data.table fwrite
#' @importFrom rlang .data
#'
#' @export
buildDataQualityIndex <- function(sourceFolders, outputFolder) {
  networkIndex <- data.frame()

  # iterate on sources
  for (sourceFolder in sourceFolders) {

    # skip index for source if ignore file present
    if (file.exists(file.path(sourceFolder,".aresIndexIgnore"))){

      writeLines(paste("AresIndexIgnore file present, skipping source folder: ", sourceFolder))
    }else
    {
      historicalIndex <- AresIndexer::buildDataQualityHistoryIndex(sourceFolder)
      historicalFile <- file.path(sourceFolder, "data-quality-index.json")
      write(jsonlite::toJSON(historicalIndex),historicalFile)


      releaseFolders <- list.dirs(sourceFolder, recursive = F)
      # iterate on source releases and process all failed results
      for (releaseFolder in releaseFolders) {

        # skip index for release if ignore file present

        if (releaseFolder %in% AresIndexer::getIgnoredReleases(sourceFolder)){

          writeLines(paste("AresIndexIgnore file present, skipping release folder: ", releaseFolder))

        }else {

          dataQualityResultsFile <- file.path(releaseFolder, "dq-result.json")

          # process each data quality result file
          if (file.exists(dataQualityResultsFile)) {
            dataQualityResults <- jsonlite::fromJSON(dataQualityResultsFile)
            results <- dataQualityResults$CheckResults

            # for each release, generate a summary of failures by cdm_table_name
            domainAggregates <- results %>% filter(.data$failed==1) %>% count(tolower(.data$cdmTableName))
            names(domainAggregates) <- c("cdm_table_name", "count_failed")
            data.table::fwrite(domainAggregates, file.path(releaseFolder,"domain-issues.csv"))

            # collect all failures from this result file for network analysis
            outColNames <- c("checkName", "checkLevel", "cdmTableName", "category", "subcategory", "context", "cdmFieldName", "conceptId", "unitConceptId")
            missingColNames <- setdiff(outColNames, names(results))
            for (colName in missingColNames) {
              writeLines(paste0("Expected column is missing in DQD results. Adding column with NA values: ", colName))
              results[,colName] <- NA
            }
            sourceFailures <- results[results[,"failed"]==1,outColNames]
            sourceFailures$CDM_SOURCE_NAME <- dataQualityResults$Metadata$cdmSourceName
            sourceFailures$CDM_SOURCE_ABBREVIATION <- dataQualityResults$Metadata$cdmSourceAbbreviation
            sourceFailures$CDM_SOURCE_KEY <- gsub(" ","_",dataQualityResults$Metadata$cdmSourceAbbreviation)
            sourceFailures$RELEASE_NAME <- format(lubridate::ymd(dataQualityResults$Metadata$cdmReleaseDate),"%Y-%m-%d")
            sourceFailures$RELEASE_ID <- format(lubridate::ymd(dataQualityResults$Metadata$cdmReleaseDate),"%Y%m%d")
            networkIndex <- rbind(networkIndex, sourceFailures)
          } else {
            writeLines(paste("missing data quality result file ",dataQualityResultsFile))
          }
        }
      }
    }
  }

  # limit network analysis to the latest release per database
  latestResults <- data.frame(row.names = F)
  sources <- unique(networkIndex$CDM_SOURCE_KEY)
  for (source in sources) {
    latestRelease <- max(networkIndex[networkIndex$CDM_SOURCE_KEY==source,"RELEASE_ID"])
    latestResults <- rbind(latestResults,networkIndex[networkIndex$CDM_SOURCE_KEY==source & networkIndex$RELEASE_ID==latestRelease,])
  }
  data.table::fwrite(latestResults, file.path(outputFolder, "network-data-quality-summary.csv"))
  invisible(latestResults)
}
