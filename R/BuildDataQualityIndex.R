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

#' Data Quality Stratified Network Index
#'
#' @details
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
#'
#' @export
buildDataQualityIndex <- function(sourceFolders, outputFolder) {
  networkIndex <- data.frame()

  # iterate on sources
  for (sourceFolder in sourceFolders) {
    historicalIndex <- AresIndexer::buildDataQualityHistoryIndex(sourceFolder)
    historicalFile <- file.path(sourceFolder, "data-quality-index.json")
    write(jsonlite::toJSON(historicalIndex),historicalFile)


    releaseFolders <- list.dirs(sourceFolder, recursive = F)
    # iterate on source releases and process all failed results
    for (releaseFolder in releaseFolders) {
      dataQualityResultsFile <- file.path(releaseFolder, "dq-result.json")

      # process each data quality result file
      if (file.exists(dataQualityResultsFile)) {
        dataQualityResults <- jsonlite::fromJSON(dataQualityResultsFile)
        results <- dataQualityResults$CheckResults

        # for each release, generate a summary of failures by cdm_table_name
        domainAggregates <- results %>% filter(FAILED==1) %>% count(tolower(CDM_TABLE_NAME))
        names(domainAggregates) <- c("cdm_table_name", "count_failed")
        data.table::fwrite(domainAggregates, file.path(releaseFolder,"domain-issues.csv"))

        # collect all failures from this result file for network analysis
        sourceFailures <- results[results[,"FAILED"]==1,c("CHECK_NAME", "CHECK_LEVEL", "CDM_TABLE_NAME", "CATEGORY", "SUBCATEGORY", "CONTEXT", "CDM_FIELD_NAME", "CONCEPT_ID", "UNIT_CONCEPT_ID")]
        sourceFailures$CDM_SOURCE_NAME <- dataQualityResults$Metadata$CDM_SOURCE_NAME
        sourceFailures$CDM_SOURCE_ABBREVIATION <- dataQualityResults$Metadata$CDM_SOURCE_ABBREVIATION
        sourceFailures$CDM_SOURCE_KEY <- gsub(" ","_",dataQualityResults$Metadata$CDM_SOURCE_ABBREVIATION)
        sourceFailures$RELEASE_NAME <- format(lubridate::ymd(dataQualityResults$Metadata$CDM_RELEASE_DATE),"%Y-%m-%d")
        sourceFailures$RELEASE_ID <- format(lubridate::ymd(dataQualityResults$Metadata$CDM_RELEASE_DATE),"%Y%m%d")
        networkIndex <- rbind(networkIndex, sourceFailures)
      } else {
        writeLines(paste("missing data quality result file ",dataQualityResultsFile))
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
