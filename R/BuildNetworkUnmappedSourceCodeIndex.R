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
#' @param sourceFolders A vector of data source folders
#'
#' @param outputFolder Location of the Ares data folder
#' @return
#' A data frame with a the network unmapped source code index
#'
#' @import jsonlite
#' @import dplyr
#' @importFrom data.table fwrite
#'
#' @export
buildNetworkUnmappedSourceCodeIndex <-
  function(sourceFolders, outputFolder) {
    # this should contain a line per source code, count of the number of data sources that list it as unmapped, and total
    # of unmapped records across all databases.  It should have a link back to the data source release key where it came from.

    # iterate on sources
    networkIndex <- data.frame()
    for (sourceFolder in sourceFolders) {
      # find the latest release in the source folder
      releaseFolders <- list.dirs(sourceFolder, recursive = F)
      releaseFolders <- sort(releaseFolders, decreasing = T)
      if (length(releaseFolders) > 0) {

      }
      latestReleaseFolder <- releaseFolders[1]
      completenessFile <-
        file.path(latestReleaseFolder, "quality-completeness.csv")
      cdmsourceFile <-
        file.path(latestReleaseFolder, "cdmsource.csv")
      if (file.exists(cdmSourceFile)) {
        if (file.exists(completenessFile)) {
          cdmSourceData <- read.csv(cdmsourceFile)

          completenessData <- read.csv(completenessFile)
          completenessData$NETWORK_SOURCE <- completenessFile
          networkIndex <- rbind(networkIndex, completenessData)
        }
      }
    }

    data.table::fwrite(latestResults,
                       file.path(outputFolder, "network-unmapped-source-codes.csv"))
    invisible(latestResults)
  }
