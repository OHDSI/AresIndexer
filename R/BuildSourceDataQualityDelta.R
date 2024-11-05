# @file BuildSourceDataQualityDelta.R
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

#' Build Source Data Quality Delta

#' @details Generates data quality delta file that contains information on changes of issues between releases
#'
#' @param sourceFolders A vector of folder locations that contain the files
#' exported from Achilles in the ARES Option format (Achilles::exportAO)
#' to be included in the network data quality index.
#' @import jsonlite
#' @importFrom utils write.csv

#' @export

buildSourceDataQualityDelta <- function(sourceFolders) {
  statusCountsTemplate <- c(NEW = 0, EXISTING = 0, RESOLVED = 0, STABLE = 0)

  countStatuses <- function(dirPath) {
    statusCounts <- statusCountsTemplate
    releaseName <- NA

    dataQualityPath <- file.path(dirPath, "dq-result.json")
    if (file.exists(dataQualityPath)) {
      data <- fromJSON(dataQualityPath)

      if (!is.null(data$CheckResults)) {
        statuses <- data$CheckResults$delta
        statusCounts <- statusCounts + table(factor(statuses, levels = names(statusCountsTemplate)))
      }

      if (!is.null(data$Metadata$cdmReleaseDate)) {
        releaseName <- data$Metadata$cdmReleaseDate
      }
    }

    return(list(statusCounts = statusCounts, releaseName = releaseName))
  }

  for (sourceFolder in sourceFolders) {
    writeLines(paste0('Building source quality delta for: ', sourceFolder))
    releases <- list.dirs(sourceFolder, full.names = TRUE, recursive = FALSE)
    sourceCounts <- data.frame()

    for (release in releases) {
      result <- countStatuses(release)
      counts <- result$statusCounts
      releaseName <- result$releaseName

      if (is.na(releaseName)) {
        releaseName <- basename(release)
      }

      counts <- c(releaseName, as.numeric(counts))

      countsDf <- as.data.frame(t(counts), stringsAsFactors = FALSE)
      colnames(countsDf) <- c("release", names(statusCountsTemplate))
      sourceCounts <- rbind(sourceCounts, countsDf)
    }
    utils::write.csv(sourceCounts, file = file.path(sourceFolder, 'data-quality-delta.csv'), row.names = FALSE)
  }
}
