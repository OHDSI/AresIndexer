# @file BuildNetworkIndex.R
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

#' General Network Index
#'
#' @details
#'
#'
#' @param sourceFolders A vector of folder locations that contain the files
#' exported from Achilles in the ARES Option format (Achilles::exportAO)
#' to be included in the network data quality index.
#'
#' @param outputFolder The location of the Ares data folder.
#'
#' @return
#' The JSON object that is written to the ARES data folder (outputFolder parameter).
#'
#' @importFrom data.table data.table
#'
#' @export
buildNetworkIndex <- function(sourceFolders, outputFolder) {
	index <- {}
	index$sources <- list()

	sourceCount <- 0
	# iterate on sources
	for (sourceFolder in sourceFolders) {
		writeLines(paste("processing source folder: ", sourceFolder))
	  source <- {}

		sourceCount <- sourceCount + 1
		releaseCount <- 0

		source$releases <- data.table()
		releaseFolders <- list.dirs(sourceFolder, recursive = F)

		# as part of building the network index - build the history index for this source
		writeLines(paste("processing data source history index: ", sourceFolder))
		dataSourceHistoryIndex <- AresIndexer::buildDataSourceHistoryIndex(sourceFolder)
		write(jsonlite::toJSON(dataSourceHistoryIndex), file.path(sourceFolder,"data-source-history-index.json"))

		# iterate on source releases
		for (releaseFolder in releaseFolders) {
		  writeLines(paste("processing release folder: ", releaseFolder))
		  # todo - could make this function more resilient and pull necessary information from achilles export of dqd result is unavailable
			dataQualityResultsFile <- file.path(releaseFolder, "dq-result.json")
      personResultsFile <- file.path(releaseFolder, "person.json")
      observationPeriodResultsFile <- file.path(releaseFolder, "observationperiod.json")

			# add data quality details
			if (file.exists(dataQualityResultsFile)) {
				dataQualityResults <- jsonlite::fromJSON(dataQualityResultsFile)

				# add person results
				if (file.exists(personResultsFile)) {
				  personResults <- jsonlite::fromJSON(personResultsFile)
				  count_person <- sum(personResults$BIRTH_YEAR_DATA$COUNT_PERSON)
				} else {
				  writeLines(paste("missing person results file ", personResultsFile))
				}

				# add observation period results
				if (file.exists(observationPeriodResultsFile)) {
				  observationPeriodResults <- jsonlite::fromJSON(observationPeriodResultsFile)
				  obs_period_start <- min(observationPeriodResults$OBSERVED_BY_MONTH$MONTH_YEAR)
				  obs_period_end <- max(observationPeriodResults$OBSERVED_BY_MONTH$MONTH_YEAR)
				} else {
				  writeLines(paste("missing observation period results file ", observationPeriodResultsFile))
				}

				source$cdm_source_name <- dataQualityResults$Metadata$CDM_SOURCE_NAME
				source$cdm_source_abbreviation <- dataQualityResults$Metadata$CDM_SOURCE_ABBREVIATION
				source$cdm_source_key <- gsub(" ", "_", source$cdm_source_abbreviation)
				source$cdm_holder <- dataQualityResults$Metadata$CDM_HOLDER
				source$source_description <- dataQualityResults$Metadata$SOURCE_DESCRIPTION

        source$releases <- rbind(
          source$releases,
          list(
            release_name = format(lubridate::ymd(dataQualityResults$Metadata$CDM_RELEASE_DATE),"%Y-%m-%d"),
            release_id = format(lubridate::ymd(dataQualityResults$Metadata$CDM_RELEASE_DATE),"%Y%m%d"),
            cdm_version = dataQualityResults$Metadata$CDM_VERSION,
            vocabulary_version = dataQualityResults$Metadata$VOCABULARY_VERSION,
            dqd_version = dataQualityResults$Metadata$DQD_VERSION,
            count_data_quality_issues = dataQualityResults$Overview$countOverallFailed,
            count_data_quality_checks = dataQualityResults$Overview$countTotal,
            dqd_execution_date = format(lubridate::ymd_hms(dataQualityResults$endTimestamp),"%Y-%m-%d"),
            count_person = count_person,
            obs_period_start = paste0(toupper(lubridate::month(lubridate::ym(obs_period_start), label=TRUE, abbr=TRUE)),"-",format(lubridate::ym(obs_period_start),"%Y")),
            obs_period_end = paste0(toupper(lubridate::month(lubridate::ym(obs_period_end), label=TRUE, abbr=TRUE)),"-",format(lubridate::ym(obs_period_end),"%Y"))
          )
        )
			} else {
				writeLines(paste("missing data quality result file ",dataQualityResultsFile))
			}
		}
		source$releases <- source$releases[order(-dqd_execution_date)]
		source$count_releases <- nrow(source$releases)
		index$sources[[sourceCount]] <- source
	}

	indexJson <- jsonlite::toJSON(index,auto_unbox = T)
	write(indexJson, file.path(outputFolder,"index.json"))
  invisible(indexJson)
}