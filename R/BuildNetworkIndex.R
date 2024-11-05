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

#' Build Network Index
#'
#' @details Generates a network index containing data source history across releases including performance, and data quality metrics.
#'
#' @param sourceFolders A vector of folder locations that contain the files
#' exported from Achilles in the ARES Option format (Achilles::exportAO)
#' to be included in the network data quality index.
#'
#' @param outputFolder The location of the Ares data folder.
#'
#' @return The JSON object that is written to the ARES data folder (outputFolder parameter).
#'
#'
#' @importFrom data.table data.table
#' @importFrom rlang .data
#'
#' @export
buildNetworkIndex <- function(sourceFolders, outputFolder) {
	index <- {}
	index$sources <- list()

	sourceCount <- 0

	writeLines("Generating export query index")
	AresIndexer::buildExportQueryIndex(outputFolder)
	networkPerformanceIndex <- data.frame()

	# iterate on sources
	for (sourceFolder in sourceFolders) {
		writeLines(paste("processing source folder: ", sourceFolder))

	  # skip index for source if ignore file present
	  if (file.exists(file.path(sourceFolder,".aresIndexIgnore"))){

	    writeLines(paste("AresIndexIgnore file present, skipping source folder: ", sourceFolder))
	  }else
	  {

	  source <- {}

		sourceCount <- sourceCount + 1
		releaseCount <- 0

		source$releases <- data.table()
		releaseFolders <- list.dirs(sourceFolder, recursive = F)
		releaseFolders <- sort(releaseFolders, decreasing = T)

		# as part of building the network index - build the history index for this source
		writeLines(paste("processing data source history index: ", sourceFolder))
		dataSourceHistoryIndex <- AresIndexer::buildDataSourceHistoryIndex(sourceFolder)
		write(jsonlite::toJSON(dataSourceHistoryIndex), file.path(sourceFolder,"data-source-history-index.json"))

		writeLines(paste("processing network performance index", sourceFolder))
		networkPerformanceIndex <- rbind(networkPerformanceIndex, buildNetworkPerformanceIndex(sourceFolder))


		releaseIntervalData <- data.frame()

		# iterate on source releases
		for (releaseFolder in releaseFolders) {
		  writeLines(paste("processing release folder: ", releaseFolder))

		  # skip index for release if ignore file present
		  if (releaseFolder %in% AresIndexer::getIgnoredReleases(sourceFolder)){

		    writeLines(paste("AresIndexIgnore file present, skipping release folder: ", releaseFolder))

		  }else {

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

  				source$cdm_source_name <- dataQualityResults$Metadata$cdmSourceName
  				source$cdm_source_abbreviation <- dataQualityResults$Metadata$cdmSourceAbbreviation
  				source$cdm_source_key <- gsub(" ", "_", source$cdm_source_abbreviation)
  				source$cdm_holder <- dataQualityResults$Metadata$cdmHolder
  				source$source_description <- dataQualityResults$Metadata$sourceDescription

          source$releases <- rbind(
            source$releases,
            list(
              release_name = format(lubridate::ymd(dataQualityResults$Metadata$cdmReleaseDate),"%Y-%m-%d"),
              release_id = format(lubridate::ymd(dataQualityResults$Metadata$cdmReleaseDate),"%Y%m%d"),
              cdm_version = dataQualityResults$Metadata$cdmVersion,
              vocabulary_version = dataQualityResults$Metadata$vocabularyVersion,
              dqd_version = dataQualityResults$Metadata$dqdVersion,
              count_data_quality_issues = dataQualityResults$Overview$countOverallFailed,
              count_data_quality_checks = dataQualityResults$Overview$countTotal,
              dqd_execution_date = format(lubridate::ymd_hms(dataQualityResults$endTimestamp),"%Y-%m-%d"),
              count_person = count_person,
              obs_period_start = format(lubridate::ym(obs_period_start),"%Y-%m"),
              obs_period_end = format(lubridate::ym(obs_period_end),"%Y-%m")
            )
          )
  			} else {
  				writeLines(paste("missing data quality result file ",dataQualityResultsFile))
  			}

        cdmSourceFile <- file.path(releaseFolder, "cdmsource.csv")
        if (file.exists(cdmSourceFile)) {
          cdmSourceData <- read.csv(cdmSourceFile)
          releaseIntervalData <- rbind(releaseIntervalData, cdmSourceData)
        }

    		averageUpdateIntervalDays <- "n/a"
    		if (nrow(releaseIntervalData) > 1 ) {
      		processedIndex <- releaseIntervalData %>%
      		  mutate(DAYS_ELAPSED = as.Date(.data$CDM_RELEASE_DATE) - lag(as.Date(.data$CDM_RELEASE_DATE))) %>%
      		  filter(!is.na(.data$DAYS_ELAPSED))

      		averageUpdateIntervalDays <- round(as.numeric(abs(mean(processedIndex$DAYS_ELAPSED)), units="days"))
    		}

    		source$releases <- source$releases[order(-source$releases$dqd_execution_date)]
    		source$count_releases <- nrow(source$releases)
    		source$average_update_interval_days <- averageUpdateIntervalDays
    		index$sources[[sourceCount]] <- source
		  }
  }

	indexJson <- jsonlite::toJSON(index,auto_unbox = T)
	write(indexJson, file.path(outputFolder,"index.json"))
	if(file.exists(file.path(outputFolder, "network-performance.csv"))) {
	  file.remove(file.path(outputFolder, "network-performance.csv"))
	}
	data.table::fwrite(networkPerformanceIndex, file=paste0(outputFolder, "/network-performance.csv"))
  invisible(indexJson)
	  }
	}
}
