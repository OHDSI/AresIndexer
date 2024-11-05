# @file ComputeCharacterizationDifference
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

#' Generate characterization difference results.
#'
#' @name ComputeCharacterizationDifference
#'
#' @details Computes characterization difference reports for successive data sources releases of a data source.
#' @param sourceFolders Vector of paths to source folders
#'
#' @return Table of difference results.
#'
#' @import jsonlite
#' @import dplyr
#' @import stringr
#' @importFrom rlang .data
#'
#' @export
computeCharacterizationDifference <-
  function(sourceFolders) {

    for (sourceFolder in sourceFolders) {
      releaseFolders <- list.dirs(sourceFolder, recursive = F)
      releaseCount <- length(releaseFolders)
      releaseFolders <- sort(releaseFolders, decreasing = F)

      #add a loop to iterate through the releases
      baseReleaseIndex <- 1
      while (baseReleaseIndex + 1 <= releaseCount) {
        baseReleaseFolder <- releaseFolders[baseReleaseIndex]
        nextReleaseFolder <- releaseFolders[baseReleaseIndex + 1]

        domains <-
          c(
            "drug_exposure",
            "drug_era",
            "condition_occurrence",
            "condition_era",
            "observation",
            "measurement",
            "device_exposure",
            "procedure_occurrence",
            "visit_occurrence",
            "visit_detail"
          )

        # iterate through domains
        for (domain in domains) {
          writeLines(paste(
            "Comparing",
            baseReleaseFolder,
            nextReleaseFolder,
            domain
          ))
          domainFilename <-
            paste0("domain-summary-", domain, ".csv")
          baseDomainFile <-
            file.path(baseReleaseFolder, domainFilename)
          nextDomainFile <-
            file.path(nextReleaseFolder, domainFilename)

          if (file.exists(baseDomainFile) &&
              file.exists(nextDomainFile)) {

            #limit the columns we read to ensure we're performing clean calculations for diff values
            baseData <- read.csv(baseDomainFile)[, c(
              "CONCEPT_ID",
              "CONCEPT_NAME",
              "NUM_PERSONS",
              "PERCENT_PERSONS",
              "RECORDS_PER_PERSON",
              "PERCENT_PERSONS_NTILE",
              "RECORDS_PER_PERSON_NTILE"
            )]

            nextData <- read.csv(nextDomainFile)[, c(
              "CONCEPT_ID",
              "CONCEPT_NAME",
              "NUM_PERSONS",
              "PERCENT_PERSONS",
              "RECORDS_PER_PERSON",
              "PERCENT_PERSONS_NTILE",
              "RECORDS_PER_PERSON_NTILE"
            )]

            fullData <-
              dplyr::right_join(baseData,
                                nextData,
                                by = "CONCEPT_ID",
                                na_matches = "never") %>%
              mutate_if(is.numeric, coalesce, 0)

            mutatedFullData <- fullData %>% mutate(
              DIFF_NUM_PERSONS = .data$NUM_PERSONS.y - .data$NUM_PERSONS.x,
              DIFF_PERCENT_PERSONS = format(
                round(.data$PERCENT_PERSONS.y - .data$PERCENT_PERSONS.x, 4),
                nsmall = 4
              ),
              DIFF_RECORDS_PER_PERSON = format(
                round(.data$RECORDS_PER_PERSON.y - .data$RECORDS_PER_PERSON.x,
                      1),
                nsmall = 1
              )
            )

            mutatedLimitedColumns <- mutatedFullData[, c(
              "CONCEPT_ID",
              "CONCEPT_NAME.y",
              "NUM_PERSONS.y",
              "PERCENT_PERSONS.y",
              "RECORDS_PER_PERSON.y",
              "PERCENT_PERSONS_NTILE.y",
              "RECORDS_PER_PERSON_NTILE.y",
              "DIFF_NUM_PERSONS",
              "DIFF_PERCENT_PERSONS",
              "DIFF_RECORDS_PER_PERSON"
            )]

            colnames(mutatedLimitedColumns) <-
              c(
                "CONCEPT_ID",
                "CONCEPT_NAME",
                "NUM_PERSONS",
                "PERCENT_PERSONS",
                "RECORDS_PER_PERSON",
                "PERCENT_PERSONS_NTILE",
                "RECORDS_PER_PERSON_NTILE",
                "DIFF_NUM_PERSONS",
                "DIFF_PERCENT_PERSONS",
                "DIFF_RECORDS_PER_PERSON"
              )

            mutatedLimitedColumns$PERCENT_PERSONS <- format(round(mutatedLimitedColumns$PERCENT_PERSONS,4), nsmall=4)

            # write out augmented summary file
            differenceFilename <-
              paste0("domain-summary-", domain, ".csv")
            data.table::fwrite(mutatedLimitedColumns,
                               file.path(nextReleaseFolder, differenceFilename))

          } # end if check for domain file existence
        } #end domain loop

        baseReleaseIndex <- baseReleaseIndex + 1
      } # end while loop
    } # end source loop
  }
