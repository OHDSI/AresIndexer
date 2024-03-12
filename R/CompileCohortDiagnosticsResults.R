#' compileCohortDiagnosticsResult
#'
#' #' A short description of what the function does.
#' @export
#' @details Compile CohortsDiagnostics results into files compatible with Ares
#' @param sourceFolders  A vector of folder locations that contain the files
#' exported from Achilles in the ARES Option format


compileCohortDiagnosticsResult <- function(sourceFolders) {

  releaseFolders <- list.dirs(sourceFolders, recursive = F)

  for (releaseFolder in releaseFolders) {
    cohortsFile <- file.path(releaseFolder, 'cohortDiagnosticsResults', "cohort.csv")
    cohortCountsFile <- file.path(releaseFolder, 'cohortDiagnosticsResults', "cohort_count.csv")
    temporalCovariateValueFile <- file.path(releaseFolder, 'cohortDiagnosticsResults', "temporal_covariate_value.csv")
    temporalCovariateRefFile <- file.path(releaseFolder, 'cohortDiagnosticsResults', "temporal_covariate_ref.csv")
    temporalAnalysisRefFile <- file.path(releaseFolder, 'cohortDiagnosticsResults', "temporal_analysis_ref.csv")
    temporalTimeRefFile <- file.path(releaseFolder, 'cohortDiagnosticsResults', "temporal_time_ref.csv")
    temporalCovariateValueDistFile <- file.path(releaseFolder, 'cohortDiagnosticsResults', "temporal_covariate_value_dist.csv")
    indexEventBreakdownFile <- file.path(releaseFolder, 'cohortDiagnosticsResults', "index_event_breakdown.csv")
    conceptFile <- file.path(releaseFolder, 'cohortDiagnosticsResults', 'concept.csv')


    cohortsData <- read.csv(cohortsFile)
    cohortsCount <- read.csv(cohortCountsFile)
    temporalCovariateValueData <- read.csv(temporalCovariateValueFile)
    temporalCovariateRefData <- read.csv(temporalCovariateRefFile)
    temporalAnalysisRefData <- read.csv(temporalAnalysisRefFile)
    temporalTimeRefData <- read.csv(temporalTimeRefFile)
    temporalCovariateValueDistData <- read.csv(temporalCovariateValueDistFile)
    indexEventBreakdownData <- read.csv(indexEventBreakdownFile)
    conceptData <- read.csv(conceptFile)

    # Merge data with reference files
    cohortsTable <- cohortsData %>%
      left_join(cohortsCount, by = "cohort_id")

    cohortsCharacterizationTable <- temporalCovariateValueData %>%
      left_join(temporalCovariateRefData, by = "covariate_id") %>%
      left_join(temporalAnalysisRefData, by = "analysis_id") %>%
      left_join(cohortsData %>% select(cohort_name, cohort_id), by = "cohort_id") %>%
      left_join(temporalTimeRefData, by = "time_id") %>%
      mutate(temporal_choice = paste("T", "(", start_day, "to", end_day, ")", sep = " "))

    cohortsIndexEventBreakdownTable <- indexEventBreakdownData %>%
      left_join(cohortsData %>% select(cohort_name, cohort_id), by = "cohort_id") %>%
      left_join(conceptData %>% select(concept_name, concept_id, vocabulary_id), by = "concept_id")

    temporalCovariateValueDistTable <- temporalCovariateValueDistData %>%
      left_join(cohortsData %>% select(cohort_name, cohort_id), by = "cohort_id") %>%
      left_join(temporalTimeRefData, by = "time_id") %>%
      left_join(temporalCovariateRefData %>% select(covariate_name, covariate_id), by = "covariate_id")


    # Write the final data to a new CSV file
    write.csv(cohortsTable, file.path(releaseFolder, "cohort_index.csv"), row.names = FALSE)
    write.csv(cohortsCharacterizationTable, file.path(releaseFolder, "cohort_characterization.csv"), row.names = FALSE)
    write.csv(cohortsIndexEventBreakdownTable, file.path(releaseFolder, "cohort_index_event_breakdown.csv"), row.names = FALSE)
    write.csv(temporalCovariateValueDistTable, file.path(releaseFolder, "cohort_temporal_covariate_distribution.csv"), row.names = FALSE)
  }
}
