library(CohortGenerator)
library(FeatureExtraction)
library(CohortDiagnostics)
library(DatabaseConnector)
library(dplyr)
library(tools)
library(arrow)

## Generates cohorts for given definitions and uses them to populate data
## required for cohort report
buildAresCohortReport <- function(
    connectionDetails,
    cdmDatabaseSchema,
    vocabularyDatabaseSchema,
    cohortDatabaseSchema,
    tempEmulationSchema,
    cohortDefinitionSet,
    minCellCount,
    cohortTable,
    releaseFolder
) {
  ## This fixes object_usage_linter warning:
  ## no visible binding for global variable '.data'
  require(rlang)

  dir.create(
    path = releaseFolder,
    recursive = TRUE
  )
  dir.create(file.path(releaseFolder, "temp"))

  message("Generate cohorts")

  cohortTableNames <-
    CohortGenerator::getCohortTableNames(cohortTable = cohortTable)

  CohortGenerator::createCohortTables(
    connectionDetails = connectionDetails,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTableNames = cohortTableNames,
    incremental = FALSE
  )

  CohortGenerator::generateCohortSet(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = cdmDatabaseSchema,
    tempEmulationSchema = tempEmulationSchema,
    cohortTableNames = cohortTableNames,
    cohortDefinitionSet = cohortDefinitionSet,
    cohortDatabaseSchema = cohortDatabaseSchema,
    incremental = FALSE,
    incrementalFolder = file.path(releaseFolder, "incremental")
  )

  cohortIndex <-
    cohortDefinitionSet |>
    dplyr::select(
      # "atlasId",
      "cohortId",
      "cohortName",
      # "logicDescription",
      # "generateStats",
      "subsetParent",
      "isSubset",
      "subsetDefinitionId"
    ) |>
    dplyr::rename(dplyr::all_of(c(
      "cohort_id" = "cohortId",
      "cohort_name" = "cohortName",
      "subset_parent" = "subsetParent",
      "is_subset" = "isSubset",
      "subset_definition_id" = "subsetDefinitionId"
    )))
  cohortIds <- cohortDefinitionSet$cohortId




  message("Characterize cohorts")

  customCovariateSettings <- FeatureExtraction::createTemporalCovariateSettings(
    useDemographicsGender = TRUE,
    useDemographicsAge = TRUE,
    useDemographicsAgeGroup = TRUE,
    useDemographicsRace = TRUE,
    useDemographicsEthnicity = TRUE,
    useDemographicsIndexYear = TRUE,
    useDemographicsIndexMonth = TRUE,
    useDemographicsIndexYearMonth = TRUE,
    useDemographicsPriorObservationTime = TRUE,
    useDemographicsPostObservationTime = TRUE,
    useDemographicsTimeInCohort = TRUE,
    useConditionOccurrence = TRUE,
    useProcedureOccurrence = TRUE,
    useDrugEraStart = TRUE,
    useMeasurement = TRUE,
    useConditionEraStart = TRUE,
    useConditionEraOverlap = TRUE,
    ## do not use the following
    ## because https://github.com/OHDSI/FeatureExtraction/issues/144
    useConditionEraGroupStart = FALSE,
    useConditionEraGroupOverlap = FALSE,
    ## leads to too many concept id
    useDrugExposure = FALSE,
    useDrugEraOverlap = FALSE,
    ## do not use the following
    ## because https://github.com/OHDSI/FeatureExtraction/issues/144
    useDrugEraGroupStart = FALSE,
    useDrugEraGroupOverlap = FALSE,
    useObservation = TRUE,
    useDeviceExposure = TRUE,
    useCharlsonIndex = FALSE,
    useDcsi = FALSE,
    useChads2 = FALSE,
    useChads2Vasc = FALSE,
    useHfrs = FALSE,
    temporalStartDays = c(-1095, -30),
    temporalEndDays = c(0, 0)
  )

  covariateData <- FeatureExtraction::getDbCovariateData(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTableNames$cohortTable,
    covariateSettings = customCovariateSettings,
    aggregated = TRUE,
    cohortIds = cohortIds,
    minCharacterizationMean = 0.01 ## remove records with zeros
  )
  covariateResult <- NULL
  populationSize <-
    attr(x = covariateData, which = "metaData")$populationSize
  populationSize <-
    dplyr::tibble(
      cohortId = names(populationSize) |> as.numeric(),
      populationSize = populationSize
    )

  if ("covariates" %in% names(covariateData) &&
      dplyr::pull(dplyr::count(covariateData$covariates)) > 0) {
    covariates <-
      covariateData$covariates |>
      dplyr::rename("cohortId" = "cohortDefinitionId") |>
      dplyr::left_join(populationSize, by = "cohortId", copy = TRUE) |>
      dplyr::mutate(p = .data$sumValue / populationSize)

    if (
      nrow(
        covariates |>
        dplyr::filter(.data$p > 1) |>
        dplyr::collect()
      ) > 0
    ) {
      stop(
        paste0(
          "During characterization, population size (denominator) was found ",
          "to be smaller than features Value (numerator).",
          " - this may have happened because of an error in Feature ",
          "generation process. Please contact the package developer."
        )
      )
    }

    covariates <- covariates |>
      dplyr::mutate(sd = sqrt(p * (1 - p))) |> # nolint: object_usage_linter.
      dplyr::select(-p) |>
      dplyr::rename("mean" = "averageValue") |>
      dplyr::select(-populationSize)

    if (FeatureExtraction::isTemporalCovariateData(covariateData)) {
      covariates <- covariates |>
        dplyr::select(
          "cohortId",
          "timeId",
          "covariateId",
          "sumValue",
          "mean",
          "sd"
        )

      tidNaCount <- covariates |>
        dplyr::filter(is.na(.data$timeId)) |>
        dplyr::count() |>
        dplyr::pull()

      if (tidNaCount > 0) {
        covariates <-
          covariates |>
          dplyr::mutate(
            timeId = dplyr::if_else(
              is.na(.data$timeId),
              -1,
              .data$timeId
            )
          )
      }
    } else {
      covariates <- covariates |>
        dplyr::mutate(timeId = 0) |>
        dplyr::select(
          "cohortId",
          "timeId",
          "covariateId",
          "sumValue",
          "mean",
          "sd"
        )
    }

    covariateResult <-
      covariates |>
      dplyr::rename(dplyr::all_of(c(
        "cohort_id" = "cohortId",
        "covariate_id" = "covariateId",
        "time_id" = "timeId",
        "sum_value" = "sumValue"
      )))
  }

  analysisRef <-
    covariateData$analysisRef |>
    dplyr::rename(dplyr::all_of(c(
      "analysis_name" = "analysisName",
      "domain_id" = "domainId",
      "analysis_id" = "analysisId",
      "is_binary" = "isBinary",
      "missing_means_zero" = "missingMeansZero"
    )))

  covariateRef <-
    covariateData$covariateRef |>
    dplyr::rename(dplyr::all_of(c(
      "covariate_name" = "covariateName",
      "covariate_id" = "covariateId",
      "analysis_id" = "analysisId",
      "concept_id" = "conceptId"
    )))

  timeRef <-
    covariateData$timeRef |>
    dplyr::rename(dplyr::all_of(c(
      "time_id" = "timeId",
      "start_day" = "startDay",
      "end_day" = "endDay"
    )))

  if ("covariatesContinuous" %in% names(covariateData) &&
      dplyr::pull(dplyr::count(covariateData$covariatesContinuous)) > 0) {
    covariatesContinuous <-
      covariateData$covariatesContinuous |>
      dplyr::rename(dplyr::all_of(c(
        "cohort_id" = "cohortDefinitionId",
        "covariate_id" = "covariateId",
        "count_value" = "countValue",
        "min_value" = "minValue",
        "max_value" = "maxValue",
        "mean" = "averageValue",
        "sd" = "standardDeviation",
        "median_value" = "medianValue",
        "p_10_value" = "p10Value",
        "p_25_value" = "p25Value",
        "p_75_value" = "p75Value",
        "p_90_value" = "p90Value"
      )))
  }

  cohortCounts <-
    CohortDiagnostics::getCohortCounts(
      connectionDetails = connectionDetails,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTable = cohortTableNames$cohortTable,
      cohortIds = cohortIds
    ) |>
    dplyr::rename(dplyr::all_of(c(
      "cohort_id" = "cohortId",
      "cohort_entries" = "cohortEntries",
      "cohort_subjects" = "cohortSubjects"
    )))

  connection <- DatabaseConnector::connect(connectionDetails)
  CohortDiagnostics:::createConceptTable(connection, tempEmulationSchema)

  CohortDiagnostics:::runConceptSetDiagnostics(
    connection = connection,
    tempEmulationSchema = tempEmulationSchema,
    cdmDatabaseSchema = cdmDatabaseSchema,
    vocabularyDatabaseSchema = vocabularyDatabaseSchema,
    databaseId = cdmDatabaseSchema,
    cohorts = cohortDefinitionSet,
    runIncludedSourceConcepts = FALSE,
    runOrphanConcepts = FALSE,
    runBreakdownIndexEvents = TRUE,
    exportFolder = file.path(releaseFolder, "temp"),
    minCellCount = minCellCount,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTableNames$cohortTable,
    recordKeepingFile = file.path(releaseFolder, "CreatedDiagnostics.csv")
  )

  message("Export data to ARES")

  ## index_event_breakdown.csv may contain UTF Byte Order Mark (BOM), which
  ## prepends first column name in the CSV making it "∩..concept_id" instead of
  ## "concept_id"
  indexEventBreakdownData <-
    read.csv(file.path(releaseFolder, "temp", "index_event_breakdown.csv"))
  if (names(indexEventBreakdownData)[1] != "concept_id") {
    saveEncoding <- getOption("encoding")
    options("encoding" = "UTF-8-BOM")
    indexEventBreakdownData <-
      read.csv(file.path(releaseFolder, "temp", "index_event_breakdown.csv"))
    options("encoding" = saveEncoding)
  }

  cohortsTable <-
    cohortIndex |>
    dplyr::left_join(cohortCounts, by = "cohort_id")

  matching_concept_ids <- indexEventBreakdownData |>
    dplyr::select("concept_id") |>
    dplyr::distinct() |>
    dplyr::pull()

  concept_query <- paste0(
    "SELECT concept_id, concept_name, vocabulary_id ",
    "FROM ", cdmDatabaseSchema, ".concept ",
    "WHERE CONCEPT_ID IN (", paste(matching_concept_ids, collapse = ","), ")"
  )

  conceptTable <- DatabaseConnector::dbGetQuery(connection, concept_query)

  DatabaseConnector::dbDisconnect(connection)

  cohortsCharacterizationTable <- covariateResult |>
    dplyr::left_join(
      covariateRef,
      by = "covariate_id"
    ) |>
    dplyr::left_join(
      analysisRef,
      by = "analysis_id"
    ) |>
    dplyr::left_join(
      cohortIndex |> dplyr::select("cohort_name", "cohort_id"),
      by = "cohort_id",
      copy = TRUE
    ) |>
    dplyr::left_join(
      timeRef,
      by = "time_id"
    ) |>
    dplyr::mutate(
      temporal_choice = paste(
        "T", "(", .data$start_day, "to", .data$end_day, ")",
        sep = " "
      )
    )

  domains <- c(
    "condition_occurrence",
    "condition_era",
    "drug_exposure",
    "drug_era",
    "measurement",
    "observation",
    "procedure_occurrence",
    "device_exposure",
    "visit_detail",
    "visit_occurrence"
  )

  fixFeatureExtractionOutput <- function(data, domains) {
    domains_list <- paste0("'", paste(domains, collapse = "','"), "'")

    domain_check_condition <- paste0( # nolint: object_usage_linter.
      "CHARINDEX(':', covariate_name) > 0 AND ",
      "SUBSTRING(covariate_name, 1, CHARINDEX(':', covariate_name) - 1) IN ",
      "(", domains_list, ")"
    )

    data |>
      dplyr::mutate(
        domain_id = dplyr::case_when(
          dplyr::sql(domain_check_condition) ~
            dplyr::sql(
              "SUBSTRING(covariate_name, 1, CHARINDEX(':', covariate_name) - 1)"
            ),
          TRUE ~ domain_id
        ),
        covariate_name = dplyr::case_when(
          dplyr::sql(domain_check_condition) ~
            dplyr::sql(
              "TRIM(SUBSTRING(covariate_name, CHARINDEX(':', covariate_name) + 1, LENGTH(covariate_name) - CHARINDEX(':', covariate_name)))"),
          TRUE ~ covariate_name
        )
      ) |>
      dplyr::collect() |>
      dplyr::mutate(
        domain_id = dplyr::case_when(
          !is.na(domain_id) & domain_id != "" ~
            gsub("_", " ", domain_id) |>
            tools::toTitleCase(),
          TRUE ~ domain_id
        )
      )
  }

  cohortsCharacterizationTable <-
    fixFeatureExtractionOutput(cohortsCharacterizationTable, domains)

  cohortsIndexEventBreakdownTable <-
    indexEventBreakdownData |>
    dplyr::left_join(
      cohortIndex |> dplyr::select("cohort_name", "cohort_id"),
      by = "cohort_id"
    ) |>
    dplyr::left_join(
      conceptTable |>
        dplyr::select("concept_name", "concept_id", "vocabulary_id"),
      by = "concept_id"
    )

  temporalCovariateValueDistTable <-
    covariatesContinuous |>
    dplyr::left_join(
      cohortIndex |> dplyr::select("cohort_name", "cohort_id"),
      by = "cohort_id",
      copy = TRUE
    ) |>
    dplyr::left_join(
      covariateRef |> dplyr::select("covariate_name", "covariate_id"),
      by = "covariate_id"
    ) |>
    dplyr::collect()


  ## Write the final data to a new file
  options("encoding" = "UTF-8")
  options(scipen = 99999)
  writeResults <- function(x, path) {
    x |>
    dplyr::arrange("cohort_id") |>
    dplyr::collect() |>
    arrow::write_parquet(
      sink = path,
      version = "2.6",
      compression = "gzip",
      compression_level = 5,
      use_dictionary = TRUE,
      write_statistics = TRUE,
      chunk_size = 8 * 1024
    )
  }

  cohortsTable |>
    writeResults(
      path = file.path(releaseFolder, "cohort_index.parquet")
    )
  cohortsCharacterizationTable |>
    writeResults(
      path = file.path(releaseFolder, "cohort_characterization.parquet")
    )
  cohortsIndexEventBreakdownTable |>
    writeResults(
      path = file.path(releaseFolder, "cohort_index_event_breakdown.parquet")
    )
  temporalCovariateValueDistTable |>
    writeResults(
      path = file.path(releaseFolder, "cohort_temporal_covariate_distribution.parquet")
    )

  unlink(file.path(releaseFolder, "temp"), recursive = TRUE)

  message("ARES cohort report is ready")
}
