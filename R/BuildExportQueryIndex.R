# @file BuildExportQueryIndex
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

#' Export Query Details
#'
#' @details Exports a query index that contains links to the queries used to generate all data summaries.
#'
#' @name buildExportQueryIndex
#'
#' @param outputFolder Path to source folder
#'
#' @import jsonlite
#'
#' @export
buildExportQueryIndex <-
  function(outputFolder) {
   procedure_occurrence <- list(
     "SUMMARY" = "export/procedure/sqlProcedureTable.sql",
     "PREVALENCE_BY_GENDER_AGE_YEAR" = "export/procedure/sqlPrevalenceByGenderAgeYear.sql",
     "PREVALENCE_BY_MONTH" = "export/procedure/sqlPrevalenceByMonth.sql",
     "PROCEDURE_FREQUENCY_DISTRIBUTION" = "export/procedure/sqlFrequencyDistribution.sql",
     "PROCEDURES_BY_TYPE" = "export/procedure/sqlProceduresByType.sql",
     "AGE_AT_FIRST_OCCURRENCE" = "export/procedure/sqlAgeAtFirstOccurrence.sql"
   )
   person <- list(
     "SUMMARY" = "export/person/population.sql",
     "AGE_GENDER_DATA" = "export/person/population_age_gender.sql",
     "GENDER_DATA" = "export/person/gender.sql",
     "RACE_DATA" = "export/person/race.sql",
     "ETHNICITY_DATA" = "export/person/ethnicity.sql",
     "BIRTH_YEAR_DATA" = "export/person/yearofbirth.sql"
   )
   achilles_performance <- "export/performance/sqlAchillesPerformance.sql"

   death <- list(
     "PREVALENCE_BY_GENDER_AGE_YEAR" = "export/death/sqlPrevalenceByGenderAgeYear.sql",
     "PREVALENCE_BY_MONTH" = "export/death/sqlPrevalenceByMonth.sql",
     "DEATH_BY_TYPE" = "export/death/sqlDeathByType.sql",
     "AGE_AT_DEATH" = "export/death/sqlAgeAtDeath.sql")

   observation_period <- list(
     "AGE_AT_FIRST_OBSERVATION" = "export/observationperiod/ageatfirst.sql",
     "AGE_BY_GENDER" = "export/observationperiod/agebygender.sql",
     "OBSERVATION_LENGTH_HISTOGRAM" = c("export/observationperiod/observationlength_stats.sql", "export/observationperiod/observationlength_data.sql"),
     "CUMULATIVE_DURATION" = "export/observationperiod/cumulativeduration.sql",
     "OBSERVATION_PERIOD_LENGTH_BY_GENDER" = "export/observationperiod/observationlengthbygender.sql",
     "OBSERVATION_PERIOD_LENGTH_BY_AGE" ="export/observationperiod/observationlengthbyage.sql",
     "OBSERVED_BY_YEAR_HISTOGRAM" = c("export/observationperiod/observedbyyear_stats.sql", "export/observationperiod/observedbyyear_data.sql"),
     "OBSERVED_BY_MONTH" = "export/observationperiod/observedbymonth.sql",
     "PERSON_PERIODS_DATA" = "export/observationperiod/periodsperperson.sql"
   )

   visit_occurrence <- list(
     "SUMMARY" = "export/visit/sqlVisitTreemap.sql",
     "PREVALENCE_BY_GENDER_AGE_YEAR" = "export/visit/sqlPrevalenceByGenderAgeYear.sql",
     "PREVALENCE_BY_MONTH" = "export/visit/sqlPrevalenceByMonth.sql",
     "VISIT_DURATION_BY_TYPE" = "export/visit/sqlVisitDurationByType.sql",
     "AGE_AT_FIRST_OCCURRENCE" = "export/visit/sqlAgeAtFirstOccurrence.sql"
   )

   visit_detail <- list(
     "SUMMARY" = "export/visitdetail/sqlVisitDetailTreemap.sql",
     "PREVALENCE_BY_GENDER_AGE_YEAR" = "export/visitdetail/sqlPrevalenceByGenderAgeYear.sql",
     "PREVALENCE_BY_MONTH" = "export/visitdetail/sqlPrevalenceByMonth.sql",
     "VISIT_DETAIL_DURATION_BY_TYPE" = "export/visitdetail/sqlVisitDetailDurationByType.sql",
     "AGE_AT_FIRST_OCCURRENCE" = "export/visitdetail/sqlAgeAtFirstOccurrence.sql"
   )

   metadata <- "export/metadata/sqlMetadata.sql"

   observation <- list(
     "SUMMARY" = "export/observation/sqlObservationTable.sql",
     "PREVALENCE_BY_GENDER_AGE_YEAR" = "export/observation/sqlPrevalenceByGenderAgeYear.sql",
     "PREVALENCE_BY_MONTH" = "export/observation/sqlPrevalenceByMonth.sql",
     "OBS_FREQUENCY_DISTRIBUTION" = "export/observation/sqlFrequencyDistribution.sql",
     "OBSERVATIONS_BY_TYPE" = "export/observation/sqlObservationsByType.sql",
     "AGE_AT_FIRST_OCCURRENCE" = "export/observation/sqlAgeAtFirstOccurrence.sql"
   )

   cdm_source <- "export/metadata/sqlCdmSource.sql"

   measurement <- list(
     "SUMMARY" = "export/measurement/sqlMeasurementTable.sql",
     "PREVALENCE_BY_GENDER_AGE_YEAR" = "export/measurement/sqlPrevalenceByGenderAgeYear.sql",
     "PREVALENCE_BY_MONTH" = "export/measurement/sqlPrevalenceByMonth.sql",
     "FREQUENCY_DISTRIBUTION" = "export/measurement/sqlFrequencyDistribution.sql",
     "MEASUREMENTS_BY_TYPE" = "export/measurement/sqlMeasurementsByType.sql",
     "AGE_AT_FIRST_OCCURRENCE" = "export/measurement/sqlAgeAtFirstOccurrence.sql",
     "RECORDS_BY_UNIT" = "export/measurement/sqlRecordsByUnit.sql",
     "MEASUREMENT_VALUE_DISTRIBUTION" = "export/measurement/sqlMeasurementValueDistribution.sql",
     "LOWER_LIMIT_DISTRIBUTION" = "export/measurement/sqlLowerLimitDistribution.sql",
     "UPPER_LIMIT_DISTRIBUTION" = "export/measurement/sqlUpperLimitDistribution.sql",
     "VALUES_RELATIVE_TO_NORM" = "export/measurement/sqlValuesRelativeToNorm.sql"
   )

   drug_era <- list(
     "SUMMARY" = "export/drugera/sqlDrugEraTable.sql",
     "AGE_AT_FIRST_EXPOSURE" = "export/drugera/sqlAgeAtFirstExposure.sql",
     "PREVALENCE_BY_GENDER_AGE_YEAR" = "export/drugera/sqlPrevalenceByGenderAgeYear.sql",
     "PREVALENCE_BY_MONTH" = "export/drugera/sqlPrevalenceByMonth.sql",
     "LENGTH_OF_ERA" = "export/drugera/sqlLengthOfEra.sql"
   )

   drug_exposure <- list(
     "SUMMARY" = "export/drug/sqlDrugTable.sql",
     "AGE_AT_FIRST_EXPOSURE" = "export/drug/sqlAgeAtFirstExposure.sql",
     "DAYS_SUPPLY_DISTRIBUTION" = "export/drug/sqlDaysSupplyDistribution.sql",
     "DRUGS_BY_TYPE" = "export/drug/sqlDrugsByType.sql",
     "PREVALENCE_BY_GENDER_AGE_YEAR" = "export/drug/sqlPrevalenceByGenderAgeYear.sql",
     "PREVALENCE_BY_MONTH" = "export/drug/sqlPrevalenceByMonth.sql",
     "DRUG_FREQUENCY_DISTRIBUTION" = "export/drug/sqlFrequencyDistribution.sql",
     "QUANTITY_DISTRIBUTION" = "export/drug/sqlQuantityDistribution.sql",
     "REFILLS_DISTRIBUTION" = "export/drug/sqlRefillsDistribution.sql"
   )

   device_exposure <- list(
     "SUMMARY" = "export/device/sqlDeviceTable.sql",
     "AGE_AT_FIRST_EXPOSURE" = "export/device/sqlAgeAtFirstExposure.sql",
     "DEVICES_BY_TYPE" = "export/device/sqlDevicesByType.sql",
     "PREVALENCE_BY_GENDER_AGE_YEAR" = "export/device/sqlPrevalenceByGenderAgeYear.sql",
     "PREVALENCE_BY_MONTH" = "export/device/sqlPrevalenceByMonth.sql",
     "DEVICE_FREQUENCY_DISTRIBUTION" = "export/device/sqlFrequencyDistribution.sql"
   )

   condition_occurrence <- list(
     "SUMMARY" = "export/condition/sqlConditionTable.sql",
     "PREVALENCE_BY_GENDER_AGE_YEAR" = "export/condition/sqlPrevalenceByGenderAgeYear.sql",
     "PREVALENCE_BY_MONTH" = "export/condition/sqlPrevalenceByMonth.sql",
     "CONDITIONS_BY_TYPE" = "export/condition/sqlConditionsByType.sql",
     "AGE_AT_FIRST_DIAGNOSIS" = "export/condition/sqlAgeAtFirstDiagnosis.sql"
   )

   condition_era <- list(
     "SUMMARY" = "export/conditionera/sqlConditionEraTable.sql",
     "AGE_AT_FIRST_EXPOSURE" = "export/conditionera/sqlAgeAtFirstDiagnosis.sql",
     "PREVALENCE_BY_GENDER_AGE_YEAR" = "export/conditionera/sqlPrevalenceByGenderAgeYear.sql",
     "PREVALENCE_BY_MONTH" = "export/conditionera/sqlPrevalenceByMonth.sql",
     "LENGTH_OF_ERA" = "export/conditionera/sqlLengthOfEra.sql"
   )

   domain_summary <- list(
     "CONDITION_OCCURRENCE" = "export/condition/sqlConditionTable.sql",
     "CONDITION_ERA" = "export/conditionera/sqlConditionEraTable.sql",
     "DRUG_EXPOSURE" = "export/drug/sqlDrugTable.sql",
     "DOMAIN_DRUG_STRATIFICATION" = "export/drug/sqlDomainDrugStratification.sql",
     "DRUG_ERA" = "export/drugera/sqlDrugEraTable.sql",
     "MEASUREMENT" = "export/measurement/sqlMeasurementTable.sql",
     "OBSERVATION" = "export/observation/sqlObservationTable.sql",
     "VISIT_DETAIL" = "export/visitdetail/sqlVisitDetailTreemap.sql",
     "VISIT_OCCURRENCE" = "export/visit/sqlVisitTreemap.sql",
     "DOMAIN_VISIT_STRATIFICATION" = "export/visit/sqlDomainVisitStratification.sql",
     "PROCEDURE_OCCURRENCE" = "export/procedure/sqlProcedureTable.sql",
     "DEVICE_EXPOSURE" = "export/device/sqlDeviceTable.sql",
     "PROVIDER" = "export/provider/sqlProviderSpecialty.sql",
     "RECORDS_BY_DOMAIN" = "export/datadensity/totalrecords.sql"
   )


   data_density <- list(
     "DATADENSITY_TOTAL" = "export/datadensity/totalrecords.sql",
     "DATADENSITY_RECORDS_PER_PERSON" = "export/datadensity/recordsperperson.sql",
     "DATADENSITY_CONCEPTS_PER_PERSON" = "export/datadensity/conceptsperperson.sql",
     "DATADENSITY_DOMAINS_PER_PERSON" = "export/datadensity/domainsperperson.sql"
   )

   quality_completeness <- "export/quality/sqlCompletenessTable.sql"

   temporal_characterization <- "temporal/achilles_temporal_data.sql"

   sqlFilesIndex <- list(
     "PROCEDURE_OCCURRENCE" = procedure_occurrence,
     "PERSON" = person,
     "ACHILLES_PERFORMANCE" = achilles_performance,
     "DEATH" = death,
     "OBSERVATION" = observation,
     "OBSERVATION_PERIOD" = observation_period,
     "VISIT_DETAIL" = visit_detail,
     "VISIT_OCCURRENCE" = visit_occurrence,
     "CONDITION_OCCURRENCE" = condition_occurrence,
     "CONDITION_ERA" = condition_era,
     "DEVICE_EXPOSURE" = device_exposure,
     "DRUG_EXPOSURE" = drug_exposure,
     "DRUG_ERA" = drug_era,
     "CDM_SOURCE" = cdm_source,
     "METADATA" = metadata,
     "MEASUREMENT" = measurement,
     "DOMAIN_SUMMARY" = domain_summary,
     "DATA_DENSITY" = data_density,
     "QUALITY_COMPLETENESS" = quality_completeness,
     "TEMPORAL_CHARACTERIZATION" = temporal_characterization

   )
   jsonOutput = jsonlite::toJSON(sqlFilesIndex)
   write(jsonOutput, file=paste(outputFolder, "/export_query_index.json", sep=""))
  }
