#' @title getSourceReleaseKey
#' @description Returns the source release key for this CDM database for use within ARES
#'
#' @param connectionDetails Connection details pointing to CDM database
#' @param cdmDatabaseSchema CDM schema name
#'
#' @export
getSourceReleaseKey <- function(connectionDetails, cdmDatabaseSchema) {
  sql <- "SELECT * from @cdmDatabaseSchema.cdm_source"
  renderedSql <- SqlRender::render(sql,cdmDatabaseSchema = cdmDatabaseSchema)
  translatedRenderedSql <- SqlRender::translate(renderedSql,connectionDetails$dbms)

  connection <- DatabaseConnector::connect(connectionDetails)
  results <- DatabaseConnector::querySql(connection = connection, sql = translatedRenderedSql)
  releaseId <- format(lubridate::ymd(results[1,"CDM_RELEASE_DATE"]),"%Y%m%d")
  sourceKey <- gsub(" ", "_", results[1,"CDM_SOURCE_ABBREVIATION"])

  return(file.path(sourceKey,releaseId))
}
