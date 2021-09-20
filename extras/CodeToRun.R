# generate using synthea with csv export using synthea - https://github.com/synthetichealth/synthea
# $ ./run_synthea -p 1000 --exporter.csv.export true

# by default synthea generates fhir formatted files
# alternatively convert fhir data to CDM files using https://github.com/OHDSI/FhirToCdm
# $ ./FHIRtoCDM.exe -f "D:\ohdsi\synthea\output\fhir" -s cdm -v "Driver={PostgreSQL UNICODE}; Server=localhost; Port=5432; Database=ohdsi;Uid=postgres;Pwd=password;UseDeclareFetch=1"

# DatabaseConnector::downloadJdbcDrivers("postgresql","D:/OHDSI/Drivers")

connectionDetails <- DatabaseConnector::createConnectionDetails(
  dbms = "postgresql",
  server = "localhost/ohdsi",
  user = "postgres",
  password = 'password',
  port = "5432",
  pathToDriver = "D:/OHDSI/Drivers"
)
options(connectionObserver = NULL)

cdmDatabaseSchema <- resultsDatabaseSchema <- vocabDatabaseSchema <- "s20210306_cdm_v531"
cdmDatabaseSchema <- resultsDatabaseSchema <- vocabDatabaseSchema <- "s20210212_cdm_v531"
cdmDatabaseSchema <- resultsDatabaseSchema <- vocabDatabaseSchema <- "s20210203_cdm_v531"
nativeSchema <- "s20210306_native"
numThreads <- 1

# run database characterization with Achilles
Achilles::achilles(
  connectionDetails,
  cdmDatabaseSchema = cdmDatabaseSchema ,
  resultsDatabaseSchema=resultsDatabaseSchema,
  vocabDatabaseSchema = vocabDatabaseSchema,
  numThreads = 1,
  cdmVersion = "5.3.0",
  runHeel = F,
  runCostAnalysis = F,
  createIndices = F,
  createTable = T,
  smallCellCount = 0,
  analysisIds = c(410,430,431,432)
)

test <- Achilles::performTemporalCharacterization(
  connectionDetails = connectionDetails,
  cdmDatabaseSchema = cdmDatabaseSchema,
  resultsDatabaseSchema = resultsDatabaseSchema,
  outputFile = "temporal-characterization.csv"
)

aresDataDirectory <- "c:/ares/public/data"
sourceReleaseKey <- AresIndexer::getSourceReleaseKey(connectionDetails,cdmDatabaseSchema)
outputFolder <- file.path(aresDataDirectory,sourceReleaseKey)

# run data quality checks with DataQualityDashboard
dqResults <- DataQualityDashboard::executeDqChecks(
  connectionDetails = connectionDetails,
  cdmDatabaseSchema = cdmDatabaseSchema,
  resultsDatabaseSchema = resultsDatabaseSchema,
  vocabDatabaseSchema = vocabDatabaseSchema,
  cdmSourceName = cdmSourceName,
  numThreads = numThreads,
  outputFolder = outputFolder,
  outputFile = "dq-result.json",
  verboseMode = T,
  writeToTable = F
)

# run ares specific export routine for achilles database characterization data
options(connectionObserver = NULL)
outputPath <- "c:/ares/public/data"
Achilles::exportAO(
  connectionDetails = connectionDetails,
  cdmDatabaseSchema = cdmDatabaseSchema,
  resultsDatabaseSchema = resultsDatabaseSchema,
  vocabDatabaseSchema = vocabDatabaseSchema,
  outputPath = outputPath
)

# process ares index which updates some achilles concept files with data quality issues
# as well as generates separate index files for the ares platform


sourceFolders <- c("C:/ares/public/data/synthea")
AresIndexer::buildDataQualityIndex(sourceFolders,aresDataDirectory)
AresIndexer::buildNetworkIndex(sourceFolders,aresDataDirectory)

