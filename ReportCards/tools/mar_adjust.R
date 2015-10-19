# active <- shell('echo %HOMEPATH%', intern=TRUE)
# active <- gsub('\\\\', '/', active)
setwd("")

source("U:/R/tomkit.R")
source("./school_functions.R")
##source("./state_functions.R")
require(XML)

## Clean up addressmeh
Scrub <- function(x){
	x <- gsub('\\.|\\,|#|-', '', x)
	
	x <- strsplit(x, " ")[[1]]
	x <- subset(x, x != '')
	
	x <- paste(x, collapse="+")
	return(x)
}


ReMar <- function(org_code){
	.add_line <- sqlQuery(dbrepcard, sprintf("SELECT *
		FROM [dbo].[schooldir_sy1314]
		WHERE [school_code] = '%s'", leadgr(org_code, 4)))
	
	print(.add_line)
	if(nrow(.add_line)==1){
		
		.addr <- Scrub(.add_line$address_1[1])
		
		.webcall <- "http://citizenatlas.dc.gov/newwebservices/locationverifier.asmx/findLocation?str=" %+% .addr
		
		.xmlobj <- xmlTreeParse(.webcall, useInternalNodes = TRUE)
		.xmlobj <- xmlToList(.xmlobj)
		
		if(is.null(.xmlobj)){
			cat("Webcall did not work", fill=TRUE)
		} else if(!is.null(.xmlobj$returnCodes)){
			cat(.addr, "was a bad address", sep=" ", fill=TRUE)
		} else{
		
			.long <- .xmlobj$returnDataset$diffgram$NewDataSet$Table1$LONGITUDE
			.lat <- .xmlobj$returnDataset$diffgram$NewDataSet$Table1$LATITUDE
			.ward <- .xmlobj$returnDataset$diffgram$NewDataSet$Table1$WARD_2012
			.tract <- .xmlobj$returnDataset$diffgram$NewDataSet$Table1$CENSUS_TRACT			
						
			.updt_qry <- sprintf("UPDATE [dbo].[schooldir_sy1314]
			SET [ward] = '%s',
				[latitude] = %f,
				[longitude] = %f,
				[census_tract] = '%s'				
			WHERE [school_code] = '%s'", 
			.ward, as.numeric(.lat), as.numeric(.long), as.character(as.integer(.tract)), leadgr(org_code, 4))
			
			cat("\n\n Updating...", fill=TRUE)
			cat(.updt_qry, fill=TRUE)
			print(sqlQuery(dbrepcard, .updt_qry))
		}
	}
}

