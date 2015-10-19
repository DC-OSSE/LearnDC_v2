## Generate State File ## 
## Detail Data File Build ##
setwd("U:/LearnDC ETL V2/ReportCards")
library(dplyr)

source("U:/R/tomkit.R")
source("./school_functions.R")
source("./state_functions.R")
source("./generalized.R")

checkna <- function(x){
    if(is.na(x)){
        return('null')
    }
    return(x)
}

checkna_str <- function(x){
    if(is.na(x)){
        return('null')
    }
    return('"' %+% x %+%'"')
}

org_type <- "state"
org_code <- "STATE"

# state_version <- sqlQuery(dbrepcard, "SELECT TOP 1 
#         [version_number],
#         [timestamp]
#     FROM [dbo].[ver_control_statefile]
#     ORDER BY [version_number] DESC")

# next_version <- state_version$version_number + 0.1

newfile <- file("accountability.json", encoding="UTF-8")

setwd("U:/LearnDC ETL V2/Export/JSON/state/DC")

sink(newfile)
cat('{', fill=TRUE)

level <- 1
cat(indent(level),'"timestamp": "',date(),'",', sep="", fill=TRUE)
cat(indent(level),'"org_type": "state",', sep="", fill=TRUE)
cat(indent(level),'"org_name": "District of Columbia",', sep="", fill=TRUE)


######### INSERT DATA HERE ##################
{	
	### Accountability
	cat(indent(level), '"exhibit": {', fill=TRUE)
    up(level)
	cat(indent(level), '"id": "accountability",', sep="", fill=TRUE)
	cat(indent(level), '"data": [', sep="")
	up(level)
	cat(ExStateAcct(level), fill=TRUE) 
	down(level)
	cat(indent(level),']', sep="", fill=TRUE)
	down(level)
	cat(indent(level),'}', sep="", fill=TRUE)
}
# {
# 	## Assessment
# 	cat(indent(level),'{', sep="", fill=TRUE)
# 	up(level)
# 	cat(indent(level), '"id": "dccas",', sep="", fill=TRUE)
# 	cat(indent(level), '"data": [', sep="", fill=TRUE)
# 	cat(ExStateCAS(level+1))
# 	cat('\n',indent(level), ']', sep="", fill=TRUE)
# 	down(level)
# 	cat(indent(level),'},', sep="", fill=TRUE)
# }
# {
# 	## AMOs
# 	cat(indent(level),'{', sep="", fill=TRUE)
# 	up(level)
# 	cat(indent(level), '"id": "amo_targets",', sep="", fill=TRUE)
# 	cat(indent(level), '"data": [', sep="", fill=TRUE)
# 	cat(ExStateAMOs(level+1))
# 	cat('\n',indent(level), ']', sep="", fill=TRUE)
# 	down(level)
# 	cat(indent(level),'},', sep="", fill=TRUE)
# }
# {
# 	## Highly Qualified Teacher Status
# 	cat(indent(level),'{', sep="", fill=TRUE)
# 	up(level)
# 	cat(indent(level), '"id": "hqt_status",', sep="", fill=TRUE)
# 	cat(indent(level), '"data":0.824', sep="", fill=TRUE)
	
# 	down(level)
# 	cat(indent(level),'},', sep="", fill=TRUE)
# }
# {
# 	#College Enrollment
# 	cat(indent(level),'{', sep="", fill=TRUE)
# 	up(level)
# 	cat(indent(level), '"id": "college_enroll",', sep="", fill=TRUE)
# 	cat(indent(level), '"data": [', sep="", fill=TRUE)
# 	cat(ExCollegeEnroll(org_code, level+1), fill=TRUE)
# 	cat(indent(level), ']', sep="", fill=TRUE)
# 	down(level)
# 	cat(indent(level),'},', sep="", fill=TRUE)
# }
# {
# 	## Graduation  -- end of report card section
# 	cat(indent(level),'{', sep="", fill=TRUE)
# 	up(level)
# 	cat(indent(level), '"id": "graduation",', sep="", fill=TRUE)
# 	cat(indent(level), '"data": [', sep="", fill=TRUE)
# 	cat(ExGraduation(level=level+1), fill=TRUE)
# 	cat('\n',indent(level), ']', sep="", fill=TRUE)
# 	down(level)
# 	cat(indent(level),'},', sep="", fill=TRUE)
# }
# {
# 	## Diploma Count
# 	cat(indent(level),'{', sep="", fill=TRUE)
# 	up(level)
# 	cat(indent(level), '"id": "diploma_count",', sep="", fill=TRUE)
# 	cat(indent(level), '"data": [', sep="", fill=TRUE)
# 	cat(ExDiplCount(level+1), fill=TRUE)
# 	cat('\n',indent(level), ']', sep="", fill=TRUE)
# 	down(level)
# 	cat(indent(level),'},', sep="", fill=TRUE)

# }
# {
# 	## Graduation targets
# 	cat(indent(level),'{', sep="", fill=TRUE)
# 	up(level)
# 	cat(indent(level), '"id": "graduation_targets",', sep="", fill=TRUE)
# 	cat(indent(level), '"data": [', sep="", fill=TRUE)
# 	cat(ExGradTargets(level+1), fill=TRUE)
# 	cat(indent(level), ']', sep="", fill=TRUE)
# 	down(level)
# 	cat(indent(level),'},', sep="", fill=TRUE)
# }
# {
#     ## NAEP Stuff
#     cat(indent(level),'{', sep="", fill=TRUE)
#     up(level)
#     cat(indent(level), '"id": "naep_results",', sep="", fill=TRUE)

#     cat(indent(level), '"data": [', sep="", fill=TRUE)
#     cat(ExNaepResult(level), fill=TRUE)

#     cat(indent(level), ']', sep="", fill=TRUE)
#     down(level)
# 	cat(indent(level),'},', sep="", fill=TRUE)
# }
# {
#     #Enrollment
#     cat(indent(level),'{', sep="", fill=TRUE)
#     up(level)
#     cat(indent(level), '"id": "enrollment",', sep="", fill=TRUE)
#     cat(indent(level), '"data": [', sep="", fill=TRUE)
#     cat(ExEnrollChunk(level = level+1), fill=TRUE)
#     cat(indent(level), ']', sep="", fill=TRUE)
#     down(level)
#     cat(indent(level),'},', sep="", fill=TRUE)
# }
# {
#     ## College Readiness##
#     cat(indent(level),'{', sep="", fill=TRUE)
#     up(level)
#     cat(indent(level), '"id": "college_readiness",', sep="", fill=TRUE)
#     cat(indent(level), '"data": [', sep="", fill=TRUE)
#     cat(ExStateCReady(level+1), fill=TRUE)
#     cat(indent(level), ']', sep="", fill=TRUE)
#     down(level)
#     cat(indent(level),'},', sep="", fill=TRUE)
# }
# {
#     ## SPED Testing##
#     cat(indent(level),'{', sep="", fill=TRUE)
#     up(level)
#     cat(indent(level), '"id": "special_ed",', sep="", fill=TRUE)
#     cat(indent(level), '"data": [', sep="", fill=TRUE)
#     cat(ExStateSPEDChunk(level+1), fill=TRUE)
#     cat(indent(level), ']', sep="", fill=TRUE)
#     down(level)
#     cat(indent(level),'},', sep="", fill=TRUE)
# }
# {
#     ## PreK Testing##
#     cat(indent(level),'{', sep="", fill=TRUE)
#     up(level)
#     cat(indent(level), '"id": "prek_cas_results",', sep="", fill=TRUE)
#     cat(indent(level), '"data": [', sep="", fill=TRUE)
#     cat(ExStatePreKCAS(level+1), fill=TRUE)
#     cat(indent(level), ']', sep="", fill=TRUE)
#     down(level)
#     cat(indent(level),'},', sep="", fill=TRUE)
# }
# {
#     cat(indent(level),hard_code_apr, fill=TRUE)
# }
# {
#     #AMAO
#     cat(indent(level),'{', sep="", fill=TRUE)
#     up(level)
#     cat(indent(level), '"id": "ell",', sep="", fill=TRUE)
#     cat(indent(level), '"data": {', sep="", fill=TRUE)
#     up(level)
#     cat(indent(level), '"amao_1": {"val":0.54, "target": 0.60},', sep="", fill=TRUE)
#     cat(indent(level), '"amao_2": {"val":0.23, "target": 0.15},', sep="", fill=TRUE)
#     cat(indent(level), '"amao_3m": {"val":0.33, "target": 0.53},', sep="", fill=TRUE)
#     cat(indent(level), '"amao_3r": {"val":0.22, "target": 0.44}', sep="", fill=TRUE)
#     down(level)

#     cat(indent(level), '}', sep="", fill=TRUE)
#     down(level)
#     cat(indent(level),'},', sep="", fill=TRUE)
# }
# {
# 	cat(indent(level),hard_code_equity, fill=TRUE)
# }

cat('}', fill=TRUE)
sink()
close(newfile)

# ## SUCCESSFUL PUSH!  
# update_vcontrol <- state_version
# update_vcontrol$version_number <- next_version
# update_vcontrol$timestamp <- Sys.time()

# sqlSave(dbrepcard, update_vcontrol, tablename="ver_control_statefile", append=TRUE, safer=TRUE, rownames=FALSE, varType=c(timestamp="datetime"))

