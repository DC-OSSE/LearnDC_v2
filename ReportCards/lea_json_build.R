
setwd("U:/REPORT CARD/GIT Report Cards/ReportCards")

source("./imports/tomkit.R")
source("./imports/ODBC.R")
source("./school_functions.R")
source("./generalized.R")
source("./lea_functions.R")

library(dplyr)

school_dir <- sqlFetch(dbrepcard, 'schooldir_sy1314')

lea_dir <- unique(school_dir[c("lea_code","lea_name")])

# lea codes need to be four digits and in quotes
lea_dir$lea_code <- sprintf("%04d", lea_dir$lea_code)

subDir <- "lea_report"
mainDir <- "./data/"

dir.create(file.path(mainDir, subDir), showWarnings = FALSE)
setwd(file.path(mainDir, subDir))

for(i in 1:nrow(lea_dir)){
    org_type <- "lea"
    lea_name <- lea_dir$lea_name[i]
    lea_code <- lea_dir$lea_code[i]
    cat(lea_name, lea_code, sep=" ", fill=TRUE)
    newfile <- file(paste(org_type, '_', lea_code, '.JSON', sep=""), , encoding="UTF-8")

    sink(newfile)
    cat('{', fill=TRUE)
    level <- 1
    cat(indent(level),'"timestamp": "',date(),'",', sep="", fill=TRUE)
    cat(indent(level),'"org_type": "',org_type, '",', sep="", fill=TRUE)
    cat(indent(level),'"org_name": "',lea_name,'",', sep="", fill=TRUE)

    cat(indent(level),'"report_card": {', sep="", fill=TRUE)
    up(level)
    cat(indent(level),'"sections": [', sep="", fill=TRUE)
    up(level)
    # {
    #     ## Assessment
    #     cat(indent(level),'{', sep="", fill=TRUE)
    #     up(level)
    #     cat(indent(level), '"id": "dccas",', sep="", fill=TRUE)
    #     cat(indent(level), '"data": [', sep="", fill=TRUE)
    #     cat(LeaCasChunk(lea_code, level+1))
    #     cat('\n',indent(level), ']', sep="", fill=TRUE)
    #     down(level)
    #     cat(indent(level),'},', sep="", fill=TRUE)
    # }
    # {
    #     ## AMOs
    #     cat(indent(level),'{', sep="", fill=TRUE)
    #     up(level)
    #     cat(indent(level), '"id": "amo_targets",', sep="", fill=TRUE)
    #     cat(indent(level), '"data": [', sep="", fill=TRUE)
    #     cat(ExAMOs(lea_code, level+1, "lea"))
    #     cat('\n',indent(level), ']', sep="", fill=TRUE)
    #     down(level)
    #     cat(indent(level),'},', sep="", fill=TRUE)
    # }
    # {
    #     ## Highly Qualified Teacher Status
    #     cat(indent(level),'{', sep="", fill=TRUE)
    #     up(level)
    #     cat(indent(level), '"id": "hqt_status",', sep="", fill=TRUE)
        
    #     cat(indent(level), '"data":', LeaHQTStatus(lea_code), sep="", fill=TRUE)	
    #     down(level)
    #     cat(indent(level),'},', sep="", fill=TRUE)
    # }
    # {
    #     ## Graduation
    #     cat(indent(level),'{', sep="", fill=TRUE)
    #     up(level)
    #     cat(indent(level), '"id": "graduation",', sep="", fill=TRUE)
    #     cat(indent(level), '"data": [', sep="", fill=TRUE)
    #     cat(ExGraduation(lea_code, level+1, "lea"), fill=TRUE)
    #     cat('\n',indent(level), ']', sep="", fill=TRUE)
    #     down(level)
    #     cat(indent(level),'},', sep="", fill=TRUE)
    # }
    # # {
    # #     #College Enrollment
    # #     cat(indent(level),'{', sep="", fill=TRUE)
    # #     up(level)
    # #     cat(indent(level), '"id": "college_enroll",', sep="", fill=TRUE)
    # #     cat(indent(level), '"data": [', sep="", fill=TRUE)
    # #     cat(LeaCollegeEnroll(lea_code, level+1), fill=TRUE)
    # #     cat(indent(level), ']', sep="", fill=TRUE)
    # #     down(level)
    # #     cat(indent(level),'},', sep="", fill=TRUE)
    # # }
    # {
    #     #Enrollment
    #     cat(indent(level),'{', sep="", fill=TRUE)
    #     up(level)
    #     cat(indent(level), '"id": "enrollment",', sep="", fill=TRUE)
    #     cat(indent(level), '"data": [', sep="", fill=TRUE)
    #     cat(ExEnrollChunk(lea_code, level+1, "lea"), fill=TRUE)
    #     cat(indent(level), ']', sep="", fill=TRUE)
    #     down(level)
    #     cat(indent(level),'},', sep="", fill=TRUE)
    # }
    # {
    #     ## Median Growth Percentile ##
    #     cat(indent(level),'{', sep="", fill=TRUE)
    #     up(level)
    #     cat(indent(level), '"id": "mgp_scores",', sep="", fill=TRUE)
    #     cat(indent(level), '"data": [', sep="", fill=TRUE)
    #     cat(LeaMGPResult(lea_code, level+1), fill=TRUE)
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
    #     cat(LeaCollegeReadiness(lea_code, level+1), fill=TRUE)
    #     cat(indent(level), ']', sep="", fill=TRUE)
    #     down(level)
    #     cat(indent(level),'},', sep="", fill=TRUE)
    # }
    {
        ## SPED Testing
        cat(indent(level),'{', sep="", fill=TRUE)
        up(level)
        cat(indent(level), '"id": "special_ed",', sep="", fill=TRUE)
        cat(indent(level), '"data": [', sep="", fill=TRUE)
        cat(LeaSPEDChunk(lea_code, level+1), fill=TRUE)
        cat(indent(level), ']', sep="", fill=TRUE)
        down(level)
        cat(indent(level),'},', sep="", fill=TRUE)
    }
    # {
    #     ## SPED APR URL
    #     cat(indent(level),'{', sep="", fill=TRUE)
    #     up(level)
    #     cat(indent(level), '"id": "special_ed_apr_url",', sep="", fill=TRUE)
    #     cat(indent(level), '"data": [', sep="", fill=TRUE)
    #     cat(LeaSpedAprUrl(lea_code, level+1), fill=TRUE)
    #     cat(indent(level), ']', sep="", fill=TRUE)
    #     down(level)
    #     cat(indent(level),'}', sep="", fill=TRUE)
    # }
    down(level)
    cat(indent(level), ']', sep="", fill=TRUE)
    down(level)
    cat(indent(level), '}', fill=TRUE)

    cat('}', fill=TRUE)

    sink()
    close(newfile)
    
}