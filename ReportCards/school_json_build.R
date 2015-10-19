## Generate School File ## 
## Detail Data File Build ##
setwd("U:/LearnDC ETL V2/ReportCards")

# active <- shell('echo %CODE_PATH%', intern=TRUE)
# active <- gsub('\\\\', '/', active)
# setwd(paste0(active, 'ReportCards'))

source("U:/R/tomkit.R")
source("./school_functions.R")
source("./generalized.R")
library(dplyr)

school_dir <- sqlQuery(dbrepcard,'select * from dbo.schooldir_linked_sy1314')
school_dir$school_code <- sapply(school_dir$school_code, leadgr, 4)
school_dir$lea_code <- sapply(school_dir$lea_code, leadgr, 4)
school_dir <- subset(school_dir, school_code %notin% c("7000", "0948", "0958", "0480", "0472", "0465"))

## Start File Generation
for(i in unique(school_dir$school_code)){
    
    setwd("U:/LearnDC ETL V2/Export/JSON/school")

    if(file.exists(i)){
        setwd(file.path(i))
    }

    .tmp <- subset(school_dir, school_code == i)

    .org_type <- "school"
    .org_code <- .tmp$school_code[1]
    .lea_code <- .tmp$lea_code[1]
    .lea_name <- .tmp$lea_name[1]
    .school_name <- .tmp$school_name[1]

    newfile <- file("accountability.json", encoding="UTF-8")
    # newfile <- file("special_ed.json", encoding="UTF-8")

    if(!is.na(.tmp$profile_name[1])){
        .prof_name <- .tmp$profile_name[1]
    } else{
        .prof_name <- .tmp$school_name[1]
    }

    sink(newfile)
    cat('{', fill=TRUE)

    level <- 1
    cat(indent(level),'"timestamp": "',date(),'",', sep="", fill=TRUE)
    cat(indent(level),'"org_type": "school",', sep="", fill=TRUE)
    cat(indent(level),'"org_name": "',.school_name,'",', sep="", fill=TRUE)
    cat(indent(level),'"org_code": "',.org_code,'",', sep="", fill=TRUE)

    ######### INSERT DATA HERE ##################
    {
        ### Accountability
        cat(indent(level), '"exhibit": {', fill=TRUE)
        up(level)
        cat(indent(level), '"id": "accountability",', sep="", fill=TRUE)
        cat(indent(level), '"data": [', sep="")
        up(level)
        cat(ExAccountability(.org_code, level), fill=TRUE) 
        down(level)
        cat(indent(level),']', sep="", fill=TRUE)
        down(level)
        cat(indent(level),'}', sep="", fill=TRUE)
    }
    # {
    #     ## Assessment
    #     cat(indent(level),'{', sep="", fill=TRUE)
    #     up(level)
    #     cat(indent(level), '"id": "dccas",', sep="", fill=TRUE)
    #     cat(indent(level), '"data": [', sep="", fill=TRUE)
    #     cat(ExCasChunk(org_code, level+1))
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
    #     cat(ExAMOs(.org_code, level+1))
    #     cat('\n',indent(level), ']', sep="", fill=TRUE)
    #     down(level)
    #     cat(indent(level),'},', sep="", fill=TRUE)
    # }
    # {
    #     ## Early Childhood
    #     cat(indent(level),'{', sep="", fill=TRUE)
    #     up(level)
    #     cat(indent(level), '"id": "early_childhood",', sep="", fill=TRUE)
        
    #     cat(indent(level), '"data": [', sep="", fill=TRUE)
    #     cat(ExAccreditation(org_code, level+1), fill=TRUE)
        
    #     cat(indent(level), ']', sep="", fill=TRUE)
    #     down(level)
    #     cat(indent(level),'},', sep="", fill=TRUE)
    # }
    # {
    #     #College Enrollment
    #     cat(indent(level),'{', sep="", fill=TRUE)
    #     up(level)
    #     cat(indent(level), '"id": "college_enroll",', sep="", fill=TRUE)
    #     cat(indent(level), '"data": [', sep="", fill=TRUE)
    #     ##cat(ExCollegeEnroll(org_code, level+1), fill=TRUE)
    #     cat(indent(level), ']', sep="", fill=TRUE)
    #     down(level)
    #     cat(indent(level),'},', sep="", fill=TRUE)
    # }
    # if(school_dir$lea_code[1] != '0001'){
    #     #PCSB PMF
    #     cat(indent(level),'{', sep="", fill=TRUE)
    #     up(level)
    #     cat(indent(level), '"id": "pcsb_pmf",', sep="", fill=TRUE)
    #     cat(indent(level), '"data": [', sep="", fill=TRUE)
    #     cat(ExPMF(.org_code, level+1), fill=TRUE)
    #     cat(indent(level), ']', sep="", fill=TRUE)
    #     down(level)
    #     cat(indent(level),'},', sep="", fill=TRUE)
    # }
    # {
    #     ## Graduation  -- end of report card section
    #     cat(indent(level),'{', sep="", fill=TRUE)
    #     up(level)
    #     cat(indent(level), '"id": "graduation",', sep="", fill=TRUE)
    #     cat(indent(level), '"data": [', sep="", fill=TRUE)
    #     cat(ExGraduation(.org_code, level+1, "school"), fill=TRUE)
    #     cat('\n',indent(level), ']', sep="", fill=TRUE)
    #     down(level)
    #     cat(indent(level),'},', sep="", fill=TRUE)
    # }
    # {
    #     # ## Program Info
    #     # cat(indent(level),'{', sep="", fill=TRUE)
    #     # up(level)
    #     # cat(indent(level), '"id": "program_info",', sep="", fill=TRUE)
        
    #     # cat(indent(level), '"data": [', sep="")
    #     # cat(AppendProgramInfo(.org_code), fill=TRUE)
        
    #     # cat(indent(level), ']', sep="", fill=TRUE)
    #     # down(level)
    #     # cat(indent(level),'},', sep="", fill=TRUE)
    # }
    # {
    #     #Enrollment
    #     cat(indent(level),'{', sep="", fill=TRUE)
    #     up(level)
    #     cat(indent(level), '"id": "enrollment",', sep="", fill=TRUE)
    #     cat(indent(level), '"data": [', sep="", fill=TRUE)
    #     cat(ExEnrollChunk(org_code, level+1, "school"), fill=TRUE)
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
    #     cat(ExMGPResult(org_code, level+1), fill=TRUE)
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
    #     cat(ExCollegeReadiness(org_code, level+1), fill=TRUE)
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
    #     cat(ExSPEDChunk(.org_code, level+1), fill=TRUE)
    #     cat(indent(level), ']', sep="", fill=TRUE)
    #     down(level)
    #     cat(indent(level),'},', sep="", fill=TRUE)
    # }
    # {
    #     ## ELL/AMAO Stuff ##
    # }
    ## Equity Report Starts Here
    # {
    #     #### Absences
    #     cat(indent(level),'{', sep="", fill=TRUE)
    #     up(level)
    #     cat(indent(level), '"id": "unexcused_absences",', sep="", fill=TRUE)
    #     cat(indent(level), '"data": [', sep="", fill=TRUE)
    #     cat(WriteAbsences(org_code, level+1), fill=TRUE)
    #     cat(indent(level), ']', sep="", fill=TRUE)
    #     down(level)
    #     cat(indent(level),'},', sep="", fill=TRUE)
    # }
    # {
    #     #### Attendance
    #     cat(indent(level),'{', sep="", fill=TRUE)
    #     up(level)
    #     cat(indent(level), '"id": "attendance",', sep="", fill=TRUE)
    #     cat(indent(level), '"data": [', sep="", fill=TRUE)
    #     cat(WriteAttendance(org_code, level+1), fill=TRUE)
    #     cat(indent(level), ']', sep="", fill=TRUE)
    #     down(level)
    #     cat(indent(level),'},', sep="", fill=TRUE)
    # }
    # {
    #     #### Suspensions
    #     cat(indent(level),'{', sep="", fill=TRUE)
    #     up(level)
    #     cat(indent(level), '"id": "suspensions",', sep="", fill=TRUE)
    #     cat(indent(level), '"data": [', sep="", fill=TRUE)
    #     cat(WriteSuspensions(org_code, level+1), fill=TRUE)
    #     cat(indent(level), ']', sep="", fill=TRUE)
    #     down(level)
    #     cat(indent(level),'},', sep="", fill=TRUE)
    # }
    # {
    #     #### Expulsions
    #     cat(indent(level),'{', sep="", fill=TRUE)
    #     up(level)
    #     cat(indent(level), '"id": "expulsions",', sep="", fill=TRUE)
    #     cat(indent(level), '"data": [', sep="", fill=TRUE)
    #     cat(WriteExpulsions(org_code, level+1), fill=TRUE)
    #     cat(indent(level), ']', sep="", fill=TRUE)
    #     down(level)
    #     cat(indent(level),'},', sep="", fill=TRUE)
    # }
    # {
    #     #### Mid-Year Entry and Withdrawal
    #     cat(indent(level),'{', sep="", fill=TRUE)
    #     up(level)
    #     cat(indent(level), '"id": "mid_year_entry_and_withdrawal",', sep="", fill=TRUE)
    #     cat(indent(level), '"data": [', sep="", fill=TRUE)
    #     cat(WriteEnterWithdraw(org_code, level+1), fill=TRUE)
    #     cat(indent(level), ']', sep="", fill=TRUE)
    #     down(level)
    #     cat(indent(level),'},', sep="", fill=TRUE)
    # }
    # {
    #     #### Staff Degree Data
    #     cat(indent(level),'{', sep="", fill=TRUE)
    #     up(level)
    #     cat(indent(level), '"id": "staff_degree",', sep="", fill=TRUE)
    #     cat(indent(level), '"data": [', sep="", fill=TRUE)
    #     cat(WriteStaffExp(org_code, level+1), fill=TRUE)
    #     cat(indent(level), ']', sep="", fill=TRUE)
    #     down(level)
    #     cat(indent(level),'}', sep="", fill=TRUE)
    # }

    
    cat('}', fill=TRUE)
    sink()
    close(newfile)
}