## Generalized Functions for ReportCards
qstr <- function(x) sprintf('"%s"', x)

WriteJSONChunk <- function(elem_list, wrapper=0){
    wrap_map <- c('"key": ', '"val": ')
    
    json_elems <- mapply(function(elem_name, elem_str_value){
        sprintf('"%s": %s',elem_name, elem_str_value)
    }, names(elem_list), elem_list)

    return(wrap_map[wrapper] %+% '{' %+% paste(json_elems, collapse=", ") %+% '}')
}

ExGraduation <- function(org_code=NA, level=1, entity='state'){
    entity_query <- c(state='', 
        lea=sprintf("AND [lea_code] = '%s'", leadgr(org_code,4)),
        school=sprintf("AND [fy14_entity_code] = '%s'", leadgr(org_code,4)))
    
    .qry <- sprintf("SELECT A.*, B.[fy14_entity_code], B.[fy14_entity_name]
        FROM [dbo].[graduation] A
        LEFT JOIN [dbo].[fy14_mapping] B
        ON A.[school_code] = B.[school_code] 
            AND B.[grade] = '09'
            AND (A.[cohort_year]+2) = B.[ea_year]
        WHERE [cohort_status] = 1 %s", entity_query[entity])

    .grad <- sqlQuery(dbrepcard, .qry)
    
    if(nrow(.grad)>0){
        .ret <- lapply(split(.grad, .grad$cohort_year), WriteGraduation, level)
        .ret <- unlist(.ret)
        .ret <- sort(subset(.ret, .ret!=''))
    } else {
        .ret <- ''
    }
    return(paste(.ret, collapse=',\n'))
}


WriteGraduation <- function(gdata, level){
    .lv <- level
    .ret <- c()
    year <- gdata$cohort_year[1] +4
    .subgroups <- c("African American","White","Hispanic","Asian","American Indian or Alaskan Native", "Pacific Islander", "Multiracial","Special Education","English Learner","Economically Disadvantaged","Male", "Female")

    for(s in 0:length(.subgroups)){
        soutput <- "All"
        .tmps <- gdata
        if(s > 0){
            soutput <- .subgroups[s]
            .tmps <- SubProcGrad(gdata, s)
        }
        
        if(nrow(.tmps)>=10){
            .add <- indent(.lv) %+% '{\n'
            
            up(.lv)
            
            .add <- .add %+% indent(.lv) %+% WriteJSONChunk(c(
                subgroup = qstr(soutput),
                year = qstr(year)), 1) %+% ',\n'
            
            .add <- .add %+% indent(.lv) %+% WriteJSONChunk(c(
                graduates=sum(.tmps$graduated, na.rm=TRUE),
                cohort_size=nrow(.tmps)
                ),2) %+% '\n'
            down(.lv)
            .add <- .add %+% paste(indent(.lv), '}', sep="")
            
            .ret <- c(.ret, .add)
        }
    }
    return(paste(.ret, collapse=',\n'))
}

## Write function for DC CAS Scores
WriteCAS <- function(.casdat_mr, spaces, entity='state'){
    year <- .casdat_mr$year[1]    
    .casdat_mr <- subset(.casdat_mr, exclude %notin% c("M", "A"))
    
    .casdat_mr$math_level[.casdat_mr$exclude %in% c('I','Y')] <- NA
    .casdat_mr$read_level[.casdat_mr$exclude %in% c('I','Y')] <- NA

    .subjects <- c("Math", "Reading")
    .fay <- c("all", "full_year")

    .lv <- spaces

    .ret <- c()
    .plevels <- c("Below Basic", "Basic", "Proficient", "Advanced")

    .entity_fay <- list('state'=c("S", "C", "D"), 'lea'=c("S", "C"), 'school'=c("S"))

    ## A = Subject, 1 for Math, 2 for Reading
    for(a in 1:2){
        ## b = full year or not
        for(b in 1:2){
            ## d = each grade 
            .glevels <- sort(unique(.casdat_mr$tested_grade))
            for(g in 0:length(.glevels)){
                goutput <- ''
                .tmp <- .casdat_mr
                
                if(g == 0){
                    goutput <- 'all'
                } else{
                    goutput <- paste('grade', .glevels[g], sep=" ")
                    .tmp <- subset(.tmp, tested_grade==.glevels[g])
                }
                
                .flevels <- c("N", "S", "D", "C")
                
                if(b==2){
                    .flevels <- .entity_fay[entity][[1]]
                    .tmp <- subset(.tmp, new_to_us =='NO')
                    .tmp <- subset(.tmp, school_grade==tested_grade | alt_tested=="YES")
                    .tmp <- subset(.tmp, school_code %notin% c("0948", "0958"))
                }
                
                .subgroups <- c("African American","White","Hispanic","Asian", "Pacific Islander", "Multiracial","Special Education","English Learner","Economically Disadvantaged","TANF/SNAP Eligible", "Female", "Male")
                
                for(h in 0:12){
                    .tmps <- SubProc(.tmp, h, b)
                    
                    if(h == 0){
                        soutput <- 'All'
                    } else{
                        soutput <- .subgroups[h]
                    }
                    
                    if((nrow(.tmps)>=10 & b==1) | (nrow(.tmps)>=25 & b==2)){
                        invisible(ifelse(a==1, 
                            .profs <- .tmps$math_level[.tmps$full_academic_year %in% .flevels], 
                            .profs <- .tmps$read_level[.tmps$full_academic_year %in% .flevels]))
                        
                        prof_tab <- c(table(.profs))
                        
                        .add <- indent(.lv) %+% '{\n'
                        up(.lv)
                        .add <- .add %+% indent(.lv) %+% WriteJSONChunk(c(
                            subject= qstr(.subjects[a]),
                            grade= qstr(goutput),
                            enrollment_status=qstr(.fay[b]),
                            subgroup = qstr(soutput),
                            year = qstr(year)), 1) %+% ',\n'
                            
                        .add <- .add %+% indent(.lv) %+% WriteJSONChunk(c(
                            n_eligible=checkna(length(.profs)),
                            n_test_takers=length(.profs[.profs %in% .plevels]),
                            advanced_or_proficient=checkna(length(.profs[.profs %in% c("Proficient", "Advanced")])),
                            advanced=checkna(length(.profs[.profs %in% "Advanced"])),
                            proficient=checkna(length(.profs[.profs %in% "Proficient"])),
                            basic=checkna(length(.profs[.profs %in% "Basic"])),
                            below_basic=checkna(length(.profs[.profs %in% "Below Basic"]))
                            ),2) %+% '\n'
                        down(.lv)
                        .add <- .add %+% paste(indent(.lv), '}', sep="")
                        .ret <- c(.ret, .add)
                    }
                }
            }
        }
    }

    for(z in .fay){
        .tmp <- subset(.casdat_mr, new_to_us=='YES')
        if(z == "full_year"){
            .tmp <- subset(.tmp, full_academic_year %in% .entity_fay[entity][[1]])
            .tmp <- subset(.tmp, school_grade==tested_grade | alt_tested=="YES")
            .tmp <- subset(.tmp, school_code %notin% c("0948", "0958"))
        }
        
        .profs <- .tmp$read_level
        
        if(length(.profs) >= 10){
            
            prof_tab <- c(table(.profs))
            
            .add <- indent(.lv) %+% '{\n'
            up(.lv)
            .add <- .add %+% indent(.lv) %+% WriteJSONChunk(c(
                subject= '"Reading"',
                grade= '"all"',
                enrollment_status= qstr(z),
                subgroup = '"New to the US (ELL)"',
                year = qstr(year)), 1) %+% ',\n'
            
            .add <- .add %+% indent(.lv) %+% WriteJSONChunk(c(
                    n_eligible=checkna(length(.profs)),
                    n_test_takers=length(.profs[.profs %in% .plevels]),
                    advanced_or_proficient=checkna(length(.profs[.profs %in% c("Proficient", "Advanced")])),
                    advanced=checkna(length(.profs[.profs %in% "Advanced"])),
                    proficient=checkna(length(.profs[.profs %in% "Proficient"])),
                    basic=checkna(length(.profs[.profs %in% "Basic"])),
                    below_basic=checkna(length(.profs[.profs %in% "Below Basic"]))
                ),2) %+% '\n'
            down(.lv)
            .add <- .add %+% paste(indent(.lv), '}', sep="")
            .ret <- c(.ret, .add)
        }
    }

    return(paste(.ret, collapse=',\n'))
}


### THE ENROLLMENT SECTION
ExEnrollChunk <- function(org_code=NA, level=1, entity="state"){
    .lv <- level
    
    entity_query <- c(state='', 
    lea=sprintf("AND [lea_code] = '%s'", leadgr(org_code,4)),
    school=sprintf("AND [fy14_entity_code] = '%s'", leadgr(org_code,4)))

    .qry_enr <- sprintf("SELECT A.*,
            B.[fy14_entity_code],
            B.[fy14_entity_name]
          FROM [dbo].[enrollment] A
        LEFT JOIN [dbo].[fy14_mapping] B
        ON A.[school_code] = B.[school_code] 
            AND A.[ea_year] = B.[ea_year] 
            AND A.[grade] = B.[grade]
        WHERE A.[ea_year] is not NULL %s
        ORDER BY A.[ea_year]", entity_query[entity])

    .dat_enr <- sqlQuery(dbrepcard, .qry_enr)
    ##print(head(.dat_enr))
    if(nrow(.dat_enr)>= 10){
        .ret <- lapply(split(.dat_enr, .dat_enr$ea_year), WriteEnroll, level)
    } else{ .ret <- '' }

    return(paste(unlist(.ret), collapse=',\n'))
}

WriteEnroll <- function(.edat, level){
    .ret <- c()

    if(nrow(.edat) < 10){
        return(NULL)
    }

    year <- .edat$ea_year[1]
    .lv <- level
    .edat$grade <- sapply(.edat$grade, leadgr, 2)

    .glist <- unique(.edat$grade)

    .subgroups <- c("African American","White","Hispanic","Asian","American Indian", "Pacific Islander", "Multiracial","Special Education","English Learner","Economically Disadvantaged","Male", "Female","TANF/SNAP Eligible")

    for(g in 0:length(.glist)){
        .tmp <- .edat
        goutput <- "All"
        if(g>0){
            .tmp <- subset(.tmp, grade==.glist[g])
            goutput <- .glist[g]
        }
        
        for(s in 0:length(.subgroups)){
            
            soutput <- "All"
            .tmps <- .tmp
            if(s > 0){
                soutput <- .subgroups[s]
                .tmps <- SubProcEnr(.tmp, s)
            }
            
            if(nrow(.tmps)>=10){
                .add <- indent(.lv) %+% '{\n'
                up(.lv)
                .add <- .add %+% indent(.lv) %+% WriteJSONChunk(c(
                    grade=sprintf('"%s"', goutput), 
                    subgroup=sprintf('"%s"', soutput),
                    year=sprintf('"%s"', year)),1) %+% ',\n'
                .add <- .add %+% paste(indent(.lv), '"val": ',nrow(.tmps),'\n', sep="")
                down(.lv)
                .add <- .add %+% paste(indent(.lv), '}', sep="")
                .ret <- c(.ret, .add)
            }
        }
    }

    return(paste(.ret, collapse=',\n'))
}

### AMO Development
ExAMOs <- function(org_code, level, entity="school"){
    .qry <- sprintf("SELECT * 
        FROM [dbo].[amo_status_export]
        WHERE [current_entity_code] = '%s'
            AND [entity_level] = '%s'", leadgr(org_code, 4), entity)
    .amo_dat <- sqlQuery(dbrepcard, .qry)
    .ret <- c()
    
    .lv <- level
    if(nrow(.amo_dat) > 0){
        for(i in 1:nrow(.amo_dat)){
            for(j in c("math", "read")){
                .add <- indent(.lv) %+% '{\n'
                up(.lv)
                .add <- .add %+% indent(.lv) %+% WriteJSONChunk(c(
                    subject=ifelse(j=='math', '"Math"', '"Reading"'), 
                    grade='"all"',
                    enrollment_status='"full_year"',
                    subgroup=sprintf('"%s"', .amo_dat$subgroup[i]),
                    year=sprintf('"%s"', .amo_dat$year[i])),1) %+% ', \n'
                .add <- .add %+% indent(.lv) %+% WriteJSONChunk(c(
                    baseline=checkna(.amo_dat[i, j %+% '_baseline']),
                    target=checkna(.amo_dat[i, j %+% '_target'])),2) %+% '\n'
                
                down(.lv)
                .add <- .add %+% paste(indent(.lv), '}', sep="")
                .ret <- c(.ret, .add)
            }
        }
        return(paste(.ret, collapse=',\n'))
    }
}
