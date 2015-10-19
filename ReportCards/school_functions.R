## cat(ExAMOs(210, 1), fill=TRUE)
##"subgroup": ("All","African American","White","Hispanic","Asian","Special Education","English Learner","Economically Disadvantaged","Homeless Students"),
AppendProgramInfo <- function(org_code){
    .qry <- "SELECT DISTINCT * FROM [dbo].[school_programs]
        WHERE [school_code] = '" %+% leadgr(org_code,4) %+% "'"
    .prog <- sqlQuery(dbrepcard, .qry)

    .ret <- c()
    if(nrow(.prog)>0){

        .prog$program_string <- gsub('\n', "", .prog$program_string)
        .prog$program_string <- gsub('\r', "", .prog$program_string)
        .prog$program_string <- gsub('"', "'", .prog$program_string)
        .prog$program_string <- paste0('"',.prog$program_string,'"')
        
        return(paste(.prog$program_string, collapse=','))
    } else{
        return('')
    }
}

##"subgroup": ("All","African American","White","Hispanic","Asian","Special Education","English Learner","Economically Disadvantaged","Homeless Students")
RetMGPGroup <- function(.ingrp){
	if(.ingrp == 'All Students'){
		return('All')
	} else if(.ingrp == 'WH7'){
		return('White')
	} else if(.ingrp == 'Not-SpEd'){
		return('Not Special Education')
	} else if(.ingrp == 'SpEd'){
		return('Special Education')
	} else if (.ingrp == 'Not-LEP'){
		return('Not English Learner')
	} else if (.ingrp == 'Not-FARMS'){
		return('Not Economically Disadvantaged')
	} else if (.ingrp == 'FARMS'){
		return('Economically Disadvantaged')
	} else if (.ingrp == 'AS7'){
		return('Asian')
	} else if (.ingrp == 'BL7'){
		return('African American')
	} else if (.ingrp == 'HI7'){
		return('Hispanic')
	} else if (.ingrp == 'AM7'){
		return('American Indian/Alaskan Native')
	} else if (.ingrp == 'MU7'){
		return('Multi Racial')
	} else if (.ingrp == 'LEP'){
		return('English Learner')
	} else if (.ingrp == 'PI7'){
		return('Pacific Islander')
	} else{
		return(.ingrp)
	}
}

ExMGPResult <- function(org_code, level){
    .lv <- level
    .qry <- "SELECT * FROM [dbo].[mgp_summary]
        WHERE [fy13_entity_code] = '" %+% leadgr(org_code,4) %+% "'"
    .mgp <- sqlQuery(dbrepcard, .qry)
    .ret <- c()
    if(nrow(.mgp)>0){
        for(i in 1:nrow(.mgp)){
            if(.mgp$group_fay_size[i] >= 10 ){
                .add <- indent(.lv) %+% '{\n'
                up(.lv)
                .add <- .add %+% indent(.lv) %+% '"key": '
                .add <- .add %+% WriteJSONChunk(c(subject=sprintf('"%s"', .mgp$subject[i]), 
                    subgroup=sprintf('"%s"', RetMGPGroup(.mgp$group[i])),
                    year=sprintf('"%s"', .mgp$year[i]))) %+% ',\n'
                .add <- .add %+% indent(.lv) %+% '"val": '
                .add <- .add %+% WriteJSONChunk(c(group_size=checkna(.mgp$group_fay_size[i]),
                    mgp_1yr=checkna(.mgp$mgp_1yr[i]),                    
                    mgp_2yr=checkna(.mgp$mgp_2yr[i]))) %+% '\n'
                down(.lv)
                .add <- .add %+% paste(indent(.lv), '}', sep="")
                .ret <- c(.ret, .add)
            }
        }
    }
    return(paste(.ret, collapse=',\n'))
}

ExCollegeReadiness <- function(org_code, level){
    .lv <- level

    .qry <- "SELECT * FROM [dbo].[college_readiness]
        WHERE [fy13_entity_code] = '" %+% org_code %+% "'"
        
    .cready <- sqlQuery(dbrepcard, .qry)
    ##print(.cready)

    years <- unique(.cready$year)

    .ret <- c()

    for(i in years){
        .tmp <- subset(.cready, year == i)
        .ret <- c(.ret, EncodeCReady(.tmp, level))
    }

    return(paste(.ret, collapse=',\n'))
}

EncodeCReady <- function(.dat, level){
    .lv <- level
    .subgroups <- c("African American","White","Hispanic","Asian","American Indian or Alaskan Native", "Pacific Islander", "Multiracial","Special Education","English Learner","Economically Disadvantaged","Male", "Female")

    .ret <- c()
    for(j in 0:12){
        .tmp <- .dat
        .slice <- 'All'
        if(j > 0){
            .tmp <- SubProcGrad(.tmp, j)
            .slice <- .subgroups[j]
        }
        
        if(nrow(.tmp)>=10){
            .add <- indent(.lv) %+% '{\n'
            
            up(.lv)
            .add <- .add %+% paste(indent(.lv), '"key": {\n', sep="")
            up(.lv)
            .add <- .add %+% paste(indent(.lv), '"subgroup": "', .slice,'", \n', sep="")
            .add <- .add %+% paste(indent(.lv), '"year": "',.tmp$year[1],'" \n', sep="")
            down(.lv)
            .add <- .add %+% paste(indent(.lv), '},\n', sep="")
            
            .add <- .add %+% paste(indent(.lv), '"val": {\n', sep="")
            up(.lv)
            .add <- .add %+% paste(indent(.lv), '"graduates": ', nrow(.tmp),',\n', sep="")
            .add <- .add %+% paste(indent(.lv), '"act_taker": ', nrow(subset(.tmp, act_taker=='YES')),',\n', sep="")
            .add <- .add %+% paste(indent(.lv), '"sat_taker": ', nrow(subset(.tmp, sat_taker=='YES')),',\n', sep="")
            .add <- .add %+% paste(indent(.lv), '"ap_taker": ', nrow(subset(.tmp, ap_taker=='YES')),',\n', sep="")
            .add <- .add %+% paste(indent(.lv), '"psat_taker": ', nrow(subset(.tmp, psat_taker=='YES')),'\n', sep="")
            down(.lv)
            .add <- .add %+% paste(indent(.lv), '}\n', sep="")
            down(.lv)
            .add <- .add %+% paste(indent(.lv), '}', sep="")
            
            .ret <- c(.ret, .add)
        }
    }
    return(paste(.ret, collapse=',\n'))
}

SubProcGrad <- function(.dat, lv){
    if(lv==1){
        return(subset(.dat, race=="BL7"))
    } else if(lv==2){
        return(subset(.dat, race=="WH7"))
    } else if(lv==3){
        return(subset(.dat, race=="HI7"))
    } else if(lv==4){
        return(subset(.dat, race=="AS7"))
    } else if(lv==5){
        return(subset(.dat, race=="AM7"))
    } else if(lv==6){
        return(subset(.dat, race=="PI7"))
    } else if(lv==7){
        return(subset(.dat, race=="MU7"))
    } else if(lv==8){
        return(subset(.dat, special_ed=="YES"))
    } else if(lv==9){
        return(subset(.dat, ell_prog=="YES"))
    } else if(lv==10){
        return(subset(.dat, economy == "YES"))
    } else if(lv==11){
        return(subset(.dat, gender %in% c("M", "MALE")))
    } else if(lv==12){
        return(subset(.dat, gender %in% c("F", "FEMALE")))
    } else if(lv==13){
        return(subset(.dat, tanf_snap == 'YES'))
    }
    return(.dat[NULL,])
}

ExTest <- function(org_code){
    pull_tables <- c('
        _sy1011', 'pmf_sy1112', 'pmf_sy1213')
    .pmf_dat <- lapply(sprintf("SELECT * FROM [dbo].[%s]
    WHERE [school_code] = '%s'", pull_tables, as.character(org_code)), sqlQuery, channel=dbrepcard)
    .pmf_dat
}

ExPMF <- function(org_code, level){
    .lv <- level
    pull_tables <- c('pmf_sy1011', 'pmf_sy1112', 'pmf_sy1213','pmf_sy1314')
    
    .pmf_dat <- lapply(sprintf("SELECT * FROM [dbo].[%s]
        WHERE [school_code] = '%s'", pull_tables, as.character(org_code)), sqlQuery, channel=dbrepcard)

    names(.pmf_dat) <- c("2011", "2012", "2013","2014")
    .pmf_dat <- .pmf_dat[sapply(.pmf_dat, nrow) > 0]
    
    if(length(.pmf_dat) >0){    
        .qry_profile <- "SELECT * FROM [dbo].[pmf_sy1314]
            WHERE [school_code] = '" %+% org_code %+% "'"
        .prog_profile <- sqlQuery(dbrepcard, .qry_profile)
        
        if(nrow(.prog_profile)>0){ 
            .profile <- '"' %+% .prog_profile$link %+% '"'
        }
        else{
            .profile <- 'null'
        }
        .ret <- mapply(function(x,pmf_yr){
            .ret <- c()
            for(i in 1:nrow(x)){
                if(is.na(pmax(x$score)) || x$score[i] == max(x$score)){
                .add <- indent(.lv) %+% '{\n'
                up(.lv)
                    .add <- .add %+% indent(.lv) %+% '"key": '
                    .add <- .add %+% WriteJSONChunk(c(year= sprintf('"%s"', pmf_yr), 
                        category=sprintf('"%s"', x$framework[i]))) %+% ', \n'
                    .add <- .add %+% indent(.lv) %+% '"val": '
                    .add <- .add %+% WriteJSONChunk(c(score=checkna(x$score[i]),
                        tier=checkna_str(x$tier[i]),url =.profile)) %+% '\n'                
                    down(.lv)
                    .add <- .add %+% paste(indent(.lv), '}', sep="")
                    .ret <- c(.ret, .add)
                }
            }
            return(.ret)
        }, .pmf_dat, names(.pmf_dat))
        return(paste(.ret, collapse=',\n'))
    }
}

## .subgroups <- c("African American","White","Hispanic","Asian","American Indian", "Pacific Islander", "Multi Racial","Special Education","English Learner","Economically Disadvantaged","Male", "Female")
SubCEnroll <- function(subgroup){
	if(subgroup == 'Total high school graduates'){
		return('All')
	} else if(subgroup=='Black or African American'){
		return('African American')
	} else if(subgroup=='Hispanic / Latino'){
		return('Hispanic')
	} else if(subgroup=='Two or more races'){
		return('Multiracial')
	} else if(subgroup=='Economically Disadvantaged=Y'){
		return('Economically Disadvantaged')
	} else if(subgroup=='Economically Disadvantaged=N'){
		return('Not Economically Disadvantaged')
	} else if(subgroup=='Limited English=Y'){
		return('English Learner')
	} else if(subgroup=='Limited English=N'){
		return('Not English Learner')
	} else if(subgroup=='Disability=Y'){
		return('Special Education')
	} else if(subgroup=='Disability=N'){
		return('Not Special Education')
	}
	return(subgroup)
}

ExCollegeEnroll <- function(org_code, level){
	.lv <- level
	
	.qry <- "SELECT * FROM [dbo].[college_enroll_2010]
		WHERE [fy13_entity_code] = '" %+% org_code %+% "'
		AND [Graduates] >= 10;"
	.cenr10 <- sqlQuery(dbrepcard, .qry)
	.ret <- c()

	if(nrow(.cenr10)> 0){
		.ret <- c(.ret, WriteCEnroll(.cenr10, .lv, 2010))
	}

	.qry <- "SELECT * FROM [dbo].[college_enroll_2009]
		WHERE [fy13_entity_code] = '" %+% org_code %+% "'
		AND [Graduates] >= 10;"
	.cenr09 <- sqlQuery(dbrepcard, .qry)

	if(nrow(.cenr09)> 0){
		.ret <- c(.ret, WriteCEnroll(.cenr09, .lv, 2009))
	}		
	
	.qry <- "SELECT * FROM [dbo].[college_enroll_2008]
		WHERE [fy13_entity_code] = '" %+% org_code %+% "'
		AND [Graduates] >= 10;"
	.cenr08 <- sqlQuery(dbrepcard, .qry)

	if(nrow(.cenr08)> 0){
		.ret <- c(.ret, WriteCEnroll(.cenr08, .lv, 2008))
	}		
		
	.qry <- "SELECT * FROM [dbo].[college_enroll_2007]
		WHERE [fy13_entity_code] = '" %+% org_code %+% "'
		AND [Graduates] >= 10;"
	.cenr07 <- sqlQuery(dbrepcard, .qry)

	if(nrow(.cenr07)> 0){
		.ret <- c(.ret, WriteCEnroll(.cenr07, .lv, 2007))
	}		
	
	if(length(.ret)>0){
		return(paste(.ret, collapse=',\n'))
	}else{
		return('')
	}
}

WriteCEnroll <- function(.cenr, level, year){
    .ret <- c()
    .lv <- level
    for(i in 1:nrow(.cenr)){
        .add <- indent(.lv) %+% '{\n'
        up(.lv)
        .add <- .add %+% indent(.lv) %+% '"key": '
        .add <- .add %+% WriteJSONChunk(c(cohort_year=sprintf('"%s"', year), 
            subgroup=sprintf('"%s"', SubCEnroll(.cenr$Group[i])))) %+% ',\n'
        .add <- .add %+% indent(.lv) %+% '"val": '
        .add <- .add %+% WriteJSONChunk(c(hs_graduates=checkna(.cenr$Graduates[i]),
            enroll_within_16mo=checkna(.cenr$Initial_Enroll_16mo[i]),
            enroll_within_16mo_instate=checkna(.cenr$Initial_Enroll_InState_16mo[i]),
            complete_1yr_instate=checkna(.cenr$Complete_1Yr_in_State[i]))
            ) %+% '\n'
        down(.lv)
        .add <- .add %+% paste(indent(.lv), '}', sep="")
        .ret <- c(.ret, .add)
    }
    return(.ret)
}

ExAccountability <- function(org_code, level){
    ## MATH/READING
    .lv <- level

    .qry <- "SELECT * FROM [dbo].[accountability_sy1314]
        WHERE [school_code] = '" %+% org_code %+% "';"
    .acct <- sqlQuery(dbrepcard, .qry)

    ##print(.acct)
    .ret <- c()
    if(nrow(.acct)>0){
        .qry <- "SELECT * FROM [dbo].[accountability_sy1314_sg]
        WHERE [school_code] = '" %+% org_code %+% "';"
        .acct_sg <- sqlQuery(dbrepcard, .qry)

        .ret <- '{\n'
        up(.lv)
        
        .ret <- .ret %+% paste(indent(.lv), '"year": "2013", \n', sep="")
        .ret <- .ret %+% paste(indent(.lv), '"score": ',checkna(round(.acct$acct_score[1],2)),', \n', sep="")
        .ret <- .ret %+% paste(indent(.lv), '"classification": ',checkna_str(.acct$classification[1]),', \n', sep="")
        .ret <- .ret %+% paste(indent(.lv), '"growth": ',checkna(round(.acct$growth[1],2)),', \n', sep="")
        
        .ret <- .ret %+% paste(indent(.lv), '"subgroups": [\n', sep="")
        up(.lv)
        .sgstrings <- c()
        if(nrow(.acct_sg)>0){
            for(i in 1:nrow(.acct_sg)){
                .add <- ''
                if(.acct_sg$read_size[i] >= 25 | .acct_sg$math_size[i] >= 25 | .acct_sg$comp_size[i] >= 25){
                    .add <- indent(.lv) %+% '{\n'
                    up(.lv)
                    
                    .add <- .add %+% paste(indent(.lv), '"subgroup": ',checkna_str(.acct_sg$subgroup[i]),',\n', sep="")
                    
                    .add <- .add %+% paste(indent(.lv), '"read_size": ',checkna(.acct_sg$read_size[i]),',\n', sep="")
                    .add <- .add %+% paste(indent(.lv), '"read_score": ',checkna(round(.acct_sg$read_score[i],2)),',\n', sep="")
                    
                    .add <- .add %+% paste(indent(.lv), '"math_size": ',checkna(.acct_sg$math_size[i]),',\n', sep="")
                    .add <- .add %+% paste(indent(.lv), '"math_score": ',checkna(round(.acct_sg$math_score[i],2)),',\n', sep="")
                    
                    .add <- .add %+% paste(indent(.lv), '"comp_size": ',checkna(.acct_sg$comp_size[i]),',\n', sep="")
                    .add <- .add %+% paste(indent(.lv), '"comp_score": ',checkna(round(.acct_sg$comp_score[i],2)),'\n', sep="")
                    
                    
                    down(.lv)		
                    .add <- .add %+% paste(indent(.lv), '}', sep="")
                    .sgstrings <- c(.sgstrings, .add)
                }
            }
            .ret <- .ret %+% paste(.sgstrings, collapse=',\n') %+% '\n'
        }
        down(.lv)
        .ret <- .ret %+% paste(indent(.lv), ']\n', sep="")
        down(.lv)
        .ret <- .ret %+% paste(indent(.lv), '}', sep="")
        return(.ret)
    } else{
        return('null')
    }
    return('null')
}

ExAccreditation <- function(org_code, level=0){
    ## MATH/READING
    .lv <- level

    .qry <- "SELECT * FROM [dbo].[ece_accr_sy1213]
        WHERE [school_code] = '" %+% org_code %+% "';"
    .accr <- sqlQuery(dbrepcard, .qry)

    .ret <- c()
    if(nrow(.accr)>0){
        for(i in 1:nrow(.accr)){
            .add <- indent(level) %+% '{\n'
            up(.lv)
            
            .add <- .add %+% sprintf(indent(.lv) %+% '"type": %s,\n', checkna_str(.accr$accreditation_type[i]))
            .add <- .add %+% sprintf(indent(.lv) %+% '"level": %s,\n', checkna_str(.accr$accreditation_level[i]))
            .add <- .add %+% sprintf(indent(.lv) %+% '"exp_date": %s\n', checkna_str(.accr$exp_date[i]))

            down(.lv)
            .add <- .add %+% indent(.lv) %+% '}'
            .ret <- c(.ret, .add)
        }
    } 
    return(paste(.ret, collapse=',\n'))
}

ExSPEDChunk <- function(scode, level){
	.qry_sped_cas <- sprintf("SELECT A.* FROM [dbo].[assessment] A
		INNER JOIN (SELECT 
			[ea_year]
			,[school_code]
			,[grade]
		FROM [dbo].[fy14_mapping]
		WHERE [fy14_entity_code] = '%s') B
	ON A.[ea_year] = B.[ea_year] 
		AND A.[school_grade] = B.[grade]
		AND A.[school_code] = B.[school_code]
	WHERE A.[sped_indicator] = '1'", leadgr(scode,4))
	
    .dat_mr <- sqlQuery(dbrepcard, .qry_sped_cas)
    # .ret <- ifelse(nrow(.dat_mr)>=10, do(group_by(.dat_mr, ea_year), WriteSPED, level), "")
    .ret <- lapply(split(.dat_mr, .dat_mr["ea_year"]), WriteSPED, level)

	.ret <- subset(.ret, .ret != '')
	return(paste(.ret, collapse=',\n'))
}

WriteSPED <- function(.casdat_mr, level){
    year <- .casdat_mr$year[1]
    .subjects <- c("Math", "Reading")
    .lv <- level

    .ret <- c()
    .plevels <- c("Below Basic", "Basic", "Proficient", "Advanced")

    ## A = Subject, 1 for Math, 2 for Reading
    for(a in 1:2){
        for(b in 1:4){
            soutput <- c("All SPED Students", "With Accommodations", "No Accommodations", "ALT Test Takers")
            .tmp <- .casdat_mr
            if(b == 2){
                .tmp <- subset(.casdat_mr, sped_acc == '1')
            } else if(b == 3){
                .tmp <- subset(.casdat_mr, sped_acc != '1')
            } else if(b==4){
                .tmp <- subset(.casdat_mr, alt_tested=='1')
            }
            
            bleh <- ifelse(a==1, .profs<- .tmp$math_level, .profs<- .tmp$read_level)
            ## START WRITE ##
            if(length(.profs[.profs %in% .plevels]) >= 10){
                .add <- indent(.lv) %+% '{\n'
                up(.lv)
                .add <- .add %+% indent(.lv) %+% '"key": '
                .add <- .add %+% WriteJSONChunk(c(subject=sprintf('"%s"', .subjects[a]), 
                    subgroup=sprintf('"%s"', soutput[b]),
                    year=sprintf('"%s"', year))) %+% ',\n'

                .add <- .add %+% indent(.lv) %+% '"val": '
                .add <- .add %+% WriteJSONChunk(c(n_eligible=checkna(length(.profs)),
                    n_test_takers=length(.profs[.profs %in% .plevels]),
                    advanced_or_proficient=checkna(length(.profs[.profs %in% c("Proficient", "Advanced")])),
                    advanced=checkna(length(.profs[.profs %in% "Advanced"])),
                    proficient=checkna(length(.profs[.profs %in% "Proficient"])),
                    basic=checkna(length(.profs[.profs %in% "Basic"])),
                    below_basic=checkna(length(.profs[.profs %in% "Below Basic"]))
                    )) %+% '\n'
                down(.lv)
                .add <- .add %+% paste(indent(.lv), '}', sep="")
                .ret <- c(.ret, .add)
            }
        }
    }
    return(paste(.ret, collapse=',\n'))
}

##"African American","White","Hispanic","Asian","American Indian", "Pacific Islander", "Multi Racial","Special Education","English Learner","Economically Disadvantaged","Male", "Female"
SubProcEnr <- function(.dat, lv){
	if(lv==1){
		return(subset(.dat, race=="BL7" & ethnicity=='NO'))
	} else if(lv==2){
		return(subset(.dat, race=="WH7" & ethnicity=='NO'))
	} else if(lv==3){
		return(subset(.dat, ethnicity =='YES'))
	} else if(lv==4){
		return(subset(.dat, race=="AS7" & ethnicity=='NO'))
	} else if(lv==5){
		return(subset(.dat, race=="AM7" & ethnicity=='NO'))
	} else if(lv==6){
		return(subset(.dat, race=="PI7" & ethnicity=='NO'))
	} else if(lv==7){
		return(subset(.dat, race=="MU7" & ethnicity=='NO'))
	} else if(lv==8){
		return(subset(.dat, !is.na(sped_level)))
	} else if(lv==9){
		return(subset(.dat, ell_prog=='YES'))
	} else if(lv==10){
		return(subset(.dat, economy %in% c("FREE", "REDUCED", "DCERT", "CEO", "YES")))
	} else if(lv==11){
		return(subset(.dat, gender %in% c("M", "MALE")))
	} else if(lv==12){
		return(subset(.dat, gender %in% c("F", "FEMALE")))
	} else if(lv==13){
        return(subset(.dat, tanf_snap == 'YES'))
    }
	return(.dat[NULL,])
}


SubProc <- function(.dat, lv, b=0){
	if(lv==0){
		return(.dat)
	} else if(lv==1){
		return(subset(.dat, race=="BL7"))
	} else if(lv==2){
		return(subset(.dat, race=="WH7"))
	} else if(lv==3){
		return(subset(.dat, race=="HI7"))
	} else if(lv==4){
		return(subset(.dat, race=="AS7"))
	} else if(lv==5){
		return(subset(.dat, race=="PI7"))
	} else if(lv==6){
		return(subset(.dat, race=="MU7"))
	} else if(lv==7){
		if(b==1){
			return(subset(.dat, special_ed=='YES' | sped_monitored=='YES'))
		} else{
			.tmp_proc <- subset(.dat, special_ed == 'YES')
			if(nrow(.tmp_proc) < 25){
				return(.tmp_proc)
			} else{
				return(subset(.dat, special_ed=='YES' | sped_monitored=='YES'))
			}
		}
	} else if(lv==8){
		if(b==1){
			return(subset(.dat, ell_prog=='YES' | ell_monitored=='YES'))
		} else{
			.tmp_proc <- subset(.dat, ell_prog == 'YES')
			if(nrow(.tmp_proc) < 25){
				return(.tmp_proc)
			} else{
				return(subset(.dat, ell_prog=='YES' | ell_monitored=='YES'))
			}
		}
	} else if(lv==9){
		return(subset(.dat, economy=="YES"))
	} else if(lv==10){
		 return(subset(.dat, tanf_snap == 'YES'))
	} else if(lv==11){
		return(subset(.dat, gender %in% c("F", "FEMALE")))
	} else if(lv==12){
        return(subset(.dat, gender %in% c("M", "MALE")))
    }
	return(0)
}

WriteComp <- function(.casdat_comp, level){
## Composition 
	year <- .casdat_comp$year[1]
	.fay <- c("all", "full_year")
	.lv <- level
	
	.ret <- c()
	.plevels <- c("Below Basic", "Basic", "Proficient", "Advanced")
	
	## d = each grade 
	.glevels <- sort(unique(.casdat_comp$tested_grade))
	
	for(g in 0:length(.glevels)){
		goutput <- ''
		.tmp <- .casdat_comp
		
		if(g == 0){
			goutput <- 'all'
		} else{
			goutput <- paste('grade', .glevels[g], sep=" ")
			.tmp <- subset(.tmp, tested_grade==.glevels[g])
		}
		
		if(nrow(.tmp)>=10){
				for(z in .fay){
				.add <- indent(.lv) %+% '{\n'
				
				up(.lv)
				.add <- .add %+% paste(indent(.lv), '"key": {\n', sep="")
				up(.lv)
				
				.profs <- .tmp$comp_level
				
				.add <- .add %+% paste(indent(.lv), '"subject": "Composition",\n', sep="")			
				.add <- .add %+% paste(indent(.lv), '"grade": "',goutput,'", \n', sep="")
				.add <- .add %+% paste(indent(.lv), '"enrollment_status": "',z,'", \n', sep="")
				.add <- .add %+% paste(indent(.lv), '"subgroup": "All", \n', sep="")
				.add <- .add %+% paste(indent(.lv), '"year": "',year,'" \n', sep="")
				
				down(.lv)
				
				.add <- .add %+% paste(indent(.lv), '},\n', sep="")					
				.add <- .add %+% paste(indent(.lv), '"val": {\n', sep="")
				up(.lv)
				
				.add <- .add %+% paste(indent(.lv), '"n_eligible":',length(.profs),',\n', sep="")
				.add <- .add %+% paste(indent(.lv), '"n_test_takers":',length(.profs[.profs %in% .plevels]),',\n', sep="")
				.add <- .add %+% paste(indent(.lv), '"advanced_or_proficient":', length(.profs[.profs %in% c("Proficient", "Advanced")]),',\n', sep="")
				
				.add <- .add %+% paste(indent(.lv), '"advanced":',length(.profs[.profs %in% "Advanced"]),',\n', sep="")
				.add <- .add %+% paste(indent(.lv), '"proficient":',length(.profs[.profs %in% "Proficient"]),',\n', sep="")
				.add <- .add %+% paste(indent(.lv), '"basic":',length(.profs[.profs %in% "Basic"]),',\n', sep="")
				.add <- .add %+% paste(indent(.lv), '"below_basic":',length(.profs[.profs %in% "Below Basic"]),'\n', sep="")
				
				down(.lv)
				.add <- .add %+% paste(indent(.lv), '}\n', sep="")
				down(.lv)
				.add <- .add %+% paste(indent(.lv), '}', sep="")
				
				.ret <- c(.ret, .add)
			}
		}
	}
	return(paste(sort(.ret), collapse=',\n'))
}

ExCasChunk <- function(scode, level){
    ## MATH/READING
    .qry_mr <- sprintf("SELECT A.* FROM [dbo].[assessment_w2014] A
        INNER JOIN (SELECT 
            [ea_year]
            ,[school_code]
            ,[grade]
        FROM [dbo].[fy14_mapping]
        WHERE [fy14_entity_code] = '%s') B
    ON A.[ea_year] = B.[ea_year] 
        AND A.[school_grade] = B.[grade] 
        AND A.[school_code] = B.[school_code]", leadgr(scode,4))

    .dat_mr <- sqlQuery(dbrepcard, .qry_mr)

    .add  <- lapply(split(.dat_mr, .dat_mr$ea_year), WriteCAS, level, entity="school")

    .ret <- .add

    ## Comp
    .qry_comp <- sprintf("SELECT A.* FROM [dbo].[assm_comp] A
        INNER JOIN (SELECT 
            [ea_year]
            ,[school_code]
            ,[grade]
        FROM [dbo].[fy14_mapping]
        WHERE [fy14_entity_code] = '%s') B
    ON A.[ea_year] = B.[ea_year] 
        AND A.[tested_grade] = B.[grade] 
        AND A.[school_code] = B.[school_code]", leadgr(scode,4))

    .dat_comp <- sqlQuery(dbrepcard, .qry_comp)

    .add  <- lapply(split(.dat_comp, .dat_comp$ea_year), WriteComp, level)

    .ret <- c(.ret, .add)

    ## Science
    .qry_sci <- sprintf("SELECT A.* FROM [dbo].[assm_science] A
        INNER JOIN (SELECT 
            [ea_year]
            ,[school_code]
            ,[grade]
        FROM [dbo].[fy14_mapping]
        WHERE [fy14_entity_code] = '%s') B
    ON A.[ea_year] = B.[ea_year] 
        AND A.[tested_grade] = B.[grade] 
        AND A.[school_code] = B.[school_code]", leadgr(scode,4))

    .dat_sci <- sqlQuery(dbrepcard, .qry_sci)
    
    .add <- lapply(split(.dat_sci, .dat_sci$ea_year), WriteScience, level)

    .ret <- c(.ret, .add)

    .ret <- subset(.ret, .ret != '')
    return(paste(.ret, collapse=',\n'))
}


## Science
WriteScience <- function(.casdat_sci, level){
	year <- .casdat_sci$year[1]
	.fay <- c("all", "full_year")
	.lv <- level
	
	.ret <- c()
	.plevels <- c("Below Basic", "Basic", "Proficient", "Advanced")
	
	## d = each grade 
	.glevels <- sort(unique(.casdat_sci$tested_grade))
	
	for(g in 0:length(.glevels)){
		goutput <- ''
		.tmp <- .casdat_sci
		
		if(g == 0){
			goutput <- 'all'
		} else{
			goutput <- paste('grade', .glevels[g], sep=" ")
			.tmp <- subset(.tmp, tested_grade==.glevels[g])
		}
		
		if(nrow(.tmp)>=10){
			for(z in .fay){
				.add <- indent(.lv) %+% '{\n'
				
				up(.lv)
				.add <- .add %+% paste(indent(.lv), '"key": {\n', sep="")
				up(.lv)
				
				.profs <- .tmp$science_level
				
				.add <- .add %+% paste(indent(.lv), '"subject": "Science",\n', sep="")
				
				.add <- .add %+% paste(indent(.lv), '"grade": "',goutput,'", \n', sep="")
				.add <- .add %+% paste(indent(.lv), '"enrollment_status": "',z,'", \n', sep="")
				.add <- .add %+% paste(indent(.lv), '"subgroup": "All", \n', sep="")
				.add <- .add %+% paste(indent(.lv), '"year": "',year,'" \n', sep="")
				
				down(.lv)
				
				.add <- .add %+% paste(indent(.lv), '},\n', sep="")
					
				.add <- .add %+% paste(indent(.lv), '"val": {\n', sep="")
				up(.lv)
				
				.add <- .add %+% paste(indent(.lv), '"n_eligible":',length(.profs),',\n', sep="")
				.add <- .add %+% paste(indent(.lv), '"n_test_takers":',length(.profs[.profs %in% .plevels]),',\n', sep="")
				.add <- .add %+% paste(indent(.lv), '"advanced_or_proficient":', length(.profs[.profs %in% c("Proficient", "Advanced")]),',\n', sep="")
				
				.add <- .add %+% paste(indent(.lv), '"advanced":',length(.profs[.profs %in% "Advanced"]),',\n', sep="")
				.add <- .add %+% paste(indent(.lv), '"proficient":',length(.profs[.profs %in% "Proficient"]),',\n', sep="")
				.add <- .add %+% paste(indent(.lv), '"basic":',length(.profs[.profs %in% "Basic"]),',\n', sep="")
				.add <- .add %+% paste(indent(.lv), '"below_basic":',length(.profs[.profs %in% "Below Basic"]),'\n', sep="")
				
				down(.lv)
				.add <- .add %+% paste(indent(.lv), '}\n', sep="")
				down(.lv)
				.add <- .add %+% paste(indent(.lv), '}', sep="")
				
				.ret <- c(.ret, .add)
			}
		}
	}
	return(paste(sort(.ret), collapse=',\n'))
}

WriteAttendance <- function(org_code, level){
	.ret <- c()	
	.lv <- level

	att_qry <- paste0("SELECT * FROM [dbo].[equity_report_prelim]
				WHERE School_Code = '",org_code,"' AND [Metric] = 'In-Seat Attendance Rate' AND [ReportType] = 'External'")
	att <- sqlQuery(dbrepcard, att_qry)
	
	if(nrow(att) >= 1){
		for(i in unique(att$School_Year)){
			tmp <- subset(att, School_Year == i)
			.add <- indent(.lv) %+% '{\n'
			
			up(.lv)
			.add <- .add %+% paste(indent(.lv), '"key": {\n', sep="")
			up(.lv)

			.add <- .add %+% paste(indent(.lv), '"year": "',substr(i,1,4),'"\n', sep="")		
							
			down(.lv)	
			.add <- .add %+% paste(indent(.lv), '},\n', sep="")		

			.add <- .add %+% paste(indent(.lv), '"val": {\n', sep="")
			up(.lv)

			.add <- .add %+% paste(indent(.lv), '"in_seat_attendance":',make_null(
				(tmp$SchoolScore[tmp$Metric=="In-Seat Attendance Rate"])/100
				),',\n', sep="")
			.add <- .add %+% paste(indent(.lv), '"state_in_seat_attendance":',make_null(
				(tmp$AverageScore[tmp$Metric=="In-Seat Attendance Rate"])/100
				),',\n', sep="")

			.add <- .add %+% paste(indent(.lv), '"average_daily_attendance":null,\n', sep="")
			.add <- .add %+% paste(indent(.lv), '"state_average_daily_attendance":null\n', sep="")

			down(.lv)
			.add <- .add %+% paste(indent(.lv), '}\n', sep="")
			down(.lv)
			
			.add <- .add %+% paste(indent(.lv), '}', sep="")

			.ret[length(.ret)+1] <- .add
		}
	}
	return(paste(.ret, collapse=',\n'))
}

WriteAbsences <- function(org_code, level){
	.ret <- c()	
	.lv <- level

	abs_qry <- paste0("SELECT * FROM [dbo].[equity_report_prelim]
				WHERE School_Code = '",org_code,"' AND [Metric] in ('Unexcused Absences 1-5', 'Unexcused Absences 6-10','Unexcused Absences 11-15','Unexcused Absences 16-25','Unexcused Absences > 25') AND [ReportType] = 'External'")
	abs <- sqlQuery(dbrepcard, abs_qry)
	if(nrow(abs) >= 1){
		for(i in unique(abs$School_Year)){
			tmp <- subset(abs, School_Year == i)
			.add <- indent(.lv) %+% '{\n'
			
			up(.lv)
			.add <- .add %+% paste(indent(.lv), '"key": {\n', sep="")
			up(.lv)

			.add <- .add %+% paste(indent(.lv), '"year": "',substr(i,1,4),'"\n', sep="")		
							
			down(.lv)
							
			.add <- .add %+% paste(indent(.lv), '},\n', sep="")
								
			.add <- .add %+% paste(indent(.lv), '"val": {\n', sep="")
			up(.lv)
							
			.add <- .add %+% paste(indent(.lv), '"1-5_days":',make_null(tmp$SchoolScore[tmp$Metric=="Unexcused Absences 1-5"]),',\n', sep="")
			.add <- .add %+% paste(indent(.lv), '"6-10_days":',make_null(tmp$SchoolScore[tmp$Metric=="Unexcused Absences 6-10"]),',\n', sep="")
			.add <- .add %+% paste(indent(.lv), '"11-15_days":',make_null(tmp$SchoolScore[tmp$Metric=="Unexcused Absences 11-15"]),',\n', sep="")
			.add <- .add %+% paste(indent(.lv), '"16-25_days":',make_null(tmp$SchoolScore[tmp$Metric=="Unexcused Absences 16-25"]),',\n', sep="")
			.add <- .add %+% paste(indent(.lv), '"more_than_25_days":',make_null(tmp$SchoolScore[tmp$Metric=="Unexcused Absences > 25"]),',\n', sep="")

			.add <- .add %+% paste(indent(.lv), '"state_1-5_days":',make_null(tmp$AverageScore[tmp$Metric=="Unexcused Absences 1-5"]),',\n', sep="")
			.add <- .add %+% paste(indent(.lv), '"state_6-10_days":',make_null(tmp$AverageScore[tmp$Metric=="Unexcused Absences 6-10"]),',\n', sep="")
			.add <- .add %+% paste(indent(.lv), '"state_11-15_days":',make_null(tmp$AverageScore[tmp$Metric=="Unexcused Absences 11-15"]),',\n', sep="")
			.add <- .add %+% paste(indent(.lv), '"state_16-25_days":',make_null(tmp$AverageScore[tmp$Metric=="Unexcused Absences 16-25"]),',\n', sep="")
			.add <- .add %+% paste(indent(.lv), '"state_more_than_25_days":',make_null(tmp$AverageScore[tmp$Metric=="Unexcused Absences > 25"]),'\n', sep="")

			down(.lv)
			.add <- .add %+% paste(indent(.lv), '}\n', sep="")
			down(.lv)
			
			.add <- .add %+% paste(indent(.lv), '}', sep="")

			.ret[length(.ret)+1] <- .add
		}
	}
	return(paste(.ret, collapse=',\n'))
}

RetSuspensionGroup <- function(x){
	if(x == 'All Students'){
		return('All')
	} else if(x == 'Black non-Hispanic'){
		return('African American')
	} else if (x == 'Hispanic / Latino'){
		return('Hispanic')
	} else if (x == 'Asian'){
		return('Asian')
	} else if (x == 'White non-Hispanic'){
		return('White')
	} else if (x == 'Multiracial'){
		return('Multi Racial')
	} else if (x == 'Limited English Proficiency'){
		return('English Learner')
	} else if (x == 'Special Education'){
		return('Special Education')
	} else if(x == 'Free or Reduced Lunch'){
		return('Economically Disadvantaged')
	} else{
		return(x)
	}
} 

WriteSuspensions <- function(org_code, level){
	.ret <- c()	
	.lv <- level

	sus_qry <- paste0("SELECT * FROM [dbo].[equity_report_prelim]
				WHERE School_Code = '",org_code,"' AND [Metric] in ('Suspended 1+','Suspended 11+') AND [ReportType] = 'External'")
	sus <- sqlQuery(dbrepcard, sus_qry)

	if(nrow(sus) >= 1){
		for(i in unique(sus$School_Year)){
			tmp <- subset(sus, School_Year == i)
			for(f in unique(tmp$Student_Group)){	
				.add <- indent(.lv) %+% '{\n'
				
				up(.lv)
				.add <- .add %+% paste(indent(.lv), '"key": {\n', sep="")
				up(.lv)

				.add <- .add %+% paste(indent(.lv), '"year": "',substr(i,1,4),'",\n', sep="")		
				.add <- .add %+% paste(indent(.lv), '"subgroup": "',RetSuspensionGroup(f),'"\n', sep="")				
				down(.lv)
								
				.add <- .add %+% paste(indent(.lv), '},\n', sep="")
									
				.add <- .add %+% paste(indent(.lv), '"val": {\n', sep="")
				up(.lv)

				.add <- .add %+% paste(indent(.lv), '"suspended_1":',make_null((tmp$SchoolScore[tmp$Metric=="Suspended 1+" & tmp$Student_Group ==f])/100),',\n', sep="")
				.add <- .add %+% paste(indent(.lv), '"suspended_11":',make_null((tmp$SchoolScore[tmp$Metric=="Suspended 11+" & tmp$Student_Group ==f])/100),',\n', sep="")
				.add <- .add %+% paste(indent(.lv), '"state_suspended_1":',make_null((tmp$AverageScore[tmp$Metric=="Suspended 1+" & tmp$Student_Group ==f])/100),',\n', sep="")
				.add <- .add %+% paste(indent(.lv), '"state_suspended_11":',make_null((tmp$AverageScore[tmp$Metric=="Suspended 11+" & tmp$Student_Group ==f])/100),'\n', sep="")

				down(.lv)
				.add <- .add %+% paste(indent(.lv), '}\n', sep="")
				down(.lv)
				
				.add <- .add %+% paste(indent(.lv), '}', sep="")

				.ret[length(.ret)+1] <- .add
			}
		}	
	}		
	return(paste(.ret, collapse=',\n'))
}

WriteExpulsions <- function(org_code, level){
	.ret <- c()	
	.lv <- level

	exp_qry <- paste0("SELECT * FROM [dbo].[equity_report_prelim]
				WHERE School_Code = '",org_code,"' AND [Metric] = 'Expulsions' AND [ReportType] = 'External'")
	exp <- sqlQuery(dbrepcard, exp_qry)

	if(nrow(exp) >= 1){
		for(i in unique(exp$School_Year)){
			tmp <- subset(exp, School_Year == i)

			.add <- indent(.lv) %+% '{\n'
			
			up(.lv)
			.add <- .add %+% paste(indent(.lv), '"key": {\n', sep="")
			up(.lv)

			.add <- .add %+% paste(indent(.lv), '"year": "',substr(i,1,4),'"\n', sep="")		
							
			down(.lv)
							
			.add <- .add %+% paste(indent(.lv), '},\n', sep="")
								
			.add <- .add %+% paste(indent(.lv), '"val": {\n', sep="")
			up(.lv)
							
			.add <- .add %+% paste(indent(.lv), '"expulsions":',make_null(tmp$SchoolScore),',\n', sep="")
			.add <- .add %+% paste(indent(.lv), '"state_expulsions":',make_null(tmp$AverageScore),'\n', sep="")

			down(.lv)
			.add <- .add %+% paste(indent(.lv), '}\n', sep="")
			down(.lv)
			
			.add <- .add %+% paste(indent(.lv), '}', sep="")

			.ret[length(.ret)+1] <- .add
		}
	}
	return(paste(.ret, collapse=',\n'))
}

RetMonthInt <- function(x){
	if(x == 'January'){
		return(1)
	} else if(x == 'February'){
		return(2)
	} else if (x == 'March'){
		return(3)
	} else if (x == 'April'){
		return(4)
	} else if (x == 'May'){
		return(5)
	} else if (x == 'June'){
		return(6)
	} else if (x == 'July'){
		return(7)
	} else if (x == 'August'){
		return(8)
	} else if(x == 'September'){
		return(9)
	} else if(x == 'October'){
		return(10)
	} else if(x == 'November'){
		return(11)
	} else if(x == 'December'){
		return(12)
	} else{
		return(x)
	}
}

WriteEnterWithdraw <- function(org_code, level){
	.ret <- c()
	.lv <- level

	ent_qry <- paste0("SELECT * FROM [dbo].[equity_report_prelim]
				WHERE School_Code = '",org_code,"' AND [Metric] in ('Entry','Net Cumulative','Withdrawal') AND [ReportType] = 'External'")
	ent <- sqlQuery(dbrepcard, ent_qry)

	if(nrow(ent) >= 1){
		for(i in unique(ent$School_Year)){
			for(f in unique(ent$Month)){	
				tmp <- subset(ent, School_Year == i & Month == f)

				.add <- indent(.lv) %+% '{\n'
				
				up(.lv)
				.add <- .add %+% paste(indent(.lv), '"key": {\n', sep="")
				up(.lv)

				.add <- .add %+% paste(indent(.lv), '"year": "',substr(i,1,4),'",\n', sep="")		
				.add <- .add %+% paste(indent(.lv), '"month": ',RetMonthInt(f),'\n', sep="")				
				down(.lv)

				.add <- .add %+% paste(indent(.lv), '},\n', sep="")
									
				.add <- .add %+% paste(indent(.lv), '"val": {\n', sep="")
				up(.lv)

				.add <- .add %+% paste(indent(.lv), '"entry":',make_null(tmp$SchoolScore[tmp$Metric=="Entry"]),',\n', sep="")
				.add <- .add %+% paste(indent(.lv), '"withdrawal":',make_null(tmp$SchoolScore[tmp$Metric=="Withdrawal"]),',\n', sep="")
				.add <- .add %+% paste(indent(.lv), '"net_cumulative":',make_null(tmp$SchoolScore[tmp$Metric=="Net Cumulative"]),'\n', sep="")

				down(.lv)
				.add <- .add %+% paste(indent(.lv), '}\n', sep="")
				down(.lv)
				
				.add <- .add %+% paste(indent(.lv), '}', sep="")

				.ret[length(.ret)+1] <- .add
			}
		}
	}
	return(paste(.ret, collapse=',\n'))
}


WriteStaffExp <- function(org_code, level){
    .lv <- level
    staff_exp <- sqlQuery(dbrepcard, paste0("SELECT [local_staff_id]
      ,[lea_code]
      ,[school_code]
      ,[Is_AA]
      ,[Is_Bachelors]
      ,[Is_Masters]
      ,[Is_PHD]
        FROM [dbo].[tmp_staff_degree_sy1213] 
        WHERE [school_code] = '", as.numeric(org_code),"'"))

    .prog <- sqlQuery(dbrepcard, "SELECT * FROM [dbo].[hqt_status_sy1112]
        WHERE [school_code] = '" %+% org_code %+% "'")


    if(nrow(staff_exp) >= 10){
        
        .add <- indent(.lv) %+% '{\n'

        up(.lv)
        .add <- .add %+% paste(indent(.lv), '"key": {', sep="")
        up(.lv)

        .add <- .add %+% paste('"year": "2012"', sep="")       
                            
        down(.lv)
        .add <- .add %+% paste('},\n', sep="")
                                
        .add <- .add %+% paste(indent(.lv), '"val": {', sep="")
        up(.lv)    

        .add <- .add %+% paste('"None":', round(nrow(staff_exp[staff_exp$Is_AA == 0 & staff_exp$Is_Bachelors == 0 & staff_exp$Is_Masters == 0 & staff_exp$Is_PHD == 0,])/nrow(staff_exp),4),', ', sep="")

        .add <- .add %+% paste('"AA":',round(mean(staff_exp$Is_AA, na.rm=TRUE),4),', ', sep="")
        .add <- .add %+% paste('"BA":',round(mean(staff_exp$Is_Bachelors, na.rm=TRUE),4),', ', sep="")
        .add <- .add %+% paste('"MA":',round(mean(staff_exp$Is_Masters, na.rm=TRUE),4),', ', sep="")
        .add <- .add %+% paste('"PhD":',round(mean(staff_exp$Is_PHD, na.rm=TRUE),4),', ', sep="")
        if(nrow(.prog)>0){
            .add <- .add %+% paste('"HQT":',.prog$percent_hqt_classes, sep="")
        }  else{ 
            .add <- .add %+% paste('"HQT":','null', sep="")
        }
        down(.lv)
        .add <- .add %+% paste('}\n', sep="")
      
        down(.lv)
  
        .add <- .add %+% paste(indent(.lv), '}', sep="")
    return(.add)
    }
}



