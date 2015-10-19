LeaHQTStatus <- function(lea_code){
    .qry <- "SELECT * FROM [dbo].[hqt_status_lea_sy1112]
        WHERE [lea_code] = '" %+% lea_code %+% "'"
    .prog <- sqlQuery(dbrepcard, .qry)

    if(nrow(.prog)>0){
        .ret <- .prog$percent_hqt_classes
        return(.ret)
        }
    else{
        return('null')
        }
}

LeaCasChunk <- function(lea_code, level){
    .lv <- level

        ## MATH/READING
    .qry_mr <- sprintf("SELECT * 
        FROM [dbo].[assessment_w2014]
        WHERE [lea_code] = '%s'", leadgr(lea_code,4))
    
    .dat_mr <- sqlQuery(dbrepcard, .qry_mr)
    .dat_mr$school_grade <- as.numeric(.dat_mr$school_grade)

    if(nrow(.dat_mr) >= 10){
        # .ret <- do(group_by(.dat_mr, ea_year), WriteCAS, level, "lea")
        .ret <- lapply(split(.dat_mr, .dat_mr$ea_year), WriteCAS, level, "lea")

    } else{
        .ret <- ''
    }
    
    .qry13c <- "SELECT * FROM (
					SELECT A.[year]
				      ,A.[ea_year]
				      ,A.[usi]
				      ,A.[test_uid]
				      ,A.[tested_grade]
				      ,A.[comp_level] 
					  ,B.[fy14_entity_code] as [lea_code]
					FROM [dbo].[assm_comp] A
					LEFT JOIN [dbo].[fy14_mapping] B
						ON A.[tested_grade] = B.[grade] 
						AND A.[school_code] = B.[school_code]
						AND A.[ea_year] = B.[ea_year]) X
				WHERE [year] = '2013'
    			AND [lea_code] = '" %+% lea_code %+% "';" 
    .dat13_c <- sqlQuery(dbrepcard, .qry13c)
    if(nrow(.dat13_c)>=10){
        .ret <- c(.ret, WriteComp(.dat13_c, 2013, .lv))
    }

    .qry12c <- "SELECT * FROM (
					SELECT A.[year]
				      ,A.[ea_year]
				      ,A.[usi]
				      ,A.[test_uid]
				      ,A.[tested_grade]
				      ,A.[comp_level] 
					  ,B.[fy14_entity_code] as [lea_code]
					FROM [dbo].[assm_comp] A
					LEFT JOIN [dbo].[fy14_mapping] B
						ON A.[tested_grade] = B.[grade] 
						AND A.[school_code] = B.[school_code]
						AND A.[ea_year] = B.[ea_year]) X
				WHERE [year] = '2012'
    			AND [lea_code] = '" %+% lea_code %+% "';" 
    .dat12_c <- sqlQuery(dbrepcard, .qry12c)
    if(nrow(.dat12_c)>=10){
        .ret <- c(.ret, WriteComp(.dat12_c, 2012, .lv))
    }

    .qry11c <- "SELECT * FROM (
					SELECT A.[year]
				      ,A.[ea_year]
				      ,A.[usi]
				      ,A.[test_uid]
				      ,A.[tested_grade]
				      ,A.[comp_level] 
					  ,B.[fy14_entity_code] as [lea_code]
					FROM [dbo].[assm_comp] A
					LEFT JOIN [dbo].[fy14_mapping] B
						ON A.[tested_grade] = B.[grade] 
						AND A.[school_code] = B.[school_code]
						AND A.[ea_year] = B.[ea_year]) X
				WHERE [year] = '2011'
    			AND [lea_code] = '" %+% lea_code %+% "';" 
    .dat11_c <- sqlQuery(dbrepcard, .qry11c)
    if(nrow(.dat11_c)>=10){
        .ret <- c(.ret, WriteComp(.dat11_c, 2011, .lv))
    }

    ## 
    .qry13s <- "SELECT * FROM (
					SELECT A.[year]
				      ,A.[ea_year]
				      ,A.[usi]
				      ,A.[test_uid]
				      ,A.[tested_grade]
				      ,A.[science_level] 
					  ,B.[fy14_entity_code] as [lea_code]
					FROM [dbo].[assm_science] A
					LEFT JOIN [dbo].[fy14_mapping] B
						ON A.[tested_grade] = B.[grade] 
						AND A.[school_code] = B.[school_code]
						AND A.[ea_year] = B.[ea_year]
					WHERE  [science_empty] = 0) X
				WHERE [year] = '2013'
    			AND [lea_code] = '" %+% lea_code %+% "';"
    .dat13_s <- sqlQuery(dbrepcard, .qry13s)
    if(nrow(.dat13_s)>=10){
        .ret <- c(.ret, WriteScience(.dat13_s, 2013, .lv))
    }

     .qry12s <- "SELECT * FROM (
					SELECT A.[year]
				      ,A.[ea_year]
				      ,A.[usi]
				      ,A.[test_uid]
				      ,A.[tested_grade]
				      ,A.[science_level] 
					  ,B.[fy14_entity_code] as [lea_code]
					FROM [dbo].[assm_science] A
					LEFT JOIN [dbo].[fy14_mapping] B
						ON A.[tested_grade] = B.[grade] 
						AND A.[school_code] = B.[school_code]
						AND A.[ea_year] = B.[ea_year]
					WHERE  [science_empty] = 0) X
				WHERE [year] = '2012'
    			AND [lea_code] = '" %+% lea_code %+% "';"
    .dat12_s <- sqlQuery(dbrepcard, .qry12s)
    if(nrow(.dat12_s)>=10){
        .ret<- c(.ret, WriteScience(.dat12_s, 2012, .lv))
    }

    .qry11s <- "SELECT * FROM (
					SELECT A.[year]
				      ,A.[ea_year]
				      ,A.[usi]
				      ,A.[test_uid]
				      ,A.[tested_grade]
				      ,A.[science_level] 
					  ,B.[fy14_entity_code] as [lea_code]
					FROM [dbo].[assm_science] A
					LEFT JOIN [dbo].[fy14_mapping] B
						ON A.[tested_grade] = B.[grade] 
						AND A.[school_code] = B.[school_code]
						AND A.[ea_year] = B.[ea_year]
					WHERE  [science_empty] = 0) X
				WHERE [year] = '2011'
    			AND [lea_code] = '" %+% lea_code %+% "';"
    .dat11_s <- sqlQuery(dbrepcard, .qry11s)
    if(nrow(.dat11_s)>=10){
        .ret <- c(.ret, WriteScience(.dat11_s, 2011, .lv))
    }

    return(paste(.ret, collapse=',\n'))
}


WriteScience <- function(.casdat_sci, year, level){
## Science
	.fay <- c("all")
	.lv <- level
	
	.ret <- c()
	.plevels <- c("Below Basic", "Basic", "Proficient", "Advanced", "1","2","3","4")
	
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
			.add <- indent(.lv) %+% '{\n'
			
			up(.lv)
			.add <- .add %+% paste(indent(.lv), '"key": {', sep="")
			up(.lv)
			
			.profs <- .tmp$science_level
			
			.add <- .add %+% paste('"subject": "Science",', sep="")
		
			.add <- .add %+% paste('"grade": "',goutput,'", ', sep="")
			.add <- .add %+% paste('"enrollment_status": "',.fay,'", ', sep="")
			.add <- .add %+% paste('"subgroup": "All", ', sep="")
			.add <- .add %+% paste('"year": "',year,'" ', sep="")
			
			down(.lv)
			
			.add <- .add %+% paste('},\n', sep="")
				
			.add <- .add %+% paste(indent(.lv), '"val": {', sep="")
			up(.lv)
			
			.add <- .add %+% paste('"n_eligible":',length(.profs),',', sep="")
			.add <- .add %+% paste('"n_test_takers":',length(.profs[.profs %in% .plevels]),',', sep="")
			.add <- .add %+% paste('"advanced_or_proficient":', length(.profs[.profs %in% c("Proficient", "Advanced", "3", "4")]),',', sep="")
			
			.add <- .add %+% paste('"advanced":',length(.profs[.profs %in% c("Advanced", "4")]),',', sep="")
			.add <- .add %+% paste('"proficient":',length(.profs[.profs %in% c("Proficient", "3")]),',', sep="")
			.add <- .add %+% paste('"basic":',length(.profs[.profs %in% c("Basic","2")]),',', sep="")
			.add <- .add %+% paste('"below_basic":',length(.profs[.profs %in% c("Below Basic", "1")]),'', sep="")
			
			down(.lv)
			.add <- .add %+% paste('}\n', sep="")
			down(.lv)
			.add <- .add %+% paste(indent(.lv), '}', sep="")
			
			.ret[length(.ret)+1] <- .add
		}
	}
	return(paste(.ret, collapse=',\n'))
}




WriteComp <- function(.casdat_comp, year, level){
## Composition 
	.fay <- c("all")
	.lv <- level
	
	.ret <- c()
	.plevels <- c("Below Basic", "Basic", "Proficient", "Advanced", "1","2","3","4")
	
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
			.add <- indent(.lv) %+% '{\n'
			
			up(.lv)
			.add <- .add %+% paste(indent(.lv), '"key": {', sep="")
			up(.lv)
			
			.profs <- .tmp$comp_level
			
			.add <- .add %+% paste('"subject": "Composition",', sep="")			
			.add <- .add %+% paste('"grade": "',goutput,'", ', sep="")
			.add <- .add %+% paste('"enrollment_status": "',.fay,'", ', sep="")
			.add <- .add %+% paste('"subgroup": "All", ', sep="")
			.add <- .add %+% paste('"year": "',year,'" ', sep="")
			
			down(.lv)
			
			.add <- .add %+% paste('},\n', sep="")
				
			.add <- .add %+% paste(indent(.lv), '"val": {', sep="")
			up(.lv)
			
			.add <- .add %+% paste('"n_eligible":',length(.profs),', ', sep="")
			.add <- .add %+% paste('"n_test_takers":',length(.profs[.profs %in% .plevels]),', ', sep="")
			.add <- .add %+% paste('"advanced_or_proficient":', length(.profs[.profs %in% c("Proficient", "Advanced", "3", "4")]),', ', sep="")
			
			.add <- .add %+% paste('"advanced":',length(.profs[.profs %in% c("Advanced", "4")]),', ', sep="")
			.add <- .add %+% paste('"proficient":',length(.profs[.profs %in% c("Proficient", "3")]),', ', sep="")
			.add <- .add %+% paste('"basic":',length(.profs[.profs %in% c("Basic", "2")]),', ', sep="")
			.add <- .add %+% paste('"below_basic":',length(.profs[.profs %in% c("Below Basic", "1")]),'', sep="")
			
			down(.lv)
			.add <- .add %+% paste('}\n', sep="")
			down(.lv)
			.add <- .add %+% paste(indent(.lv), '}', sep="")
			
			.ret[length(.ret)+1] <- .add
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
	}	
	return(.dat[NULL,])
}


LeaCollegeReadiness <- function(lea_code, level){
    .lv <- level
    
    .qry <- "SELECT * FROM [dbo].[college_readiness]
                    WHERE [lea_code] in (
	SELECT DISTINCT [lea_code] FROM [dbo].[enrollment] 
	WHERE [ea_year] = '2012' 
	AND [lea_code] = '" %+% lea_code %+% "')"
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
            .add <- .add %+% paste(indent(.lv), '"key": {', sep="")
            up(.lv)                                   
            .profs <- .tmp$comp_level
            .add <- .add %+% paste('"subgroup": "', .slice,'", ', sep="")
            .add <- .add %+% paste('"year": "',.tmp$year[1],'" ', sep="")
            down(.lv)
            .add <- .add %+% paste('},\n', sep="")
                            
            .add <- .add %+% paste(indent(.lv), '"val": {', sep="")
            up(.lv)
            .add <- .add %+% paste('"graduates": ', nrow(.tmp),', ', sep="")
            .add <- .add %+% paste('"act_taker": ', nrow(subset(.tmp, act_taker=='YES')),', ', sep="")
            .add <- .add %+% paste('"sat_taker": ', nrow(subset(.tmp, sat_taker=='YES')),', ', sep="")
            .add <- .add %+% paste('"ap_taker": ', nrow(subset(.tmp, ap_taker=='YES')),', ', sep="")
            .add <- .add %+% paste('"psat_taker": ', nrow(subset(.tmp, psat_taker=='YES')), sep="")
            down(.lv)
            .add <- .add %+% paste('}\n', sep="")
            down(.lv)
            .add <- .add %+% paste(indent(.lv), '}', sep="")
            
            .ret <- c(.ret, .add)
        }              
    }
    return(paste(.ret, collapse=',\n'))
}

LeaSPEDChunk <- function(scode, level){
	.lv <- level
	
	.qry <- "SELECT X.*,
	Y.[lea_code] as [lea_code_1314]
		FROM (
			SELECT A.* ,
				B.[fy14_entity_code]
			FROM [dbo].[assessment] A
			LEFT JOIN [dbo].[fy14_mapping] B
				ON A.[school_grade] = B.[grade] 
				AND A.[school_code] = B.[school_code]
				AND A.[ea_year] = B.[ea_year]
			) X
		LEFT JOIN [dbo].[schooldir_sy1314] Y
		ON X.[fy14_entity_code]  = Y.[school_code]
	WHERE Y.[lea_code] = '" %+% lea_code %+% "'"
	.dat_mr <- sqlQuery(dbrepcard, .qry)

	.ret <- c()

	for(i in unique(.dat_mr$year)){
		.tmp_mr <- subset(.dat_mr, year ==i)
		if(nrow(.tmp_mr)>=10 & !is.null(.tmp_mr)){
			year <- i
			.ret <- c(.ret, WriteSPED(.tmp_mr, year, .lv))
		}
	}

	.ret <- subset(.ret, .ret != '')

	return(paste(.ret, collapse=',\n'))
}

WriteSPED <- function(.casdat_mr, year, level){
	.subjects <- c("Math", "Reading")
	.lv <- level
	
	.ret <- c()
	.plevels <- c("Below Basic", "Basic", "Proficient", "Advanced")
	
	## A = Subject, 1 for Math, 2 for Reading
	for(a in 1:2){
		for(b in 1:4){
			soutput <- "All SPED Students"
			.tmp <- .casdat_mr

			if(b == 2){
				.tmp <- subset(.casdat_mr, sped_acc == '1')
				soutput <- "With Accommodations"
			} else if(b == 3){
				.tmp <- subset(.casdat_mr, sped_acc != '1')
				soutput <- "No Accommodations"
			} else if(b==4){
				.tmp <- subset(.casdat_mr, alt_tested=='1')
				soutput <- "ALT Test Takers"
			}
			
			if(a ==1){
				.profs <- .tmp$math_level
			} else if(a == 2){
				.profs <- .tmp$read_level
			}
			
			## START WRITE ##
			if(length(.profs[.profs %in% .plevels]) >= 10){
			
				.add <- indent(.lv) %+% '{\n'
				
				up(.lv)
				.add <- .add %+% paste(indent(.lv), '"key": {', sep="")
				up(.lv)				

				.add <- .add %+% paste('"subject": "',.subjects[a],'", ', sep="")
				.add <- .add %+% paste('"subgroup": "',soutput,'",  ', sep="")
				.add <- .add %+% paste('"year": "',year,'" ', sep="")
				
				down(.lv)
				
				.add <- .add %+% paste('},\n', sep="")
					
				.add <- .add %+% paste(indent(.lv), '"val": {', sep="")
				up(.lv)
				
				.add <- .add %+% paste('"n_eligible":',length(.profs),', ', sep="")
				.add <- .add %+% paste('"n_test_takers":',length(.profs[.profs %in% .plevels]),', ', sep="")
				.add <- .add %+% paste('"advanced_or_proficient":', length(.profs[.profs %in% c("Proficient", "Advanced")]),', ', sep="")
				
				.add <- .add %+% paste('"advanced":',length(.profs[.profs %in% "Advanced"]),', ', sep="")
				.add <- .add %+% paste('"proficient":',length(.profs[.profs %in% "Proficient"]),', ', sep="")
				.add <- .add %+% paste('"basic":',length(.profs[.profs %in% "Basic"]),', ', sep="")
				.add <- .add %+% paste('"below_basic":',length(.profs[.profs %in% "Below Basic"]),'', sep="")
				
				down(.lv)
				.add <- .add %+% paste('}\n', sep="")
				down(.lv)
				.add <- .add %+% paste(indent(.lv), '}', sep="")
				
				.ret[length(.ret)+1] <- .add
			}
		}
	}
	return(paste(.ret, collapse=',\n'))	
}


LeaCollegeEnroll <- function(lea_code, level){
	.lv <- level
	.ret <- c()
	.qry <- "SELECT * FROM [dbo].[college_enroll_2010]
				WHERE [fy13_entity_code] in (
					SELECT DISTINCT [school_code] FROM [dbo].[enrollment] 
						WHERE [ea_year] = '2012'
						AND [lea_code] = '" %+% lea_code %+% "')
				AND [Graduates] >= 10;"
	.cenr10 <- sqlQuery(dbrepcard, .qry)


	if(nrow(.cenr10)> 0){
		.ret <- c(.ret, WriteCEnroll(.cenr10, .lv, 2010))
	}

	.qry <- "SELECT * FROM [dbo].[college_enroll_2009]
				WHERE [fy13_entity_code] in (
					SELECT DISTINCT [school_code] FROM [dbo].[enrollment] 
						WHERE [ea_year] = '2012'
						AND [lea_code] = '" %+% lea_code %+% "')
				AND [Graduates] >= 10;"
	.cenr09 <- sqlQuery(dbrepcard, .qry)

	if(nrow(.cenr09)> 0){
		.ret <- c(.ret, WriteCEnroll(.cenr09, .lv, 2009))
	}		
	
	.qry <- "SELECT * FROM [dbo].[college_enroll_2008]
				WHERE [fy13_entity_code] in (
					SELECT DISTINCT [school_code] FROM [dbo].[enrollment] 
						WHERE [ea_year] = '2012'
						AND [lea_code] = '" %+% lea_code %+% "')
				AND [Graduates] >= 10;"
	.cenr08 <- sqlQuery(dbrepcard, .qry)

	if(nrow(.cenr08)> 0){
		.ret <- c(.ret, WriteCEnroll(.cenr08, .lv, 2008))
	}		
		
	.qry <- "SELECT * FROM [dbo].[college_enroll_2007]
				WHERE [fy13_entity_code] in (
					SELECT DISTINCT [school_code] FROM [dbo].[enrollment] 
						WHERE [ea_year] = '2012'
						AND [lea_code] = '" %+% lea_code %+% "')
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
		.add <- .add %+% paste(indent(.lv), '"key": {', sep="")
		up(.lv)

		.add <- .add %+% paste('"cohort_year": "',year,'", ', sep="")
		.add <- .add %+% paste('"subgroup": "', SubCEnroll(.cenr$Group[i]),'" ', sep="")
		
		down(.lv)
		.add <- .add %+% paste('},\n', sep="")
			
		.add <- .add %+% paste(indent(.lv), '"val": {', sep="")
		up(.lv)
		.add <- .add %+% paste('"hs_graduates": ',.cenr$Graduates[i],', ', sep="")
		.add <- .add %+% paste('"enroll_within_16mo": ',.cenr$Initial_Enroll_16mo[i],', ', sep="")
		.add <- .add %+% paste('"enroll_within_16mo_instate": ',.cenr$Initial_Enroll_InState_16mo[i],', ', sep="")
		.add <- .add %+% paste('"complete_1yr_instate": ',.cenr$Complete_1Yr_in_State[i],'', sep="")

		down(.lv)
		.add <- .add %+% paste('}\n', sep="")

		down(.lv)
		.add <- .add %+% paste(indent(.lv), '}', sep="")
		.ret <- c(.ret, .add)
	}
	return(.ret)
}

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

RetMGPGroup <- function(.ingrp){
	if(.ingrp == 'All Students'){
		return('All')
	} else if(.ingrp == 'WH7'){
		return('White')
	} else if(.ingrp == 'Not-SpEd'){
		return('Not Special Education')
	} else if(.ingrp == 'SpEd'){
		return('Special Education')
	} else if (.ingrp == 'Not-ELP'){
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
		return('American Indian or Alaskan Native')
	} else if (.ingrp == 'MU7'){
		return('Multiracial')
	} else if (.ingrp == 'LEP'){
		return('English Learner')
	} else if (.ingrp == 'PI7'){
		return('Pacific Islander')
	}
}


LeaMGPResult <- function(lea_code, level){
	.lv <- level
	.qry <- "SELECT * FROM [dbo].[mgp_summary]
		WHERE [fy13_entity_code] in (
			SELECT DISTINCT [school_code] FROM [dbo].[enrollment] 
				WHERE [ea_year] = '2012' 
				AND [lea_code] = '" %+% lea_code %+% "')
		AND [group] not like 'Grade%'"
	.mgp <- sqlQuery(dbrepcard, .qry)	
	.ret <- c()	

	if(nrow(.mgp)>0){
		for(i in unique(.mgp$subject)){
			.mgp_sub <- subset(.mgp, subject == i)

			for(j in unique(.mgp_sub$group)){
				.mgp_sub_group <- subset(.mgp_sub, group == j)

				for(k in unique(.mgp_sub_group$year)){
					.tmp <- subset(.mgp_sub_group, year == k)

					.add <- indent(.lv) %+% '{\n'
					up(.lv)
					.add <- .add %+% paste(indent(.lv), '"key": {', sep="")
					up(.lv)			
					.add <- .add %+% paste('"subject": "',i ,'", ', sep="")
					.add <- .add %+% paste('"subgroup": "',RetMGPGroup(j),'", ', sep="")
					.add <- .add %+% paste('"year": "',k,'" ', sep="")
					down(.lv)
					.add <- .add %+% paste('},\n', sep="")
			
					.add <- .add %+% paste(indent(.lv), '"val": {', sep="")
					up(.lv)
					.add <- .add %+% paste('"group_size": ', checkna(sum(.tmp$group_fay_size)),', ', sep="")
					.add <- .add %+% paste('"mgp_1yr": ', checkna(round(sum(.tmp$group_fay_size * .tmp$mgp_1yr)/sum(.tmp$group_fay_size),2)),', ', sep="")
					.add <- .add %+% paste('"mgp_2yr": ', checkna(round(sum(.tmp$group_fay_size * .tmp$mgp_2yr)/sum(.tmp$group_fay_size),2)),'', sep="")
					down(.lv)
					.add <- .add %+% paste('}\n', sep="")
					down(.lv)
					.add <- .add %+% paste(indent(.lv), '}', sep="")
					.ret <- c(.ret, .add)
				}
			}
		}
	}
	return(paste(.ret, collapse=',\n'))
}

LeaSpedAprUrl <- function(lea_code, level){
	.lv <- level

	.sped_url <- sqlQuery(dbrepcard, paste0("SELECT * FROM [dbo].[sped_apr_url] WHERE [lea_code] = '", lea_code,"'"))

	if(nrow(.sped_url)==1){
		 if(.sped_url$url_exists == 1){
			.add <- indent(.lv) %+% '{\n'

			up(.lv)
			.add <- .add %+% paste(indent(.lv), '"key": {', sep="")
			up(.lv)		

			.add <- .add %+% paste0('"year": "2011"')		
			down(.lv)

			.add <- .add %+% paste('},\n', sep="")
					
			.add <- .add %+% paste(indent(.lv), '"val": {', sep="")
			up(.lv)
			.add <- .add %+% paste('"sped_apr_url": "', .sped_url$url_name,'"', sep="")

			down(.lv)
			.add <- .add %+% paste('}\n', sep="")
			down(.lv)
			.add <- .add %+% paste(indent(.lv), '}', sep="")

			return(.add)
		}
	}
}	



