ExStateAMOs <- function(.lv){
    .qry <- "SELECT * 
        FROM [dbo].[amo_state_targets]"
        
    .amo_dat <- sqlQuery(dbrepcard, .qry)
    .ret <- c()

    if(nrow(.amo_dat) > 0){
        for(i in 1:nrow(.amo_dat)){
            for(j in c("math", "read")){
                .add <- indent(.lv) %+% '{\n'
                up(.lv)
                .add <- .add %+% indent(.lv) %+% '"key": '
                .add <- .add %+% WriteJSONChunk(
                    c(subject=ifelse(j=='math', '"Math"', '"Reading"'), 
                    grade='"all"',
                    enrollment_status='"full_year"',
                    subgroup=sprintf('"%s"', .amo_dat$subgroup[i]),
                    year=sprintf('"%s"', .amo_dat$year[i]))) %+% ', \n'
                .add <- .add %+% indent(.lv) %+% '"val": '
                .add <- .add %+% WriteJSONChunk(c(basline=checkna(.amo_dat[i, j %+% '_baseline']),
                    target=checkna(.amo_dat[i, j %+% '_target']))) %+% '\n'
                
                down(.lv)
                .add <- .add %+% paste(indent(.lv), '}', sep="")
                .ret <- c(.ret, .add)
            }
        }
        return(paste(.ret, collapse=',\n'))
    }
}

ExDiplCount <- function(.lv){
    .qry <- "SELECT * 
    FROM [dbo].[state_reg_dipl_count]"
        
    .dipl_data <- sqlQuery(dbrepcard, .qry)
    .ret <- c()
    if(nrow(.dipl_data) > 0){
        for(i in 1:nrow(.dipl_data)){
            .add <- indent(.lv) %+% sprintf('{"key":{"year":"%d"}, "value": "%d"}', .dipl_data$grad_year[i], .dipl_data$diplomas_issued[i])
            .ret <- c(.ret, .add)
        }
        return(paste(.ret, collapse=',\n'))
    }
}

ExGradTargets <- function(level){
    .ret <- sapply(2012:2017, function(x, lv){
        sprintf('%s{"key": %d, "value": %f}', indent(lv), x, 0.59 +(x-2011)*(0.78-0.59)/6)}, level 
    )
    return(paste(.ret, collapse=',\n'))
}


## 
ExStatePreKCAS <- function(level){
    .qry <- "SELECT A.*,
                CASE
                    WHEN A.[usi] in (SELECT [usi] from [dbo].[historical_prek_static]) THEN 1
                    ELSE 0
                END as [prek_participant]
            FROM [dbo].[assessment] A
            WHERE A.[tested_grade] = '03' AND A.[year] = '2013'"

    .prekcas13 <- sqlQuery(dbrepcard, .qry)
    .ret <- c()

    .ret <- c(.ret, WritePreKCAS(.prekcas13, level))

    .qry <- "SELECT A.*,
            CASE
                WHEN A.[usi] in (SELECT [usi] from [dbo].[historical_prek_static]) THEN 1
                ELSE 0
            END as [prek_participant]
        FROM [dbo].[assessment] A
        WHERE A.[tested_grade] = '03' AND A.[year] = '2012'"

    .prekcas12 <- sqlQuery(dbrepcard, .qry)

    .ret <- c(.ret, WritePreKCAS(.prekcas12, level))
    return(paste(.ret, collapse=',\n'))
}

WritePreKCAS <- function(.prekcas, level){
    .lv <- level
    .ret <- c()

    .group <- c("PreK Participant", "Non-PreK Participant")
    .year <- .prekcas$year[1]
    .subject <- c("Math", "Reading")
    .plevels <- c("Below Basic", "Basic", "Proficient", "Advanced")

    for(i in 1:2){
        if(i == 1){
            .tmp <- subset(.prekcas, prek_participant==1)
        } else{
            .tmp <- subset(.prekcas, prek_participant!=1)
        }
        for(a in 1:2){
            invisible(ifelse(a==1, .profs <- c(table(.tmp$math_level)), .profs <- c(table(.tmp$read_level))))
            
            .add <- indent(.lv) %+% '{\n'
            up(.lv)
            
            .add <- .add %+% indent(.lv) %+% '"key": '
            .add <- .add %+% WriteJSONChunk(
                c(year=checkna(.year),
                grade='"grade 3"',
                subject=sprintf('"%s"', .subject[a]),
                subgroup=sprintf('"%s"', .group[i])
                )) %+% ', \n'
            .add <- .add %+% indent(.lv) %+% '"val": '
            .add <- .add %+% WriteJSONChunk(
                c(test_takers=checkna(sum(.profs)),
                below_basic=checkna(.profs["Below Basic"]),
                basic=checkna(.profs["Basic"]),
                proficient=checkna(.profs["Proficient"]),
                advanced=checkna(.profs["Advanced"]))) %+% '\n'
            down(.lv)
            .add <- .add %+% paste(indent(.lv), '}', sep="")
            
            .ret <- c(.ret, .add)
        }
    }
    return(paste(.ret, collapse=',\n'))
}

ExStateCReady <- function(level){
    .lv <- level

    .qry <- "SELECT * FROM [dbo].[college_readiness]"	
        
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

ExStateSPEDChunk <- function(level){
    .lv <- level

    .qry <- "SELECT * FROM [dbo].[assessment]
        WHERE [special_ed] = 'YES';"
        
    .dat_mr <- sqlQuery(dbrepcard, .qry)

    # .ret <- do(group_by(.dat_mr, ea_year), WriteSPED, level)
    .ret <- lapply(split(.dat_mr, .dat_mr$ea_year), WriteSPED, level)   
    .ret <- subset(.ret, .ret != '')
    return(paste(.ret, collapse=',\n'))
}

ExStateAcct <- function(level){
	## MATH/READING
	.lv <- level
	
	.ret <- c()

	.acct_st <- sqlFetch(dbrepcard, 'accountability_state')

	.ret <- '{\n'
	up(.lv)
	
	.ret <- .ret %+% paste(indent(.lv), '"year": "2013", \n', sep="")
	.ret <- .ret %+% paste(indent(.lv), '"score": null,\n', sep="")
	.ret <- .ret %+% paste(indent(.lv), '"classification": null,\n', sep="")
	.ret <- .ret %+% paste(indent(.lv), '"growth": null, \n', sep="")
	
	.ret <- .ret %+% paste(indent(.lv), '"subgroups": [\n', sep="")
	up(.lv)
	.sgstrings <- c()
	for(i in 1:nrow(.acct_st)){
		.add <- ''
		if(.acct_st$read_size[i] >= 25 | .acct_st$math_size[i] >= 25 | .acct_st$comp_size[i] >= 25){
			.add <- indent(.lv) %+% '{'
			up(.lv)
			
			.add <- .add %+% paste('"subgroup": ',checkna_str(.acct_st$subgroup[i]),', ', sep="")
			
			.add <- .add %+% paste('"read_size": ',checkna(.acct_st$read_size[i]),', ', sep="")
			.add <- .add %+% paste('"read_score": ',checkna(round(.acct_st$read_score[i],2)),', ', sep="")
			
			.add <- .add %+% paste('"math_size": ',checkna(.acct_st$math_size[i]),', ', sep="")
			.add <- .add %+% paste('"math_score": ',checkna(round(.acct_st$math_score[i],2)),', ', sep="")
			
			.add <- .add %+% paste('"comp_size": ',checkna(.acct_st$comp_size[i]),', ', sep="")
			.add <- .add %+% paste('"comp_score": ',checkna(round(.acct_st$comp_score[i],2)), sep="")
			
			down(.lv)
			.add <- .add %+% paste('}', sep="")
			.sgstrings <- c(.sgstrings, .add)
		}
	}
	
	.ret <- .ret %+% paste(.sgstrings, collapse=',\n') %+% '\n'
	
	down(.lv)
	.ret <- .ret %+% paste(indent(.lv), ']\n', sep="")
	down(.lv)
	.ret <- .ret %+% paste(indent(.lv), '}', sep="")
	return(.ret)
}


SubNAEP <- function(subgroup){
	if(subgroup == 'American Indian/Alaska Native'){
		return('American Indian')
	} else if(subgroup=='Black'){
		return('African American')
	} else if(subgroup=='Hispanic / Latino'){
		return('Hispanic')
	} else if(subgroup %in% c('Eligible FRL', 'FRL Eligible')){
		return('Economically Disadvantaged')
	} else if(subgroup %in% c('FRL Not eligible', 'Not eligible FRL')){
		return('Not Economically Disadvantaged')
	} else if(subgroup=='ELL'){
		return('English Learner')
	} else if(subgroup=='Not ELL'){
		return('Not English Learner')
	} else if(subgroup=='SD'){
		return('Special Education')
	} else if(subgroup == 'Not SD'){
		return('Not Special Education')
	} else if(subgroup=='Asian/Pacific Islander'){
		return('Asian')
	}
	
	return(subgroup)
}

ExNaepResult <- function(level){
	.qry <- "SELECT 
		[subject]
      ,[grade]
      ,[subgroup]
      ,[year]
      ,[state]
      ,cast([average_scale_score] as int) as [average_scale_score]
      ,cast([below_basic] as int) as [below_basic]
      ,cast([at_or_above_basic] as int) as [at_or_above_basic]
      ,cast([at_or_above_proficient] as int) as [at_or_above_proficient]
      ,cast([at_advanced] as int) as [at_advanced]
      ,cast([national_average_scale_score] as int) as [national_average_scale_score]
      ,cast([national_below_basic] as int) as [national_below_basic]
      ,cast([national_at_or_above_basic] as int) as [national_at_or_above_basic]
      ,cast([national_at_or_above_proficient] as int) as [national_at_or_above_proficient]
      ,cast([national_at_advanced] as int) as [national_at_advanced]

		FROM [dbo].[naep_state_report]
		WHERE [subgroup] not in ('FRL Information not available',
			'Information not available (FRL)',
			'Parents  Education: Unknown',
			'Parents Education: Did not finish high school',
			'Parents Education: Graduated from college',
			'Parents Education: Graduated from high school',
			'Parents Education: Some education after high school',
			'Parents Education: Unknown',
			'Race: Unclassified')"
	.naepdat <- sqlQuery(dbrepcard, .qry)
	names(.naepdat) <- c("subject", "grade", "subgroup", "year", "state", "avg_scale_score", "below_basic", "at_or_above_basic", "at_or_above_proficient", "at_advanced","national_avg_scale_score", "national_below_basic", "national_at_or_above_basic", "national_at_or_above_proficient", "national_at_advanced")
	
	.lv <- level
	.ret <- c()
	
	for(i in 1:nrow(.naepdat)){
		if(!is.na(.naepdat$avg_scale_score[i])){
			.add <- indent(.lv) %+% '{\n'
			up(.lv)
			.add <- .add %+% paste(indent(.lv), '"key": {', sep="")
			up(.lv)
			.add <- .add %+% paste('"year": "', .naepdat$year[i], '", ', sep="")
			.add <- .add %+% paste('"subject": "', .naepdat$subject[i], '", ', sep="")
			.add <- .add %+% paste('"grade": "', .naepdat$grade[i], '", ', sep="")
			.add <- .add %+% paste('"subgroup": "', SubNAEP(.naepdat$subgroup[i]), '"', sep="")
			
			down(.lv)
			.add <- .add %+% paste('},\n', sep="")
			down(.lv)
			
			up(.lv)
			.add <- .add %+% paste(indent(.lv), '"val": {', sep="")

			up(.lv)
			.add <- .add %+% paste('"average_scale_score": ', checkna(.naepdat$avg_scale_score[i]), ', ', sep="")
			.add <- .add %+% paste('"at_below_basic": ', checkna(.naepdat$below_basic[i]), ', ', sep="")
			.add <- .add %+% paste('"at_or_above_basic": ', checkna(.naepdat$at_or_above_basic[i]), ', ', sep="")
			.add <- .add %+% paste('"at_or_above_proficient": ', checkna(.naepdat$at_or_above_proficient[i]), ', ', sep="")
			.add <- .add %+% paste('"at_advanced": ', checkna(.naepdat$at_advanced[i]), ', ', sep="")
			.add <- .add %+% paste('"national_avg_scale_score": ', checkna(.naepdat$national_avg_scale_score[i]), ', ', sep="")
			.add <- .add %+% paste('"national_below_basic": ', checkna(.naepdat$national_below_basic[i]), ', ', sep="")
			.add <- .add %+% paste('"national_at_or_above_basic": ', checkna(.naepdat$national_at_or_above_basic[i]), ', ', sep="")
			.add <- .add %+% paste('"national_at_or_above_proficient": ', checkna(.naepdat$national_at_or_above_proficient[i]), ', ', sep="")
			.add <- .add %+% paste('"national_at_advanced": ', checkna(.naepdat$national_at_advanced[i]), ', ', sep="")
			
			part <- ExNaepParticipation(.naepdat$year[i], .naepdat$subject[i], .naepdat$grade[i], SubNAEP(.naepdat$subgroup[i]))

			.add <- .add %+% paste('"participation": ', part, sep="")

			down(.lv)
			.add <- .add %+% paste('}\n', sep="")
			down(.lv)
			.add <- .add %+% paste(indent(.lv), '}', sep="")
			
			.ret <- c(.ret, .add)
		}
	}
	##print(.naepdat)
	return(paste(.ret, collapse=',\n'))
}



ExNaepParticipation <- function(.year, .subject, .grade, .subgroup){
	.dat <- sqlQuery(dbrepcard, "SELECT * FROM [dbo].[naep_state_participation]")

	.dat <- subset(.dat, grade == .grade & .subject == subject & year == .year & subgroup == .subgroup)
	if(nrow(.dat) == 1){return(.dat$participation_rate)}
	else {return("null")}
}



ExStateCAS <- function(level){
	.lv <- level
	
	.dat_mr <- sqlFetch(dbrepcard, "assessment_w2014")
	# .ret <- do(group_by(.dat_mr, ea_year), WriteCAS, level)
	.ret <- lapply(split(.dat_mr, .dat_mr$ea_year), WriteCAS, level)   

	##
	.dat_comp <- sqlFetch(dbrepcard, 'assm_comp')
	# .ret <- c(.ret, do(group_by(.dat_comp, ea_year), WriteComp, level))
	.ret <- c(.ret, lapply(split(.dat_comp, .dat_comp$ea_year), WriteComp, level))

	.dat_sci <- sqlFetch(dbrepcard, 'assm_science')
	# .ret <- c(.ret, do(group_by(.dat_sci, ea_year), WriteScience, level))
	.ret <- c(.ret, lapply(split(.dat_sci, .dat_sci$ea_year), WriteScience, level))
	
	return(paste(.ret, collapse=',\n'))
}
##ISA was incorrectly coded as .95. Changed to .91
hard_code_equity <- '			{
				"id": "unexcused_absences",
				"data": [
					{
						"key": {
							"year": "2012"
						},
						"val": {
							"1-5_days": 43,
							"6-10_days": 22,
							"11-15_days": 9,
							"16-25_days": 8,
							"more_than_25_days": 8
						}
					}
				]
			},
			{
				"id": "attendance",
				"data": [
					{
						"key": {"year": "2012"},
						"val": {"in_seat_attendance": 0.91,"average_daily_attendance": null}
					}
				]
			},
			{
				"id": "suspensions",
				"data": [
					{
						"key": {"year": "2012","subgroup": "All"},
						"val": {"suspended_1": 0.12,"suspended_11": 0}
					},
					{
						"key": {"year": "2012","subgroup": "African American"},
						"val": {"suspended_1": 0.16,"suspended_11": 0.01}
					},
					{
						"key": {"year": "2012","subgroup": "Asian"},
						"val": {"suspended_1": 0.02,"suspended_11": 0}
					},
					{
						"key": {"year": "2012","subgroup": "Economically Disadvantaged"},
						"val": {"suspended_1": 0.15,"suspended_11": 0.01}
					},
					{
						"key": {"year": "2012","subgroup": "English Learner"},
						"val": {"suspended_1": 0.04,"suspended_11": 0}
					},
					{
						"key": {"year": "2012","subgroup": "Hispanic"},
						"val": {"suspended_1": 0.04,"suspended_11":0}
					},
					{
						"key": {"year": "2012","subgroup": "White"},
						"val": {"suspended_1": 0.01,"suspended_11":0}
					},
					{
						"key": {"year": "2012","subgroup": "Pacific Islander"},
						"val": {"suspended_1": 0.1,"suspended_11":0}
					},
					{
						"key": {"year": "2012","subgroup": "Multiracial"},
						"val": {"suspended_1": 0.05,"suspended_11":0}
					},
					{
						"key": {"year": "2012","subgroup": "Special Education"},
						"val": {"suspended_1": 0.23,"suspended_11": 0.01}
					}
				]
			},
			{
				"id": "expulsions",
				"data": [
					{
						"key": {"year": "2012"},
						"val": {"expulsions":187,"expulsion_rate": 0.22}
					}
				]
			}'



hard_code_apr <- '			{
				"id": "apr",
				"data": [
					{
						"year": 2012,
						"indicators": {
							"1": {"on_target": false,"weight": 1,"val": 0.44,"target": 0.85,"target_dir": "up"},
							"2": {"on_target": false,"weight": 1,"val": 0.06,"target": 0.06,"target_dir": "down"},
							"3a": {"on_target": false,"weight": 0.3333,"val": 0.0,"target": 0.5,"target_dir": "up"},
							"3b_reading": {"on_target": true,"weight": 0.1667,"val": 0.99,"target": 0.95,"target_dir": "up"},
							"3b_math": {"on_target": true,"weight": 0.1667,"val": 0.99,"target": 0.95,"target_dir": "up"},
							"3c_elem-reading": {"on_target": false,"weight": 0.0833,"val": 0.19,"target": 0.8685,"target_dir": "up"},
							"3c_elem-math": {"on_target": false,"weight": 0.0833,"val": 0.24,"target": 0.8507,"target_dir": "up"},
							"3c_sec-reading": {"on_target": false,"weight": 0.0833,"val": 0.19,"target": 0.8594,"target_dir": "up"},
							"3c_sec-math": {"on_target": false,"weight": 0.0833,"val": 0.24,"target": 0.8514,"target_dir": "up"},

							"4a": {"on_target": false,"weight": 0.5,"val": 0.28,"target": 0,"target_dir": "down"},
							"4b_a": {"on_target": false,"weight": 0.25,"val": 0.28,"target": 0,"target_dir": "down"},
							"4b_b": {"on_target": false,"weight": 0.25,"val": 0.08,"target": 0,"target_dir": "down"},


							"5a": {"on_target": true,"weight": 0.3333,"val": 0.50,"target": 0.175,"target_dir": "up"},
							"5b": {"on_target": true,"weight": 0.3333,"val": 0.12,"target": 0.15,"target_dir": "down"},
							"5c": {"on_target": false,"weight": 0.3333,"val": 0.19,"target": 0.15,"target_dir": "down"},

							"6a": {"on_target": false,"weight": 0.5,"val": 0.56,"target": 0.63,"target_dir": "up"},
							"6b": {"on_target": false,"weight": 0.5,"val": 0.16,"target": 0.15,"target_dir": "down"},



							"7a_a": {"on_target": true,"weight": 0.1667,"val": 0.76,"target": 0.7,"target_dir": "up"},
							"7a_b": {"on_target": true,"weight": 0.1667,"val": 0.67,"target": 0.6,"target_dir": "up"},
							"7b_a": {"on_target": false,"weight": 0.1667,"val": 0.81,"target": 0.90,"target_dir": "up"},
							"7b_b": {"on_target": true,"weight": 0.1667,"val": 0.67,"target": 0.6,"target_dir": "up"},
							"7c_a": {"on_target": true,"weight": 0.1667,"val": 0.79,"target": 0.6,"target_dir": "up"},
							"7c_b": {"on_target": false,"weight": 0.1667,"val": 0.70,"target": 0.8,"target_dir": "up"},


							
							"8": {"on_target": true,"weight": 1,"val": 0.93,"target": 0.75,"target_dir": "up"},
							"9": {"on_target": false,"weight": 1,"val": 0.13,"target": 0,"target_dir": "down"},
							"10": {"on_target": false,"weight": 1,"val": 0.17,"target": 0,"target_dir": "down"},
							"11": {"on_target": false,"weight": 1,"val": 0.93,"target": 1,"target_dir": "up"},
							"12": {"on_target": false,"weight": 1,"val": 0.96,"target": 1,"target_dir": "up"},
							"13": {"on_target": false,"weight": 1,"val": 0.40,"target": 1,"target_dir": "up"},
							"14_a": {"on_target": false,"weight": 0.3333,"val": 0.2324,"target": 0.27,"target_dir": "up"},
							"14_b": {"on_target": false,"weight": 0.3333,"val": 0.2562,"target": 0.51,"target_dir": "up"},
							"14_c": {"on_target": false,"weight": 0.3333,"val": 0.3081,"target": 0.64,"target_dir": "up"},
							"15": {"on_target": false,"weight": 1,"val": 0.77,"target": 1,"target_dir": "up"},
							"18": {"on_target": true,"weight": 1,"val": 0.64,"target": [0.55, 0.7],"target_dir": "btw"},
							"19": {"on_target": true,"weight": 1,"val": 0.46,"target": [0.45, 0.6],"target_dir": "btw"},
							"20": {"on_target": false,"weight": 1,"val": null,"target": 1,"target_dir": "up"}
						}
					},
					{
						"year": 2011,
						"indicators": {
							"1": {"on_target": false,"weight": 1,"val": 0.39,"target": 0.85,"target_dir": "up"},
							"2": {"on_target": false,"weight": 1,"val": 0.39,"target": 0.06,"target_dir": "down"},
							"3a": {"on_target": false,"weight": 0.3333,"val": 0.11,"target": 0.5,"target_dir": "up"},
							"3b_reading": {"on_target": true,"weight": 0.1667,"val": 0.95,"target": 0.95,"target_dir": "up"},
							"3b_math": {"on_target": true,"weight": 0.1667,"val": 0.95,"target": 0.95,"target_dir": "up"},
							"3c_elem-reading": {"on_target": false,"weight": 0.0833,"val": 0.15,"target": 0.7369,"target_dir": "up"},
							"3c_elem-math": {"on_target": false,"weight": 0.0833,"val": 0.18,"target": 0.7014,"target_dir": "up"},
							"3c_sec-reading": {"on_target": false,"weight": 0.0833,"val": 0.12,"target": 0.7179,"target_dir": "up"},
							"3c_sec-math": {"on_target": false,"weight": 0.0833,"val": 0.16,"target": 0.7027,"target_dir": "up"},
							"4a": {"on_target": false,"weight": 0.5,"val": 0.43,"target": 0,"target_dir": "down"},
							"4b_a": {"on_target": false,"weight": 0.25,"val": 0.43,"target": 0,"target_dir": "down"},
							"4b_b": {"on_target": false,"weight": 0.25,"val": 0.14,"target": 0,"target_dir": "down"},
							"5a": {"on_target": true,"weight": 0.3333,"val": 0.46,"target": 0.165,"target_dir": "up"},
							"5b": {"on_target": true,"weight": 0.3333,"val": 0.13,"target": 0.13,"target_dir": "down"},
							"5c": {"on_target": true,"weight": 0.3333,"val": 0.2,"target": 0.2,"target_dir": "down"},
							"6a": {"on_target": false,"weight": 0.5,"val": 0.53,"target": 0.63,"target_dir": "up"},
							"6b": {"on_target": false,"weight": 0.5,"val": 0.18,"target": 0.15,"target_dir": "down"},
							"7a_a": {"on_target": true,"weight": 0.1667,"val": 0.64,"target": 0.6,"target_dir": "up"},
							"7a_b": {"on_target": false,"weight": 0.1667,"val": 0.29,"target": 0.5,"target_dir": "up"},
							"7b_a": {"on_target": false,"weight": 0.1667,"val": 0.7,"target": 0.85,"target_dir": "up"},
							"7b_b": {"on_target": false,"weight": 0.1667,"val": 0.42,"target": 0.5,"target_dir": "up"},
							"7c_a": {"on_target": true,"weight": 0.1667,"val": 0.67,"target": 0.5,"target_dir": "up"},
							"7c_b": {"on_target": false,"weight": 0.1667,"val": 0.62,"target": 0.7,"target_dir": "up"},
							"8": {"on_target": false,"weight": 1,"val": 0.68,"target": 0.73,"target_dir": "up"},
							"9": {"on_target": false,"weight": 1,"val": 0.05,"target": 0,"target_dir": "down"},
							"10": {"on_target": false,"weight": 1,"val": 0.1,"target": 0,"target_dir": "down"},
							"11": {"on_target": false,"weight": 1,"val": 0.89,"target": 1,"target_dir": "up"},
							"12": {"on_target": false,"weight": 1,"val": 0.89,"target": 1,"target_dir": "up"},
							"13": {"on_target": false,"weight": 1,"val": 0.28,"target": 1,"target_dir": "up"},
							"14_a": {"on_target": true,"weight": 0.3333,"val": 0.35,"target": 0.26,"target_dir": "up"},
							"14_b": {"on_target": true,"weight": 0.3333,"val": 0.56,"target": 0.49,"target_dir": "up"},
							"14_c": {"on_target": true,"weight": 0.3333,"val": 0.68,"target": 0.61,"target_dir": "up"},
							"15": {"on_target": false,"weight": 1,"val": 0.61,"target": 1,"target_dir": "up"},
							"18": {"on_target": false,"weight": 1,"val": 0.27,"target": [0.55, 0.7],"target_dir": "btw"},
							"19": {"on_target": false,"weight": 1,"val": 0.7,"target": [0.45, 0.6],"target_dir": "btw"},
							"20": {"on_target": false,"weight": 1,"val": 0.9565,"target": 1,"target_dir": "up"}
						}
					}
				]
			},'