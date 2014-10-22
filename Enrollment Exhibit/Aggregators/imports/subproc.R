
subproc <- function(x, subgroup){
	if(subgroup == "All"){
		return(x)
	} else if(subgroup %in% c("MALE","FEMALE")){
		return(subset(x, gender == subgroup))
	} else if(subgroup %in% c("AM7","AS7","BL7","HI7","MU7","PI7","WH7")) {
		return(subset(x, race == subgroup))
	} else if(subgroup == "SPED"){
		return(subset(x, !is.na(sped_level)))
	} else if(subgroup == "SPED Level 1"){
		return(subset(x, sped_level == "LEVEL 1"))
	} else if(subgroup == "SPED Level 2"){
		return(subset(x, sped_level == "LEVEL 2"))
	} else if(subgroup == "SPED Level 3"){
		return(subset(x, sped_level == "LEVEL 3"))
	} else if(subgroup == "SPED Level 4"){
		return(subset(x, sped_level == "LEVEL 4"))
	} else if(subgroup == "LEP"){
		return(subset(x, ell_prog == "YES"))
	} else if(subgroup == "Economy"){
		return(subset(x, economy %in% c("CEO","DCERT","FREE","REDUCED")))
	} else if(subgroup == "Direct Cert") {
		return(subset(x, tanf_snap == "YES"))
	} 
}