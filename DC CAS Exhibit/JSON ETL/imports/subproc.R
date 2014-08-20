
subproc <- function(x, subgroup){
	if(subgroup == "All"){
		return(x)
	} else if(subgroup %in% c("MALE","FEMALE")){
		return(subset(x, gender == subgroup))
	} else if(subgroup %in% c("AM7","AS7","BL7","HI7","MU7","PI7","WH7"))  {
		return(subset(x, race == subgroup))
	} else if(subgroup == "SPED"){
		return(subset(x, sped_indicator == 1 | sped_monitored == 1))
	} else if(subgroup == "LEP"){
		return(subset(x, ell_indicator == 1 | ell_monitored == 1))
	} else if(subgroup == "Economy"){
		return(subset(x, economy == 1))
	} else if(subgroup == "Direct Cert") {
		return(subset(x, direct_cert == 1))
	}
}

