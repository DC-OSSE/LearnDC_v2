## USEFUL FUNCTIONS## 
options(scipen=20)
options(stringsAsFactors = FALSE)

## Operator Functions ##
`%notin%` <- function(x,y) !(x %in% y) 
`%+%` <- function(x,y) paste(x,y,sep="")

## Wait for Key
readkey <- function()
{
    cat ("Press [enter] to continue", fill=TRUE)
    line <- readline()
}

## Generation of Indentation by Level
indent <- function(n){
	return(paste(rep("\t", n), collapse=''))
}

## ADD LEADING ZEROES to X Digits ## 
leadgr <- function(x, y){
	if(!is.na(x)){
		while(nchar(x)<y){
			x <- paste("0",x,sep="")
		}
	}
	return(x)
}

trimall <- function(tstring){
	return(gsub("(^ +)|( +$)", "", tstring))
}

increment <- function(x)
{
	eval.parent(substitute(x <- x + 1))
}

up <- function(x){
	eval.parent(substitute(x <- x + 1))
}
down <- function(x){
	eval.parent(substitute(x <- x - 1))
}

## PLOT MULTIPLE DISTRIBUTIONS
plot.multi.dens <- function(s, toplb="")
{
	junk.x = NULL
	junk.y = NULL
	for(i in 1:length(s))
	{
		junk.x = c(junk.x, density(s[[i]],na.rm=TRUE)$x)
		junk.y = c(junk.y, density(s[[i]],na.rm=TRUE)$y)
	}
	xr <- range(junk.x)
	yr <- range(junk.y)
	plot(density(s[[1]],na.rm=TRUE), xlim = xr, ylim = yr, main = toplb)
	for(i in 1:length(s))
	{
		lines(density(s[[i]], na.rm=TRUE), xlim = xr, ylim = yr, col = i)
	}
}

## RENAME
rename <- function(badname, replacement, dataset){
	x <- names(dataset) 
	
	if(typeof(replacement) != "character"){
		print("Send me a valid name man")
	} else if(badname %in% x){
		x[x==badname] <- trimall(replacement)
		names(dataset) <- x
		return(dataset)
		
	}else{
		print("Sorry bro, name not found")
		return(dataset)
	}
}

## FINDS INTEGER MODE OF A STRING SET
mhack <- function(x){
	temp <- table(as.vector(x))
	if(length(subset(x, is.na(x)))/length(x)>.5){
		return(NA)
	} else{
		return(as.integer(names(temp)[temp == max(temp)]))
	}
}

## FINDS STRING MODE OF A STRING SET
mhack2 <- function(x){
	temp <- table(as.vector(x))
	if(length(x[is.na(x)])>500){
		return(NA)
	} else{
		return(names(temp)[temp == max(temp)])
	}
}

## Exporting Stuff to JSON
checkna <- function(x){
	if(is.na(x)){
		return('null')
	}
	return(as.character(x))
}

checkna_str <- function(x){
	if(is.na(x)| length(x) ==0){
		return('null')
	}
	return('"' %+% x %+%'"')
}

sampdf <- function(x,y) {
	x[sample(1:nrow(x),y),]
}

make_null <- function(x){
	if(length(x) == 0) {x <- 'null'}
	else if (is.na(x) | is.null(x)){x <- 'null'}
	else return(x)
}


## DO NOT USE ## 
