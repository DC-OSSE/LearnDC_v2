## USEFUL FUNCTIONS##
options(scipen=20)
options(stringsAsFactors = FALSE)

## Operator Functions ##
`%notin%` <- function(x,y) !(x %in% y) 
`%+%` <- function(x,y) paste(x,y,sep="")

## rounding up instead of statistically fair rounding ##
round <- function(x, n) {
	posneg <- sign(x)
	z <- abs(x)*10^n
	z <- z + 0.5
	z <- trunc(z)
	z <- z/10^n
	z*posneg
}

## Wait for Key
readkey <- function()
{
    cat ("Press [enter] to continue", fill=TRUE)
    line0 <- readline()
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

## FINDS INTEGER MODE OF AN ARRAY
mhack <- function(x){
	temp <- table(as.vector(x))
	return(as.integer(names(temp)[temp == max(temp)]))
}

## FINDS STRING MODE OF A STRING SET
mhack2 <- function(x){
	temp <- table(as.vector(x))
	
	x <- subset(x, !is.na(x))
	
	if(length(x)==0){
		return(NA)
	} else{
		return(names(temp)[temp == max(temp)])
	}
}

## Exporting Stuff to JSON... check for NAs
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

strtable <- function(df, n=4, width=60, 
					 n.levels=n, width.levels=width, 
					 factor.values=as.character) {
	stopifnot(is.data.frame(df))
	tab <- data.frame(variable=names(df),
					  class=rep(as.character(NA), ncol(df)),
					  levels=rep(as.character(NA), ncol(df)),
					  examples=rep(as.character(NA), ncol(df)),
					  stringsAsFactors=FALSE)
	collapse.values <- function(col, n, width) {
		result <- NA
		for(j in 1:min(n, length(col))) {
			el <- ifelse(is.numeric(col),
						 paste0(col[1:j], collapse=', '),
						 paste0('"', col[1:j], '"', collapse=', '))
			if(nchar(el) <= width) {
				result <- el
			} else {
				break
			}
		}
		if(length(col) > n) {
			return(paste0(result, ', ...'))
		} else {
			return(result)
		}
	}
	
	for(i in seq_along(df)) {
		if(is.factor(df[,i])) {
			tab[i,]$class <- paste0('Factor w/ ', nlevels(df[,i]), ' levels')
			tab[i,]$levels <- collapse.values(levels(df[,i]), n=n.levels, width=width.levels)
			tab[i,]$examples <- collapse.values(factor.values(df[,i]), n=n, width=width)
		} else {
			tab[i,]$class <- class(df[,i])[1]
			tab[i,]$examples <- collapse.values(df[,i], n=n, width=width)
		}
		
	}
	
	class(tab) <- c('strtable', 'data.frame')
	return(tab)
}

jsonBoolean <- function(x){	
	x <- toupper(x)

	if(is.na(x) | is.null(x)){
		return('null')
	} else if(x %in% c('YES', 'TRUE', '1')){
		return('true')
	} else if(x %in% c('NO', 'FALSE', '0')){
		return('false')
	} else{
		return('null')
	}
}