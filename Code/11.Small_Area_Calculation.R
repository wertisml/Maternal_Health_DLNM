# DEFINE SPLINES OF DAY OF THE YEAR
spldoy <- onebasis(Data$doy, "ns", df=3)

# DEFINE THE CROSS-BASIS FOR TEMPERATURE FROM THE EXPOSURE HISTORY MATRIX
# NB: USE group TO IDENTIFY LACK OF CONTINUITY IN SERIES BY MSOA AND YEAR
range <- round(range(Data$temp, na.rm = T),0)
knots <- range[1] + (range[2]-range[1])/4*1:3

argvar <- list(fun="ns", knots=knots)
arglag <- list(fun="ns", knots=1)

group <- factor(paste(Data$Zip, Data$year, sep="-"))
#table <- data.table(table(group))
cbtmean <- crossbasis(Data$temp, lag=7, argvar=argvar, arglag=arglag,
                      group=group)
#summary(cbtmean)

# DEFINE THE STRATA 
Data[, stratum:=factor(paste(Zip, year, month, sep=":"))]

# RUN THE MODEL
# NB: EXCLUDE EMPTY STRATA, OTHERWISE BIAS IN gnm WITH quasipoisson
Data[,  keep:=sum(Outcome)>0, by=stratum]
modfull <- gnm(Outcome ~ cbtmean + ns(RH, df = 2) + spldoy:factor(year) + factor(dow), 
               eliminate=stratum, data=Data, family=quasipoisson, subset=keep)

cpfull <- crosspred(cbtmean, modfull, cen=mean(Data$temp, na.rm=T))
