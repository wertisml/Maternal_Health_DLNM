# Load the "haven" package
library(haven)
library(arrow)

setwd("~/Maternal_Mental_Health/files/Initial_Files")

# Read in the SAS file
sas_data <- read_sas("nc_pregnancyed13.sas7bdat")

# Save the data as a CSV file
write_parquet(sas_data, "nc_pregnancyed13.parquet")

