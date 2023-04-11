library(tidyverse)
library(arrow)

setwd("~/Maternal_Mental_Health/files/Create_Data/Pre_Datasets")

Regions <- read_csv("Regions.csv")

#==============================================================================#
# Create Regions
#==============================================================================#

# Mountains
Mountains <- Regions %>%
  filter(Region == "Mountains") %>%
  dplyr::select(ZCTA)

# Piedmont
Piedmont <- Regions %>%
  filter(Region == "Piedmont") %>%
  dplyr::select(ZCTA)

# Coast
Coast <- Regions %>%
  filter(Region == "Coast") %>%
  dplyr::select(ZCTA)

#==============================================================================#
# Create the filtered data
#==============================================================================#

Temp_and_Preg_with_Regions <- left_join(open_dataset("Temperature_Data.parquet") %>%
                    select(Zip, Date, TAVG, TMAX, TMIN, RH),
                  open_dataset("Pregnancy_data.parquet") %>%
                    rename(Date = admitdt,
                           Zip = ptzip) %>%
                    group_by(Zip, Date) %>%
                    summarise(any_outcome = sum(any_outcome),
                              pregnant = sum(pregnant),
                              ab_pregnant = sum(ab_pregnant),
                              spon_abort = sum(spon_abort),
                              elect_abort = sum(elect_abort),
                              delivery = sum(delivery),
                              preg_comp = sum(preg_comp),
                              livebirth = sum(livebirth),
                              some_livebirth = sum(some_livebirth),
                              stillbirth = sum(stillbirth),
                              other_birth = sum(other_birth),
                              gestation = sum(gestation),
                              postpartum = sum(postpartum),
                              Depression_new = sum(Depression_new),
                              Depression = sum(Depression),
                              Anxiety_new = sum(Anxiety_new),
                              Anxiety = sum(Anxiety),
                              Bipolar = sum(Bipolar),
                              Psych_Dis = sum(Psych_Dis),
                              Other_MMH_t1 = sum(Other_MMH_t1),
                              Other_MMH_t2 = sum(Other_MMH_t2),
                              Other_MMH_t3 = sum(Other_MMH_t3),
                              Other_MMH_t4 = sum(Other_MMH_t4),
                              Mental_disorders = sum(Mental_disorders),
                              suicide_thought = sum(suicide_thought),
                              suicide_attempt = sum(suicide_attempt),
                              PMAD = sum(PMAD),
                              SMI = sum(SMI),
                              MDP = sum(MDP),
                              PMAD_primary = sum(PMAD_primary),
                              SMI_primary = sum(SMI_primary),
                              MDP_primary = sum(MDP_primary),
                              suicide_thought_primary = sum(suicide_thought_primary),
                              suicide_attempt_primary = sum(suicide_attempt_primary),
                              drug_therapy = sum(drug_therapy),
                              sleep_disturb = sum(sleep_disturb)),
                  by = c("Zip", "Date")) %>%
  collect() %>% 
  filter(Date >= "2015-08-26") %>%
  replace(is.na(.), 0) %>%
  mutate(region = ifelse(Zip %in% Mountains$ZCTA, "Mountains",
                         ifelse(Zip %in% Piedmont$ZCTA, "Piedmont",
                                ifelse(Zip %in% Coast$ZCTA, "Coast", ""))))

#==============================================================================#
# Finalize
#==============================================================================#

setwd("~/Maternal_Mental_Health/files")
write_parquet(Temp_and_Preg_with_Regions, "Preg_Temp_Regions.parquet")
