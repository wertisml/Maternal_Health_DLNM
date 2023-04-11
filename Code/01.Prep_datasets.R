library(arrow)
library(tidyverse)

setwd("~/Maternal_Mental_Health/files/Initial_Files")

Data <- open_dataset("nc_pregnancyed13.parquet") %>%
  select(shepsid, fyear, ptzip, ptcnty, agey, sex, race, birthwt, admitdt, 
         ethnicity, age_grp, sex_rev, race_rec, race_rev, enth_rev, pregnant,
         ab_pregnant, spon_abort, elect_abort, delivery, preg_comp, livebirth, 
         some_livebirth, stillbirth, other_birth, gestation, postpartum, Depression_new,
         Depression, Anxiety_new, Anxiety, Bipolar, Psych_Dis, Other_MMH_t1, Other_MMH_t2,
         Other_MMH_t3, Other_MMH_t4, Mental_disorders, suicide_thought, suicide_attempt,
         PMAD, SMI, MDP, any_outcome, Gestation_Weeks, trimester, preg_age, PMAD_primary,
         SMI_primary, MDP_primary, suicide_attempt_primary, suicide_thought_primary,
         drug_therapy, sleep_disturb, drug_use_comp, raceth, age_cat) %>%
  collect() 

Data$ptzip <- as.integer(Data$ptzip)

setwd("~/Maternal_Mental_Health/files/Create_Data/Pre_Datasets")
write_parquet(Data, "Pregnancy_data.parquet")
