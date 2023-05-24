rm(list = ls())

library(tidyverse)
# library(modelr)
# library(stargazer)
# library(broom)
library(DBI)
library(dbplyr)
library(dplyr)
library(lubridate)
library(tidyverse)
library(magrittr)
# library(Hmisc)
library(gmodels)
library(tidyr)
# library(broom)
# library(pander)
library(stargazer)
# library(epiDisplay) -- not available
library(forcats)
library(PHEindicatormethods)

# connect to Databricks
con <- dbConnect(odbc::odbc(), "Databricks", timeout = 60, PWD = "[hidden")

# ____________________________________________________________
# 
#          INFECTED
# ____________________________________________________________
# 

df = dbGetQuery(con, "select * from dars_nic_391419_j3w9t_collab.ccu030_20221015_1600_patient_skinny_record_enhanced27_isrs_infected")
nrow(df)

df[is.na(df)] <- 0

head(df)

df$pop = 1
# df$obs = df$hospitalised

# descriptives
df %>%
  group_by(autism_no_id) %>%
  summarise(obs = sum(hospitalised), pop = sum(pop))

df %>%
  filter(covid_in_2020 == 1) %>%
  group_by(autism_no_id) %>%
  summarise(obs = sum(hospitalised), pop = sum(pop))

df %>%
  filter(covid_in_2021 == 1) %>%
  group_by(autism_no_id) %>%
  summarise(obs = sum(hospitalised), pop = sum(pop))  

df_std = df

# create reference population

# whole period
df_ref <- df_std %>%
  filter(autism_no_id == 0) %>%
  group_by(ageband_sex_id) %>%
  summarise(obs = sum(hospitalised), pop = sum(pop))

head(df_ref)

# 2020
df_ref_20 <- df_std %>%
  filter(autism_no_id == 0 & covid_in_2020 == 1) %>%
  group_by(ageband_sex_id) %>%
  summarise(obs = sum(hospitalised), pop = sum(pop))

df_ref_21 <- df_std %>%
  filter(autism_no_id == 0 & covid_in_2021 == 1) %>%
  group_by(ageband_sex_id) %>%
  summarise(obs = sum(hospitalised), pop = sum(pop))

# SMR

# whole period
df_std %>%
  filter(autism_no_id == 1) %>%
  group_by(ageband_sex_id) %>%
  summarise(obs = sum(hospitalised), pop = sum(pop)) %>%
  phe_smr(obs, pop, df_ref$obs, df_ref$pop)

# 2020
df_std %>%
  filter(autism_no_id == 1 & covid_in_2020 == 1) %>%
  group_by(ageband_sex_id) %>%
  summarise(obs = sum(hospitalised), pop = sum(pop)) %>%
  phe_smr(obs, pop, df_ref_20$obs, df_ref_20$pop)

# 2021
df_std %>%
  filter(autism_no_id == 1 & covid_in_2021 == 1) %>%
  group_by(ageband_sex_id) %>%
  summarise(obs = sum(hospitalised), pop = sum(pop)) %>%
  phe_smr(obs, pop, df_ref_21$obs, df_ref_21$pop)

# ____________________________________________________________
# 
#          WHOLE POP 
# ____________________________________________________________
# 

df = dbGetQuery(con, "select * from dars_nic_391419_j3w9t_collab.ccu030_20221015_1600_patient_skinny_record_enhanced27_isrs_all")
nrow(df) 

df[is.na(df)] <- 0

ls(df)

df$pop = 1
# df$obs = df$hospitalised

# descriptives
df %>%
  group_by(autism_no_id) %>%
  summarise(obs = sum(hospitalised), pop = sum(pop))

df %>%
  filter(alive_on_1_1_20 == 1) %>%
  group_by(autism_no_id) %>%
  summarise(obs = sum(hospitalisation_in_2020), pop = sum(pop))
  
df %>%
  filter(alive_on_1_1_21 == 1) %>%
  group_by(autism_no_id) %>%
  summarise(obs = sum(hospitalisation_in_2021), pop = sum(pop))  

df_std = df

head(df_std)

# create reference population

# whole period
df_ref <- df_std %>%
  filter(autism_no_id == 0) %>%
  group_by(ageband_sex_id) %>%
  summarise(obs = sum(hospitalised), pop = sum(pop))

head(df_ref)

# 2020
df_ref_20 <- df_std %>%
  filter(autism_no_id == 0 & alive_on_1_1_20 == 1) %>%
  group_by(ageband_sex_id) %>%
  summarise(obs = sum(hospitalisation_in_2020), pop = sum(pop))

df_ref_21 <- df_std %>%
  filter(autism_no_id == 0 & alive_on_1_1_21 == 1) %>%
  group_by(ageband_sex_id) %>%
  summarise(obs = sum(hospitalisation_in_2021), pop = sum(pop))

# SMR

# whole period
df_std %>%
  filter(autism_no_id == 1) %>%
  group_by(ageband_sex_id) %>%
  summarise(obs = sum(hospitalised), pop = sum(pop)) %>%
  phe_smr(obs, pop, df_ref$obs, df_ref$pop)

# 2020
df_std %>%
  filter(autism_no_id == 1 & alive_on_1_1_20 == 1) %>%
  group_by(ageband_sex_id) %>%
  summarise(obs = sum(hospitalisation_in_2020), pop = sum(pop)) %>%
  phe_smr(obs, pop, df_ref_20$obs, df_ref_20$pop)

# 2021
df_std %>%
  filter(autism_no_id == 1 & alive_on_1_1_21 == 1) %>%
  group_by(ageband_sex_id) %>%
  summarise(obs = sum(hospitalisation_in_2021), pop = sum(pop)) %>%
  phe_smr(obs, pop, df_ref_21$obs, df_ref_21$pop)
