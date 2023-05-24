--- 
title: "Modelling"
output: html_document
---

```{r include = FALSE}

rm(list = ls())

load("~/data/df4_non_covid.RData")

ls(df4)

df4$covid_death_in_2021 = ifelse(df4$covid_death == 1 & df4$death_in_2021 == 1, 1, 0)

df4$age_on_1_1_21 = df4$age_on_1_1_20 + 1

library(tidyverse)
library(modelr)
library(stargazer)
library(broom)

# required packages
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


# ********************************
# ******* NON-COVID DEATHS *******
# ********** ADULTS ONLY *********
# ********************************


tempdf = df4 %>% filter(age_on_1_1_20 >= 18 & age_on_1_1_20 <= 100)

alllog = glm(non_covid_death_in_2020 ~ autism_no_id*age_on_1_1_20 + autism_no_id*female + autism_no_id*ethnicity + autism_no_id*DECI_IMD + autism_no_id*ltc_count2 + autism_no_id*medcount2 , data=tempdf, family = 'binomial', weight = count)

```

The table below contains results of a logistic model where the outcome of interest is 'non-covid death in 2020'. The population is all adults alive on 1 January 2020.

```{r echo = TRUE}
stargazer(alllog, object.names=TRUE, no.space = TRUE,  apply.coef=exp, type='text', t.auto=F, p.auto=F, report = "vc*", digits=3)

```