--- 
title: "Oaxaca decomposition"
output: html_document
---

```{r include = FALSE}


rm(list = ls())
options(scipen=999)
load("/mnt/efs/filip.sosenko/data/df4.RData")

library(tidyverse)
# create a reverse of autism_no_id 
df4$autism_no_id_rev = with(df4, ifelse(autism_no_id == 1, 0, 1))

df4$astrazeneca1 = ifelse(df4$vaccination == "1_astrazeneca", 1, 0)
df4$astrazeneca2 = ifelse(df4$vaccination == "2_astrazeneca", 1, 0)
df4$pfizer1 = ifelse(df4$vaccination == "1_pfizer", 1, 0)
df4$pfizer2 = ifelse(df4$vaccination == "2_pfizer", 1, 0)
df4$moderna1 = ifelse(df4$vaccination == "1_moderna", 1, 0)
df4$moderna2 = ifelse(df4$vaccination == "2_moderna", 1, 0)

# table(df4$autism_no_id, useNA = 'always')
# table(df4$autism_no_id_rev, useNA = 'always')


library(oaxaca)
tempdf = df4 %>% filter(age_at_covid >= 18 & age_at_covid <= 100)

results = oaxaca(formula = severe ~ age_at_covid + female + black + asian + mixed + other_ethnicity + deci_imd + astrazeneca + pfizer + moderna + ltc_count2 + medcount2 | autism_no_id_rev | black + asian + mixed + other_ethnicity, data = tempdf, R= NULL, reg.fun = glm, family='binomial')
# !!! oaxaca cannot take 2+ categorical variables after the | autism_no_id | part of the formula
# I've decided to choose ethnicity rather than vaccine name

results$n

results$y

# threefold
results$threefold$overall

png("~/oaxaca/graphs/test/oaxaca_autism_no_id_3fold.png")
```

The plot below presents results of three-fold Oaxaca decomposition following the logistic model where the outcome variable is severe covid. The plot is also saved as a PNG file:    

```{r echo = TRUE}
plot.oaxaca(results,  components = c("endowments","coefficients"), variables = c("age_at_covid", "female", "black", "asian", "mixed", "other_ethnicity", "deci_imd", "astrazeneca", "pfizer", "moderna", "ltc_count2", "medcount2"), variable.labels = c("age_at_covid" = "Age at Covid", "female" = "Female", "black" = "Black", "asian" = "Asian", "mixed" = "Mixed ethnicity", "other_ethnicity" = "Other ethnicity", "deci_imd" = "IMD decile", "astrazeneca" = "AstraZeneca", "pfizer" = "Pfizer", "moderna" = "Moderna", "ltc_count2" = "Count of long-term conditions", "medcount2" = "Count of prescription medication"))
```
```{r include = FALSE}
dev.off()
# component.labels = c("endowments" = "Due to different prevalence","coefficients" = "Due to different coefficients"), 
```
