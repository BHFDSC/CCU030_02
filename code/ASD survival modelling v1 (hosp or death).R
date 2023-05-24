--- 
title: "Survival Modelling - hospitaliation or death due to covid"
output: html_document
---

```{r include = FALSE}

rm(list = ls())
load("~/data/df4_surv_hosp_or_death.RData")

ls(df4)
# str(df4)
df4$time = df4$days_to_censoring2
df4$status = df4$effective_outcome

# table(df4$autism_no_id)

library(tidyverse)
library(forcats)

library(survival)
library(survminer)
# library(modelr)
library(stargazer)
library(broom)
# library(emmeans)

tempdf = df4 %>% filter(age_on_1_1_20 >= 18 & age_on_1_1_20 <= 100)

KM.autism_no_id = survfit(Surv(time, status) ~ autism_no_id, data = tempdf)

myplot = ggsurvplot(KM.autism_no_id, conf.int=T, risk.table = T)$plot
myplot2 = myplot + scale_y_continuous(limits =c(0.85, 1)) + xlab('days since PCR test') + ylab('probability of not having severe covid')
ggsave("~/modelling/survival/hosp_or_death/KM_hosp_or_death_by_autism_no_id.png")

```
Below is a Kaplan-Meier curve for all adults aged 18-100, stratified by autism_no_id status (also saved as a PNG file):

```{r echo = TRUE}

print(myplot2)

```
```{r include = FALSE}
survdiff(Surv(time, status) ~ autism_no_id, data = tempdf)


tempdf = df4 %>% filter(age_on_1_1_20 >= 18 & age_on_1_1_20 <= 100)

cox = coxph(Surv(time, status) ~ autism_no_id + age_on_1_1_20 + female + black + asian + mixed + other_ethnicity + deci_imd + astrazeneca + pfizer + moderna + ltc_count2 + medcount2, data = tempdf)

summary(cox)
cox.tab = tidy(cox, exponentiate = T , conf.int = T)
write.table(cox.tab, file = "~/modelling/survival/hosp_or_death/cox_hosp_or_death_all.csv")

cox.zph(cox)
```
Below are results of a Cox PH model for all adults aged 18-100 (also saved as a csv file):

```{r echo = TRUE}
summary(cox)

```
```{r include = FALSE}
tempdf = df4 %>% filter(autism_no_id == 1 & age_on_1_1_20 >= 18 & age_on_1_1_20 <= 100)

cox = coxph(Surv(time, status) ~ age_on_1_1_20 + female + black + asian + mixed + other_ethnicity + deci_imd + astrazeneca + pfizer + moderna + ltc_count2*medcount2, data = tempdf)

summary(cox)
cox.tab = tidy(cox, exponentiate = T , conf.int = T)
write.table(cox.tab, file = "~/modelling/survival/hosp_or_death/cox_hosp_or_death_autism_no_id.csv")

cox.zph(cox)
royston(cox)
```
Below are results of a Cox PH model for all adults with autism_no_id aged 18-100 (also saved as a csv file):

```{r echo = TRUE}
summary(cox)

```
```{r include = FALSE}
tempdf = df4 %>% filter(autism_no_id == 0 & age_on_1_1_20 >= 18 & age_on_1_1_20 <= 100)

cox = coxph(Surv(time, status) ~ age_on_1_1_20 + female + black + asian + mixed + other_ethnicity + deci_imd + astrazeneca + pfizer + moderna + ltc_count2*medcount2, data = tempdf)

summary(cox)
cox.tab = tidy(cox, exponentiate = T , conf.int = T)
write.table(cox.tab, file = "~/modelling/survival/hosp_or_death/cox_hosp_or_death_GPop.csv")
cox.zph(cox)
royston(cox)
```
Below are results of a Cox PH model for all adults without autism_no_id aged 18-100 (also saved as a csv file):

```{r echo = TRUE}
summary(cox)

```



