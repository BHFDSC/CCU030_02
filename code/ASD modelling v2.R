--- 
title: "Modelling"
output: html_document
---

```{r include = FALSE}

rm(list = ls())
load("~/data/df4.RData")

# ls(df4)

# table(df4$autism_no_id)

library(tidyverse)
library(forcats)


library(modelr)
library(stargazer)
library(broom)
library(margins)
library(emmeans)

tempdf = df4 %>% filter(autism_no_id == 1 & age_at_covid >= 18 & age_at_covid <= 100)
IDall = glm(severe ~ age_at_covid + female + ethnicity + astrazeneca + pfizer + moderna + ltc_count2*medcount2, data=tempdf, family = 'binomial')

tempdf = df4 %>% filter(autism_no_id == 0 & age_at_covid >= 18 & age_at_covid <= 100)
GPopall = glm(severe ~ age_at_covid + female + ethnicity + deci_imd + astrazeneca + pfizer + moderna + ltc_count2*medcount2, data=tempdf, family = 'binomial')

summary(IDall)
summary(GPopall)

```

The table below contains results of a logistic model where the outcome of interest is 'severe covid', i.e. a covid hospitalisation or death. The population is all adults with a confirmed covid infection. 

```{r echo = TRUE}
stargazer(IDall, GPopall, object.names=TRUE, no.space = TRUE,  apply.coef=exp, ci=TRUE, type='text', t.auto=F, p.auto=F, report = "vcsp", digits=3, single.row=TRUE)
```
```{r include = FALSE}

# MARGINS

tempdf = df4 %>% filter(age_at_covid >= 18 & age_at_covid <= 100)
alllog = glm(severe ~ autism_no_id + age_at_covid + female + ethnicity + vaccination6 + ltc_count2 + medcount2, data=tempdf, family = 'binomial')

# no vaccine
no_vac = emmeans(alllog, specs = "autism_no_id", at = list(age_at_covid = 75, female = 0, ethnicity = "White", deci_imd = 5, ltc_count2 = median(tempdf$ltc_count2, na.rm = TRUE), medcount2 = median(tempdf$medcount2, na.rm = TRUE), vaccination6 = "none"), regrid="response")

res_no_vac = summary(no_vac)
res_no_vac = cbind(n=1, res_no_vac)

# 1 dose of Astra
A1 = emmeans(alllog, specs = "autism_no_id", at = list(age_at_covid = 75, female = 0, ethnicity = "White", deci_imd = 5, ltc_count2 = median(tempdf$ltc_count2, na.rm = TRUE), medcount2 = median(tempdf$medcount2, na.rm = TRUE), vaccination6 = "astrazeneca"), regrid="response")

res_A1 = summary(A1)
res_A1 = cbind(n=2, res_A1)

# 1 dose of Pfizer
P1 = emmeans(alllog, specs = "autism_no_id", at = list(age_at_covid = 75, female = 0, ethnicity = "White", deci_imd = 5, ltc_count2 = median(tempdf$ltc_count2, na.rm = TRUE), medcount2 = median(tempdf$medcount2, na.rm = TRUE), vaccination6 = "pfizer"), regrid="response")

res_P1 = summary(P1)
res_P1 = cbind(n=3, res_P1)

# 1 dose of Moderna
M1 = emmeans(alllog, specs = "autism_no_id", at = list(age_at_covid = 75, female = 0, ethnicity = "White", deci_imd = 5, ltc_count2 = median(tempdf$ltc_count2, na.rm = TRUE), medcount2 = median(tempdf$medcount2, na.rm = TRUE), vaccination6 = "moderna"), regrid="response")

res_M1 = summary(M1)
res_M1 = cbind(n=4, res_M1)

# not doing two doses to avoid overloading the plot

# export results to csv
results = rbind(res_no_vac, res_A1, res_P1, res_M1)
results

write.csv(results, "~/modelling/emmeans_results.csv")

#  plot
library(ggplot2)
library(grid)
results$autism_no_id = as.factor(results$autism_no_id)
results$n = as.factor(results$n)
p = ggplot(results, aes(x=n, y=prob, fill=autism_no_id)) + 
  geom_bar(stat="identity", position=position_dodge()) +
  geom_errorbar(aes(ymin=asymp.LCL, ymax=asymp.UCL), width=0.2, position=position_dodge(0.9)) +
  scale_x_discrete(labels=c("No vaccine", "AstraZeneca", "Pfizer", "Moderna")) +
  xlab("\nvaccination status") +
  ylab("probability of severe Covid-19 infection\n") +
  theme(plot.margin = unit(c(.5,.5,.5,.5), "cm")) +
  scale_fill_manual(values=c('#999999', '#E69F00'), labels=c("0" = "Not autism_no_id", "1" = "autism_no_id"), name="Group")
ggsave("~/modelling/emmeans_plot.png")
plot(p)

# difference + CI
emmeans(alllog, specs = "autism_no_id", at = list(age_at_covid = 75, female = 0, ethnicity = "White", deci_imd = 5, ltc_count2 = median(tempdf$ltc_count2, na.rm = TRUE), medcount2 = median(tempdf$medcount2, na.rm = TRUE), n_doses = 0, astrazeneca = 0, pfizer = 0, moderna = 0), regrid="response") %>% contrast(method="revpairwise") %>% confint()

```