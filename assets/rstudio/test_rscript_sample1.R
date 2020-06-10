#! /usr/bin/env Rscript
# Licensed Materials - Property of IBM
# 5737-C49
# 5737-B37
# 5737-D37
# (C) Copyright IBM Corp. 2020    All Rights Reserved.
# US Government Users Restricted Rights - Use, duplication or disclosure
# restricted by GSA ADP Schedule Contract with IBM Corp.

# Test script for R
x <- 5
print(x)

# args
options_args <- commandArgs(trailingOnly = TRUE)
print("User arguments:")
options_args

# envs
print("User Environment variables:")
print(paste0('ENV1 : ', Sys.getenv('ENV1')))
print(paste0('ENV2 : ', Sys.getenv('ENV2')))
print(paste0('ENV3 : ', Sys.getenv('ENV3')))

# data frame test
library(dplyr)
localDF <- data.frame(name=c("John", "Smith", "Sarah", "Mike", "Bob"), age=c(19, 23, 18, 25, 30))

localDF

library(tensorflow)
library(keras)
library(reticulate)
use_condaenv(condaenv = 'R-3.6', required = TRUE)
py_config()
reticulate::py_available()
model <- keras::keras_model_sequential()
model %>% 
  layer_dense(units = 32, input_shape = c(784)) %>% 
  layer_activation('relu') %>% 
  layer_dense(units = 10) %>% 
  layer_activation('softmax')
summary(model)

for (i in 1:10) {
  print("Wait..")
  print(i)
  Sys.sleep(1)
}

print("Done")
