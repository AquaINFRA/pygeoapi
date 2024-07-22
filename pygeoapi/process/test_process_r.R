## Imports
library(sf)
library(magrittr)
library(dplyr)
library(sp)
library(data.table)

## Args
#args <- commandArgs(trailingOnly = TRUE)
#print(paste0('R Command line args: ', args))
#t1 = args[1]
#t2 = args[2]
#t3 = args[3]

## Output: Write t1, t2, and t3 to a text file
output_file <- "./out.txt"
writeLines("c(t1, t2, t3)", output_file)

print(paste0('Successfully wrote to file: ', output_file))
