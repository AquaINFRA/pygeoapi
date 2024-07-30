## Imports
library(sf)
library(magrittr)
library(dplyr)
library(sp)
library(data.table)

#install.packages('remotes',repos = "http://cran.us.r-project.org")
#remotes::install_github('chrisschuerz/SWATrunR')
#library(SWATrunR)

#install.packages("devtools",repos = "http://cran.us.r-project.org")
#devtools::install_github("chrisschuerz/SWATdata")

## Args
args <- commandArgs(trailingOnly = TRUE)
print(paste0('R Command line args: ', args))
t1 = args[1]
t2 = args[2]
t3 = args[3]
out = args[4]

## Output: Write t1, t2, and t3 to a text file
output_file <- "./out.txt"
writeLines(c(t1, t2, t3), out)
print(args)
print(paste0('Successfully wrote to file: ', out))