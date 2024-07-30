## Imports
library(sf)
library(magrittr)
library(dplyr)
library(sp)
library(data.table)

install.packages('remotes',repos = "http://cran.us.r-project.org")
remotes::install_github('chrisschuerz/SWATrunR')
library(SWATrunR)

install.packages("devtools",repos = "http://cran.us.r-project.org")
devtools::install_github("chrisschuerz/SWATdata")

## Args
args <- commandArgs(trailingOnly = TRUE)
print(paste0('R Command line args: ', args))
t1 = args[1]
t2 = args[2]
t3 = args[3]
out = args[4]

demo_path <- "./"

# Loading a SWAT+ demo project
path_plus <- load_demo(dataset = 'project',
                       path = demo_path,
                      version = 'plus')
# Loading a SWAT2012 demo project
path_2012 <- load_demo(dataset = 'project',
                       path = demo_path,
                       version = '2012')

# Observation data
q_obs <- load_demo(dataset = 'observation')

q_obs

png(file="./saving_plot2.png")
plot(q_obs, type = 'l') # plotdata
dev.off()

q_sim_plus <- run_swatplus(project_path = path_plus,
                           output = define_output(file = 'channel_sd_day',
                                                  variable = 'flo_out',
                                                  unit = 1))

q_sim_2012 <- run_swat2012(project_path = path_2012,
                           output = define_output(file = 'rch',
                                                  variable = 'FLOW_OUT',
                                                  unit = 1:3))

#### EXPLORING SIMULATION OUTPUTS ####

q_sim_plus
q_sim_2012

## Output: Write t1, t2, and t3 to a text file
output_file <- "./out.txt"
writeLines(c(t1, t2, t3), out)
print(args)
print(paste0('Successfully wrote to file: ', out))