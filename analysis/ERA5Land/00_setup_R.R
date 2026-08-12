#!/usr/bin/env Rscript

# script is only needed for initial setup. later scripts reuse it by using renv::restore("~/GitHub/geco-bern/cwd_global/analysis/ERA5Land/")
options(repos = c(CRAN = "https://cloud.r-project.org"))
library(renv)

renv::init(project = "~/GitHub/geco-bern/cwd_global/analysis/ERA5Land/") # ONLY FIRST TIME
# if you get errors on "hasn't been activated yet", possibly do first a source(renv/activate.R):
source("~/GitHub/geco-bern/cwd_global/analysis/ERA5Land/renv/activate.R")

renv::install("Rcpp@1.1.2", prompt=FALSE)
renv::install("RcppParallel@6.2.0", prompt=FALSE)
renv::install("stringi@1.8.9", prompt=FALSE)
renv::install("ncdf4@1.24", prompt=FALSE)
renv::install("RNetCDF@2.11-1", prompt=FALSE) # 2.11-2 gave an error: libnetcdf.so.19: cannot open shared object file
renv::install("tidync@0.5.0", prompt=FALSE)

renv::install("geco-bern/map2tidy@v2.1.4", prompt=FALSE) # 26b49b12493272cfe6aa087d35fafb289ff77a7b
renv::install("geco-bern/cwd@v2.0", prompt=FALSE)        # fae00b96c6fa9a9aa400e823b82c7341be63db08

renv::snapshot(prompt=FALSE)

renv::status()
