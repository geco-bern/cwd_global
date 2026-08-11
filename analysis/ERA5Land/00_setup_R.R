#!/usr/bin/env Rscript

# script is only needed for initial setup. later scripts reuse it by using renv::restore("~/GitHub/geco-bern/cwd_global/analysis/ERA5Land/")
library(renv)

renv::init(project = "~/GitHub/geco-bern/cwd_global/analysis/ERA5Land/")
pak::pak("geco-bern/map2tidy@v2.1.4") # 26b49b12493272cfe6aa087d35fafb289ff77a7b
pak::pak("geco-bern/cwd@v2.0")        # fae00b96c6fa9a9aa400e823b82c7341be63db08

# TODO: go through all packages and install them


renv::snapshot()
