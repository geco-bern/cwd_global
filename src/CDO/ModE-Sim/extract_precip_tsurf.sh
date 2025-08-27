#!/bin/bash

set -ex

# Directory containing files
input_dir="/mnt/climstor/ERC_PALAEO/ModE-Sim/outdata/set_1850-2/abs/m033/by_year/"  #change ensemble member, epoch and sets (m001-m020 in set-...-1 etc)
# Directory to save output
output_dir="/scratch2/phelpap/precip/m033_1850_2/"  #raw netcdf file output; change precip to or tsurf

# Define the list of years
years=$(seq 1850 2009)  # Use space-separated values without curly braces, test; needs to go til 1850; change to 1420 1850 or 1850 2009

# Loop through the list of years
for i in ${years}; do
    # Construct file names
    input_file="${input_dir}ModE-Sim_set_1850-2_m033_${i}_day.toasurf.grb"   #change epoch, set and ensemble member
    output_file="${output_dir}set1850_2_m033_precip_${i}.nc"   #change to precip or tsurf; change epoch, set and ensemble member
    # Run CDO command
    cdo -f nc -t echam6 -selvar,precip "$input_file" "$output_file"    #change variable: tsurf or precip
       echo "Processed $input_file and saved to $output_file"
done
