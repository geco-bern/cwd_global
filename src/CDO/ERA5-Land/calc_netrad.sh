#!/bin/bash

# Input directories
STR_DIR="/storage/research/giub_geco/data_2/scratch/phelpap/ERA5Land_1950-2024/ERA5Land_regridded/Monthly_str"
SSR_DIR="/storage/research/giub_geco/data_2/scratch/phelpap/ERA5Land_1950-2024/ERA5Land_regridded/Monthly_ssr"
OUTPUT_DIR="/storage/research/giub_geco/data_2/scratch/phelpap/ERA5Land_1950-2024/ERA5Land_regridded/Monthly_netrad"

# Create output directory if needed
mkdir -p "$OUTPUT_DIR"

# Loop over all STR files and find matching SSR files by year
for year in 1951 1952; do
    str_file="$STR_DIR"/monthly_ERA5Land_UTCDaily.tot_str.${year}.nc
    # Skip if no files match
    [ -e "$str_file" ] || continue

    # Extract the year from the filename
    filename=$(basename "$str_file")
    year=$(echo "$filename" | grep -oE '[0-9]{4}')

    # Construct the matching SSR file path
    ssr_file="$SSR_DIR/monthly_ERA5Land_UTCDaily.tot_ssr.${year}.nc"
    
    # Output file path
    netrad_file="$OUTPUT_DIR/monthly_ERA5Land_UTCDaily.netrad.${year}.nc"

    # Check that the SSR file exists
    if [[ -f "$ssr_file" ]]; then
        cdo add "$str_file" "$ssr_file" "$netrad_file"
        echo "Net radiation calculated for $year → $netrad_file"
    else
        echo "SSR file missing for $year — skipping."
    fi
done

echo "Done calculating surface net radiation for all available years."

