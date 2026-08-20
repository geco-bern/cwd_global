#!/bin/bash

# Input directories
#STR_DIR="/storage/research/giub_geco/data_2/scratch/phelpap/ERA5Land_1950-2024/ERA5Land_regridded/Daily_str"
#SSR_DIR="/storage/research/giub_geco/data_2/scratch/phelpap/ERA5Land_1950-2024/ERA5Land_regridded/Daily_ssr"
STR_DIR="/storage/capacity/occr_geco/data_2/scratch/fbernhard/ERA5Land_2025-2026_forPCWDUpdate/aggregated_UTCDaily_regridded_1.875deg/Monthly_str"
SSR_DIR="/storage/capacity/occr_geco/data_2/scratch/fbernhard/ERA5Land_2025-2026_forPCWDUpdate/aggregated_UTCDaily_regridded_1.875deg/Monthly_ssr"
OUTPUT_DIR="/storage/capacity/occr_geco/data_2/scratch/fbernhard/ERA5Land_2025-2026_forPCWDUpdate/aggregated_UTCDaily_regridded_1.875deg/Monthly_netrad"

# Create output directory if needed
mkdir -p "$OUTPUT_DIR"

# Loop over all STR files and find matching SSR files by year
# ls -lh "$STR_DIR"/monthly_era5-land_UTCDaily.tot_str.*.nc
# ls -lh "$SSR_DIR"/monthly_era5-land_UTCDaily.tot_ssr.*.nc
for year in 2025 2026; do
    str_file="$STR_DIR"/monthly_era5-land_UTCDaily.tot_str.${year}.nc  # TODO or possibly: era5-land__UTCDaily or ERA5Land_UTCDaily
    # Skip if no files match
    if [[ -e "$str_file" ]]; then 
        # case where str exists, continue with loop
        echo ""
    else
        echo "STR file missing for $year — skipping."
        continue # skip loop iteration
    fi

    # Extract the year from the filename
    filename=$(basename "$str_file")
    year=$(echo "$filename" | grep -oE '[0-9]{4}')

    # Construct the matching SSR file path
    ssr_file="$SSR_DIR/monthly_era5-land_UTCDaily.tot_ssr.${year}.nc"  # TODO or possibly: era5-land__UTCDaily or ERA5Land_UTCDaily
    
    # Output file path
    netrad_file="$OUTPUT_DIR/monthly_era5-land_UTCDaily.netrad.${year}.nc"  # TODO or possibly: era5-land__UTCDaily or ERA5Land_UTCDaily

    # Check that the SSR file exists
    if [[ -f "$ssr_file" ]]; then
        cdo add "$str_file" "$ssr_file" "$netrad_file"
        echo "Net radiation calculated for $year → $netrad_file"
    else
        echo "SSR file missing for $year — skipping."
    fi
done

echo "Done calculating surface net radiation for all available years."

