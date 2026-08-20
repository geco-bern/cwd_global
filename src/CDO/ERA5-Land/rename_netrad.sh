#!/bin/bash

NETRAD_DIR="/storage/capacity/occr_geco/data_2/scratch/fbernhard/ERA5Land_2025-2026_forPCWDUpdate/aggregated_UTCDaily_regridded_1.875deg/Monthly_netrad"

for year in 2020 2021 2022 2023 2024 2025 2026; do
    file="$NETRAD_DIR"/monthly_era5-land_UTCDaily.netrad.${year}.nc  # TODO or possibly: era5-land__UTCDaily or ERA5Land_UTCDaily
    # Skip if no files match
    if [[ -e "$file" ]]; then 
        # case where str exists, continue with loop
        echo ""
    else
        echo "NETRAD file missing for $year — skipping."
        continue # skip loop iteration
    fi

    echo "?? Processing: $file"

    # Step 1: Rename variable and update metadata
    cdo -setattribute,netrad@standard_name="surface_net_radiation" \
        -setattribute,netrad@long_name="Surface net radiation" \
        -setattribute,netrad@GRIB_shortName="netrad" \
        -setattribute,netrad@GRIB_name="Surface net radiation" \
        -chname,tot_str,netrad "$file" "${file}.tmp1.nc"

    # Step 2: Convert J/m� to W/m� (divide by 86400 seconds)
    cdo divc,86400 "${file}.tmp1.nc" "${file}.tmp2.nc"

    # Step 3: Update units attribute
    cdo setattribute,netrad@units="W m-2" "${file}.tmp2.nc" "${file}.tmp.nc"

    # Finalize (in-place overwrite)
    mv "${file}.tmp.nc" "$file"
    rm -f "${file}.tmp1.nc" "${file}.tmp2.nc"

    echo "Done: $file ? Now in W/m�"
done

echo "All net radiation files updated with correct variable name and metadata."

