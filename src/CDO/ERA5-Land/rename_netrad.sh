#!/bin/bash

NETRAD_DIR="/storage/research/giub_geco/data_2/scratch/phelpap/ERA5Land_1950-2024/ERA5Land_regridded/Monthly_netrad/"

for year in 1951 1952; do
    file="$NETRAD_DIR"/monthly_ERA5Land_UTCDaily.netrad.${year}.nc
    [ -e "$file" ] || continue

    echo "?? Processing: $file"

    # Step 1: Rename variable and update metadata
    cdo -setattribute,netrad@standard_name="surface_net_radiation" \
        -setattribute,netrad@long_name="Surface net radiation" \
        -setattribute,netrad@GRIB_shortName="netrad" \
        -setattribute,netrad@GRIB_name="Surface net radiation" \
        -chname,tot_str,netrad "$file" "${file}.tmp1.nc"

    # Step 2: Convert J/m² to W/m² (divide by 86400 seconds)
    cdo divc,86400 "${file}.tmp1.nc" "${file}.tmp2.nc"

    # Step 3: Update units attribute
    cdo setattribute,netrad@units="W m-2" "${file}.tmp2.nc" "${file}.tmp.nc"

    # Finalize (in-place overwrite)
    mv "${file}.tmp.nc" "$file"
    rm -f "${file}.tmp1.nc" "${file}.tmp2.nc"

    echo "Done: $file ? Now in W/m²"
done

echo "All net radiation files updated with correct variable name and metadata."

