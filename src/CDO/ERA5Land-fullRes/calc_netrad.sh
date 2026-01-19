#! /usr/bin/bash -l
#SBATCH --job-name="ERA5Land_calc_netrad"
#SBATCH --time=6-00:00:00
#SBATCH --account=invest
#SBATCH --qos=job_icpu-stocker
#SBATCH --ntasks=1               # nr of tasks (processes), used for MPI jobs that may run distributed on multiple compute nodes
#SBATCH --cpus-per-task=4        # nr of threads, used for shared memory jobs that run locally on a single compute node (default: 1)
#SBATCH --mem-per-cpu=18G
#SBATCH --mail-user=fabian.bernhard@unibe.ch
#SBATCH --mail-type=fail         # when do you want to get notified: none, all, begin, end, fail, requeue, array_tasks

echo "Started on: $(date --rfc-3339=seconds)"
echo "Hostname: $(hostname)"
echo "Working directory: $PWD"   # Is most likely the HOME directory. Allows to check in the log.

module load CDO

## Run the shell script
## TODO: this is currently done as a pre-processing step.
##       Would be simpler and more robust to use tidied str and ssr instead of precomputing netrad.

# Input directories
STR_DIR="/storage/capacity/occr_geco/data_2/archive/era5land_munoz-sabater_2021/data/data_dailyUTC_v3/"       # ERA5Land_regridded/Monthly_str
SSR_DIR="/storage/capacity/occr_geco/data_2/archive/era5land_munoz-sabater_2021/data/data_dailyUTC_v3/"       # ERA5Land_regridded/Monthly_ssr
OUTPUT_DIR="/storage/scratch/giub_geco/fbernhard/era5land_munoz-sabater_2021/data/data_dailyUTC_v3/00_temp_netrad"

# Create output directory if needed
mkdir -p "$OUTPUT_DIR"

# Loop over all STR files and find matching SSR files by year
for year in {1950..2024}; do
    str_file="$STR_DIR"/ERA5Land_UTCDaily.tot_str.${year}.nc
    # Skip if no files match
    [ -e "$str_file" ] || continue

    # Extract the year from the filename
    filename=$(basename "$str_file")
    year=$(echo "$filename" | grep -oE '[0-9]{4}')

    # Construct the matching SSR file path
    ssr_file="$SSR_DIR/ERA5Land_UTCDaily.tot_ssr.${year}.nc"

    # Output file path
    netrad_file="$OUTPUT_DIR/ERA5Land_UTCDaily.netrad.${year}.nc"

    # Check that the SSR file exists
    if [[ -f "$ssr_file" ]]; then

        # Step 0: Compute sum
        cdo add "$str_file" "$ssr_file" "$netrad_file"
        echo "Net radiation calculated for $year → $netrad_file"

        # Step 1: Rename variable and update metadata
        cdo -setattribute,netrad@standard_name="surface_net_radiation" \
            -setattribute,netrad@long_name="Surface net radiation" \
            -setattribute,netrad@GRIB_shortName="netrad" \
            -setattribute,netrad@GRIB_name="Surface net radiation" \
            -chname,tot_str,netrad "$netrad_file" "${netrad_file}.tmp1.nc"

        # Step 2: Convert J/m? to W/m? (divide by 86400 seconds)
        cdo divc,86400 "${netrad_file}.tmp1.nc" "${netrad_file}.tmp2.nc"

        # Step 3: Update units attribute
        cdo setattribute,netrad@units="W m-2" "${netrad_file}.tmp2.nc" "${netrad_file}.tmp.nc"

        # Finalize (in-place overwrite)
        mv "${netrad_file}.tmp.nc" "$netrad_file"
        rm -f "${netrad_file}.tmp1.nc" "${netrad_file}.tmp2.nc"

        echo "Done: $netrad_file ? Now in W/m?"

    else
        echo "SSR file missing for $year — skipping."
    fi
done

echo "Done calculating surface net radiation for all available years."

echo "Finished on: $(date --rfc-3339=seconds)"
