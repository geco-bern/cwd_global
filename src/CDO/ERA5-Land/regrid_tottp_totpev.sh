#!/bin/bash

# Define input and output directories
INPUT_DIR="/storage/research/giub_geco/data_2/scratch/phelpap/ERA5Land_1950-2024/test_tottp/"
OUTPUT_DIR="/storage/research/giub_geco/data_2/scratch/phelpap/ERA5Land_1950-2024/test_regrid/"

# Ensure the output directory exists
mkdir -p "$OUTPUT_DIR"

# Temporary grid description file
GRID_FILE="target_grid.txt"

# Create a target grid file for regridding (192x96 resolution, -180 to 180 longitude)
cat > $GRID_FILE <<EOF
gridtype  = lonlat
xsize     = 192
ysize     = 96
xfirst    = -180
xinc      = 1.875
yvals     = -88.572169, -86.722531, -84.861970, -82.998942, -81.134977, -79.270559, -77.405888, -75.541061, -73.676132, -71.811132, -69.946081, -68.080991, -66.215872, -64.350730, -62.485571, -60.620396, -58.755209, -56.890013, -55.024808, -53.159596, -51.294377, -49.429154, -47.563926, -45.698694, -43.833459, -41.968220, -40.102979, -38.237736, -36.372491, -34.507243, -32.641994, -30.776744, -28.911492, -27.046239, -25.180986, -23.315731, -21.450475, -19.585219, -17.719962, -15.854704, -13.989446, -12.124187, -10.258928, -8.393669, -6.528409, -4.663150, -2.797890, -0.932630, 0.932630, 2.797890, 4.663150, 6.528409, 8.393669, 10.258928, 12.124187, 13.989446, 15.854704, 17.719962, 19.585219, 21.450475, 23.315731, 25.180986, 27.046239, 28.911492, 30.776744, 32.641994, 34.507243, 36.372491, 38.237736, 40.102979, 41.968220, 43.833459, 45.698694, 47.563926, 49.429154, 51.294377, 53.159595, 55.024808, 56.890013, 58.755209, 60.620396, 62.485571, 64.350730, 66.215872, 68.080991, 69.946081, 71.811132, 73.676132, 75.541061, 77.405888, 79.270559, 81.134977, 82.998942, 84.861970, 86.722531, 88.572169
EOF

# Loop through all NetCDF files in the input directory
for file in "$INPUT_DIR"/*.nc; do
    # Extract filename without path
    filename=$(basename "$file")
    
    # Define output file path
    output_file="$OUTPUT_DIR/$filename"

    # Apply regridding (bilinear interpolation) and correct longitude range
    cdo remapbil,$GRID_FILE "$file" "$output_file"
    
    echo "Processed: $filename → $output_file"
done

# Clean up temporary grid file
rm $GRID_FILE

echo "All files have been successfully regridded and saved in $OUTPUT_DIR."
