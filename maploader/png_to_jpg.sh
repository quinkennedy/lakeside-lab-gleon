for file in *.png; do sips -s format jpeg $file --out "../jpg/`basename "$file" .png`.jpg";done
