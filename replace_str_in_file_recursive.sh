for file in $(find . -name "*.py" -type f)
do
    echo "replacing str for file: "$file
    sed -i 's/home\/junhe\///g' $file
done
