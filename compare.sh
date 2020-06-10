./seff-array.py -v $1 > seff_output.txt
./retrieve.py $1 > expected.txt
diff seff_output.txt expected.txt 

# echo "seff_output"
# cat seff_output.txt

# echo "expected"
# cat expected.txt

# exit 0
