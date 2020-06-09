./seff-array -v $1 > seff_output.txt
./retrieve $1 > expected.txt
diff seff_output.txt expected.txt
