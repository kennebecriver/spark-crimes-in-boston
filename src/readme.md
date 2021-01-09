`spark-submit --master local[*] --class com.example.BostonCrimesMap /path/to/jar {path/to/crime.csv} {path/to/offense_codes.csv} {path/to/output_folder}`


spark-submit --master local --class com.example.BostonCrimesMap .\target\scala-2.11\spark-crimes-in-boston-assembly-0.1.jar src/data/crime.csv src/data/offense_codes.csv src/data
