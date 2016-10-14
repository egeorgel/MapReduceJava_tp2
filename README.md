# MapReduce using Hadoop in Java

Using MapReduce on a csv file.
It contains semicolon separated csv exports of first names. The header (column name) is
name;gender (m/f) ;origin ;version (don't pay attention to this value)
When multiples values are possible (gender, origin), they are comma separated.

Subject :
Create M/R Softwares to get these stats (1 M/R per stat) :
- Count first name by origin
- Count number of first name by number of origin (how many first name has x origins ? For x = 1,2,3...)
-  Proportion (in%) of male or female


## Running the tests

The test are written in the test folder
For running it 
```
mvn test
```

### Running the program -> jar creation

Run
```
mvn clean install
```

The main program is located in App.cpp


feel free to fork and improve the code.

## Authors

* **Edgar Georgel** - *Initial work* - [MapReduce in Java tp2](https://github.com/egeorgel/MapReduceJava_tp2)
