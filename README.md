CarbonadoSleepycatDB
=====================

CarbonadoSleepycatDB contains a repository supporting Sleepycat/Oracle,
[Berkeley DB][BDB-Homepage]. Berkeley DB code must be downloaded and installed separately.


Installing Berkeley DB
----------------------

1. Download and install [Berkeley DB][BDB-Download]. Note that this is different than
   Berkeley DB Java Edition. You may also be able to install Berkeley DB through your
   operating system's package manager. For example, on Ubuntu: `sudo apt-get install libdb-java-dev`.

2. Install db.jar into your local maven repository:

    ```
    mvn install:install-file -Dfile=/path/to/db.jar -DgroupId=com.sleepycat \
                             -DartifactId=db -Dversion=5.1 \
                             -Dpackaging=jar
    ```

[BDB-Homepage]: http://www.oracle.com/technetwork/database/database-technologies/berkeleydb/overview/index.html
[BDB-Download]: http://www.oracle.com/technetwork/database/database-technologies/berkeleydb/downloads/index.html
