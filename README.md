**Key Value Store**

*Description*

This is implementation of key value store as per defined requirements below.
The objective of this task is to implement a network-available persistent Key/Value system that
exposes the interfaces listed below in any programming language of choice. You should rely
only on the standard libraries of the language.
1. Put(Key, Value)
2. Read(Key)
3. ReadKeyRange(StartKey, EndKey)
4. BatchPut(..keys, ..values)
5. Delete(key)


The implementation should strive to achieve the following requirements,
1. Low latency per item read or written
2. High throughput, especially when writing an incoming stream of random items
3. Ability to handle datasets much larger than RAM w/o degradation
4. Crash friendliness, both in terms of fast recovery and not losing data
5. Predictable behavior under heavy access load or large volume

You are at liberty to make any trade off to achieve these objectives.

Bonus points
1. Replicate data to multiple nodes
2. Handle automatic failover to the other nodes


**How to build**

mvn clean install

This command clean the target folder, build and generate executable jar.

**How to run**

java -jar <location of jar file> (target/kvstore-1.0-SNAPSHOT.jar)

jar file can be located inside target folder post build.

**How to test after running above execution command**

nc localhost 9191
PUT <key_name> <value>

nc localhost 9191
GET <key_name>

nc localhost 9191
DEL <key_name>

nc localhost 9191
GET <key_name>

nc localhost 9191
BATCH 2
<key_name> <value>
<key_name> <value>
