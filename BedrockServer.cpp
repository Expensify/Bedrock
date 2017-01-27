 test:$  ./test -only Write
Temp file for this run: bedrocktest_8Sy1Ks.db
[--------------]
[ RUN          ] WriteTest::insert
[       PASSED ] WriteTest::insert
[ RUN          ] WriteTest::parallelInsert
Testing with 50 commands.
worker0 successfully committed: parallelCommand#0
worker2 conflict committing: parallelCommand#1
worker4 conflict committing: parallelCommand#3
worker1 conflict committing: parallelCommand#2
worker5 conflict committing: parallelCommand#4
worker1 successfully committed: parallelCommand#2
worker6 conflict committing: parallelCommand#7
worker7 conflict committing: parallelCommand#5
worker0 conflict committing: parallelCommand#8
worker2 conflict committing: parallelCommand#1
worker2 conflict limit reached, giving to sync thread: parallelCommand#1
worker4 conflict committing: parallelCommand#3
worker4 conflict limit reached, giving to sync thread: parallelCommand#3
worker3 conflict committing: parallelCommand#6
worker5 conflict committing: parallelCommand#4
worker5 conflict limit reached, giving to sync thread: parallelCommand#4
worker3 successfully committed: parallelCommand#6
worker6 conflict committing: parallelCommand#7
worker6 conflict limit reached, giving to sync thread: parallelCommand#7
worker7 conflict committing: parallelCommand#5
worker7 conflict limit reached, giving to sync thread: parallelCommand#5
worker0 conflict committing: parallelCommand#8
worker0 conflict limit reached, giving to sync thread: parallelCommand#8
worker2 conflict committing: parallelCommand#10
Sync thread conflict committing: parallelCommand#1
Sync thread successfully committed: parallelCommand#1
worker1 conflict committing: parallelCommand#9
worker5 conflict committing: parallelCommand#12
worker2 conflict committing: parallelCommand#10
worker2 conflict limit reached, giving to sync thread: parallelCommand#10
worker4 conflict committing: parallelCommand#11
worker1 successfully committed: parallelCommand#9
worker6 conflict committing: parallelCommand#13
worker6 successfully committed: parallelCommand#13
worker4 conflict committing: parallelCommand#11
worker4 conflict limit reached, giving to sync thread: parallelCommand#11
Sync thread conflict committing: parallelCommand#3
worker7 conflict committing: parallelCommand#14
worker5 conflict committing: parallelCommand#12
worker5 conflict limit reached, giving to sync thread: parallelCommand#12
worker3 successfully committed: parallelCommand#15
worker7 conflict committing: parallelCommand#14
worker7 conflict limit reached, giving to sync thread: parallelCommand#14
Sync thread conflict committing: parallelCommand#4
worker3 successfully committed: parallelCommand#16
worker0 successfully committed: parallelCommand#17
Sync thread successfully committed: parallelCommand#7
worker3 conflict committing: parallelCommand#18
Sync thread successfully committed: parallelCommand#5
worker3 conflict committing: parallelCommand#18
worker3 conflict limit reached, giving to sync thread: parallelCommand#18
Sync thread successfully committed: parallelCommand#8
Sync thread successfully committed: parallelCommand#3
Sync thread successfully committed: parallelCommand#4
worker1 successfully committed: parallelCommand#20
worker3 conflict committing: parallelCommand#19
worker4 conflict committing: parallelCommand#22
worker0 successfully committed: parallelCommand#23
worker7 conflict committing: parallelCommand#24
worker5 conflict committing: parallelCommand#21
worker3 conflict committing: parallelCommand#19
worker3 conflict limit reached, giving to sync thread: parallelCommand#19
worker4 conflict committing: parallelCommand#22
worker4 conflict limit reached, giving to sync thread: parallelCommand#22
Sync thread conflict committing: parallelCommand#10
worker1 successfully committed: parallelCommand#25
worker7 conflict committing: parallelCommand#24
worker7 conflict limit reached, giving to sync thread: parallelCommand#24
worker5 conflict committing: parallelCommand#21
worker5 conflict limit reached, giving to sync thread: parallelCommand#21
worker3 successfully committed: parallelCommand#26
Sync thread conflict committing: parallelCommand#11
worker6 successfully committed: parallelCommand#27
worker3 successfully committed: parallelCommand#28
Sync thread conflict committing: parallelCommand#12
worker1 successfully committed: parallelCommand#29
worker4 successfully committed: parallelCommand#30
Sync thread conflict committing: parallelCommand#14
worker5 successfully committed: parallelCommand#31
worker3 successfully committed: parallelCommand#32
Sync thread conflict committing: parallelCommand#18
worker6 successfully committed: parallelCommand#33
worker1 successfully committed: parallelCommand#34
Sync thread conflict committing: parallelCommand#10
worker0 successfully committed: parallelCommand#35
worker2 successfully committed: parallelCommand#36
Sync thread conflict committing: parallelCommand#11
worker6 successfully committed: parallelCommand#37
worker3 successfully committed: parallelCommand#38
worker7 successfully committed: parallelCommand#39
Sync thread conflict committing: parallelCommand#12
worker5 successfully committed: parallelCommand#40
worker4 successfully committed: parallelCommand#41
Sync thread conflict committing: parallelCommand#14
worker6 successfully committed: parallelCommand#42
worker5 successfully committed: parallelCommand#43
Sync thread conflict committing: parallelCommand#18
worker6 successfully committed: parallelCommand#44
worker1 successfully committed: parallelCommand#45
Sync thread conflict committing: parallelCommand#10
worker1 successfully committed: parallelCommand#46
worker2 successfully committed: parallelCommand#47
Sync thread conflict committing: parallelCommand#11
worker0 successfully committed: parallelCommand#48
Sync thread conflict committing: parallelCommand#12
worker2 successfully committed: parallelCommand#49
Sync thread successfully committed: parallelCommand#14
Sync thread successfully committed: parallelCommand#18
Sync thread successfully committed: parallelCommand#10
Sync thread successfully committed: parallelCommand#11
Sync thread successfully committed: parallelCommand#12
Sync thread successfully committed: parallelCommand#19
Sync thread successfully committed: parallelCommand#22
Sync thread successfully committed: parallelCommand#24
Sync thread successfully committed: parallelCommand#21
[       PASSED ] WriteTest::parallelInsert
[--------------]

[==============]
[ TEST RESULTS ] Passed: 2, Failed: 0
[==============]
 test:$
