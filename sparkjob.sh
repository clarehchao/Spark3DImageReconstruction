#!/usr/bin/env bash

Ntest=1
Niter=10
Npar=16
Nnode=16
classname=[FILLTHIS]
sdir=[FILLTHIS]
exemem=[FILLTHIS]

nohup ./bin/spark-submit --class $classname --master local[$Nnode] --executor-memory $exemem $sdir/target/scala-2.10/mlem-project_2.10-1.0.jar $Niter $Npar $sdir/data ${Ntest}_${Nnode}nodes > joblog_${Nnode}nodes_par${Npar}_iter${Niter}_test${Ntest}.txt &
echo "done with local[$Nnode]"
