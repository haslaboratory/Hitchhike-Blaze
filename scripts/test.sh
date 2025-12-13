#!/bin/bash
# sudo ~/blaze/build/bin/bfs -computeWorkers 16 -startNode 0 /home/zhengxd/mnt/nvme/sc22/rmat27.gr.index /home/zhengxd/mnt/nvme/sc22/rmat27.gr.adj.0 
# sudo ~/blaze/build/bin/bfs -computeWorkers 8 -startNode 0 /home/zhengxd/mnt/nvme1/sc22/rmat27.gr.index /home/zhengxd/mnt/nvme1/sc22/rmat27.gr.adj.0  
# sudo ~/blaze/build/bin/bfs -computeWorkers 16 -startNode 12 /home/zhengxd/mnt/nvme/sc22/twitter.gr.index /home/zhengxd/mnt/nvme/sc22/twitter.gr.adj.0  
# sudo ~/blaze/build/bin/bfs -computeWorkers 16 -startNode 50395005 /home/zhengxd/mnt/nvme/sc22/sk2005.gr.index /home/zhengxd/mnt/nvme/sc22/sk2005.gr.adj.0 
# sudo ~/blaze/build/bin/bfs -computeWorkers 16 -startNode 0 /home/zhengxd/mnt/nvme/sc22/uran27.gr.index /home/zhengxd/mnt/nvme/sc22/uran27.gr.adj.0 
# sudo ~/blaze/build/bin/bfs -computeWorkers 16 -startNode 0 /home/zhengxd/mnt/nvme2/sc22/rmat27.gr.index /home/zhengxd/mnt/nvme2/sc22/rmat27.gr.adj.0 
# sudo ~/blaze/build/bin/bfs -computeWorkers 16 -startNode 101 /home/zhengxd/mnt/nvme1/sc22/friendster.gr.index /home/zhengxd/mnt/nvme1/sc22/friendster.gr.adj.0 


#path
# SCRIPT_PATH=`realpath $0`
# BASE_DIR=`dirname $SCRIPT_PATH`

APP_PATH=/home/zhengxd/blaze/build/bin/
# INDEX=$HOME/dataset/mnt/nvme_haslab2
# DATA=$HOME/dataset/mnt/nvme_haslab2
INDEX=$HOME/dataset/mnt/nvme3
DATA=$HOME/dataset/mnt/nvme3
RESULT=$HOME/blaze-old/blaze/analysis/graph/

# parameter
COMPUTEWORKERS=14
STARTNODE=50395005
# 101
HIT=1
TIMES=1

# libaio arg
# declare -a depth=("16")
# declare -a depth=("1" "2" "4" "6" "8" "12" "16" "20" "24" "28" "32" "64" "128")

#hit arg
qd=1
# declare -a hitSize=("1" "2" "4" "8" "16" "32" "64" "96" "127" "3" "6" "10" "12" "14" "18" "20" "24" "28" "36" "40" "44" "48" "52" "56" "60")
declare -a hitSize=("1" "2" "4" "6" "8" "12" "16" "20" "24" "32" "48" "64" "96" "127")


# common arg
declare -a apps=("bfs")
declare -a index=("sk2005.gr.index")
declare -a data=("sk2005.gr.adj.0")
# declare -a index=("friendster.gr.index")
# declare -a data=("friendster.gr.adj.0")
#"rmat27.gr.index" "rmat30.gr.index" "uran27.gr.index" 
# declare -a apps=("bfs" "bc" "pagerank" "wcc" "spmv")


# libaio test
# for ((n=0; n<TIMES; n++)); do
#     for e in "${apps[@]}"; do
#         for qd in "${depth[@]}"; do
#             for ((i=0; i<${#index[@]}; i++)); do
#                 k="${index[i]}"
#                 j="${data[i]}"
#                 sudo $APP_PATH/${e} -computeWorkers $COMPUTEWORKERS -startNode $STARTNODE -queueDepth $qd -hit $HIT  $INDEX/${k} $DATA/${j} >> ${RESULT}/bfs_libaio_nomerge_4k_D${qd}.out
#                 # > breakdown_nomerge_qd128.out
#                 # >> ${RESULT}/bfs_libaio_nomerge_4k_D${qd}.out
#             done
#         done
#     done
# done

#hit test

for ((n=0; n<TIMES; n++)); do
    for h in "${hitSize[@]}"; do
        for e in "${apps[@]}"; do
            for ((i=0; i<${#index[@]}; i++)); do
                k="${index[i]}"
                j="${data[i]}"
                sudo $APP_PATH/${e} -computeWorkers $COMPUTEWORKERS -startNode $STARTNODE -hit $HIT -queueDepth $qd -hitSize ${h} $INDEX/${k} $DATA/${j} >> ${RESULT}/bfs_hit_H${h}_D${qd}.out
                # >> breakdown_hit${h}_qd1.out
                # >> ${RESULT}/bfs_hit_H${h}_D${qd}.out
                # > tag.out
                # >> ${RESULT}/bfs_hit_H${h}_D1.out
            done
        done
    done
done