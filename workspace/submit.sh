spark-submit \
    --master local[6] \
    --num-executors 2 \
    --executor-memory 8G \
    --executor-cores 2 \
    --conf spark.yarn.am.waitTime=900000 \
    --conf spark.core.connection.ack.wait.timeout=600s \
    --conf spark.dynamicAllocation.enabled=false \
    --conf spark.default.parallelism=6 \
    ./app.py eventByUser