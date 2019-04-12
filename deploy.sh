#!/bin/sh
#scp -i ~/.ssh/id_rsa.pub nn02.hdp02.stdp.ru:pointsaggregator.py ~/spark-jobs
scp -i ~/.ssh/id_rsa.pub pointsaggregator.py nn02.hdp02.stdp.ru:~/spark-jobs


#ssh nn02.hdp02.stdp.ru 'spark-submit ~/spark-jobs/pointsaggregator.py --master yarn --deploy-mode client'
ssh nn02.hdp02.stdp.ru 'spark-submit ~/spark-jobs/pointsaggregator.py --directory hdfs:///user/dsuser1/history/MSSQL_DATA/1c_data/utc_union_points/2018/*/*.orc --environment cluster'
ssh nn02.hdp02.stdp.ru 'spark-submit ~/spark-jobs/pointsaggregator.py --directory hdfs:///user/dsuser1/history/MSSQL_DATA/1c_data/utc_union_points/*/*/*.orc --environment cluster --out-directory hdfs:///user/analyst/Data_en/point_aggs --out-filename point_aggs.orc'