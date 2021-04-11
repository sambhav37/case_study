$SPARK_HOME/bin/spark-submit \
--master yarn \
--packages 'IF NOT AVAILABLE ON CLUSTER' \
--repositories 'CUSTOEM REPOSITORIES' \
--py-files packages.zip \
--files configs/case_study_config.json \
jobs/Analysis.py