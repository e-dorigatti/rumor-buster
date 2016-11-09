../../spark-1.6.2-bin-hadoop2.6/bin/spark-submit --master local[4] --packages TargetHolding:pyspark-elastic:0.4.2 compute_peaks.py  "$@"
