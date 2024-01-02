python3 run_experiments.py -e --dc 3 -c vcloud ycsb_thread -l 20 0
sleep 10
python3 run_experiments.py -e --dc 3 -c vcloud ycsb_skew -l 20 0
sleep 10
python3 run_experiments.py -e --dc 3 -c vcloud ycsb_thread -l 20 0
sleep 10
python3 run_experiments.py -e --dc 3 -c vcloud ycsb_skew -l 20 0
sleep 10

