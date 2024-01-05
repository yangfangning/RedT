python3 run_experiments.py -e --dc 3 -c vcloud ycsb_writes1 -l 20 0
sleep 10
python3 run_experiments.py -e --dc 3 -c vcloud ycsb_skew1 -l 20 0
sleep 10
python3 run_experiments.py -e --dc 3 -c vcloud ycsb_hot1 -l 20 0
sleep 10
python3 run_experiments.py -e --dc 3 -c vcloud tpcc_neworder_payment1 -l 20 0
sleep 10
python3 run_experiments.py -e --dc 3 -c vcloud ycsb_cross_dc1 -l 20 0
sleep 10
python3 run_experiments.py -e --dc 3 -c vcloud tpcc_cross_dc1 -l 20 0
sleep 10
python3 run_experiments.py -e --dc 3 -c vcloud tpcc_wh1 -l 20 0
sleep 10