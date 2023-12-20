
# bash set_delay.sh 10
# bash set_delay.sh 20 5 5 0 #40
# python3 run_experiments.py -e -c vcloud ycsb_thread
# sleep 30
# python3 run_experiments.py -e -c vcloud ycsb_skew
# sleep 30
python3 run_experiments.py -e -c vcloud ycsb_cross_dc
# bash set_delay.sh 20
# python3 run_experiments.py -e -c vcloud ycsb_cross_dc
# sleep 30
# bash set_delay.sh 40
# python3 run_experiments.py -e -c vcloud ycsb_cross_dc
# sleep 30
python3 run_experiments.py -e -c vcloud ycsb_tapir_cross_dc
# sleep 30
# python3 run_experiments.py -e -c vcloud ycsb_writes
# sleep 30
# python3 run_experiments.py -e -c vcloud ycsb_tapir_writes
# sleep 30
# bash testdelay.sh
# python3 run_experiments.py -e -c vcloud tpcc_scaling
# sleep 3
# python3 run_experiments.py -e -c vcloud tpcc_scaling_n
# python3 run_experiments.py -e -c vcloud ycsb_network_delay
# sleep 30
# python3 run_experiments.py -e -c vcloud ycsb_tapir_network_delay
# sleep 30

# neworder
#python3 run_experiments.py -e -c vcloud tpcc_stress1
#sleep 10

# payment
#python3 run_experiments.py -e -c vcloud tpcc_stress2
#sleep 10

# skew
# 0.0
#python3 run_experiments.py -e -c vcloud ycsb_stress1
#sleep 10
# 0.25
#python3 run_experiments.py -e -c vcloud ycsb_stress2
#sleep 10
# 0.55
#python3 run_experiments.py -e -c vcloud ycsb_stress3
#sleep 10
# 0.65
#python3 run_experiments.py -e -c vcloud ycsb_stress4
#sleep 10
# 0.75
#python3 run_experiments.py -e -c vcloud ycsb_stress5
#sleep 10
# 0.9
#python3 run_experiments.py -e -c vcloud ycsb_stress6
#sleep 10

# update ratio
# 0.0
#python3 run_experiments.py -e -c vcloud ycsb_stress7
#sleep 10
# 0.2
#python3 run_experiments.py -e -c vcloud ycsb_stress8
#sleep 10
# 0.4
#python3 run_experiments.py -e -c vcloud ycsb_stress9
#sleep 10
## 0.6
#python3 run_experiments.py -e -c vcloud ycsb_stress10
#sleep 10
# 0.8
#python3 run_experiments.py -e -c vcloud ycsb_stress11
#sleep 10
# 1.0
#python3 run_experiments.py -e -c vcloud ycsb_stress12
#sleep 10


#cd ../draw
#./deneva-homepage.sh
