while true
do
    if [[ -z "$(ps -aux | grep perf | grep report)" ]]
    then
        cd /home/FlameGraph
        perf script -i /home/perf.data | ./stackcollapse-perf.pl > out.perf-folded
        ./flamegraph.pl out.perf-folded > /home/perf.svg
        break
    fi
done
