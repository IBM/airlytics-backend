#!/bin/bash

oc_ns_pod_usage () {
    # show pod usage for cpu/mem
    ns="$1"
    printf "$ns\n"
    separator=$(printf '=%.0s' {1..50})
    printf "$separator\n"
    output=$(join -a1 -a2 -o 0,1.2,1.3,2.2,2.3,2.4,2.5, -e '<none>' \
        <(kubectl top pods -n $ns) \
        <(kubectl get -n $ns pods -o custom-columns=NAME:.metadata.name,"CPU_REQ(cores)":.spec.containers[*].resources.requests.cpu,"MEMORY_REQ(bytes)":.spec.containers[*].resources.requests.memory,"CPU_LIM(cores)":.spec.containers[*].resources.limits.cpu,"MEMORY_LIM(bytes)":.spec.containers[*].resources.limits.memory))
    totals=$(printf "%s" "$output" | awk '{s+=$2; t+=$3; u+=$4; v+=$5; w+=$6; x+=$7} END {print s" "t" "u" "v" "w" "x}')
    printf "%s\n%s\nTotals: %s\n" "$output" "$separator" "$totals" | column -t -s' '
    printf "$separator\n"
}

oc_ns_pod_usage $1