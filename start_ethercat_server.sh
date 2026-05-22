#! /bin/bash

## kill the old server if already running
## this line should not do anything in the normal circumstances
## TODO: make sure that the server never stays on
pkill "server"

set -u

PRIO=80
CORE=12

## start the grpc server
cd $HOME/dev/poulpe_ethercat_controller
#cargo build --all-targets --release
RUST_LOG=info RUST_BACKTRACE=1 ./target/release/server $1 &
SRV=$!

echo "server PID is ${SRV}"

for _ in $(seq 1 50); do 
	tid=$(ps -L -o tid,comm --no-headers -p "$SRV" 2>/dev/null \
	| awk -v p="server" 'tolower($2) ~ tolower(p) {print $1; exit}')
	[[ -n ${tid:-} ]] && break
	sleep 0.1
done


if [[ -n ${tid:-} ]]; then
	echo "tid is ${tid}.. setting chrt and taskset"
	chrt -f -p "$PRIO" "$tid" && echo "setting $tid to $PRIO priority"
	taskset -pc "$CORE" "$tid" && echo "setting $tid to core $CORE"
else
	echo "tid not found. server left at default prio"
fi

wait "$SRV"

