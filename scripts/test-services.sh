#!/usr/bin/env bash
# Loopback test services without Docker (Phase 14, 14-01b; D-P14-44/46).
#
# usage: scripts/test-services.sh start [s3] [kafka]   (no names = both)
#        scripts/test-services.sh env                  (export lines for eval / .ostro-test-env)
#        scripts/test-services.sh status
#        scripts/test-services.sh stop
#
# s3:    SeaweedFS `weed mini` (target/tools/weed-4.54, `scripts/fetch-tools.sh weed`) on
#        127.0.0.1 with access key `ostro` / secret `ostroseaweed` and the bucket `ostro-test`
#        (the ci.yml spec). Exports OSTRO_TEST_S3_URL / _ACCESS_KEY / _SECRET_KEY / _BUCKET.
# kafka: Apache Kafka 4.1 in KRaft combined mode with the pinned Temurin JRE
#        (`scripts/fetch-tools.sh kafka jre`), -Xmx512m, three client listeners on free
#        loopback ports: PLAINTEXT (OSTRO_TEST_KAFKA_BOOTSTRAP), SASL_PLAINTEXT with PLAIN,
#        SCRAM-SHA-256 and SCRAM-SHA-512 for user `ostro` and a random password
#        (OSTRO_TEST_KAFKA_SASL_BOOTSTRAP / _SASL_USER / _SASL_PASSWORD) and SSL with a fresh
#        per-start CA and server certificate (SAN IP:127.0.0.1, DNS:localhost; made with the
#        JRE's keytool) (OSTRO_TEST_KAFKA_TLS_BOOTSTRAP, OSTRO_TEST_KAFKA_TLS_CA = CA PEM).
#
# State lives under ${OSTRO_TEST_SERVICES_DIR:-target/test-services}; `stop` kills only the
# PIDs in its own pidfiles (never a pattern kill) and removes the service data. Never Docker.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STATE="${OSTRO_TEST_SERVICES_DIR:-$ROOT/target/test-services}"
TOOLS="$ROOT/target/tools"
WEED="$TOOLS/weed-4.54/weed"
KAFKA_HOME="$TOOLS/kafka-4.1.2"
JRE_HOME="$TOOLS/jre-21.0.13.1_2"

S3_KEY=ostro
S3_SECRET=ostroseaweed
S3_BUCKET=ostro-test
KAFKA_USER=ostro

die() { echo "test-services: $*" >&2; exit 1; }
log() { echo "test-services: $*" >&2; }

USED_PORTS=()

port_busy() { (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null; }

# alloc_port VAR: a free loopback port in 20000-49999 (nothing listening, not handed out
# before) into VAR (no subshell, so USED_PORTS is kept).
alloc_port() {
  local p tries=0 u taken
  while :; do
    tries=$((tries + 1))
    [ "$tries" -lt 500 ] || die "no free loopback port found"
    p=$((20000 + (RANDOM * 32768 + RANDOM) % 30000))
    taken=0
    for u in "${USED_PORTS[@]+"${USED_PORTS[@]}"}"; do
      if [ "$u" = "$p" ]; then taken=1; fi
    done
    [ "$taken" = 0 ] || continue
    if ! port_busy "$p"; then
      USED_PORTS+=("$p")
      printf -v "$1" '%s' "$p"
      return 0
    fi
  done
}

random_secret() { od -An -N16 -tx1 /dev/urandom | tr -d ' \n'; }

pid_alive() { [ -f "$1" ] && kill -0 "$(cat "$1")" 2>/dev/null; }

env_line() { printf 'export %s=%q\n' "$1" "$2" >>"$STATE/$3.env"; }

# ------------------------------------------------------------------------------- s3

# A SigV4-signed request (METHOD URL) with curl; curl < 7.86 signs without
# x-amz-content-sha256, which SeaweedFS rejects, so a python3 signer is the fallback.
s3_signed() { # method url -> 0 on a 2xx answer
  local method=$1 url=$2 head=()
  [ "$method" = HEAD ] && head=(-I)
  if curl -sf -o /dev/null "${head[@]}" -X "$method" --aws-sigv4 "aws:amz:us-east-1:s3" \
    --user "$S3_KEY:$S3_SECRET" -H "x-amz-content-sha256: UNSIGNED-PAYLOAD" "$url" 2>/dev/null; then
    return 0
  fi
  command -v python3 >/dev/null 2>&1 || return 1
  python3 -I -c "$SIGV4_PY" "$method" "$url" "$S3_KEY" "$S3_SECRET"
}

SIGV4_PY='
import datetime, hashlib, hmac, sys, urllib.error, urllib.parse, urllib.request
method, url, ak, sk = sys.argv[1:5]
u = urllib.parse.urlparse(url)
now = datetime.datetime.now(datetime.timezone.utc)
amz, day = now.strftime("%Y%m%dT%H%M%SZ"), now.strftime("%Y%m%d")
ph = hashlib.sha256(b"").hexdigest()
signed = "host;x-amz-content-sha256;x-amz-date"
canon = "\n".join([method, u.path or "/", "", "host:" + u.netloc, "x-amz-content-sha256:" + ph,
                   "x-amz-date:" + amz, "", signed, ph])
scope = day + "/us-east-1/s3/aws4_request"
sts = "\n".join(["AWS4-HMAC-SHA256", amz, scope, hashlib.sha256(canon.encode()).hexdigest()])
k = ("AWS4" + sk).encode()
for part in (day, "us-east-1", "s3", "aws4_request"):
    k = hmac.new(k, part.encode(), hashlib.sha256).digest()
sig = hmac.new(k, sts.encode(), hashlib.sha256).hexdigest()
auth = "AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s" % (ak, scope, signed, sig)
req = urllib.request.Request(url, method=method, headers={"x-amz-date": amz,
      "x-amz-content-sha256": ph, "Authorization": auth})
try:
    urllib.request.urlopen(req, timeout=5)
except Exception:
    sys.exit(1)
'

start_s3() {
  local dir="$STATE/s3" pidf="$STATE/s3.pid" url ready=0 i
  if pid_alive "$pidf"; then
    log "s3 already running (pid $(cat "$pidf"))"
    return 0
  fi
  [ -x "$WEED" ] || die "missing $WEED (run: bash scripts/fetch-tools.sh weed)"
  rm -rf "$dir" "$STATE/s3.env"
  mkdir -p "$dir/data"
  local v s3p s3g master masterg volume volumeg filer filerg admin adming webdav vector
  for v in s3p s3g master masterg volume volumeg filer filerg admin adming webdav vector; do
    alloc_port "$v"
  done
  # gRPC ports (+10000 by default) are given explicitly so they are free too.
  AWS_ACCESS_KEY_ID="$S3_KEY" AWS_SECRET_ACCESS_KEY="$S3_SECRET" \
    nohup "$WEED" mini -dir="$dir/data" -ip=127.0.0.1 -ip.bind=127.0.0.1 \
    -s3.port="$s3p" -s3.port.grpc="$s3g" \
    -master.port="$master" -master.port.grpc="$masterg" \
    -volume.port="$volume" -volume.port.grpc="$volumeg" \
    -filer.port="$filer" -filer.port.grpc="$filerg" \
    -admin.port="$admin" -admin.port.grpc="$adming" -admin.port.legacy=0 -admin.ui=false \
    -webdav=false -webdav.port="$webdav" -s3.port.iceberg=0 -s3.port.lance=0 \
    -vector.port="$vector" -bucket="$S3_BUCKET" \
    >"$dir/weed.log" 2>&1 &
  echo $! >"$pidf"
  url="http://127.0.0.1:$s3p"
  # Readiness: the bucket answers a signed HEAD (the ci.yml logic); else create it.
  for i in $(seq 1 90); do
    if s3_signed HEAD "$url/$S3_BUCKET"; then
      ready=1
      break
    fi
    pid_alive "$pidf" || { tail -n 40 "$dir/weed.log" >&2; die "weed mini exited"; }
    sleep 1
  done
  if [ "$ready" != 1 ]; then
    s3_signed PUT "$url/$S3_BUCKET" || true
    s3_signed HEAD "$url/$S3_BUCKET" ||
      { tail -n 40 "$dir/weed.log" >&2; die "s3 not ready after 90 s"; }
  fi
  env_line OSTRO_TEST_S3_URL "$url" s3
  env_line OSTRO_TEST_S3_ACCESS_KEY "$S3_KEY" s3
  env_line OSTRO_TEST_S3_SECRET_KEY "$S3_SECRET" s3
  env_line OSTRO_TEST_S3_BUCKET "$S3_BUCKET" s3
  log "s3 ready at $url (bucket $S3_BUCKET, pid $(cat "$pidf"), ${i}s)"
}

# ---------------------------------------------------------------------------- kafka

keytool_() { "$JRE_HOME/bin/keytool" "$@"; }

make_tls() { # dir storepass
  local d=$1 pw=$2 san="SAN=IP:127.0.0.1,DNS:localhost"
  mkdir -p "$d"
  keytool_ -genkeypair -alias ca -keyalg RSA -keysize 2048 -dname "CN=ostro-test-kafka-ca" \
    -ext bc:c -ext KU=keyCertSign,cRLSign -validity 30 -keystore "$d/ca.p12" \
    -storetype PKCS12 -storepass "$pw" >/dev/null 2>&1
  keytool_ -exportcert -alias ca -keystore "$d/ca.p12" -storepass "$pw" -rfc \
    -file "$d/ca.pem" >/dev/null 2>&1
  keytool_ -genkeypair -alias server -keyalg RSA -keysize 2048 -dname "CN=localhost" \
    -ext "$san" -validity 30 -keystore "$d/server.p12" -storetype PKCS12 \
    -storepass "$pw" >/dev/null 2>&1
  keytool_ -certreq -alias server -keystore "$d/server.p12" -storepass "$pw" \
    -file "$d/server.csr" >/dev/null 2>&1
  keytool_ -gencert -alias ca -keystore "$d/ca.p12" -storepass "$pw" -infile "$d/server.csr" \
    -outfile "$d/server.pem" -rfc -validity 30 -ext "$san" \
    -ext KU=digitalSignature,keyEncipherment -ext EKU=serverAuth >/dev/null 2>&1
  keytool_ -importcert -alias ca -keystore "$d/server.p12" -storepass "$pw" -file "$d/ca.pem" \
    -noprompt >/dev/null 2>&1
  keytool_ -importcert -alias server -keystore "$d/server.p12" -storepass "$pw" \
    -file "$d/server.pem" -noprompt >/dev/null 2>&1
  # A client truststore for the JVM CLI tools (readiness checks, local listener checks).
  keytool_ -importcert -alias ca -keystore "$d/client-trust.p12" -storetype PKCS12 \
    -storepass "$pw" -file "$d/ca.pem" -noprompt >/dev/null 2>&1
  [ -s "$d/ca.pem" ] && [ -s "$d/server.p12" ] || die "keytool did not produce the TLS files"
}

start_kafka() {
  local dir="$STATE/kafka" pidf="$STATE/kafka.pid" plain sasl ssl ctrl pw tlspw cid i
  if pid_alive "$pidf"; then
    log "kafka already running (pid $(cat "$pidf"))"
    return 0
  fi
  [ -x "$KAFKA_HOME/bin/kafka-server-start.sh" ] ||
    die "missing $KAFKA_HOME (run: bash scripts/fetch-tools.sh kafka jre)"
  [ -x "$JRE_HOME/bin/java" ] || die "missing $JRE_HOME (run: bash scripts/fetch-tools.sh jre)"
  rm -rf "$dir" "$STATE/kafka.env"
  mkdir -p "$dir/data" "$dir/logs"
  alloc_port plain
  alloc_port sasl
  alloc_port ssl
  alloc_port ctrl
  pw=$(random_secret)
  tlspw=$(random_secret)
  make_tls "$dir/tls" "$tlspw"
  cat >"$dir/server.properties" <<EOF
process.roles=broker,controller
node.id=1
controller.quorum.bootstrap.servers=127.0.0.1:$ctrl
listeners=PLAINTEXT://127.0.0.1:$plain,SASL_PLAINTEXT://127.0.0.1:$sasl,SSL://127.0.0.1:$ssl,CONTROLLER://127.0.0.1:$ctrl
advertised.listeners=PLAINTEXT://127.0.0.1:$plain,SASL_PLAINTEXT://127.0.0.1:$sasl,SSL://127.0.0.1:$ssl
controller.listener.names=CONTROLLER
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,SASL_PLAINTEXT:SASL_PLAINTEXT,SSL:SSL
inter.broker.listener.name=PLAINTEXT
log.dirs=$dir/data
num.partitions=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
share.coordinator.state.topic.replication.factor=1
share.coordinator.state.topic.min.isr=1
group.initial.rebalance.delay.ms=0
auto.create.topics.enable=true
sasl.enabled.mechanisms=PLAIN,SCRAM-SHA-256,SCRAM-SHA-512
listener.name.sasl_plaintext.plain.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="$KAFKA_USER" password="$pw" user_$KAFKA_USER="$pw";
listener.name.sasl_plaintext.scram-sha-256.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required;
listener.name.sasl_plaintext.scram-sha-512.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required;
ssl.keystore.type=PKCS12
ssl.keystore.location=$dir/tls/server.p12
ssl.keystore.password=$tlspw
ssl.key.password=$tlspw
ssl.client.auth=none
EOF
  chmod 600 "$dir/server.properties"
  # Client properties for the JVM tools (local listener checks).
  printf 'security.protocol=SASL_PLAINTEXT\nsasl.mechanism=SCRAM-SHA-512\nsasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="%s" password="%s";\n' \
    "$KAFKA_USER" "$pw" >"$dir/client-scram512.properties"
  printf 'security.protocol=SASL_PLAINTEXT\nsasl.mechanism=SCRAM-SHA-256\nsasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="%s" password="%s";\n' \
    "$KAFKA_USER" "$pw" >"$dir/client-scram256.properties"
  printf 'security.protocol=SASL_PLAINTEXT\nsasl.mechanism=PLAIN\nsasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="%s" password="%s";\n' \
    "$KAFKA_USER" "$pw" >"$dir/client-plain.properties"
  printf 'security.protocol=SSL\nssl.truststore.type=PKCS12\nssl.truststore.location=%s\nssl.truststore.password=%s\n' \
    "$dir/tls/client-trust.p12" "$tlspw" >"$dir/client-ssl.properties"
  chmod 600 "$dir"/client-*.properties
  export JAVA_HOME="$JRE_HOME"
  export KAFKA_HEAP_OPTS="-Xmx512m -Xms128m"
  export LOG_DIR="$dir/logs"
  cid=$("$KAFKA_HOME/bin/kafka-storage.sh" random-uuid 2>/dev/null)
  "$KAFKA_HOME/bin/kafka-storage.sh" format --standalone -t "$cid" -c "$dir/server.properties" \
    --add-scram "SCRAM-SHA-256=[name=$KAFKA_USER,password=$pw]" \
    --add-scram "SCRAM-SHA-512=[name=$KAFKA_USER,password=$pw]" >"$dir/format.log" 2>&1 ||
    { cat "$dir/format.log" >&2; die "kafka-storage format failed"; }
  # kafka-run-class.sh execs java: $! is the broker's PID.
  nohup "$KAFKA_HOME/bin/kafka-server-start.sh" "$dir/server.properties" \
    >"$dir/logs/stdout.log" 2>&1 &
  echo $! >"$pidf"
  for i in $(seq 1 90); do
    if "$KAFKA_HOME/bin/kafka-broker-api-versions.sh" --bootstrap-server "127.0.0.1:$plain" \
      >/dev/null 2>&1; then
      break
    fi
    pid_alive "$pidf" || { tail -n 40 "$dir/logs/stdout.log" >&2; die "kafka exited"; }
    if [ "$i" = 90 ]; then
      tail -n 40 "$dir/logs/stdout.log" >&2
      die "kafka not ready on 127.0.0.1:$plain after 90 s"
    fi
    sleep 1
  done
  env_line OSTRO_TEST_KAFKA_BOOTSTRAP "127.0.0.1:$plain" kafka
  env_line OSTRO_TEST_KAFKA_SASL_BOOTSTRAP "127.0.0.1:$sasl" kafka
  env_line OSTRO_TEST_KAFKA_SASL_USER "$KAFKA_USER" kafka
  env_line OSTRO_TEST_KAFKA_SASL_PASSWORD "$pw" kafka
  env_line OSTRO_TEST_KAFKA_TLS_BOOTSTRAP "127.0.0.1:$ssl" kafka
  env_line OSTRO_TEST_KAFKA_TLS_CA "$dir/tls/ca.pem" kafka
  chmod 600 "$STATE/kafka.env"
  log "kafka ready: PLAINTEXT 127.0.0.1:$plain, SASL_PLAINTEXT 127.0.0.1:$sasl, SSL 127.0.0.1:$ssl (pid $(cat "$pidf"), ${i}s)"
}

# ---------------------------------------------------------------------------- verbs

stop_one() { # name
  local pidf="$STATE/$1.pid" pid i
  if [ -f "$pidf" ]; then
    pid=$(cat "$pidf")
    if kill -0 "$pid" 2>/dev/null; then
      kill -TERM "$pid" 2>/dev/null || true
      for i in $(seq 1 30); do
        kill -0 "$pid" 2>/dev/null || break
        sleep 1
      done
      if kill -0 "$pid" 2>/dev/null; then
        log "$1 (pid $pid) did not stop after 30 s; SIGKILL"
        kill -KILL "$pid" 2>/dev/null || true
      fi
    fi
    rm -f "$pidf"
    log "$1 stopped"
  fi
  rm -rf "${STATE:?}/$1" "$STATE/$1.env"
}

cmd_start() {
  mkdir -p "$STATE"
  local want=("$@") s
  [ ${#want[@]} -gt 0 ] || want=(s3 kafka)
  for s in "${want[@]}"; do
    case $s in
      s3) start_s3 ;;
      kafka) start_kafka ;;
      *) die "unknown service $s (s3, kafka)" ;;
    esac
  done
}

cmd_env() {
  local s
  for s in s3 kafka; do
    if [ -f "$STATE/$s.env" ] && pid_alive "$STATE/$s.pid"; then
      cat "$STATE/$s.env"
    fi
  done
}

cmd_status() {
  local s
  for s in s3 kafka; do
    if pid_alive "$STATE/$s.pid"; then
      echo "$s: running (pid $(cat "$STATE/$s.pid"))"
    else
      echo "$s: stopped"
    fi
  done
}

cmd_stop() {
  stop_one kafka
  stop_one s3
}

case "${1:-}" in
  start) shift; cmd_start "$@" ;;
  env) cmd_env ;;
  status) cmd_status ;;
  stop) cmd_stop ;;
  *) die "usage: $0 {start [s3] [kafka] | env | stop | status}" ;;
esac
