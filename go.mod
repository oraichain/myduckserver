module github.com/apecloud/myduckserver

go 1.22.7

require (
	github.com/Shopify/toxiproxy/v2 v2.9.0
	github.com/apache/arrow/go/v17 v17.0.0
	github.com/cockroachdb/apd/v3 v3.2.1
	github.com/dolthub/go-mysql-server v0.18.2-0.20241016193930-58d51b356103
	github.com/dolthub/vitess v0.0.0-20241016191424-d14e107a654e
	github.com/go-sql-driver/mysql v1.8.1
	github.com/jmoiron/sqlx v1.4.0
	github.com/marcboeker/go-duckdb v1.8.2-0.20241002112231-62d5fa8c0697
	github.com/prometheus/client_golang v1.20.3
	github.com/rs/zerolog v1.33.0
	github.com/shopspring/decimal v1.3.1
	github.com/sirupsen/logrus v1.8.1
	github.com/stretchr/testify v1.9.0
	golang.org/x/text v0.18.0
	gopkg.in/src-d/go-errors.v1 v1.0.0
	vitess.io/vitess v0.20.2
)

replace (
	github.com/dolthub/go-mysql-server v0.18.2-0.20241016193930-58d51b356103 => github.com/fanyang01/go-mysql-server v0.0.0-20241017031253-bef4d25c51a3
	github.com/dolthub/vitess v0.0.0-20241016191424-d14e107a654e => github.com/apecloud/dolt-vitess v0.0.0-20241017031156-06988c627a21
)

require (
	filippo.io/edwards25519 v1.1.0 // indirect
	github.com/AdaLogics/go-fuzz-headers v0.0.0-20230811130428-ced1acdcaa24 // indirect
	github.com/DATA-DOG/go-sqlmock v1.5.2 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/dolthub/flatbuffers/v23 v23.3.3-dh.2 // indirect
	github.com/dolthub/go-icu-regex v0.0.0-20240916130659-0118adc6b662 // indirect
	github.com/dolthub/jsonpath v0.0.2-0.20240227200619-19675ab05c71 // indirect
	github.com/dolthub/sqllogictest/go v0.0.0-20240618184124-ca47f9354216 // indirect
	github.com/go-kit/kit v0.10.0 // indirect
	github.com/goccy/go-json v0.10.3 // indirect
	github.com/gocraft/dbr/v2 v2.7.2 // indirect
	github.com/golang/glog v1.2.2 // indirect
	github.com/google/flatbuffers v24.3.25+incompatible // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/gorilla/mux v1.8.1 // indirect
	github.com/hashicorp/golang-lru v1.0.2 // indirect
	github.com/klauspost/compress v1.17.9 // indirect
	github.com/klauspost/cpuid/v2 v2.2.8 // indirect
	github.com/lestrrat-go/strftime v1.0.4 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/mitchellh/mapstructure v1.5.0 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/pierrec/lz4/v4 v4.1.21 // indirect
	github.com/pires/go-proxyproto v0.7.0 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/prometheus/client_model v0.6.1 // indirect
	github.com/prometheus/common v0.59.1 // indirect
	github.com/prometheus/procfs v0.15.1 // indirect
	github.com/rs/xid v1.5.0 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	github.com/tetratelabs/wazero v1.1.0 // indirect
	github.com/zeebo/xxh3 v1.0.2 // indirect
	go.opentelemetry.io/otel v1.30.0 // indirect
	go.opentelemetry.io/otel/trace v1.30.0 // indirect
	golang.org/x/exp v0.0.0-20240909161429-701f63a606c0 // indirect
	golang.org/x/mod v0.21.0 // indirect
	golang.org/x/sync v0.8.0 // indirect
	golang.org/x/sys v0.25.0 // indirect
	golang.org/x/tools v0.25.0 // indirect
	golang.org/x/xerrors v0.0.0-20240903120638-7835f813f4da // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20240903143218-8af14fe29dc1 // indirect
	google.golang.org/grpc v1.66.2 // indirect
	google.golang.org/protobuf v1.34.2 // indirect
	gopkg.in/tomb.v1 v1.0.0-20141024135613-dd632973f1e7 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
