check-sort-result:
	gsort cmd/sort/tmp/res.txt -n --check

benchmark-test:
	GOMEMLIMIT=500MiB && go test -run=cmd/sort/. -bench=. -benchtime=1x
