package opentsdbclient

import (
	"testing"
)

func TestOpenTSDBUIDMetaQuery(t *testing.T) {
	client := NewOpenTSDBClient("ec2-3-88-64-48.compute-1.amazonaws.com:4242")
	resp := client.UIDMetaData("name:%22test4-K1.t1c%22")
	if 1 != len(resp) {
		t.Fatalf("1 != %d", len(resp))
	}
}

func TestOpenTSDBTSDMetaQuery(t *testing.T) {
	client := NewOpenTSDBClient("ec2-3-88-64-48.compute-1.amazonaws.com:4242")
	resp := client.TSMetaData("tsuid:/((.)%7B6%7D)%2B((.)%7B12%7D)*000021.*/")
	if 12 != len(resp) {
		t.Fatalf("12 != %d", len(resp))
	}
}

func TestOpenTSDBUIDMetaLookup(t *testing.T) {
	client := NewOpenTSDBClient("ec2-3-88-64-48.compute-1.amazonaws.com:4242")
	resp := client.UIDMetaLookup("tagv", "000264")
	if resp.UID != "000264" {
		t.Fatalf("%s != %s", "000264", resp.UID)
	}
}
