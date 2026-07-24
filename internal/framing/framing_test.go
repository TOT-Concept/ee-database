package framing

import (
	"encoding/json"
	"testing"
)

func TestDecodeBatch(t *testing.T) {
	raw := []byte(`{"type":"batch","deltas":[{"id":7,"kind":"data","op":"upsert",` +
		`"entity_type":"Company","revision":3,"sql":"INSERT INTO x VALUES (1);"}],"has_more":true}`)
	frame, err := Decode(raw)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	batch, ok := frame.(Batch)
	if !ok {
		t.Fatalf("expected Batch, got %T", frame)
	}
	if len(batch.Deltas) != 1 || batch.Deltas[0].ID != 7 || !batch.HasMore {
		t.Fatalf("unexpected batch: %+v", batch)
	}
	if *batch.Deltas[0].EntityType != "Company" || *batch.Deltas[0].Revision != 3 {
		t.Fatalf("unexpected delta fields: %+v", batch.Deltas[0])
	}
}

func TestDecodeNullableDeltaFields(t *testing.T) {
	raw := []byte(`{"type":"batch","deltas":[{"id":1,"kind":"schema","op":"migrate",` +
		`"entity_type":null,"revision":null,"sql":"CREATE TABLE y ();"}],"has_more":false}`)
	frame, err := Decode(raw)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	batch := frame.(Batch)
	if batch.Deltas[0].EntityType != nil || batch.Deltas[0].Revision != nil {
		t.Fatalf("expected nil nullable fields: %+v", batch.Deltas[0])
	}
}

func TestDecodeUnknownType(t *testing.T) {
	if _, err := Decode([]byte(`{"type":"mystery"}`)); err == nil {
		t.Fatal("expected error for unknown frame type")
	}
}

func TestAckRoundTrip(t *testing.T) {
	raw, err := json.Marshal(Ack{Type: "ack", UpToID: 42})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	want := `{"type":"ack","up_to_id":42}`
	if string(raw) != want {
		t.Fatalf("got %s, want %s", raw, want)
	}
}
