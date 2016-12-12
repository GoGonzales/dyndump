// Copyright 2016 Gareth Watts
// Licensed under an MIT license
// See the LICENSE file for details

package dyndump

import (
	"bytes"
	"reflect"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

var attrTests = []struct {
	name     string
	src      *dynamodb.AttributeValue
	expected string
}{
	{"bytes", &dynamodb.AttributeValue{B: []byte("foo")}, `{"k":{"B":"Zm9v","BOOL":null,"BS":null,"L":null,"M":null,"N":null,"NS":null,"NULL":null,"S":null,"SS":null}}`},
	{"bool", &dynamodb.AttributeValue{BOOL: aws.Bool(true)}, `{"k":{"B":null,"BOOL":true,"BS":null,"L":null,"M":null,"N":null,"NS":null,"NULL":null,"S":null,"SS":null}}`},
	{"binary-set", &dynamodb.AttributeValue{BS: [][]byte{[]byte("foo"), []byte("bar")}}, `{"k":{"B":null,"BOOL":null,"BS":["Zm9v","YmFy"],"L":null,"M":null,"N":null,"NS":null,"NULL":null,"S":null,"SS":null}}`},
	{"attr-list", &dynamodb.AttributeValue{L: []*dynamodb.AttributeValue{
		{S: aws.String("str")},
		{BS: [][]byte{[]byte("foo"), []byte("bar")}},
	}}, `{"k":{"B":null,"BOOL":null,"BS":null,"L":[{"B":null,"BOOL":null,"BS":null,"L":null,"M":null,"N":null,"NS":null,"NULL":null,"S":"str","SS":null},{"B":null,"BOOL":null,"BS":["Zm9v","YmFy"],"L":null,"M":null,"N":null,"NS":null,"NULL":null,"S":null,"SS":null}],"M":null,"N":null,"NS":null,"NULL":null,"S":null,"SS":null}}`},
	{"attr-map", &dynamodb.AttributeValue{M: map[string]*dynamodb.AttributeValue{
		"key1": &dynamodb.AttributeValue{S: aws.String("str")},
		"key2": &dynamodb.AttributeValue{BS: [][]byte{[]byte("foo"), []byte("bar")}},
	}}, `{"k":{"B":null,"BOOL":null,"BS":null,"L":null,"M":{"key1":{"B":null,"BOOL":null,"BS":null,"L":null,"M":null,"N":null,"NS":null,"NULL":null,"S":"str","SS":null},"key2":{"B":null,"BOOL":null,"BS":["Zm9v","YmFy"],"L":null,"M":null,"N":null,"NS":null,"NULL":null,"S":null,"SS":null}},"N":null,"NS":null,"NULL":null,"S":null,"SS":null}}`},
	{"number", &dynamodb.AttributeValue{N: aws.String("123.456")}, `{"k":{"B":null,"BOOL":null,"BS":null,"L":null,"M":null,"N":"123.456","NS":null,"NULL":null,"S":null,"SS":null}}`},
	{"number-set", &dynamodb.AttributeValue{NS: []*string{aws.String("123"), aws.String("456")}}, `{"k":{"B":null,"BOOL":null,"BS":null,"L":null,"M":null,"N":null,"NS":["123","456"],"NULL":null,"S":null,"SS":null}}`},
	{"null", &dynamodb.AttributeValue{NULL: aws.Bool(true)}, `{"k":{"B":null,"BOOL":null,"BS":null,"L":null,"M":null,"N":null,"NS":null,"NULL":true,"S":null,"SS":null}}`},
	{"string", &dynamodb.AttributeValue{S: aws.String("foo")}, `{"k":{"B":null,"BOOL":null,"BS":null,"L":null,"M":null,"N":null,"NS":null,"NULL":null,"S":"foo","SS":null}}`},
	{"string-set", &dynamodb.AttributeValue{SS: []*string{aws.String("foo"), aws.String("bar")}}, `{"k":{"B":null,"BOOL":null,"BS":null,"L":null,"M":null,"N":null,"NS":null,"NULL":null,"S":null,"SS":["foo","bar"]}}`},
	{"empty-attr-list", &dynamodb.AttributeValue{L: []*dynamodb.AttributeValue{}}, `{"k":{"B":null,"BOOL":null,"BS":null,"L":[],"M":null,"N":null,"NS":null,"NULL":null,"S":null,"SS":null}}`},
}

func TestSimpleEncoder(t *testing.T) {
	for _, test := range attrTests {
		var buf bytes.Buffer
		if err := NewSimpleEncoder(&buf).WriteItem(map[string]*dynamodb.AttributeValue{
			"k": test.src,
		}); err != nil {
			t.Errorf("Unexpected error test=%q error=%v", test.name, err)
		}
		if val := buf.String(); val != test.expected+"\n" {
			t.Errorf("test=%q expected=%s actual=%s", test.name, test.expected, val)
		}
	}
}

func TestSimpleDecoder(t *testing.T) {
	buf := strings.NewReader(`{"k":{"S":"foo"}}`)
	dec := NewSimpleDecoder(buf)
	item, err := dec.ReadItem()
	if err != nil {
		t.Fatal("Unexpected error", err)
	}

	expected := map[string]*dynamodb.AttributeValue{
		"k": &dynamodb.AttributeValue{S: aws.String("foo")},
	}
	if !reflect.DeepEqual(item, expected) {
		t.Errorf("expected=%#v actual=%#v", expected, item)
	}
}
