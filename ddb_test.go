package ddb

import (
	"reflect"
	"testing"

	"github.com/seill/log"
)

type Test struct {
	DynamoDbMetaData
	Name        string
	Description string
	TestName    string
}

func TestMarshalUnmarshal(t *testing.T) {
	var err error
	var data []byte
	test1 := Test{}
	test2 := Test{}

	err = Unmarshal([]byte(`{"PK":"Primary Key", "Name":"Test Name","Description":"Test Description","TestName":"Test Test Name"}`), &test1)
	if nil != err {
		t.Error(err)
	}

	log.DebugJson("Test", "Unmarshal", test1)

	data, err = Marshal(test1)
	if nil != err {
		t.Error(err)
	}

	err = Unmarshal(data, &test2)
	if nil != err {
		t.Error(err)
	}

	log.DebugJson("Test", "Marshal", test2)

	if !reflect.DeepEqual(test1, test2) {
		t.Error("Marshal and Unmarshal are not equal")
	}
}
