package mock

import (
	context "context"

	mock "github.com/stretchr/testify/mock"

	sdk "github.com/conduitio/conduit-connector-sdk"
)

type AirtableClient struct {
	mock.Mock
}

func (_m *AirtableClient) Close() {
	_m.Called()
}

func (_m *AirtableClient) GetPage(ctx context.Context) ([]sdk.Record, error) {

	ret := _m.Called(ctx)

	var records []sdk.Record
	if rf, ok := ret.Get(0).(func(context.Context) []sdk.Record); ok {
		records = rf(ctx)
	} else {
		if ret.Get(0) != nil {
			records = ret.Get(0).([]sdk.Record)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context) error); ok {
		r1 = rf(ctx)
	} else {
		r1 = ret.Error(1)
	}

	return records, r1
}
