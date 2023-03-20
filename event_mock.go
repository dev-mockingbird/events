// Code generated by MockGen. DO NOT EDIT.
// Source: event.go

// Package events is a generated GoMock package.
package events

import (
	context "context"
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
)

// MockQueue is a mock of Queue interface.
type MockQueue struct {
	ctrl     *gomock.Controller
	recorder *MockQueueMockRecorder
}

// MockQueueMockRecorder is the mock recorder for MockQueue.
type MockQueueMockRecorder struct {
	mock *MockQueue
}

// NewMockQueue creates a new mock instance.
func NewMockQueue(ctrl *gomock.Controller) *MockQueue {
	mock := &MockQueue{ctrl: ctrl}
	mock.recorder = &MockQueueMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockQueue) EXPECT() *MockQueueMockRecorder {
	return m.recorder
}

// Add mocks base method.
func (m *MockQueue) Add(ctx context.Context, e *Event) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Add", ctx, e)
	ret0, _ := ret[0].(error)
	return ret0
}

// Add indicates an expected call of Add.
func (mr *MockQueueMockRecorder) Add(ctx, e interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Add", reflect.TypeOf((*MockQueue)(nil).Add), ctx, e)
}

// Name mocks base method.
func (m *MockQueue) Name() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Name")
	ret0, _ := ret[0].(string)
	return ret0
}

// Name indicates an expected call of Name.
func (mr *MockQueueMockRecorder) Name() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Name", reflect.TypeOf((*MockQueue)(nil).Name))
}

// Next mocks base method.
func (m *MockQueue) Next(ctx context.Context, e *Event) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Next", ctx, e)
	ret0, _ := ret[0].(error)
	return ret0
}

// Next indicates an expected call of Next.
func (mr *MockQueueMockRecorder) Next(ctx, e interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Next", reflect.TypeOf((*MockQueue)(nil).Next), ctx, e)
}

// MockHandler is a mock of Handler interface.
type MockHandler struct {
	ctrl     *gomock.Controller
	recorder *MockHandlerMockRecorder
}

// MockHandlerMockRecorder is the mock recorder for MockHandler.
type MockHandlerMockRecorder struct {
	mock *MockHandler
}

// NewMockHandler creates a new mock instance.
func NewMockHandler(ctrl *gomock.Controller) *MockHandler {
	mock := &MockHandler{ctrl: ctrl}
	mock.recorder = &MockHandlerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockHandler) EXPECT() *MockHandlerMockRecorder {
	return m.recorder
}

// Handle mocks base method.
func (m *MockHandler) Handle(ctx context.Context, e *Event) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Handle", ctx, e)
	ret0, _ := ret[0].(error)
	return ret0
}

// Handle indicates an expected call of Handle.
func (mr *MockHandlerMockRecorder) Handle(ctx, e interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Handle", reflect.TypeOf((*MockHandler)(nil).Handle), ctx, e)
}

// MockListener is a mock of Listener interface.
type MockListener struct {
	ctrl     *gomock.Controller
	recorder *MockListenerMockRecorder
}

// MockListenerMockRecorder is the mock recorder for MockListener.
type MockListenerMockRecorder struct {
	mock *MockListener
}

// NewMockListener creates a new mock instance.
func NewMockListener(ctrl *gomock.Controller) *MockListener {
	mock := &MockListener{ctrl: ctrl}
	mock.recorder = &MockListenerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockListener) EXPECT() *MockListenerMockRecorder {
	return m.recorder
}

// Listen mocks base method.
func (m *MockListener) Listen(ctx context.Context, q Queue, handle Handler) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Listen", ctx, q, handle)
	ret0, _ := ret[0].(error)
	return ret0
}

// Listen indicates an expected call of Listen.
func (mr *MockListenerMockRecorder) Listen(ctx, q, handle interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Listen", reflect.TypeOf((*MockListener)(nil).Listen), ctx, q, handle)
}
