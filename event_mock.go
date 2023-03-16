// Code generated by MockGen. DO NOT EDIT.
// Source: event.go

// Package events is a generated GoMock package.
package events

import (
	context "context"
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
)

// MockEventQueue is a mock of EventQueue interface.
type MockEventQueue struct {
	ctrl     *gomock.Controller
	recorder *MockEventQueueMockRecorder
}

// MockEventQueueMockRecorder is the mock recorder for MockEventQueue.
type MockEventQueueMockRecorder struct {
	mock *MockEventQueue
}

// NewMockEventQueue creates a new mock instance.
func NewMockEventQueue(ctrl *gomock.Controller) *MockEventQueue {
	mock := &MockEventQueue{ctrl: ctrl}
	mock.recorder = &MockEventQueueMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockEventQueue) EXPECT() *MockEventQueueMockRecorder {
	return m.recorder
}

// Add mocks base method.
func (m *MockEventQueue) Add(ctx context.Context, e *Event) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Add", ctx, e)
	ret0, _ := ret[0].(error)
	return ret0
}

// Add indicates an expected call of Add.
func (mr *MockEventQueueMockRecorder) Add(ctx, e interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Add", reflect.TypeOf((*MockEventQueue)(nil).Add), ctx, e)
}

// Next mocks base method.
func (m *MockEventQueue) Next(ctx context.Context, e *Event) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Next", ctx, e)
	ret0, _ := ret[0].(error)
	return ret0
}

// Next indicates an expected call of Next.
func (mr *MockEventQueueMockRecorder) Next(ctx, e interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Next", reflect.TypeOf((*MockEventQueue)(nil).Next), ctx, e)
}

// MockEventHandler is a mock of EventHandler interface.
type MockEventHandler struct {
	ctrl     *gomock.Controller
	recorder *MockEventHandlerMockRecorder
}

// MockEventHandlerMockRecorder is the mock recorder for MockEventHandler.
type MockEventHandlerMockRecorder struct {
	mock *MockEventHandler
}

// NewMockEventHandler creates a new mock instance.
func NewMockEventHandler(ctrl *gomock.Controller) *MockEventHandler {
	mock := &MockEventHandler{ctrl: ctrl}
	mock.recorder = &MockEventHandlerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockEventHandler) EXPECT() *MockEventHandlerMockRecorder {
	return m.recorder
}

// HandleEvent mocks base method.
func (m *MockEventHandler) HandleEvent(ctx context.Context, e *Event) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "HandleEvent", ctx, e)
	ret0, _ := ret[0].(error)
	return ret0
}

// HandleEvent indicates an expected call of HandleEvent.
func (mr *MockEventHandlerMockRecorder) HandleEvent(ctx, e interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "HandleEvent", reflect.TypeOf((*MockEventHandler)(nil).HandleEvent), ctx, e)
}

// MockEventListener is a mock of EventListener interface.
type MockEventListener struct {
	ctrl     *gomock.Controller
	recorder *MockEventListenerMockRecorder
}

// MockEventListenerMockRecorder is the mock recorder for MockEventListener.
type MockEventListenerMockRecorder struct {
	mock *MockEventListener
}

// NewMockEventListener creates a new mock instance.
func NewMockEventListener(ctrl *gomock.Controller) *MockEventListener {
	mock := &MockEventListener{ctrl: ctrl}
	mock.recorder = &MockEventListenerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockEventListener) EXPECT() *MockEventListenerMockRecorder {
	return m.recorder
}

// Listen mocks base method.
func (m *MockEventListener) Listen(ctx context.Context, q EventQueue, handle EventHandler) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Listen", ctx, q, handle)
	ret0, _ := ret[0].(error)
	return ret0
}

// Listen indicates an expected call of Listen.
func (mr *MockEventListenerMockRecorder) Listen(ctx, q, handle interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Listen", reflect.TypeOf((*MockEventListener)(nil).Listen), ctx, q, handle)
}
