package mutex

import (
	"context"
	"fmt"
	"time"

	"github.com/stretchr/testify/mock"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

const (
	// AcquireLockSignalName signal channel name for lock acquisition
	AcquireLockSignalName = "acquire-lock-event"
	// RequestLockSignalName channel name for request lock
	RequestLockSignalName = "request-lock-event"

	ClientContextKey ContextKey = "Client"
)

type (
	ContextKey string

	UnlockFunc func() error

	Mutex struct {
		currentWorkflowID string
		lockNamespace     string
	}
)

// NewMutex initializes mutex
func NewMutex(currentWorkflowID string, lockNamespace string) *Mutex {
	return &Mutex{
		currentWorkflowID: currentWorkflowID,
		lockNamespace:     lockNamespace,
	}
}

func (s *Mutex) LockWithCancellation(ctx workflow.Context,
	resourceID string, unlockTimeout time.Duration) (UnlockFunc, error) {

	activityCtx := workflow.WithLocalActivityOptions(ctx, workflow.LocalActivityOptions{
		ScheduleToCloseTimeout: time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2.0,
			MaximumInterval:    time.Minute,
			MaximumAttempts:    5,
		},
	})

	var releaseLockChannelName string
	var execution workflow.Execution
	err := workflow.ExecuteLocalActivity(activityCtx, SignalWithStartMutexWorkflowActivity, s.lockNamespace,
		resourceID, s.currentWorkflowID, unlockTimeout).Get(ctx, &execution)
	if err != nil {
		return nil, err
	}

	isCanceled := false
	selector := workflow.NewSelector(ctx)
	selector.AddReceive(workflow.GetSignalChannel(ctx, AcquireLockSignalName), func(c workflow.ReceiveChannel, more bool) {
		c.Receive(ctx, &releaseLockChannelName)
		workflow.GetLogger(ctx).Info("acquire lock", "lockName", releaseLockChannelName)
	})
	selector.AddReceive(ctx.Done(), func(c workflow.ReceiveChannel, more bool) {
		isCanceled = true
		workflow.GetLogger(ctx).Info("recieved cancel")
	})

	selector.Select(ctx)

	if !isCanceled {
		unlockFunc := func() error {
			return workflow.SignalExternalWorkflow(ctx, execution.ID, execution.RunID,
				releaseLockChannelName, "releaseLock").Get(ctx, nil)
		}
		return unlockFunc, nil
	}

	return nil, temporal.NewCanceledError(fmt.Sprintf("canceled because another deployment was triggered"))
}

// Lock - locks mutex
func (s *Mutex) Lock(ctx workflow.Context,
	resourceID string, unlockTimeout time.Duration) (UnlockFunc, error) {

	activityCtx := workflow.WithLocalActivityOptions(ctx, workflow.LocalActivityOptions{
		ScheduleToCloseTimeout: time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2.0,
			MaximumInterval:    time.Minute,
			MaximumAttempts:    5,
		},
	})

	var releaseLockChannelName string
	var execution workflow.Execution
	err := workflow.ExecuteLocalActivity(activityCtx,
		SignalWithStartMutexWorkflowActivity, s.lockNamespace,
		resourceID, s.currentWorkflowID, unlockTimeout).Get(ctx, &execution)
	if err != nil {
		return nil, err
	}
	workflow.GetSignalChannel(ctx, AcquireLockSignalName).
		Receive(ctx, &releaseLockChannelName)

	unlockFunc := func() error {
		return workflow.SignalExternalWorkflow(ctx, execution.ID, execution.RunID,
			releaseLockChannelName, "releaseLock").Get(ctx, nil)
	}
	return unlockFunc, nil
}

func MutexWorkflowWithCancellation(
	ctx workflow.Context,
	namespace string,
	resourceID string,
	unlockTimeout time.Duration,
) error {
	currentWorkflowID := workflow.GetInfo(ctx).WorkflowExecution.ID
	if currentWorkflowID == "default-test-workflow-id" {
		// unit testing hack, see https://github.com/uber-go/cadence-client/issues/663
		_ = workflow.Sleep(ctx, 10*time.Millisecond)
	}
	logger := workflow.GetLogger(ctx)
	logger.Info("started", "currentWorkflowID", currentWorkflowID)
	requestLockCh := workflow.GetSignalChannel(ctx, RequestLockSignalName)
	locked := false

	goRoutineCount := 0
	queuedIds := make([]string, 0)
	workflow.Go(ctx, func(ctx workflow.Context) {
		for {
			var senderWorkflowID string
			if requestLockCh.Receive(ctx, &senderWorkflowID) {
				goRoutineCount++
				if locked {
					// when locked, we only allow one queued item, and the rest is canceled
					for _, id := range queuedIds {
						cancelSender(ctx, id)
						goRoutineCount--
					}
					queuedIds = make([]string, 0)
				}

				logger.Info(fmt.Sprintf("Adding [%s] to [%v]", senderWorkflowID, queuedIds))
				queuedIds = append(queuedIds, senderWorkflowID)
			}
		}
	})

	// Since we are starting a go routine to listen to for "request-lock" signals, let's delay by a second
	// Note sure if this is needed
	workflow.Sleep(ctx, time.Second)
	for {
		if !locked {
			logger.Info(fmt.Sprintf("Processing [%v]", queuedIds))
			if len(queuedIds) == 0 {
				break
			} else if len(queuedIds) == 1 {
				locked = true
				id := queuedIds[0]
				queuedIds = make([]string, 0)
				tryLock(ctx, id, unlockTimeout)
				locked = false
				goRoutineCount--
			} else {
				logger.Error("queue should never be more then 1")
			}
		}
	}

	workflow.Await(ctx, func() bool {
		return goRoutineCount == 0
	})

	return nil
}

func cancelSender(ctx workflow.Context, senderWorkflowID string) {
	logger := workflow.GetLogger(ctx)
	future := workflow.RequestCancelExternalWorkflow(ctx, senderWorkflowID, "")
	err := future.Get(ctx, nil)
	if err != nil {
		logger.Info("CancelExternalWorkflow error", "Error", err)
		return
	} else {
		logger.Info(fmt.Sprintf("Request to cancel workflow: [%s]", senderWorkflowID))
	}
}

func tryLock(ctx workflow.Context, senderWorkflowID string, unlockTimeout time.Duration) {
	logger := workflow.GetLogger(ctx)
	var releaseLockChannelName string
	_ = workflow.SideEffect(ctx, func(ctx workflow.Context) interface{} {
		return generateUnlockChannelName(senderWorkflowID)
	}).Get(&releaseLockChannelName)
	logger.Info("generated release lock channel name", "releaseLockChannelName", releaseLockChannelName)
	// Send release lock channel name back to a senderWorkflowID, so that it can
	// release the lock using release lock channel name
	err := workflow.SignalExternalWorkflow(ctx, senderWorkflowID, "",
		AcquireLockSignalName, releaseLockChannelName).Get(ctx, nil)
	if err != nil {
		// .Get(ctx, nil) blocks until the signal is sent.
		// If the senderWorkflowID is closed (terminated/canceled/timeouted/completed/etc), this would return error.
		// In this case we release the lock immediately instead of failing the mutex workflow.
		// Mutex workflow failing would lead to all workflows that have sent requestLock will be waiting.
		logger.Info("SignalExternalWorkflow error", "Error", err)
		return
	}
	logger.Info("signaled external workflow")
	var ack string
	ok, _ := workflow.GetSignalChannel(ctx, releaseLockChannelName).ReceiveWithTimeout(ctx, unlockTimeout,
		&ack)
	logger.Info("release signal received: " + ack)
	if !ok {
		// mutex canceled so lets cancel the workflow
		err = workflow.RequestCancelExternalWorkflow(ctx, senderWorkflowID, "").Get(ctx, nil)
		if err != nil {
			logger.Error(fmt.Sprintf("failed to cancel workflow [%s]", senderWorkflowID), "error", err)
		}
	}

}

// MutexWorkflow used for locking a resource
func MutexWorkflow(
	ctx workflow.Context,
	namespace string,
	resourceID string,
	unlockTimeout time.Duration,
) error {
	currentWorkflowID := workflow.GetInfo(ctx).WorkflowExecution.ID
	if currentWorkflowID == "default-test-workflow-id" {
		// unit testing hack, see https://github.com/uber-go/cadence-client/issues/663
		_ = workflow.Sleep(ctx, 10*time.Millisecond)
	}
	logger := workflow.GetLogger(ctx)
	logger.Info("started", "currentWorkflowID", currentWorkflowID)
	var ack string
	requestLockCh := workflow.GetSignalChannel(ctx, RequestLockSignalName)
	for {
		var senderWorkflowID string
		if !requestLockCh.ReceiveAsync(&senderWorkflowID) {
			logger.Info("no more signals")
			break
		}
		var releaseLockChannelName string
		_ = workflow.SideEffect(ctx, func(ctx workflow.Context) interface{} {
			return generateUnlockChannelName(senderWorkflowID)
		}).Get(&releaseLockChannelName)
		logger.Info("generated release lock channel name", "releaseLockChannelName", releaseLockChannelName)
		// Send release lock channel name back to a senderWorkflowID, so that it can
		// release the lock using release lock channel name
		err := workflow.SignalExternalWorkflow(ctx, senderWorkflowID, "",
			AcquireLockSignalName, releaseLockChannelName).Get(ctx, nil)
		if err != nil {
			// .Get(ctx, nil) blocks until the signal is sent.
			// If the senderWorkflowID is closed (terminated/canceled/timeouted/completed/etc), this would return error.
			// In this case we release the lock immediately instead of failing the mutex workflow.
			// Mutex workflow failing would lead to all workflows that have sent requestLock will be waiting.
			logger.Info("SignalExternalWorkflow error", "Error", err)
			continue
		}
		logger.Info("signaled external workflow")
		selector := workflow.NewSelector(ctx)
		selector.AddFuture(workflow.NewTimer(ctx, unlockTimeout), func(f workflow.Future) {
			logger.Info("unlockTimeout exceeded")
		})
		selector.AddReceive(workflow.GetSignalChannel(ctx, releaseLockChannelName), func(c workflow.ReceiveChannel, more bool) {
			c.Receive(ctx, &ack)
			logger.Info("release signal received")
		})
		selector.Select(ctx)
	}
	return nil
}

// SignalWithStartMutexWorkflowActivity ...
func SignalWithStartMutexWorkflowActivity(
	ctx context.Context,
	namespace string,
	resourceID string,
	senderWorkflowID string,
	unlockTimeout time.Duration,
) (*workflow.Execution, error) {

	c := ctx.Value(ClientContextKey).(client.Client)
	workflowID := fmt.Sprintf(
		"%s:%s:%s",
		"mutex",
		namespace,
		resourceID,
	)
	workflowOptions := client.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: "mutex",
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2.0,
			MaximumInterval:    time.Minute,
			MaximumAttempts:    5,
		},
	}
	wr, err := c.SignalWithStartWorkflow(
		ctx, workflowID, RequestLockSignalName, senderWorkflowID,
		workflowOptions, MutexWorkflowWithCancellation, namespace, resourceID, unlockTimeout)

	if err != nil {
		activity.GetLogger(ctx).Error("Unable to signal with start workflow", "Error", err)
	} else {
		activity.GetLogger(ctx).Info("Signaled and started Workflow", "WorkflowID", wr.GetID(), "RunID", wr.GetRunID())
	}

	return &workflow.Execution{
		ID:    wr.GetID(),
		RunID: wr.GetRunID(),
	}, nil
}

// generateUnlockChannelName generates release lock channel name
func generateUnlockChannelName(senderWorkflowID string) string {
	return fmt.Sprintf("unlock-event-%s", senderWorkflowID)
}

// MockMutexLock stubs mutex.Lock call
func MockMutexLock(env *testsuite.TestWorkflowEnvironment, resourceID string, mockError error) {
	execution := &workflow.Execution{ID: "mockID", RunID: "mockRunID"}
	env.OnActivity(SignalWithStartMutexWorkflowActivity,
		mock.Anything, mock.Anything, resourceID, mock.Anything, mock.Anything).
		Return(execution, mockError)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(AcquireLockSignalName, "mockReleaseLockChannelName")
	}, time.Millisecond*0)
	if mockError == nil {
		env.OnSignalExternalWorkflow(mock.Anything, mock.Anything, execution.RunID,
			mock.Anything, mock.Anything).Return(nil)
	}
}

func SampleWorkflowWithMutex(
	ctx workflow.Context,
	resourceID string,
) error {

	currentWorkflowID := workflow.GetInfo(ctx).WorkflowExecution.ID
	logger := workflow.GetLogger(ctx)
	logger.Info("started", "currentWorkflowID", currentWorkflowID, "resourceID", resourceID)

	mutex := NewMutex(currentWorkflowID, "TestUseCase")
	unlockFunc, err := mutex.LockWithCancellation(ctx, resourceID, 2*time.Minute)
	if err != nil {
		return err
	}
	defer func() {
		if unlockFunc != nil {
			unlockFunc()
		}
	}()

	logger.Info("critical operation started")
	selector := workflow.NewSelector(ctx)

	selector.AddFuture(workflow.NewTimer(ctx, 5*time.Minute), func(f workflow.Future) {
		logger.Info("unlockTimeout exceeded")
	})

	isCanceled := false
	selector.AddReceive(ctx.Done(), func(c workflow.ReceiveChannel, more bool) {
		logger.Info("cancel request made")
		isCanceled = true
	})
	selector.Select(ctx)

	if isCanceled {
		return temporal.NewCanceledError("workflow canceled")
	}

	logger.Info("critical operation finished")

	logger.Info("finished")
	return nil
}
