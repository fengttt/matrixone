// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package task

import (
	"container/heap"
	"context"
	"time"

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	"github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
)

const (
	taskSchedulerDefaultTimeout = 10 * time.Second
)

type scheduler struct {
	service           string
	cfg               hakeeper.Config
	taskServiceGetter func() taskservice.TaskService
}

var _ hakeeper.TaskScheduler = (*scheduler)(nil)

func NewScheduler(
	service string,
	taskServiceGetter func() taskservice.TaskService,
	cfg hakeeper.Config,
) hakeeper.TaskScheduler {
	cfg.Fill()
	s := &scheduler{
		service:           service,
		taskServiceGetter: taskServiceGetter,
		cfg:               cfg,
	}
	return s
}

func (s *scheduler) Schedule(cnState logservice.CNState, currentTick uint64) {
	cnPool := newCNPoolWithCNState(cnState)
	runningTasks := s.queryTasks(task.TaskStatus_Running)
	createdTasks := s.queryTasks(task.TaskStatus_Created)
	tasks := append(runningTasks, createdTasks...)

	runtime.ServiceRuntime(s.service).Logger().Debug("task schedule query tasks",
		zap.Int("created", len(createdTasks)),
		zap.Int("running", len(runningTasks)))
	if len(tasks) == 0 {
		return
	}
	workingCNPool := cnPool.selectCNs(notExpired(s.cfg, currentTick))
	expiredTasks := getExpiredTasks(runningTasks, workingCNPool)
	runtime.ServiceRuntime(s.service).Logger().Info("task schedule query tasks",
		zap.Int("created", len(createdTasks)),
		zap.Int("expired", len(expiredTasks)))
	s.allocateTasks(createdTasks, workingCNPool)
	s.completeTasks(expiredTasks)

	s.truncateTasks()
}

func (s *scheduler) StartScheduleCronTask() {
	if ts := s.taskServiceGetter(); ts != nil {
		ts.StartScheduleCronTask()
	}
}

func (s *scheduler) StopScheduleCronTask() {
	if ts := s.taskServiceGetter(); ts != nil {
		ts.StopScheduleCronTask()
	}
}

func (s *scheduler) queryTasks(status task.TaskStatus) []task.AsyncTask {
	ts := s.taskServiceGetter()
	if ts == nil {
		runtime.ServiceRuntime(s.service).Logger().Error("task service is nil",
			zap.String("status", status.String()))
		return nil
	}
	ctx, cancel := context.WithTimeoutCause(context.Background(), taskSchedulerDefaultTimeout, moerr.CauseQueryTasks)
	defer cancel()

	tasks, err := ts.QueryAsyncTask(ctx, taskservice.WithTaskStatusCond(status))
	if err != nil {
		err = moerr.AttachCause(ctx, err)
		runtime.ServiceRuntime(s.service).Logger().Error("failed to query tasks",
			zap.String("status", status.String()),
			zap.Error(err))
		return nil
	}
	return tasks
}

func (s *scheduler) allocateTasks(tasks []task.AsyncTask, cnPool *cnPool) {
	ts := s.taskServiceGetter()
	if ts == nil {
		return
	}

	for _, t := range tasks {
		allocateTask(s.service, ts, t, cnPool)
	}
}

func allocateTask(
	service string,
	ts taskservice.TaskService,
	t task.AsyncTask,
	cnPool *cnPool,
) {
	var rules []rule
	if len(t.Metadata.Options.Labels) != 0 {
		rules = make([]rule, 0, len(t.Metadata.Options.Labels))
		for key, label := range t.Metadata.Options.Labels {
			rules = append(rules, containsLabel(key, label))
		}
	}
	if t.Metadata.Options.Resource.GetCPU() > 0 {
		rules = append(rules, withCPU(t.Metadata.Options.Resource.CPU))
	}
	if t.Metadata.Options.Resource.GetMemory() > 0 {
		rules = append(rules, withMemory(t.Metadata.Options.Resource.Memory))
	}
	cnPool = cnPool.selectCNs(rules...)
	runner := cnPool.min()
	if runner.uuid == "" {
		runtime.ServiceRuntime(service).Logger().Error("failed to allocate task",
			zap.Uint64("task-id", t.ID),
			zap.String("task-metadata-id", t.Metadata.ID),
			zap.Error(moerr.NewInternalErrorNoCtx("no CN available")))
		return
	}
	ctx, cancel := context.WithTimeoutCause(context.Background(), taskSchedulerDefaultTimeout, moerr.CauseAllocateTasks)
	defer cancel()

	if err := ts.Allocate(ctx, t, runner.uuid); err != nil {
		err = moerr.AttachCause(ctx, err)
		runtime.ServiceRuntime(service).Logger().Error("failed to allocate task",
			zap.Uint64("task-id", t.ID),
			zap.String("task-metadata-id", t.Metadata.ID),
			zap.String("task-runner", runner.uuid),
			zap.Error(err))
		return
	}
	runtime.ServiceRuntime(service).Logger().Info("task allocated",
		zap.Uint64("task-id", t.ID),
		zap.String("task-metadata-id", t.Metadata.ID),
		zap.String("task-runner", runner.uuid))
	heap.Push(cnPool, runner)
}

func (s *scheduler) completeTasks(tasks []task.AsyncTask) {
	ts := s.taskServiceGetter()
	if ts == nil {
		return
	}

	for _, t := range tasks {
		markCompleted(s.service, ts, t)
	}
}

func markCompleted(service string, ts taskservice.TaskService, t task.AsyncTask) {
	ctx, cancel := context.WithTimeoutCause(context.Background(), taskSchedulerDefaultTimeout, moerr.CauseAllocateTasks)
	defer cancel()
	if err := ts.Complete(ctx, t.TaskRunner, t, task.ExecuteResult{
		Code:  task.ResultCode_Failed,
		Error: "heartbeat expired",
	}); err != nil {
		runtime.ServiceRuntime(service).Logger().Error("failed to complete expired task",
			zap.Uint64("task-id", t.ID),
			zap.String("task-metadata-id", t.Metadata.ID),
			zap.String("task-runner", t.TaskRunner),
			zap.Error(err))
	}
}

func (s *scheduler) truncateTasks() {
	ts := s.taskServiceGetter()
	if ts == nil {
		return
	}
	ctx, cancel := context.WithTimeoutCause(context.Background(), taskSchedulerDefaultTimeout, moerr.CauseTruncateTasks)
	defer cancel()

	_ = ts.TruncateCompletedTasks(ctx)
}

func getExpiredTasks(tasks []task.AsyncTask, workingCNPool *cnPool) []task.AsyncTask {
	expireCount := 0
	for _, t := range tasks {
		if store, ok := workingCNPool.getStore(t.TaskRunner); ok {
			heap.Push(workingCNPool, store)
		} else {
			expireCount++
		}
	}
	if expireCount == 0 {
		return nil
	}
	expired := make([]task.AsyncTask, 0, expireCount)
	for _, t := range tasks {
		if !workingCNPool.contains(t.TaskRunner) {
			expired = append(expired, t)
		}
	}
	return expired
}
