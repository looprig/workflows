package workflows

// RunStatus is the durable lifecycle state of a registered workflow run.
type RunStatus string

const (
	RunPending     RunStatus = "pending"
	RunRunning     RunStatus = "running"
	RunInterrupted RunStatus = "interrupted"
	RunCompleted   RunStatus = "completed"
	RunCancelled   RunStatus = "cancelled"
	RunFailed      RunStatus = "failed"
)

func (s RunStatus) valid() bool {
	switch s {
	case RunPending, RunRunning, RunInterrupted, RunCompleted, RunCancelled, RunFailed:
		return true
	default:
		return false
	}
}

func validRunTransition(from, to RunStatus) bool {
	if from == to {
		return from.valid()
	}
	switch from {
	case RunPending:
		return to == RunRunning || to == RunCancelled || to == RunFailed
	case RunRunning:
		return to == RunInterrupted || to == RunCompleted || to == RunCancelled || to == RunFailed
	case RunInterrupted:
		return to == RunRunning || to == RunCancelled || to == RunFailed
	default:
		return false
	}
}
