package workflows

import "github.com/looprig/harness/pkg/tool"

const SupervisorResourceName = "policy53-workflow-supervisor"

var _ tool.SessionResource = (*Supervisor)(nil)
