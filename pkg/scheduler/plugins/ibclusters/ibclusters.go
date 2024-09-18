package ibclusters

import (
	"fmt"

	"k8s.io/klog"
	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/framework"
)

const (
	PluginName = "ibclusters"
)

type ibClustersPlugin struct {
	pluginArguments framework.Arguments
}

func (ibp *ibClustersPlugin) Name() string {
	return PluginName
}

// New return gang plugin
func New(arguments framework.Arguments) framework.Plugin {
	return &ibClustersPlugin{pluginArguments: arguments}
}

func (ibp *ibClustersPlugin) OnSessionOpen(ssn *framework.Session) {
	predicateFn := func(task *api.TaskInfo, nodeInfo *api.NodeInfo) ([]*api.Status, error) {
		minAvailable := ssn.Jobs[task.Job].MinAvailable
		for nodeName, node := range ssn.Nodes {
			klog.V(3).Info("Node key: ", nodeName, "Node value: ", node.Name)
		}
		status := api.Status{
			Code:   api.UnschedulableAndUnresolvable,
			Reason: fmt.Sprintf("Node doesn't satisfy task, total nodes %d, min av = %d", len(ssn.Nodes), minAvailable),
		}
		return []*api.Status{&status}, nil
	}
	ssn.AddPredicateFn(PluginName, predicateFn)
}

func (ibp *ibClustersPlugin) OnSessionClose(ssn *framework.Session) {
}
