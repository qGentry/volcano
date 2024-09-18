package ibclusters

import (
	"fmt"
	"sort"
	// "math/rand"
	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/framework"
)

const (
	PluginName = "ibclusters"
	ibClusterLabelKey = "volcano.sh/ibcluster-name"
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

func getIbCluster2Nodes(ssn *framework.Session) map[string][]*api.NodeInfo {
	ibCluster2Nodes := make(map[string][]*api.NodeInfo)
	for _, node := range ssn.Nodes {
		ibCluster, found := node.Node.GetLabels()[ibClusterLabelKey]
		if !found {
			continue
		}
		ibCluster2Nodes[ibCluster] = append(ibCluster2Nodes[ibCluster], node)
	}
	return ibCluster2Nodes
}

func getClustersWithEnoughNodes(ibCluster2Nodes map[string][]*api.NodeInfo, minAvailable int) []string {
	clustersWithEnoughNodes := make([]string, 0)
	for ibCluster, nodes := range ibCluster2Nodes {
		if minAvailable <= len(nodes) {
			clustersWithEnoughNodes = append(clustersWithEnoughNodes, ibCluster)
		}
	}
	return clustersWithEnoughNodes
}

func (ibp *ibClustersPlugin) OnSessionOpen(ssn *framework.Session) {
	predicateFn := func(task *api.TaskInfo, nodeInfo *api.NodeInfo) ([]*api.Status, error) {
		currentCluster, found := nodeInfo.Node.GetLabels()[ibClusterLabelKey]
		if !found {
			return []*api.Status{{
				Code:   api.UnschedulableAndUnresolvable,
				Reason: fmt.Sprintf("Node %s doesn't have label %s", nodeInfo.Node.GetName(), ibClusterLabelKey),
			}}, nil
		}

		minAvailable := int(ssn.Jobs[task.Job].MinAvailable)

		ibCluster2Nodes := getIbCluster2Nodes(ssn)
		clustersWithEnoughNodes := getClustersWithEnoughNodes(ibCluster2Nodes, minAvailable)
		// randomCluster := clustersWithEnoughNodes[rand.Intn(len(clustersWithEnoughNodes))]
		if len(clustersWithEnoughNodes) == 0 {
			status := api.Status{
				Code:   api.Unschedulable,
				Reason: fmt.Sprintf("No IB Cluster has enough nodes, required = %d", minAvailable),
			}
			return []*api.Status{&status}, nil
		}
		sort.Strings(clustersWithEnoughNodes)
		randomCluster := clustersWithEnoughNodes[0]
		if randomCluster == currentCluster {
			return []*api.Status{{
				Code:   api.Success,
				Reason: "",
			}}, nil
		}

		totalNodesInCurrentIbCluster := len(ibCluster2Nodes[currentCluster])
		if totalNodesInCurrentIbCluster < minAvailable {
			status := api.Status{
				Code:   api.Unschedulable,
				Reason: fmt.Sprintf("Total nodes in current IB %d, required = %d", 
						totalNodesInCurrentIbCluster, minAvailable),
			}
			return []*api.Status{&status}, nil
		} else {
			status := api.Status{
				Code:   api.Unschedulable,
				Reason: fmt.Sprintf("IB Cluster has enough nodes %d, but wasn't selected randomly in this Session", 
									totalNodesInCurrentIbCluster),
			}
			return []*api.Status{&status}, nil
		}
	}
	ssn.AddPredicateFn(PluginName, predicateFn)
}

func (ibp *ibClustersPlugin) OnSessionClose(ssn *framework.Session) {
}
