package clusters

import (
	"fmt"
	"hash/fnv"
	"sort"

	// "math/rand"
	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/framework"
)

const (
	PluginName      = "clusters"
	clusterLabelKey = "volcano.sh/cluster-name"
)

type ClustersPlugin struct {
	pluginArguments framework.Arguments
}

func (ibp *ClustersPlugin) Name() string {
	return PluginName
}

// New return gang plugin
func New(arguments framework.Arguments) framework.Plugin {
	return &ClustersPlugin{pluginArguments: arguments}
}

func getCluster2Nodes(ssn *framework.Session) map[string][]*api.NodeInfo {
	cluster2Nodes := make(map[string][]*api.NodeInfo)
	for _, node := range ssn.Nodes {
		cluster, found := node.Node.GetLabels()[clusterLabelKey]
		if !found {
			continue
		}
		cluster2Nodes[cluster] = append(cluster2Nodes[cluster], node)
	}
	return cluster2Nodes
}

func getClustersWithEnoughNodes(cluster2Nodes map[string][]*api.NodeInfo, minAvailable int) []string {
	clustersWithEnoughNodes := make([]string, 0)
	for cluster, nodes := range cluster2Nodes {
		if minAvailable <= len(nodes) {
			clustersWithEnoughNodes = append(clustersWithEnoughNodes, cluster)
		}
	}
	return clustersWithEnoughNodes
}

func (cp *ClustersPlugin) OnSessionOpen(ssn *framework.Session) {
	// predicateFn is a callback that is called for each individual node to check
	// if the node is a feasible candidate for the task.
	predicateFn := func(task *api.TaskInfo, nodeInfo *api.NodeInfo) ([]*api.Status, error) {
		currentCluster, found := nodeInfo.Node.GetLabels()[clusterLabelKey]
		if !found {
			return []*api.Status{{
				Code:   api.UnschedulableAndUnresolvable,
				Reason: fmt.Sprintf("Node %s doesn't have label %s", nodeInfo.Node.GetName(), clusterLabelKey),
			}}, nil
		}

		minAvailable := int(ssn.Jobs[task.Job].MinAvailable)

		cluster2Nodes := getCluster2Nodes(ssn)
		clustersWithEnoughNodes := getClustersWithEnoughNodes(cluster2Nodes, minAvailable)
		if len(clustersWithEnoughNodes) == 0 {
			status := api.Status{
				Code:   api.Unschedulable,
				Reason: fmt.Sprintf("No Cluster has enough nodes, required = %d", minAvailable),
			}
			return []*api.Status{&status}, nil
		}

		sort.Strings(clustersWithEnoughNodes)
		randomCluster := getRandomItem(clustersWithEnoughNodes, string(ssn.UID))
		if randomCluster == currentCluster {
			return []*api.Status{{
				Code:   api.Success,
				Reason: "",
			}}, nil
		}

		totalNodesInCurrentCluster := len(cluster2Nodes[currentCluster])
		status := api.Status{
			Code: api.Unschedulable,
			Reason: fmt.Sprintf("Cluster has enough nodes %d, but wasn't selected randomly in this Session",
				totalNodesInCurrentCluster),
		}
		return []*api.Status{&status}, nil
	}
	ssn.AddPredicateFn(PluginName, predicateFn)
}

func hashStringToInt(s string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(s))
	return h.Sum64()
}

func getRandomItem(items []string, seed string) string {
	hash := hashStringToInt(seed)
	return items[hash%uint64(len(items))]
}

func (cp *ClustersPlugin) OnSessionClose(ssn *framework.Session) {
}
