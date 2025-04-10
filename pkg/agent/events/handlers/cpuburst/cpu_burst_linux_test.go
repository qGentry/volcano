/*
Copyright 2024 The Volcano Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package cpuburst

import (
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	"volcano.sh/volcano/pkg/agent/events/framework"
	"volcano.sh/volcano/pkg/agent/utils/cgroup"
	"volcano.sh/volcano/pkg/agent/utils/file"
)

func TestCPUBurstHandle_Handle(t *testing.T) {
	tmpDir := t.TempDir()
	tests := []struct {
		name      string
		event     interface{}
		cgroupMgr cgroup.CgroupManager
		prepare   func()
		post      func() map[string]string
		wantErr   bool
		wantVal   map[string]string
	}{
		{
			name: "not support cpu burst, return no err",
			event: framework.PodEvent{
				UID:      "fake-id1",
				QoSLevel: 0,
				QoSClass: "",
				Pod:      getPod("100000", "true"),
			},
			cgroupMgr: cgroup.NewCgroupManager("cgroupfs", tmpDir, ""),

			prepare: func() {
				prepare(t, tmpDir, "fake-id1", []info{
					{path: cgroup.CPUQuotaTotalFile, value: "100000"},
				})
			},
			wantErr: false,
		},
		{
			name: "quota=-1, no need set, return no err",
			event: framework.PodEvent{
				UID:      "fake-id2",
				QoSLevel: 0,
				QoSClass: "",
				Pod:      getPod("100000", "true"),
			},
			cgroupMgr: cgroup.NewCgroupManager("cgroupfs", tmpDir, ""),
			prepare: func() {
				prepare(t, tmpDir, "fake-id2", []info{
					{path: cgroup.CPUQuotaBurstFile, value: "0"},
					{path: cgroup.CPUQuotaTotalFile, value: "-1"}})
			},
			wantErr: false,
		},
		{
			name: "one container quota=100000, another quota=-1, set quota burst successfully",
			event: framework.PodEvent{
				UID:      "fake-id3",
				QoSLevel: 0,
				QoSClass: "",
				Pod:      getPod("50000", "true"),
			},
			cgroupMgr: cgroup.NewCgroupManager("cgroupfs", tmpDir, ""),
			prepare: func() {
				prepare(t, tmpDir, "fake-id3", []info{
					{path: cgroup.CPUQuotaBurstFile, value: "0"},
					{path: cgroup.CPUQuotaTotalFile, value: "100000"},
					{dir: "container1", path: cgroup.CPUQuotaBurstFile, value: "0"},
					{dir: "container1", path: cgroup.CPUQuotaTotalFile, value: "100000"},
					{dir: "container2", path: cgroup.CPUQuotaBurstFile, value: "0"},
					{dir: "container2", path: cgroup.CPUQuotaTotalFile, value: "-1"},
				})
			},
			post: func() map[string]string {
				return file.ReadBatchFromFile([]string{
					path.Join(tmpDir, "cpu/kubepods/podfake-id3/cpu.cfs_burst_us"),
					path.Join(tmpDir, "cpu/kubepods/podfake-id3/container1/cpu.cfs_burst_us"),
					path.Join(tmpDir, "cpu/kubepods/podfake-id3/container2/cpu.cfs_burst_us"),
				})
			},
			wantErr: false,
			wantVal: map[string]string{
				path.Join(tmpDir, "cpu/kubepods/podfake-id3/cpu.cfs_burst_us"):            "50000",
				path.Join(tmpDir, "cpu/kubepods/podfake-id3/container1/cpu.cfs_burst_us"): "50000",
				path.Join(tmpDir, "cpu/kubepods/podfake-id3/container2/cpu.cfs_burst_us"): "0",
			},
		},
		{
			name: "quota burst < one container's quota, set min quota burst",
			event: framework.PodEvent{
				UID:      "fake-id4",
				QoSLevel: 0,
				QoSClass: "",
				Pod:      getPod("100000", "true"),
			},
			cgroupMgr: cgroup.NewCgroupManager("cgroupfs", tmpDir, ""),
			prepare: func() {
				prepare(t, tmpDir, "fake-id4", []info{
					{path: cgroup.CPUQuotaBurstFile, value: "0"},
					{path: cgroup.CPUQuotaTotalFile, value: "300000"},
					{dir: "container1", path: cgroup.CPUQuotaBurstFile, value: "0"},
					{dir: "container1", path: cgroup.CPUQuotaTotalFile, value: "100000"},
					{dir: "container2", path: cgroup.CPUQuotaBurstFile, value: "0"},
					{dir: "container2", path: cgroup.CPUQuotaTotalFile, value: "200000"},
				})
			},
			post: func() map[string]string {
				return file.ReadBatchFromFile([]string{
					path.Join(tmpDir, "cpu/kubepods/podfake-id4/cpu.cfs_burst_us"),
					path.Join(tmpDir, "cpu/kubepods/podfake-id4/container1/cpu.cfs_burst_us"),
					path.Join(tmpDir, "cpu/kubepods/podfake-id4/container2/cpu.cfs_burst_us"),
				})
			},
			wantErr: false,
			wantVal: map[string]string{
				path.Join(tmpDir, "cpu/kubepods/podfake-id4/cpu.cfs_burst_us"):            "200000",
				path.Join(tmpDir, "cpu/kubepods/podfake-id4/container1/cpu.cfs_burst_us"): "100000",
				path.Join(tmpDir, "cpu/kubepods/podfake-id4/container2/cpu.cfs_burst_us"): "100000",
			},
		},
		{
			name: "all containers contains quota!=-1, set quota burst successfully",
			event: framework.PodEvent{
				UID:      "fake-id5",
				QoSLevel: 0,
				QoSClass: "",
				Pod:      getPod("200000", "true"),
			},
			cgroupMgr: cgroup.NewCgroupManager("cgroupfs", tmpDir, ""),
			prepare: func() {
				prepare(t, tmpDir, "fake-id5", []info{
					{path: cgroup.CPUQuotaBurstFile, value: "0"},
					{path: cgroup.CPUQuotaTotalFile, value: "300000"},
					{dir: "container1", path: cgroup.CPUQuotaBurstFile, value: "0"},
					{dir: "container1", path: cgroup.CPUQuotaTotalFile, value: "100000"},
					{dir: "container2", path: cgroup.CPUQuotaBurstFile, value: "0"},
					{dir: "container2", path: cgroup.CPUQuotaTotalFile, value: "200000"},
				})
			},
			post: func() map[string]string {
				return file.ReadBatchFromFile([]string{
					path.Join(tmpDir, "cpu/kubepods/podfake-id5/cpu.cfs_burst_us"),
					path.Join(tmpDir, "cpu/kubepods/podfake-id5/container1/cpu.cfs_burst_us"),
					path.Join(tmpDir, "cpu/kubepods/podfake-id5/container2/cpu.cfs_burst_us"),
				})
			},
			wantErr: false,
			wantVal: map[string]string{
				path.Join(tmpDir, "cpu/kubepods/podfake-id5/cpu.cfs_burst_us"):            "300000",
				path.Join(tmpDir, "cpu/kubepods/podfake-id5/container1/cpu.cfs_burst_us"): "100000",
				path.Join(tmpDir, "cpu/kubepods/podfake-id5/container2/cpu.cfs_burst_us"): "200000",
			},
		},
		{
			name: "all containers contains quota!=-1, set quota burst successfully",
			event: framework.PodEvent{
				UID:      "fake-id6",
				QoSLevel: 0,
				QoSClass: "",
				Pod:      getPod("200000", "false"),
			},
			cgroupMgr: cgroup.NewCgroupManager("cgroupfs", tmpDir, ""),
			prepare: func() {
				prepare(t, tmpDir, "fake-id6", []info{
					{path: cgroup.CPUQuotaBurstFile, value: "0"},
					{path: cgroup.CPUQuotaTotalFile, value: "300000"},
					{dir: "container1", path: cgroup.CPUQuotaBurstFile, value: "0"},
					{dir: "container1", path: cgroup.CPUQuotaTotalFile, value: "100000"},
					{dir: "container2", path: cgroup.CPUQuotaBurstFile, value: "0"},
					{dir: "container2", path: cgroup.CPUQuotaTotalFile, value: "200000"},
				})
			},
			post: func() map[string]string {
				return file.ReadBatchFromFile([]string{
					path.Join(tmpDir, "cpu/kubepods/podfake-id6/cpu.cfs_burst_us"),
					path.Join(tmpDir, "cpu/kubepods/podfake-id6/container1/cpu.cfs_burst_us"),
					path.Join(tmpDir, "cpu/kubepods/podfake-id6/container2/cpu.cfs_burst_us"),
				})
			},
			wantErr: false,
			wantVal: map[string]string{
				path.Join(tmpDir, "cpu/kubepods/podfake-id6/cpu.cfs_burst_us"):            "0",
				path.Join(tmpDir, "cpu/kubepods/podfake-id6/container1/cpu.cfs_burst_us"): "0",
				path.Join(tmpDir, "cpu/kubepods/podfake-id6/container2/cpu.cfs_burst_us"): "0",
			},
		},
	}
	fakeClient := fake.NewSimpleClientset()
	informerFactory := informers.NewSharedInformerFactory(fakeClient, 0)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &CPUBurstHandle{
				cgroupMgr:   tt.cgroupMgr,
				podInformer: informerFactory.Core().V1().Pods(),
			}
			if tt.prepare != nil {
				tt.prepare()
			}
			if err := c.Handle(tt.event); (err != nil) != tt.wantErr {
				t.Errorf("Handle() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.post != nil {
				assert.Equal(t, tt.wantVal, tt.post())
			}
		})
	}
}

func getPod(cpuQuotaBurst string, enableBurst string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{
				"volcano.sh/enable-quota-burst": enableBurst,
				"volcano.sh/quota-burst-time":   cpuQuotaBurst,
			},
		},
	}
}

type info struct {
	dir   string
	path  string
	value string
}

func prepare(t *testing.T, tmpDir, podUID string, infos []info) {
	for _, info := range infos {
		dir := path.Join(tmpDir, "cpu", "kubepods", "pod"+podUID, info.dir)
		err := os.MkdirAll(dir, 0644)
		assert.NoError(t, err)
		filePath := path.Join(dir, info.path)
		f, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644)
		assert.NoError(t, err)
		err = f.Chmod(0600)
		assert.NoError(t, err)
		_, err = f.WriteString(info.value)
		assert.NoError(t, err)
		err = f.Close()
		assert.NoError(t, err)
	}
}
