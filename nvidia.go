/*
 * Copyright (c) 2019, NVIDIA CORPORATION.  All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import (
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/NVIDIA/gpu-monitoring-tools/bindings/go/nvml"

	pluginapi "k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"
)

const (
	envDisableHealthChecks = "DP_DISABLE_HEALTHCHECKS"
	allHealthChecks        = "xids"

	envEnableFilterByProcess = "FILTER_BY_PROCESS"
	envEnableFilterByGPUUTIL = "FILTER_BY_GPU_UTIL"
	envEnableFilterByMemory  = "FILTER_BY_MEMORY"
)

type Device struct {
	pluginapi.Device
	Path                 string
	CustomDefinedHealthy bool
	Index                uint
}

type ResourceManager interface {
	Devices() []*Device
	CheckHealth(stop <-chan interface{}, devices []*Device, unhealthy chan<- *Device)
}

type GpuDeviceManager struct{}

func check(err error) {
	if err != nil {
		log.Panicln("Fatal:", err)
	}
}

func NewGpuDeviceManager() *GpuDeviceManager {
	return &GpuDeviceManager{}
}

func (g *GpuDeviceManager) Devices() []*Device {
	n, err := nvml.GetDeviceCount()
	check(err)

	var devs []*Device
	for i := uint(0); i < n; i++ {
		d, err := nvml.NewDeviceLite(i)
		check(err)
		devs = append(devs, buildDevice(d))
	}

	return devs
}

func (g *GpuDeviceManager) CheckHealth(stop <-chan interface{}, devices []*Device, unhealthy chan<- *Device) {
	checkHealth(stop, devices, unhealthy)
}

func buildDevice(d *nvml.Device) *Device {
	dev := Device{}
	dev.ID = d.UUID
	dev.Health = pluginapi.Healthy
	dev.Path = d.Path
	if d.CPUAffinity != nil {
		dev.Topology = &pluginapi.TopologyInfo{
			Nodes: []*pluginapi.NUMANode{
				&pluginapi.NUMANode{
					ID: int64(*(d.CPUAffinity)),
				},
			},
		}
	}
	return &dev
}

func checkHealth(stop <-chan interface{}, devices []*Device, unhealthy chan<- *Device) {
	disableHealthChecks := strings.ToLower(os.Getenv(envDisableHealthChecks))
	if disableHealthChecks == "all" {
		disableHealthChecks = allHealthChecks
	}
	if strings.Contains(disableHealthChecks, "xids") {
		return
	}

	eventSet := nvml.NewEventSet()
	defer nvml.DeleteEventSet(eventSet)

	for _, d := range devices {
		err := nvml.RegisterEventForDevice(eventSet, nvml.XidCriticalError, d.ID)
		if err != nil && strings.HasSuffix(err.Error(), "Not Supported") {
			log.Printf("Warning: %s is too old to support healthchecking: %s. Marking it unhealthy.", d.ID, err)
			unhealthy <- d
			continue
		}
		check(err)
	}

	// filter gpus which had running one or more processes
	if os.Getenv(envEnableFilterByProcess) != "" || os.Getenv(envEnableFilterByGPUUTIL) != "" || os.Getenv(envEnableFilterByMemory) != "" {
		log.Printf("Environments: %s=%s,%s=%s,%s=%s", envEnableFilterByProcess, os.Getenv(envEnableFilterByProcess), envEnableFilterByGPUUTIL, os.Getenv(envEnableFilterByGPUUTIL), envEnableFilterByMemory, os.Getenv(envEnableFilterByMemory))
		go func() {
			t := time.NewTicker(3 * time.Second)
			for {
				select {
				case <-stop:
					return
				case <-t.C:
					for i, d := range devices {
						if !d.CustomDefinedHealthy && d.Health == pluginapi.Unhealthy {
							continue
						}

						count, err := nvml.GetDeviceCount()
						if err != nil {
							log.Printf("Error: %v \n", err)
							continue
						}

						unHealthy := false
						var index uint
						for j := uint(0); j < count; j++ {
							device, err := nvml.NewDeviceLite(j)
							if err != nil {
								log.Printf("Error: %v \n", err)
								continue
							}
							if device.UUID != d.ID {
								continue
							}

							stat, err := device.Status()
							if err != nil {
								log.Printf("Error: failed to get device status %v", err)
								continue
							}

							if envProcess := os.Getenv(envEnableFilterByProcess); envProcess != "" {
								p, err := strconv.Atoi(envProcess)
								if err != nil {
									log.Printf("Error: %v \n", err)
									continue
								}

								if p == 0 || len(stat.Processes) < p {
									continue
								}
							}

							if envUtil := os.Getenv(envEnableFilterByGPUUTIL); envUtil != "" {
								util, err := strconv.Atoi(envUtil)
								if err != nil {
									log.Printf("Error: %v \n", err)
									continue
								}
								if *stat.Utilization.GPU > 100 {
									log.Printf("Warning Exception Util GPU: %d \n", *stat.Utilization.GPU)
									continue
								}

								if *stat.Utilization.GPU <= uint(util) {
									continue
								}
							}

							if envMemory := os.Getenv(envEnableFilterByMemory); envMemory != "" {
								memory, err := strconv.Atoi(envMemory)
								if err != nil {
									log.Printf("Error: %v \n", err)
									continue
								}
								used := *stat.Memory.Global.Used * uint64(100) / (*stat.Memory.Global.Free + *stat.Memory.Global.Used)
								if used <= uint64(memory) {
									continue
								}
							}

							if d.Health == pluginapi.Healthy {
								log.Printf("Prepare to set device %s-%d unHelthy, which Processes=[%+v] UTIL=[%d] Memeory=[%dMb/%dMb(%d)]\n", device.UUID, j, stat.Processes, *stat.Utilization.GPU, *stat.Memory.Global.Used, *stat.Memory.Global.Free+*stat.Memory.Global.Used, *stat.Memory.Global.Used*100/(*stat.Memory.Global.Free+*stat.Memory.Global.Used))
							}
							unHealthy = true
							index = j
							break
						}

						if unHealthy && d.Health == pluginapi.Healthy {
							devices[i].Health = pluginapi.Unhealthy
							devices[i].CustomDefinedHealthy = true
							devices[i].Index = index
							unhealthy <- d
							continue
						}
						if !unHealthy && d.Health == pluginapi.Unhealthy {
							devices[i].Health = pluginapi.Healthy
							devices[i].CustomDefinedHealthy = false
							unhealthy <- d
							continue
						}
					}
				}
			}
		}()
	}

	for {
		select {
		case <-stop:
			return
		default:
		}

		e, err := nvml.WaitForEvent(eventSet, 5000)
		if err != nil && e.Etype != nvml.XidCriticalError {
			continue
		}

		// FIXME: formalize the full list and document it.
		// http://docs.nvidia.com/deploy/xid-errors/index.html#topic_4
		// Application errors: the GPU should still be healthy
		if e.Edata == 31 || e.Edata == 43 || e.Edata == 45 {
			continue
		}

		if e.UUID == nil || len(*e.UUID) == 0 {
			// All devices are unhealthy
			log.Printf("XidCriticalError: Xid=%d, All devices will go unhealthy.", e.Edata)
			for i, d := range devices {
				devices[i].CustomDefinedHealthy = false
				unhealthy <- d
			}
			continue
		}

		for i, d := range devices {
			if d.ID == *e.UUID {
				log.Printf("XidCriticalError: Xid=%d on Device=%s, the device will go unhealthy.", e.Edata, d.ID)
				devices[i].CustomDefinedHealthy = false
				unhealthy <- d
			}
		}
	}
}
