package agent

import (
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/shirou/gopsutil/v4/process"
)

const (
	defaultTopN          = 10
	defaultProcessInterval = 60 * time.Second
)

type ProcessMonitor struct {
	enabled      bool
	topN         int
	blacklist    map[string]struct{}
	interval     time.Duration
	lastProcesses []ProcessInfo
	mu           sync.RWMutex
	lastUpdate   time.Time
}

type ProcessInfo struct {
	PID     int32   `json:"pid"`
	Name    string  `json:"name"`
	CPU     float64 `json:"cpu"`
	Memory  float32 `json:"mem"`
	CmdLine string  `json:"cmd,omitempty"`
}

func NewProcessMonitor() *ProcessMonitor {
	return &ProcessMonitor{
		enabled:   false,
		topN:      defaultTopN,
		blacklist: make(map[string]struct{}),
		interval:  defaultProcessInterval,
	}
}

func (pm *ProcessMonitor) Enable(topN int, blacklist []string, interval time.Duration) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	pm.enabled = true
	if topN > 0 {
		pm.topN = topN
	}
	if interval > 0 {
		pm.interval = interval
	}
	pm.blacklist = make(map[string]struct{}, len(blacklist))
	for _, name := range blacklist {
		pm.blacklist[strings.ToLower(name)] = struct{}{}
	}
}

func (pm *ProcessMonitor) IsEnabled() bool {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	return pm.enabled
}

func (pm *ProcessMonitor) GetTopProcesses() []ProcessInfo {
	pm.mu.RLock()

	if !pm.enabled {
		pm.mu.RUnlock()
		return nil
	}

	if time.Since(pm.lastUpdate) < pm.interval && len(pm.lastProcesses) > 0 {
		result := make([]ProcessInfo, len(pm.lastProcesses))
		copy(result, pm.lastProcesses)
		pm.mu.RUnlock()
		return result
	}
	pm.mu.RUnlock()

	return pm.refreshProcesses()
}

func (pm *ProcessMonitor) refreshProcesses() []ProcessInfo {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	procs, err := process.Processes()
	if err != nil {
		return pm.lastProcesses
	}

	var processList []ProcessInfo

	for _, p := range procs {
		name, err := p.Name()
		if err != nil {
			continue
		}

		if _, blacklisted := pm.blacklist[strings.ToLower(name)]; blacklisted {
			continue
		}

		cpuPercent, err := p.CPUPercent()
		if err != nil {
			continue
		}

		memPercent, err := p.MemoryPercent()
		if err != nil {
			continue
		}

		cmdLine, _ := p.Cmdline()

		processList = append(processList, ProcessInfo{
			PID:     p.Pid,
			Name:    name,
			CPU:     cpuPercent,
			Memory:  memPercent,
			CmdLine: cmdLine,
		})
	}

	sort.Slice(processList, func(i, j int) bool {
		return processList[i].CPU > processList[j].CPU
	})

	if len(processList) > pm.topN {
		processList = processList[:pm.topN]
	}

	pm.lastProcesses = processList
	pm.lastUpdate = time.Now()

	return processList
}
