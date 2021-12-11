package main

import (
	"fmt"
	"time"

	"github.com/shirou/gopsutil/process"

	. "github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/octosql"
	"github.com/cube2222/octosql/physical"
)

var prettyStatus = func(status string) string {
	switch status {
	case "R":
		return "Running"
	case "S":
		return "Sleeping"
	case "T":
		return "Stopped"
	case "I":
		return "Idle"
	case "Z":
		return "Zombie"
	case "W":
		return "Waiting"
	case "L":
		return "Locked"
	}
	return ""
}

type processesExecuting struct {
	fields []physical.SchemaField
}

func (d *processesExecuting) Run(ctx ExecutionContext, produce ProduceFn, metaSend MetaSendFn) error {
	processes, err := process.Processes()
	if err != nil {
		return fmt.Errorf("couldn't list processes: %w", err)
	}

	for i := range processes {
		values := make([]octosql.Value, len(d.fields))
		var memoryInfo *process.MemoryInfoStat
		var memoryInfoRead bool
		for j, field := range d.fields {
			switch field.Name {
			case "pid":
				values[j] = octosql.NewInt(int(processes[i].Pid))
			case "ppid":
				v, _ := processes[i].Ppid()
				values[j] = octosql.NewInt(int(v))
			case "name":
				v, _ := processes[i].Name()
				values[j] = octosql.NewString(v)
			case "create_time":
				v, _ := processes[i].CreateTime()
				values[j] = octosql.NewTime(time.UnixMilli(v))
			case "user":
				v, _ := processes[i].Username()
				values[j] = octosql.NewString(v)
			case "status":
				v, _ := processes[i].Status()
				values[j] = octosql.NewString(prettyStatus(v))
			case "rss":
				if !memoryInfoRead {
					memoryInfo, _ = processes[i].MemoryInfo()
					memoryInfoRead = true
				}
				if memoryInfo != nil {
					values[j] = octosql.NewInt(int(memoryInfo.RSS))
				}
			case "vms":
				if !memoryInfoRead {
					memoryInfo, _ = processes[i].MemoryInfo()
					memoryInfoRead = true
				}
				if memoryInfo != nil {
					values[j] = octosql.NewInt(int(memoryInfo.VMS))
				}
			}
		}

		if err := produce(
			ProduceFromExecutionContext(ctx),
			NewRecord(values, false, time.Time{}),
		); err != nil {
			return fmt.Errorf("couldn't produce record: %w", err)
		}
	}

	return nil
}
