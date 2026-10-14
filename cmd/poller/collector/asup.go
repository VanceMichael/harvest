package collector

import (
	"cmp"
	"crypto/sha1" //nolint:gosec // used for sha1sum not for security
	"encoding/hex"
	"fmt"
	"github.com/netapp/harvest/v2/cmd/harvest/version"
	"github.com/netapp/harvest/v2/pkg/conf"
	"github.com/netapp/harvest/v2/pkg/logging"
	"github.com/netapp/harvest/v2/pkg/matrix"
	"github.com/netapp/harvest/v2/pkg/num"
	"github.com/netapp/harvest/v2/pkg/slogx"
	"github.com/netapp/harvest/v2/third_party/shirou/gopsutil/v4/mem"
	"github.com/netapp/harvest/v2/third_party/shirou/gopsutil/v4/process"
	"log/slog"
	"os"
	"runtime"
	"slices"
	"time"
)

type Payload struct {
	Target     *TargetInfo
	Harvest    *harvestInfo
	Platform   *platformInfo
	Nodes      *InstanceInfo `json:"Nodes,omitempty"`
	Volumes    *InstanceInfo `json:"Volumes,omitempty"`
	Quotas     *InstanceInfo `json:"Quotas,omitempty"`
	Tenants    *InstanceInfo `json:"Tenants,omitempty"`
	Collectors *[]AsupCollector
}

type TargetInfo struct {
	Version     string
	Model       string
	Serial      string
	Ping        float64
	ClusterUUID string
}

type ID struct {
	SerialNumber string `json:"serial-number"`
	SystemID     string `json:"system-id"`
}

type InstanceInfo struct {
	Count           int64
	DataPoints      int64
	PollTime        int64
	APITime         int64
	ParseTime       int64
	PluginTime      int64
	PluginInstances int64
	Ids             []ID `json:"Ids,omitempty"` // revive:disable-line var-naming
}

// InstanceInfoFromMetadata builds an InstanceInfo from the "data" task instance of a
// collector's metadata matrix, as recorded during PollData. The metrics and the "data"
// instance are registered unconditionally during collector Init, so it's safe to use
// MustGetMetric/MustGetInstance here.
func InstanceInfoFromMetadata(md *matrix.Matrix) InstanceInfo {
	instance := md.MustGetInstance("data")
	count, _ := md.MustGetMetric("instances").GetValueInt64(instance)
	dataPoints, _ := md.MustGetMetric("metrics").GetValueInt64(instance)
	pollTime, _ := md.MustGetMetric("poll_time").GetValueInt64(instance)
	apiTime, _ := md.MustGetMetric("api_time").GetValueInt64(instance)
	parseTime, _ := md.MustGetMetric("parse_time").GetValueInt64(instance)
	pluginTime, _ := md.MustGetMetric("plugin_time").GetValueInt64(instance)
	return InstanceInfo{
		Count:      count,
		DataPoints: dataPoints,
		PollTime:   pollTime,
		APITime:    apiTime,
		ParseTime:  parseTime,
		PluginTime: pluginTime,
	}
}

type Process struct {
	Pid      int32
	User     string
	Ppid     int32
	Ctime    int64
	RssBytes uint64
	Threads  int32
	Cmdline  string
}

type platformInfo struct {
	OS     string
	Arch   string
	Memory struct {
		TotalKb     uint64
		AvailableKb uint64
		UsedKb      uint64
	}
	CPUs         int
	NumProcesses uint64
	Processes    []Process
}

type harvestInfo struct {
	HostHash     string
	UUID         string
	Version      string
	Release      string
	Commit       string
	BuildDate    string
	NumClusters  uint8
	NumPollers   uint64
	NumExporters uint64
	NumPortRange uint64
	Pid          int
	RssBytes     uint64
	MaxRssBytes  uint64
	EpochMilli   int64 // milliseconds since the epoch, in UTC
}

type Counters struct {
	Count int
	List  []string
}

type Schedule struct {
	Name     string
	Schedule string
}

type AsupCollector struct {
	Name          string
	Query         string
	BatchSize     string `json:"BatchSize,omitempty"`
	ClientTimeout string
	Schedules     []Schedule
	Exporters     []string
	Counters      Counters
	InstanceInfo  *InstanceInfo `json:"InstanceInfo,omitempty"`
}

const (
	workingDir = "asup"
)

func (p *Payload) AddCollectorAsup(a AsupCollector) {
	if p.Collectors == nil {
		p.Collectors = &[]AsupCollector{}
	}
	*p.Collectors = append(*p.Collectors, a)
}

// BuildAutoSupport assembles an autosupport payload from the poller's collectors
// and status metadata. It does not write the payload or invoke the sidecar: the
// AsupDeliverer is responsible for durably persisting and delivering the result.
func BuildAutoSupport(collectors []Collector, status *matrix.Matrix, pollerName string, maxRss uint64) (*Payload, error) {

	var (
		msg          *Payload
		arch         string
		cpus         int
		numPortRange uint64
		rssBytes     uint64
	)

	// add info about the platform (where Harvest is running)
	arch, cpus = getCPUInfo()
	// "ping" and "host" are registered unconditionally during poller Init, so it's safe
	// to use MustGetMetric/MustGetInstance here.
	ping, _ := status.MustGetMetric("ping").GetValueFloat64(status.MustGetInstance("host"))
	msg = &Payload{
		Platform: &platformInfo{
			Arch: arch,
			CPUs: cpus,
			OS:   GetOSName(),
		},
		Target: &TargetInfo{
			Ping: ping,
		},
	}
	attachMemory(msg)

	// give each collector the opportunity to attach autosupport information
	slices.SortStableFunc(collectors, func(a, b Collector) int {
		nameCmp := cmp.Compare(a.GetName(), b.GetName())
		if nameCmp == 0 {
			return cmp.Compare(a.GetObject(), b.GetObject())
		}
		return nameCmp
	})

	for _, c := range collectors {
		c.CollectAutoSupport(msg)
	}

	// count the number of Prometheus exporters with portRange
	for _, e := range conf.Config.Exporters {
		if e.PortRange != nil {
			numPortRange++
		}
	}

	hostname, _ := os.Hostname()

	// Get the PID and RSS in bytes of the current process.
	// If there is an error, rssBytes will be zero
	pid := os.Getpid()
	pid32, err := num.SafeConvertToInt32(pid)
	if err != nil {
		logging.Get().Error("", slogx.Err(err), slog.Int("pid", pid))
	} else {
		newProcess, err := process.NewProcess(pid32)
		if err != nil {
			slog.Default().Error("failed to get process info", slogx.Err(err))
		} else {
			memInfo, err := newProcess.MemoryInfo()
			if err != nil {
				slog.Default().Error("failed to get memory info", slogx.Err(err), slog.Int("pid", pid))
			} else {
				rssBytes = memInfo.RSS
			}
		}
	}

	// add harvest release info
	msg.Harvest = &harvestInfo{
		// harvest uuid creation from sha1 of cluster uuid
		UUID:         Sha1Sum(msg.Target.ClusterUUID),
		Version:      version.VERSION,
		Release:      version.Release,
		Commit:       version.Commit,
		BuildDate:    version.BuildDate,
		HostHash:     Sha1Sum(hostname),
		NumClusters:  1,
		NumPollers:   uint64(len(conf.Config.Pollers)),
		NumExporters: uint64(len(conf.Config.Exporters)),
		NumPortRange: numPortRange,
		Pid:          pid,
		RssBytes:     rssBytes,
		MaxRssBytes:  max(maxRss, rssBytes),
		EpochMilli:   time.Now().UnixMilli(),
	}
	return msg, nil
}

func attachMemory(msg *Payload) {
	virtualMemory, err := mem.VirtualMemory()
	if err != nil {
		return
	}
	msg.Platform.Memory.TotalKb = virtualMemory.Total / 1024
	msg.Platform.Memory.AvailableKb = virtualMemory.Available / 1024
	msg.Platform.Memory.UsedKb = virtualMemory.Used / 1024
}

func getCPUInfo() (string, int) {
	return runtime.GOARCH, runtime.NumCPU()
}

func GetOSName() string {
	return runtime.GOOS
}

func Sha1Sum(s string) string {
	hash := sha1.New() //nolint:gosec // using sha1 for a hash, not a security risk
	hash.Write([]byte(s))
	return hex.EncodeToString(hash.Sum(nil))
}

// checkAndDeleteIfPermissionsMismatch checks if the permissions of the file or directory at the given path
// match the required permissions. If they don't match, it deletes the file or directory.
func checkAndDeleteIfPermissionsMismatch(aPath string, requiredFileMode os.FileMode) error {
	// Get the file or directory information
	fileInfo, err := os.Stat(aPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("error checking permissions: %w", err)
	}
	// Check if the current permissions match the required permissions
	currentPermissions := fileInfo.Mode().Perm()
	if currentPermissions != requiredFileMode {
		err = os.RemoveAll(aPath)
		if err != nil {
			return fmt.Errorf("error deleting file or directory: %w", err)
		}
	}
	return nil
}
